/** Actual native registry image storage, projected into the independently owned B6 copier. */
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  createReach,
  REGISTRY_IMAGES_REACH,
  REGISTRY_IMAGES_LIFETIME_REACH,
  REGISTRY_IMAGES_IO_REACH,
  REGISTRY_IMAGES_WIDE_REACH,
  REGISTRY_IMAGES_WORKSPACE_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ROW_IMAGE_UNREAD, type RowImageRef } from "../access/statement-command.js";
import { TriggerBindingProgram } from "../exec/trigger-bindings.js";
import { rowImageMinimumBytes, ROW_IMAGE_FIXED_BYTES } from "../exec/row-image-layout.js";
import { numbers, texts, valueAt, I32, decimals, decimal } from "../exec/kernels/test-support.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { SpillService, SpillPageRef } from "../spill/contract.js";
import { TaskClock, yieldTask } from "../task.js";
import { bigintBytes } from "../memory/governor.js";
import { Catalog } from "../write/catalog.js";
import { column, tableInput, INT, TEXT } from "../write/test-support.js";
import { RegistryDirectory, REGISTRY_DIRECTORY_BYTES } from "./registry-directory.js";
import { RegistryFragments, REGISTRY_FRAGMENT_BYTES } from "./registry-fragments.js";
import {
  RegistryImages,
  REGISTRY_IMAGE_BYTES,
  type RegistryImageScope,
} from "./registry-images.js";

function defined<T>(value: T | null | undefined): T {
  if (value === undefined || value === null) throw new Error("Missing native image test value");
  return value;
}
function fixture(
  prepare?: RegistryImageScope["prepare"],
  afterRead?: (ref: SpillPageRef) => Promise<void>,
  payloadType = TEXT,
  payloadColumns = 1,
) {
  const catalog = Catalog.empty().change(
    {
      kind: "createTable",
      table: tableInput("t", [
        column("id", INT, false),
        ...Array.from({ length: payloadColumns }, (_, i) =>
          column(`payload${String(i)}`, payloadType, true),
        ),
      ]),
    },
    1,
  ).catalog;
  const table = catalog.tables[0];
  if (!table) throw new Error("Missing native catalog");
  const id = table.columns[0]?.id;
  if (!id) throw new Error("Missing stable column");
  const program = new TriggerBindingProgram(table, [
    { source: "old", columnId: id, name: "id", type: INT },
    { source: "old", columnId: id, name: "id", type: INT },
    { source: "new", columnId: id, name: "id", type: INT },
  ]);
  const base = rowImageMinimumBytes(table.columns.map((c) => c.type)) + program.minimumBytes;
  let held = 0,
    limit = base,
    maxProgress = 0,
    files = 0;
  const charge = {
    allocate: (n: number) => {
      if (held + n > limit) return false;
      held += n;
      return true;
    },
    free: (n: number) => {
      held -= n;
      expect(held).toBeGreaterThanOrEqual(0);
    },
  };
  const backing = new MemorySpillService();
  const spill: SpillService = {
    get bytesInUse() {
      return backing.bytesInUse;
    },
    sweep: () => backing.sweep(),
    create: async (queryId) => {
      files++;
      const file = await backing.create(queryId);
      return {
        queryId,
        get length() {
          return file.length;
        },
        append: (page) => file.append(page),
        read: (ref) => file.read(ref),
        readInto: async (ref, into) => {
          await file.readInto(ref, into);
          await afterRead?.(ref);
        },
        close: () => file.close(),
      };
    },
  };
  const signal = new AbortController().signal;
  const directory = new RegistryDirectory(charge, spill, 1, signal),
    fragments = new RegistryFragments(charge, spill, 1, signal, () => undefined);
  const images = new RegistryImages({
    rootId: 27,
    catalog,
    directory,
    fragments,
    charge,
    signal,
    prepare: async (bytes, stop) => {
      maxProgress = Math.max(maxProgress, bytes);
      await prepare?.(bytes, stop);
      stop.throwIfAborted();
      limit = base + bytes;
    },
    checkpoint: () => undefined,
  });
  const input = (n: number, payload: string | null) => ({
    batch: { length: 1, selection: null, columns: [numbers(I32, [n]), texts([payload])] },
    row: 0,
  });
  return {
    table,
    program,
    images,
    charge,
    input,
    signal,
    get held() {
      return held;
    },
    get maxProgress() {
      return maxProgress;
    },
    get files() {
      return files;
    },
    get spillBytes() {
      return spill.bytesInUse;
    },
    async close() {
      await images.close();
      await fragments.close();
      await directory.close();
      expect(held).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    },
  };
}

it.each(seedsFor("next-registry-images", [0xb5df]))(
  "replays immutable projected row images under pressure seed %i",
  async (seed) => {
    const reach = createReach("native row image registry", REGISTRY_IMAGES_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 260, max: 340 }),
        fc.integer({ min: 150000, max: 250000 }),
        fc.integer({ min: -10000, max: 10000 }),
        async (count, width, salt) => {
          reach.beginHistory();
          const f = fixture();
          const refs: RowImageRef[] = [],
            wide = "雪".repeat(width);
          try {
            expect(REGISTRY_DIRECTORY_BYTES + REGISTRY_FRAGMENT_BYTES + REGISTRY_IMAGE_BYTES).toBe(
              ROW_IMAGE_FIXED_BYTES,
            );
            expect(f.held).toBe(ROW_IMAGE_FIXED_BYTES);
            reach.reach("fixed frozen minimum");
            for (let row = 0; row < count; row++) {
              const kind = row % 3;
              refs.push(
                await f.images.append(
                  f.table.id,
                  kind === 0 ? null : f.input(salt + row, row === 1 ? wide : `old${String(row)}`),
                  kind === 2
                    ? null
                    : f.input(salt + row + 1, row === 1 ? wide : row % 2 ? null : "new"),
                ),
              );
              expect(f.held).toBe(ROW_IMAGE_FIXED_BYTES);
            }
            // Native source arrays are caller-owned and may be recycled immediately after append.
            const source = f.input(-17, "source");
            const recycled = await f.images.append(f.table.id, source, source);
            const idVector = source.batch.columns[0];
            if (idVector?.data.layout === "i32") idVector.data.values.fill(0);
            const scalar = await f.images.readRowBindings(recycled, f.program.projection, f.signal);
            try {
              expect(scalar.old?.[1]).toBe(ROW_IMAGE_UNREAD);
              const copied = scalar.old?.[0];
              if (!copied || copied === ROW_IMAGE_UNREAD) throw new Error("Missing copied ID");
              expect(valueAt(copied, 0)).toBe(-17);
            } finally {
              scalar.release();
            }
            reach.reach("caller source retired");
            for (const row of [count - 1, 1, 0, 2, 1]) {
              const ref = refs[row];
              if (!ref) throw new Error("Missing image");
              const projection = await f.images.readRowBindings(
                ref,
                f.program.projection,
                f.signal,
              );
              const old = projection.old;
              try {
                expect(projection.old?.[1] ?? ROW_IMAGE_UNREAD).toBe(ROW_IMAGE_UNREAD);
                expect(projection.new?.[1] ?? ROW_IMAGE_UNREAD).toBe(ROW_IMAGE_UNREAD);
                expect(f.maxProgress).toBeLessThan(256); // Unreferenced text is never decoded.
                expect(projection.old === null).toBe(row % 3 === 0);
                expect(projection.new === null).toBe(row % 3 === 2);
                reach.reach(
                  row % 3 === 0
                    ? "INSERT images"
                    : row % 3 === 1
                      ? "UPDATE images"
                      : "DELETE images",
                );
                reach.reach("projected stable IDs");
                reach.reach("unread is not NULL");
                reach.reach("large unrelated payload skipped");
              } finally {
                projection.release();
              }
              projection.release();
              expect(old?.length ?? 0).toBe(0);
              expect(() => projection.old).toThrow("retired");
              if (row % 3 === 1) {
                const bindings = await f.program.bind(ref, f.images, f.charge, {
                  clock: new TaskClock(() => 0),
                  yieldTask,
                  signal: f.signal,
                });
                try {
                  expect(bindings.slots[0]).toBe(bindings.slots[1]);
                  reach.reach("duplicate bindings alias");
                  expect(valueAt(defined(bindings.slots[0]), 0)).toBe(salt + row);
                  expect(valueAt(defined(bindings.slots[2]), 0)).toBe(salt + row + 1);
                } finally {
                  bindings.close();
                }
              }
            }
            const ref = refs[1];
            if (!ref) throw new Error("Missing wide image");
            const full = await f.images.readRowImages(ref, f.signal);
            const columns = full.old?.columns;
            try {
              expect(full.old?.length).toBe(1);
              expect(full.new?.length).toBe(1);
              expect(valueAt(defined(full.old?.columns[1]), 0)).toBe(wide);
              expect(valueAt(defined(full.new?.columns[1]), 0)).toBe(wide);
              expect(valueAt(defined(full.old?.columns[0]), 0)).toBe(salt + 1);
            } finally {
              full.release();
            }
            expect(columns?.length).toBe(0);
            reach.reach("full physical row");
            reach.reach("wide row replay");
            expect(f.files).toBe(2);
            reach.reach("two files independent of rows");
            expect(f.spillBytes).toBeGreaterThan(ROW_IMAGE_FIXED_BYTES);
            let foreignError: unknown;
            try {
              const unexpected = await f.images.readRowImages({ ...ref, spoolId: 28 }, f.signal);
              unexpected.release();
            } catch (error) {
              foreignError = error;
            }
            expect(foreignError).toMatchObject({ message: "Invalid or retired native row image" });
            reach.reach("foreign root refused");
          } finally {
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-image-lifetime", [0xb5e0]))(
  "fences image leases and cancellable progress seed %i",
  async (seed) => {
    const reach = createReach("native image lifetimes", REGISTRY_IMAGES_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(fc.constantFrom("lease", "prepare", "cancel"), async (phase) => {
        reach.beginHistory();
        let entered!: () => void, resolve!: () => void;
        const ready = new Promise<void>((r) => {
            entered = r;
          }),
          gate = new Promise<void>((r) => {
            resolve = r;
          });
        const f = fixture(async (_, signal) => {
          if (phase !== "lease") {
            entered();
            await Promise.race([
              gate,
              new Promise((_, reject) =>
                signal.addEventListener("abort", () => reject(signal.reason as Error), {
                  once: true,
                }),
              ),
            ]);
          }
        });
        try {
          const ref = await f.images.append(f.table.id, f.input(1, "old"), f.input(2, "new"));
          if (phase === "lease") {
            const lease = await f.images.readRowImages(ref, f.signal);
            let closed = false;
            const closing = f.images.close().then(() => {
              closed = true;
            });
            for (let n = 0; n < 8; n++) await Promise.resolve();
            const closedBeforeRelease = closed,
              heldBeforeRelease = f.held;
            lease.release();
            await closing;
            expect(closedBeforeRelease, "close must fence outstanding image leases").toBe(false);
            expect(heldBeforeRelease, "image allocation remains paid during close").toBeGreaterThan(
              ROW_IMAGE_FIXED_BYTES,
            );
            expect(closed).toBe(true);
            reach.reach("close waits lease");
            reach.reach("lease charge survives close");
          } else {
            const cancel = new AbortController(),
              reading = f.images.readRowImages(ref, cancel.signal).then(
                () => null,
                (error: unknown) => error,
              );
            await ready;
            if (phase === "cancel") {
              cancel.abort(new Error("cancel image"));
              expect(await reading).toMatchObject({ message: "cancel image" });
              reach.reach("caller cancels prepare");
            } else {
              const closing = f.images.close();
              expect(await reading).toMatchObject({ message: "Native row images are closed" });
              await closing;
              reach.reach("close cancels prepare");
            }
            resolve();
          }
        } finally {
          resolve();
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 9, examples: [["lease"], ["prepare"], ["cancel"]] },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-image-payload-cancel", [0xb5e1]))(
  "fences cancellation in fragmented image reads seed %i",
  async (seed) => {
    const reach = createReach("native image payload cancellation", REGISTRY_IMAGES_IO_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer({ min: 150000, max: 230000 }),
        async (closeRoot, width) => {
          reach.beginHistory();
          let armed = false,
            reads = 0,
            entered!: () => void,
            resolve!: () => void;
          const ready = new Promise<void>((r) => {
              entered = r;
            }),
            gate = new Promise<void>((r) => {
              resolve = r;
            });
          const f = fixture(undefined, async (ref) => {
            if (armed && ref.length > 10000) {
              reads++;
              if (reads === 1) {
                entered();
                await gate;
              }
            }
          });
          try {
            const ref = await f.images.append(f.table.id, f.input(1, "雪".repeat(width)), null);
            armed = true;
            const cancel = new AbortController();
            let settled = false,
              closed = false;
            const reading = f.images.readRowImages(ref, cancel.signal).then(
              (lease) => {
                lease.release();
                settled = true;
                return null;
              },
              (error: unknown) => {
                settled = true;
                return error;
              },
            );
            await ready;
            const closing = closeRoot
              ? f.images.close().then(() => {
                  closed = true;
                })
              : Promise.resolve();
            if (!closeRoot) cancel.abort(new Error("cancel payload"));
            for (let turn = 0; turn < 8; turn++) await Promise.resolve();
            const settledBeforeIO = settled,
              closedBeforeIO = closed,
              heldBeforeIO = f.held;
            resolve();
            const failure = await reading;
            await closing;
            expect(settledBeforeIO, "read must fence borrowed spill I/O").toBe(false);
            expect(closedBeforeIO, "close must fence borrowed spill I/O").toBe(false);
            expect(heldBeforeIO, "pending image payload remains charged").toBeGreaterThan(
              ROW_IMAGE_FIXED_BYTES,
            );
            expect(failure).toMatchObject({
              message: closeRoot ? "Native row images are closed" : "cancel payload",
            });
            expect(reads, "cancelled projection must not read a later fragment").toBe(1);
            reach.reach(closeRoot ? "close cancels read" : "caller cancels read");
            reach.reach("charge survives payload IO");
            reach.reach("no read after cancel");
          } finally {
            resolve();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, 200000],
          [true, 230000],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-image-wide", [0xb5e4]))(
  "prepays wide image capture and projected decode seed %i",
  async (seed) => {
    const reach = createReach("native wide image admission", REGISTRY_IMAGES_WIDE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 520000, max: 650000 }),
        fc.integer({ min: -3, max: 3 }),
        async (bits, sign) => {
          reach.beginHistory();
          const type = decimal(null, null),
            f = fixture(undefined, undefined, type);
          const value = BigInt(sign || 1) * (1n << BigInt(bits)) + 123456789n;
          const old = decimals(type, [[value, 7]], "wide"),
            next = decimals(type, [[-value, 13]], "wide");
          try {
            const ref = await f.images.append(
              f.table.id,
              { batch: { length: 1, selection: null, columns: [numbers(I32, [1]), old] }, row: 0 },
              { batch: { length: 1, selection: null, columns: [numbers(I32, [2]), next] }, row: 0 },
            );
            expect(f.maxProgress).toBeGreaterThan(131072);
            expect(f.held).toBe(ROW_IMAGE_FIXED_BYTES);
            if (old.data.layout === "decimal" && old.data.coefficients.repr === "wide")
              old.data.coefficients.values[0] = 0n;
            const projection = await f.images.readRowBindings(ref, f.program.projection, f.signal);
            try {
              expect(projection.old?.[1]).toBe(ROW_IMAGE_UNREAD);
              expect(projection.new?.[1]).toBe(ROW_IMAGE_UNREAD);
            } finally {
              projection.release();
            }
            const full = await f.images.readRowImages(ref, f.signal);
            try {
              const a = full.old?.columns[1],
                b = full.new?.columns[1];
              if (
                a?.form !== "flat" ||
                b?.form !== "flat" ||
                a.data.layout !== "decimal" ||
                b.data.layout !== "decimal" ||
                a.data.coefficients.repr !== "wide" ||
                b.data.coefficients.repr !== "wide"
              )
                throw new Error("Wide image changed native tier");
              expect(a.data.coefficients.values[0]).toBe(value);
              expect(b.data.coefficients.values[0]).toBe(-value);
              expect(a.data.scales?.[0]).toBe(7);
              expect(b.data.scales?.[0]).toBe(13);
            } finally {
              full.release();
            }
            expect(f.maxProgress).toBeLessThan(4 << 20);
            reach.reach("capture scratch prepaid");
            reach.reach("signed wide coefficient");
            reach.reach("wide source retired");
            reach.reach("projected wide payload skipped");
            reach.reach("exact tier and scale");
          } finally {
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [600000, -1],
          [650000, 1],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-image-width", [0xb5e7]))(
  "shares decode progress across wide image columns seed %i",
  async (seed) => {
    const reach = createReach(
      "native image width and decode workspace",
      REGISTRY_IMAGES_WORKSPACE_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 6, max: 14 }),
        fc.integer({ min: 16000, max: 32000 }),
        fc.boolean(),
        async (width, bits, negative) => {
          reach.beginHistory();
          const value = (1n << BigInt(bits)) * (negative ? -1n : 1n),
            type = decimal(null, null);
          const retained = bigintBytes(value) + 10,
            storeWorkspace = 4 * (bigintBytes(value) + 17),
            envelope = 4 * width * retained + storeWorkspace + 64;
          const f = fixture(
            (bytes) => {
              expect(
                bytes,
                "image width must not multiply transient decode workspace",
              ).toBeLessThanOrEqual(envelope);
            },
            undefined,
            type,
            width,
          );
          const source = (id: number) => ({
            batch: {
              length: 1,
              selection: null,
              columns: [
                numbers(I32, [id]),
                ...Array.from({ length: width }, (_, c) => decimals(type, [[value, c]], "wide")),
              ],
            },
            row: 0,
          });
          try {
            const ref = await f.images.append(f.table.id, source(1), source(2));
            const full = await f.images.readRowImages(ref, f.signal);
            try {
              for (const side of [full.old, full.new]) {
                if (side?.length !== 1) throw new Error("Missing physical image");
                for (let c = 1; c <= width; c++) {
                  const column = side.columns[c];
                  if (
                    column?.form !== "flat" ||
                    column.data.layout !== "decimal" ||
                    column.data.coefficients.repr !== "wide"
                  )
                    throw new Error("Missing wide projected value");
                  expect(column.data.coefficients.values[0]).toBe(value);
                  expect(column.data.scales?.[0]).toBe(c - 1);
                }
              }
            } finally {
              full.release();
            }
            expect(f.maxProgress).toBeLessThanOrEqual(envelope);
            expect(f.held).toBe(ROW_IMAGE_FIXED_BYTES);
            reach.reach("many wide columns");
            reach.reach("shared decode scratch");
            reach.reach("exact retained buffers");
            reach.reach(negative ? "negative coefficients" : "positive coefficients");
          } finally {
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [14, 32000, false],
          [6, 16000, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
