/** Actual native OLD payload capture. No SQL trigger factory is installed. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_PREFIX_IMAGES_REACH,
  NATIVE_PREFIX_IMAGES_LIFETIME_REACH,
} from "../../testing/reach.js";
import { yieldTask } from "../task.js";
import { ChunkReader } from "../page/reader.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { decimal, valueAt } from "../exec/kernels/test-support.js";
import { TableScan } from "../exec/table-scan.js";
import { int128At } from "../vector/int128.js";
import type { FlatVector } from "../vector/vector.js";
import {
  Harness,
  autocommit,
  rules,
  tableInput,
  column,
  INT,
  TEXT,
  batch,
  ints,
  texts,
} from "../write/test-support.js";
import { RegistryDirectory } from "./registry-directory.js";
import { RegistryFragments } from "./registry-fragments.js";
import { RegistryImages } from "./registry-images.js";
import {
  NativePrefixImages,
  nativePrefixImageBytes,
  nativePrefixSourceBytes,
} from "./prefix-images.js";
import * as legacyRows from "../write/rows.js";
import { StatementImpl } from "../write/transaction.js";
import { MemoryReads } from "../write/memory-read.js";

async function fixture(rows: number, width: number, published: boolean, nullable: boolean) {
  const h = new Harness("strict", "memory", 256),
    path = await h.open(),
    type = decimal(38, 0);
  let tableId = 0;
  await autocommit(path, async (s) => {
    tableId =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("p", [
            column("id", INT, false),
            column("n", type),
            column("payload", TEXT),
          ]),
        })
      ).id ?? 0;
  });
  const coefficients = Array.from({ length: rows }, (_, i) =>
    i % 3 === 0 ? BigInt(i + 1) : i % 3 === 1 ? 2n ** 56n + BigInt(i) : 10n ** 30n + BigInt(i),
  );
  const valid = new Uint32Array(1);
  for (let i = 0; i < rows; i++)
    if (!nullable || i % 3 !== 2) valid[0] = (valid[0] ?? 0) | (1 << i);
  const n: FlatVector = {
    form: "flat",
    type,
    length: rows,
    validity: nullable ? valid : null,
    data: {
      layout: "decimal",
      scale: 0,
      scales: null,
      specials: null,
      coefficients: { repr: "wide", values: coefficients },
    },
  };
  const payloads = Array.from({ length: rows }, (_, i) =>
    nullable && i % 4 === 1 ? null : "雪".repeat(width) + ":" + String(i),
  );
  await autocommit(path, (s) =>
    s
      .table(tableId)
      .insert(batch(ints(coefficients.map((_, i) => i + 1)), n, texts(payloads)), null),
  );
  if (published) {
    await path.recycleLog();
    await path.quiesce();
  }
  const tx = await path.begin(),
    statement = tx.statement({ rules });
  if (!(statement instanceof StatementImpl)) throw new Error("native statement");
  const table = statement.catalog.table(tableId);
  if (!table) throw new Error("native table");
  const root = await h.governor.admit({
      label: "prefix image root",
      minimumBytes: 4 * 1024 * 1024,
      spillable: false,
    }),
    source = await h.governor.admit({
      label: "prefix image source",
      minimumBytes: nativePrefixSourceBytes(table),
      spillable: false,
    }),
    stop = new AbortController(),
    spill = new MemorySpillService(),
    directory = new RegistryDirectory(root, spill, 1, stop.signal),
    fragments = new RegistryFragments(root, spill, 1, stop.signal, () => undefined);
  let capturing = false;
  let preparing: ((signal: AbortSignal) => Promise<void>) | null = null;
  const images = new RegistryImages({
    rootId: 73,
    catalog: statement.catalog,
    directory,
    fragments,
    charge: root,
    signal: stop.signal,
    checkpoint: () => undefined,
    prepare: (bytes) => {
      if (capturing) throw new Error("image capture requested progress while source borrowed");
      expect(root.held - root.allocated).toBeGreaterThanOrEqual(bytes);
    },
  });
  const before = root.allocated;
  const reader = new NativePrefixImages({
    rootId: 73,
    statement,
    tables: [tableId],
    images,
    reads: { open: (pages, chunk, options) => ChunkReader.open(pages, chunk, options) },
    charge: root,
    signal: stop.signal,
    checkpoint: () => undefined,
    abortRoot: (reason) => stop.abort(reason),
    prepareSource: async (_table, minimum, signal) => {
      signal.throwIfAborted();
      await preparing?.(signal);
      signal.throwIfAborted();
      expect(source.allocated).toBe(0);
      expect(source.loaned).toBe(0);
      expect(minimum).toBeLessThanOrEqual(source.held);
      return source;
    },
  });
  expect(root.allocated - before).toBe(nativePrefixImageBytes(3));
  const pin = statement.readOwn(),
    ids: number[] = [];
  try {
    const view = pin.internalTable(tableId);
    for (const segment of view.segments)
      for (let i = 0; i < segment.rowCount; i++) ids.push(view.rowId(segment.index * 65536 + i));
  } finally {
    pin.release();
  }
  return {
    h,
    path,
    statement,
    stop,
    reader,
    images,
    root,
    source,
    ids,
    tableId,
    coefficients,
    payloads,
    fragments,
    setPrepare(work: (signal: AbortSignal) => Promise<void>) {
      preparing = work;
    },
    setCapturing(value: boolean) {
      capturing = value;
    },
    async close() {
      await reader.close();
      await images.close();
      await fragments.close();
      await directory.close();
      expect(root.allocated).toBe(0);
      expect(source.allocated).toBe(0);
      expect(source.loaned).toBe(0);
      root.close();
      source.close();
      statement.rollback();
      tx.rollback();
      await h.close();
      expect(h.governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    },
  };
}

for (const seed of seedsFor("next-native-prefix-images", [0xb62b, 0xb62c]))
  it(`captures full native OLD rows through bounded sources ${String(seed)}`, async () => {
    const reach = createReach("native prefix payloads", NATIVE_PREFIX_IMAGES_REACH),
      pg = new PGlite();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 6 }),
          fc.constantFrom(0, 20, 24000, 80000),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (count, width, published, nullable, wide) => {
            reach.beginHistory();
            const f = await fixture(count, width, published, nullable),
              legacy = vi.spyOn(legacyRows, "readRows").mockImplementation(() => {
                throw new Error("legacy uncharged row copy entered");
              }),
              borrow = vi.spyOn(f.source, "borrow"),
              hints = vi.spyOn(MemoryReads.prototype, "hint"),
              added = vi.spyOn(f.stop.signal, "addEventListener"),
              removed = vi.spyOn(f.stop.signal, "removeEventListener");
            // eslint-disable-next-line @typescript-eslint/unbound-method -- Invoked with its original TableScan receiver via call.
            const original = TableScan.prototype.next;
            const decode = vi.spyOn(TableScan.prototype, "next").mockImplementation(async function (
              this: TableScan,
            ) {
              const item = await original.call(this);
              if (!wide || !item) return item;
              const vector = item.batch.columns[1];
              if (vector?.form !== "flat" || vector.data.layout !== "decimal") return item;
              const data = vector.data,
                coeff = data.coefficients;
              const first = item.batch.columns[0],
                last = item.batch.columns[2];
              if (!first || !last) throw new Error("Missing native physical column");
              const values = Array.from({ length: vector.length }, (_, i) =>
                coeff.repr === "int128"
                  ? int128At(coeff.limbs, i)
                  : coeff.repr === "safe"
                    ? BigInt(coeff.values[i] ?? 0)
                    : (coeff.values[i] ?? 0n),
              );
              return {
                ...item,
                batch: {
                  ...item.batch,
                  columns: [
                    first,
                    {
                      ...vector,
                      data: { ...data, coefficients: { repr: "wide" as const, values } },
                    },
                    last,
                  ],
                },
              };
            });
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS p;CREATE TABLE p(id INT PRIMARY KEY,n NUMERIC(38,0),payload TEXT)",
              );
              for (let i = 0; i < count; i++)
                await pg.query("INSERT INTO p VALUES($1,$2,$3)", [
                  i + 1,
                  nullable && i % 3 === 2 ? null : f.coefficients[i]?.toString(),
                  f.payloads[i],
                ]);
              for (const [i, id] of f.ids.entries()) {
                f.setCapturing(true);
                const captured = await f.reader.capture(f.tableId, id, f.stop.signal).then(
                  (ref) => ({ ref, error: null }),
                  (error: unknown) => ({ ref: null, error }),
                );
                expect(
                  captured.error,
                  "admitted native prefix needs complete progress without budget refusal",
                ).toBeNull();
                const ref = captured.ref;
                if (!ref) throw new Error("Missing native captured image");
                f.setCapturing(false);
                expect(f.source.allocated, "source storage retires before replay").toBe(0);
                expect(f.source.loaned, "source loan retires before replay").toBe(0);
                const image = await f.images.readRowImages(ref, f.stop.signal);
                try {
                  expect(image.new).toBeNull();
                  expect(image.old?.columns.length).toBe(3);
                  const old = image.old;
                  if (!old) throw new Error("missing captured OLD");
                  const actual = old.columns.map((vector) => valueAt(vector, 0)),
                    expected = (
                      await pg.query<{ id: number; n: string | null; payload: string | null }>(
                        "SELECT * FROM p WHERE id=$1",
                        [i + 1],
                      )
                    ).rows[0];
                  expect(actual[0], "archived OLD row keeps requested native identity").toBe(
                    expected?.id,
                  );
                  expect(actual[1]).toEqual(
                    expected?.n === null ? null : [BigInt(expected?.n ?? "0"), 0],
                  );
                  expect(actual[2]).toBe(expected?.payload);
                } finally {
                  image.release();
                }
              }
              expect(legacy).not.toHaveBeenCalled();
              expect(
                added.mock.calls.every(
                  ([name, listener]) =>
                    name !== "abort" ||
                    removed.mock.calls.some(
                      ([event, retired]) => event === "abort" && retired === listener,
                    ),
                ),
                "settled captures retain no root operation listeners",
              ).toBe(true);
              expect(
                hints.mock.calls.every(([request]) => request.maxRows === 1),
                "native decode matches its single-row progress sizing",
              ).toBe(true);
              if (hints.mock.calls.length > 0) reach.reach("single-row native sizing");
              if (width === 80000 && !published) {
                expect(borrow, "wide source uses protected progress").toHaveBeenCalled();
                reach.reach("source progress loan");
              }
              reach.reach(width >= 24000 ? "fragmented payload" : "small payload");
              reach.reach(published ? "published" : "resident");
              reach.reach(nullable ? "NULLs" : "non-null");
              reach.reach(wide ? "wide decoder tier" : "native decoder tier");
              reach.reach("full physical order");
              reach.reach("no legacy copies");
              reach.reach("source retired before replay");
            } finally {
              decode.mockRestore();
              borrow.mockRestore();
              hints.mockRestore();
              added.mockRestore();
              removed.mockRestore();
              legacy.mockRestore();
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 10,
          examples: [
            [6, 80000, false, true, true],
            [6, 80000, true, true, true],
            [4, 24000, true, false, false],
            [2, 0, false, false, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);

for (const seed of seedsFor("next-native-prefix-images-lifetime", [0xb62d]))
  it(`fences native prefix capture across close, faults and phase changes ${String(seed)}`, async () => {
    const reach = createReach("native prefix lifetime", NATIVE_PREFIX_IMAGES_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(
          "prepare",
          "payload",
          "phase",
          "fault",
          "synchronous close",
          "reentrant capture",
        ),
        fc.boolean(),
        fc.boolean(),
        async (kind, close, published) => {
          reach.beginHistory();
          const f = await fixture(2, 24000, published, false),
            caller = new AbortController(),
            fault = new Error("native prefix capture fault"),
            life: {
              closing?: Promise<void>;
              fence?: Promise<void>;
              recursive?: Promise<void>;
              concurrent?: Promise<unknown>;
              reentrantError?: unknown;
              done: boolean;
            } = { done: false };
          let enter: () => void = () => undefined,
            resume: () => void = () => undefined;
          const ready = new Promise<void>((resolve) => {
              enter = resolve;
            }),
            gate = new Promise<void>((resolve) => {
              resume = resolve;
            });
          const closing = () => {
            life.fence = f.reader.close();
            life.closing = life.fence.then(() => {
              life.done = true;
            });
          };
          const reenter = () => {
            life.recursive = f.reader.close();
          };
          f.stop.signal.addEventListener("abort", reenter, { once: true });
          let stored: { mockRestore(): void } | undefined;
          let attempted = false;
          if (kind === "prepare" || kind === "synchronous close" || kind === "reentrant capture")
            f.setPrepare(async () => {
              if (kind === "synchronous close") closing();
              if (kind === "reentrant capture") {
                if (attempted) return;
                attempted = true;
                try {
                  const id = f.ids[0];
                  if (id === undefined) throw new Error("Missing native row");
                  life.concurrent = f.reader
                    .capture(f.tableId, id, caller.signal)
                    .catch((reason: unknown) => reason);
                } catch (error) {
                  life.reentrantError = error;
                }
              }
              enter();
              await gate;
              if (kind === "reentrant capture") throw fault;
            });
          else {
            const append = f.fragments.append.bind(f.fragments);
            stored = vi.spyOn(f.fragments, "append").mockImplementation(async (...args) => {
              const ref = await append(...args);
              enter();
              await gate;
              if (kind === "fault") throw fault;
              return ref;
            });
          }
          try {
            const id = f.ids[0];
            if (id === undefined) throw new Error("missing native identity");
            f.setCapturing(true);
            const paid = f.root.allocated,
              pending = f.reader.capture(f.tableId, id, caller.signal).then(
                () => null,
                (reason: unknown) => reason,
              );
            await ready;
            if (kind === "phase") f.statement.beginMutation();
            else if (
              kind !== "fault" &&
              kind !== "synchronous close" &&
              kind !== "reentrant capture"
            ) {
              if (close) closing();
              else caller.abort(fault);
            }
            await yieldTask();
            const stayedPaid = f.root.allocated === paid,
              completedEarly = life.done,
              sharedFence =
                life.recursive === undefined || life.recursive === (life.fence ?? f.reader.close());
            resume();
            const error = await pending;
            await life.closing;
            await life.recursive;
            await life.concurrent;
            expect(
              error,
              "unfinished or phase-invalid capture never delivers a row image",
            ).toBeInstanceOf(Error);
            if (kind === "phase") expect((error as Error).message).toContain("locator-only");
            if (kind === "fault") expect(error).toBe(fault);
            if (kind === "reentrant capture")
              expect(
                life.reentrantError,
                "reentrant capture refuses before starting a second source preparation",
              ).toBeInstanceOf(TypeError);
            expect(stayedPaid, "prefix metadata stays charged through pending preparation/IO").toBe(
              true,
            );
            expect(completedEarly, "close awaits the installed pending operation").toBe(false);
            expect(sharedFence, "reentrant close shares one installed fence").toBe(true);
            expect(f.source.allocated).toBe(0);
            expect(f.source.loaned).toBe(0);
            if (kind === "prepare" || kind === "payload")
              reach.reach(close ? "external close" : "caller cancellation");
            reach.reach(kind);
            reach.reach(published ? "published" : "resident");
            reach.reach("paid pending work");
            reach.reach("source loan repaid");
            if (life.recursive) reach.reach("reentrant close shares fence");
          } finally {
            resume();
            await life.closing;
            await life.recursive;
            await life.concurrent;
            f.stop.signal.removeEventListener("abort", reenter);
            stored?.mockRestore();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 10,
        examples: [
          ["prepare", false, false],
          ["prepare", true, true],
          ["payload", false, true],
          ["payload", true, false],
          ["phase", false, false],
          ["fault", false, true],
          ["synchronous close", true, false],
          ["reentrant capture", false, true],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
