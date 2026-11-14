/** Atomic load law: old/absent or complete native root, never a portable prefix. */
import { Governor, type Grant } from "../memory/governor.js";
import { spoolSnapshotInput, type ReplayableSnapshotInput } from "./snapshot-source.js";
import type { PhysicalType } from "../types.js";
import { encodeSnapshotColumnHeader } from "./snapshot-column.js";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  SNAPSHOT_LOAD_REACH,
  SNAPSHOT_LOAD_FAULT_REACH,
  SNAPSHOT_NATIVE_LAYOUT_REACH,
  SNAPSHOT_LOAD_OPEN_REACH,
  SNAPSHOT_CATALOG_REFUSAL_REACH,
  SNAPSHOT_ADMISSION_REACH,
  SNAPSHOT_STRUCTURED_REACH,
  SNAPSHOT_LOAD_ROOT_REACH,
} from "../../testing/reach.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
import { FileScratch } from "../storage/file-store.js";
import { SimDisk } from "../storage/sim-disk.js";
import { openExtentSpace } from "../storage/space.js";
import type { ByteBudget, ExtentSpace } from "../storage/extents.js";
import {
  SnapshotKind,
  SnapshotStreamReader,
  SnapshotStreamWriter,
  snapshotCrc32,
  snapshotNumbers,
} from "../format/snapshot.js";
import { vectorOf } from "../page/test-support.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { ByteReader, ByteWriter } from "./bytes.js";
import { decodeCatalogStream, encodeCatalogStream } from "./catalog-stream.js";
import { ftsDef } from "../access/fts-def.js";
import { CATALOG_RECORD_KINDS } from "../durable-format.js";
import { Catalog } from "./catalog.js";
import { exportPinnedSnapshot } from "./snapshot-export.js";
import { loadSnapshot, type SnapshotLoadTarget } from "./snapshot-load.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  tableInput,
  texts,
} from "./test-support.js";
class Budget extends Governor implements ByteBudget {
  #used = 0;
  #peak = 0;
  get used(): number {
    return this.#used + this.usedBytes;
  }
  get peak(): number {
    return Math.max(this.#peak, this.peakBytes);
  }
  constructor(readonly cap = 32 << 20) {
    super(cap);
  }
  allocate(n: number): boolean {
    if (this.#used + n > this.cap) return false;
    this.#used += n;
    this.#peak = Math.max(this.#peak, this.#used);
    return true;
  }
  free(n: number): void {
    this.#used -= n;
    if (this.#used < 0) throw new Error("double free");
  }
}
function input(
  chunks: readonly Uint8Array[],
  before?: (offset: number, signal: AbortSignal) => Promise<void>,
): ReplayableSnapshotInput {
  return {
    byteLength: chunks.reduce((sum, chunk) => sum + chunk.length, 0),
    async readInto(offset, into, signal) {
      signal.throwIfAborted();
      await before?.(offset, signal);
      signal.throwIfAborted();
      let start = 0,
        copied = 0;
      for (const chunk of chunks) {
        if (start + chunk.length > offset && copied < into.length) {
          const at = Math.max(0, offset - start),
            take = Math.min(chunk.length - at, into.length - copied);
          into.set(chunk.subarray(at, at + take), copied);
          copied += take;
          offset += take;
        }
        start += chunk.length;
      }
      if (copied !== into.length) throw new Error("short fixture source");
    },
  };
}
function stalled(
  chunks: readonly Uint8Array[],
  enter: () => void,
  gate: Promise<void>,
): ReplayableSnapshotInput {
  const end = chunks.slice(0, -1).reduce((sum, chunk) => sum + chunk.length, 0);
  return input(chunks, async (offset, signal) => {
    if (offset < end) return;
    enter();
    let cancel!: () => void;
    try {
      await Promise.race([
        gate,
        new Promise<never>((_, reject) => {
          cancel = () =>
            reject(signal.reason instanceof Error ? signal.reason : new Error("source cancelled"));
          signal.addEventListener("abort", cancel, { once: true });
          if (signal.aborted) cancel();
        }),
      ]);
    } finally {
      signal.removeEventListener("abort", cancel);
    }
  });
}
async function fixture(values: ReadonlyArray<number | null>, large = false): Promise<Uint8Array[]> {
  const h = new Harness("strict", "memory"),
    path = await h.open();
  try {
    await autocommit(path, (st) =>
      st.changeCatalog({
        kind: "createTable",
        table: tableInput("loaded", [column("n", INT), column("s", { kind: "text" })], {
          primaryKey: null,
        }),
      }),
    );
    await autocommit(path, (st) =>
      st
        .table(1)
        .insert(
          batch(
            ints(values),
            texts(
              values.map((v, i) =>
                v === null
                  ? null
                  : large && i === 0
                    ? "z".repeat((3 << 20) + 17)
                    : `value${String(v)}`,
              ),
            ),
          ),
          null,
        ),
    );
    const pin = path.pin(),
      catalog = pin.catalog;
    if (!(catalog instanceof Catalog)) throw new Error("fixture requires native catalog");
    const bytes: Uint8Array[] = [],
      b = new Budget();
    await exportPinnedSnapshot(
      pin,
      new Uint8Array(16),
      (async function* () {
        yield catalog.encode();
      })(),
      {
        write(bytesIn) {
          bytes.push(bytesIn.slice());
        },
      },
      b,
    );
    expect(b.used).toBe(0);
    return bytes;
  } finally {
    await h.close();
  }
}
async function target(
  h: Harness,
  currentLsn = 0,
  empty = true,
  observe?: (space: ExtentSpace) => void,
  prepaid?: Grant,
): Promise<SnapshotLoadTarget> {
  const lease = await h.backend.acquire(),
    session = await h.backend.open(lease),
    startup =
      prepaid ??
      (await h.governor.admit({
        label: "load startup",
        minimumBytes: 8 << 20,
        spillable: true,
      }));
  try {
    const space = await openExtentSpace(session, {
      reachable: listedRootExtents(),
      walk: reachableExtents(),
      verifyAtOpen: true,
      budget: startup,
      registerCache: (cache) => h.governor.registerCache(cache),
    });
    observe?.(space);
    return {
      space,
      epoch: session.epoch,
      currentLsn,
      empty,
      async close() {
        try {
          await space.close();
        } finally {
          if (prepaid === undefined) startup.close();
          try {
            await session.close();
          } finally {
            await lease.release();
          }
        }
      },
    };
  } catch (error) {
    if (prepaid === undefined) startup.close();
    await session.close();
    await lease.release();
    throw error;
  }
}
async function contents(h: Harness): Promise<unknown[][]> {
  const path = await h.open(),
    pin = path.pin();
  try {
    return pin.catalog.tables.length === 0 ? [] : await rowsOf(pin, 1);
  } finally {
    pin.release();
    await h.close();
  }
}
for (const seed of seedsFor("next-snapshot-load", [0xb261, 0xb262]))
  it(`publishes only complete native generations and retries seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot load", SNAPSHOT_LOAD_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.option(fc.integer({ min: -100, max: 100 }), { nil: null }), {
          minLength: 1,
          maxLength: 12,
        }),
        fc.nat(),
        async (values, pick) => {
          reach.beginHistory();
          const chunks = await fixture(values),
            h = new Harness("strict", "memory");
          const old = await h.open();
          await autocommit(old, (st) =>
            st.changeCatalog({
              kind: "createTable",
              table: tableInput("old", [column("n", INT), column("s", { kind: "text" })], {
                primaryKey: null,
              }),
            }),
          );
          await autocommit(old, (st) =>
            st.table(1).insert(batch(ints([777]), texts(["old"])), null),
          );
          await old.recycleLog();
          const lsn = old.appliedLsn;
          await h.close();
          const cut = pick % chunks.length,
            b = new Budget();
          let publications = 0,
            before: Uint8Array | null = null;
          await expect(
            loadSnapshot(
              input(chunks.slice(0, cut)),
              () =>
                target(h, lsn, false, (space) => {
                  before = space.root?.slice() ?? null;
                  const publish = space.publish.bind(space);
                  space.publish = (...args) => {
                    publications++;
                    if (space.root !== null) {
                      const old = decodeManifestRoot(space.root, b),
                        next = decodeManifestRoot(args[0], b);
                      try {
                        expect(next.value.log.salt).not.toBe(old.value.log.salt);
                      } finally {
                        b.free(old.charge + next.charge);
                      }
                    }
                    return publish(...args);
                  };
                }),
              b,
              { replace: true },
            ),
          ).rejects.toThrow();
          expect(publications).toBe(0);
          expect(b.used).toBe(0);
          expect(await contents(h)).toEqual([[777, "old"]]);
          reach.reach("interrupted prefix");
          reach.reach("old target intact");
          await loadSnapshot(
            input(chunks),
            () =>
              target(h, lsn, false, (space) => {
                if (before !== null) expect(space.root).toEqual(before);
                const publish = space.publish.bind(space);
                space.publish = (...args) => {
                  publications++;
                  if (space.root !== null) {
                    const old = decodeManifestRoot(space.root, b),
                      next = decodeManifestRoot(args[0], b);
                    try {
                      expect(next.value.log.salt).not.toBe(old.value.log.salt);
                    } finally {
                      b.free(old.charge + next.charge);
                    }
                  }
                  return publish(...args);
                };
              }),
            b,
            { replace: true },
          );
          expect(publications).toBe(1);
          expect(b.used).toBe(0);
          expect(await contents(h)).toEqual(
            values.map((v) => [v, v === null ? null : `value${String(v)}`]),
          );
          reach.reach("retry complete");
          reach.reach("native reopen");
          // Native ordinary writes continue, using imported row IDs/catalog and new log incarnation.
          const path = await h.open();
          await autocommit(path, (st) =>
            st.table(1).insert(batch(ints([888]), texts(["after"])), null),
          );
          await h.close();
          expect((await contents(h)).at(-1)).toEqual([888, "after"]);
          reach.reach("continued writes");
          const fresh = new Harness("strict", "memory"),
            denied = new Budget(2 << 20);
          await expect(loadSnapshot(input(chunks), () => target(fresh), denied)).rejects.toThrow();
          expect(denied.used).toBe(0);
          expect(await contents(fresh)).toEqual([]);
          reach.reach("quota safe");
          expect(h.governor.usedBytes).toBe(0);
          expect(fresh.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60000);
it("streams large values through private native parts and refuses trailing bytes", async () => {
  const chunks = await fixture([1, null], true),
    h = new Harness("strict", "memory"),
    b = new Budget(12 << 20);
  await expect(
    loadSnapshot(input([...chunks, new Uint8Array([0])]), () => target(h), b),
  ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_FORMAT" });
  expect(b.used).toBe(0);
  expect(await contents(h)).toEqual([]);
  await loadSnapshot(input(chunks), () => target(h), b);
  expect(b.used).toBe(0);
  expect(await contents(h)).toEqual([
    [1, "z".repeat((3 << 20) + 17)],
    [null, null],
  ]);
  expect(b.peak).toBeLessThan(12 << 20);
  expect(h.governor.usedBytes).toBe(0);
}, 60000);

it("discards every incomplete frame prefix and never publishes before physical EOF", async () => {
  const chunks = await fixture([1, null, 3]);
  for (let cut = 0; cut < chunks.length; cut++) {
    const h = new Harness("strict", "memory"),
      b = new Budget();
    let calls = 0;
    await expect(
      loadSnapshot(
        input(chunks.slice(0, cut)),
        () =>
          target(h, 0, true, (space) => {
            const publish = space.publish.bind(space);
            space.publish = (...args) => {
              calls++;
              return publish(...args);
            };
          }),
        b,
      ),
    ).rejects.toThrow();
    expect(calls).toBe(0);
    expect(b.used).toBe(0);
    expect(await contents(h)).toEqual([]);
    expect(h.governor.usedBytes).toBe(0);
  }
  const h = new Harness("strict", "memory"),
    b = new Budget();
  let eof = false;
  const source = input(chunks);
  await loadSnapshot(
    {
      byteLength: source.byteLength,
      async readInto(offset, into, signal) {
        await source.readInto(offset, into, signal);
        if (offset + into.length === source.byteLength) eof = true;
      },
    },
    () =>
      target(h, 0, true, (space) => {
        const publish = space.publish.bind(space);
        space.publish = (...args) => {
          expect(eof).toBe(true);
          return publish(...args);
        };
      }),
    b,
  );
  expect(await contents(h)).toEqual([
    [1, "value1"],
    [null, null],
    [3, "value3"],
  ]);
  expect(b.used).toBe(0);
});

for (const seed of seedsFor("next-snapshot-load-faults", [0xb263, 0xb264]))
  it(`releases cancelled, quota-failed and ambiguous loads seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot load faults", SNAPSHOT_LOAD_FAULT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 0, max: 10 }), async (failAt) => {
        reach.beginHistory();
        const chunks = await fixture([5, null, 7]);
        for (const mode of [
          "quota",
          "before publication",
          "after publication",
          "cancel",
        ] as const) {
          const h = new Harness("strict", "memory"),
            b = new Budget(),
            controller = new AbortController();
          let calls = 0,
            entered!: () => void,
            unblock!: () => void;
          const ready = new Promise<void>((r) => {
              entered = r;
            }),
            gate = new Promise<void>((r) => {
              unblock = r;
            });
          const stream = mode === "cancel" ? stalled(chunks, entered, gate) : input(chunks);
          const task = loadSnapshot(
            stream,
            () =>
              target(h, 0, true, (space) => {
                if (mode === "quota") {
                  const begin = space.beginWrite.bind(space);
                  space.beginWrite = (...args) => {
                    const writer = begin(...args),
                      open = writer.open.bind(writer);
                    writer.open = (...argsIn) => {
                      const builder = open(...argsIn),
                        append = builder.append.bind(builder);
                      builder.append = (...parts) => {
                        if (calls++ === failAt)
                          return Promise.reject(
                            new DOMException("snapshot quota", "QuotaExceededError"),
                          );
                        return append(...parts);
                      };
                      return builder;
                    };
                    return writer;
                  };
                } else if (mode !== "cancel") {
                  const publish = space.publish.bind(space);
                  space.publish = async (...args) => {
                    if (mode === "after publication") await publish(...args);
                    throw new Error("publication outcome unknown");
                  };
                }
              }),
            b,
            { signal: controller.signal },
          );
          if (mode === "cancel") {
            await ready;
            controller.abort(new Error("load cancelled"));
          }
          try {
            if (mode === "quota") {
              // A late failAt may be past the final write; both complete success and quota refusal are safe.
              await task.catch((error: unknown) => {
                expect(error).toMatchObject({ name: "QuotaExceededError" });
              });
            } else
              await expect(task).rejects.toThrow(
                mode === "cancel" ? "load cancelled" : "publication outcome unknown",
              );
          } finally {
            unblock();
            await task.catch(() => undefined);
          }
          expect(b.used).toBe(0);
          const found = await contents(h);
          if (mode === "after publication")
            expect(found).toEqual([
              [5, "value5"],
              [null, null],
              [7, "value7"],
            ]);
          else if (mode !== "quota" || calls > failAt) expect(found).toEqual([]);
          expect(h.governor.usedBytes).toBe(0);
          reach.reach(mode);
        }
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60000);

async function oldTarget(h: Harness): Promise<number> {
  const path = await h.open();
  await autocommit(path, (st) =>
    st.changeCatalog({
      kind: "createTable",
      table: tableInput("old", [column("n", INT), column("s", { kind: "text" })], {
        primaryKey: null,
      }),
    }),
  );
  await autocommit(path, (st) => st.table(1).insert(batch(ints([777]), texts(["old"])), null));
  await path.recycleLog();
  const lsn = path.appliedLsn;
  await h.close();
  return lsn;
}

it("recovers old or complete data after every physical publication step", async () => {
  const chunks = await fixture([10, null, 30]);
  let steps = 0;
  const baseline = new Harness("strict", "sim"),
    initial = await oldTarget(baseline),
    b = new Budget();
  await loadSnapshot(
    input(chunks),
    () =>
      target(baseline, initial, false, (space) => {
        const publish = space.publish.bind(space);
        space.publish = async (...args) => {
          const first = baseline.disk.steps;
          const result = await publish(...args);
          steps = baseline.disk.steps - first;
          return result;
        };
      }),
    b,
    { replace: true },
  );
  expect(steps).toBeGreaterThan(0);
  for (let cut = 0; cut < steps; cut++) {
    const options = { seed: cut + 1, crashAtStep: Number.MAX_SAFE_INTEGER },
      disk = new SimDisk(options),
      h = new Harness("strict", "sim", undefined, disk),
      lsn = await oldTarget(h),
      funding = new Budget();
    await expect(
      loadSnapshot(
        input(chunks),
        () =>
          target(h, lsn, false, (space) => {
            const publish = space.publish.bind(space);
            space.publish = (...args) => {
              options.crashAtStep = disk.steps + cut;
              return publish(...args);
            };
          }),
        funding,
        { replace: true },
      ),
    ).rejects.toThrow();
    expect(disk.crashes).toBe(1);
    expect(funding.used).toBe(0);
    const found = await contents(h),
      isOld = found.length === 1;
    expect(found).toEqual(
      isOld
        ? [[777, "old"]]
        : [
            [10, "value10"],
            [null, null],
            [30, "value30"],
          ],
    );
    await loadSnapshot(input(chunks), () => target(h, lsn + (isOld ? 0 : 1), false), funding, {
      replace: true,
    });
    expect(await contents(h)).toEqual([
      [10, "value10"],
      [null, null],
      [30, "value30"],
    ]);
    expect(funding.used).toBe(0);
    expect(h.governor.usedBytes).toBe(0);
  }
  expect(b.used).toBe(0);
  expect(baseline.governor.usedBytes).toBe(0);
}, 60000);

for (const seed of seedsFor("next-snapshot-native-layouts", [0xb265, 0xb266]))
  it(`reconstructs exact portable lanes across native reopen seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot native layouts", SNAPSHOT_NATIVE_LAYOUT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: -1000, max: 1000 }),
        fc.boolean(),
        async (salt, sparse) => {
          reach.beginHistory();
          const source = new Harness("strict", "memory"),
            path = await source.open();
          const uuid = Uint8Array.from({ length: 16 }, (_, i) => (i + salt) & 255);
          const array = encodeArrayValue(INT, {
            dimensions: [2],
            lowerBounds: [-1],
            elements: [salt, null],
          });
          const vectors = [
            vectorOf({ kind: "bool" }, [true, false, true, false, true]),
            vectorOf(INT, [salt, -salt, 0, 123, 456]),
            vectorOf({ kind: "date" }, [-2147483648, 2147483647, 0, salt, -salt]),
            vectorOf({ kind: "time" }, [0n, 86400000000n, 123n, 456n, 789n]),
            vectorOf({ kind: "timestamp", zoned: true }, [
              -(2n ** 63n),
              2n ** 63n - 1n,
              2n ** 60n,
              0n,
              BigInt(salt),
            ]),
            vectorOf({ kind: "float32" }, [-0, NaN, Infinity, -Infinity, Math.fround(salt / 3)]),
            vectorOf({ kind: "float64" }, [-0, NaN, Infinity, -Infinity, salt / 7]),
            vectorOf({ kind: "int64" }, [2n ** 60n, -(2n ** 61n), 0n, 1n, BigInt(salt)]),
            vectorOf(
              { kind: "interval" },
              Array.from({ length: 5 }, (_, i) => ({
                months: salt,
                days: i,
                micros: 2n ** 60n + BigInt(i),
              })),
            ),
            vectorOf(
              { kind: "decimal", precision: null, scale: null },
              [
                { coefficient: 2n ** 100n, scale: 3, special: 0 },
                { coefficient: 0n, scale: 0, special: 1 },
                { coefficient: 0n, scale: 0, special: 2 },
                { coefficient: 0n, scale: 0, special: 3 },
                { coefficient: -123n, scale: 7, special: 0 },
              ],
              { tier: "int128" },
            ),
            vectorOf({ kind: "uuid" }, [uuid, uuid, uuid, uuid, uuid]),
            vectorOf({ kind: "array", element: INT }, [array, array, array, array, array]),
            vectorOf({ kind: "bytes" }, [
              new Uint8Array([0, 255]),
              new Uint8Array(0),
              uuid,
              uuid,
              uuid,
            ]),
          ];
          for (const vector of vectors) if (sparse) vector.validity = new Uint32Array([0b10101]);
          for (const vector of vectors)
            if (vector.data.layout === "f64" || vector.data.layout === "f32") {
              const values = vector.data.values,
                view = new DataView(values.buffer);
              if (values instanceof Float64Array) view.setBigUint64(8, 0x7ff8000000000042n, true);
              else view.setUint32(4, 0x7fc00042, true);
            }
          let archive: Uint8Array[];
          async function capture(h: Harness): Promise<Uint8Array[]> {
            const p = h.path,
              pin = p.pin(),
              catalog = pin.catalog,
              b = new Budget(),
              chunks: Uint8Array[] = [];
            if (!(catalog instanceof Catalog)) throw new Error("native catalog expected");
            await exportPinnedSnapshot(
              pin,
              new Uint8Array(16),
              (async function* () {
                yield catalog.encode();
              })(),
              {
                write(bytes) {
                  chunks.push(bytes.slice());
                },
              },
              b,
            );
            expect(b.used).toBe(0);
            return chunks;
          }
          try {
            await autocommit(path, (st) =>
              st.changeCatalog({
                kind: "createTable",
                table: tableInput(
                  "physical",
                  vectors.map((v, i) => column(`c${String(i)}`, v.type)),
                  { primaryKey: null },
                ),
              }),
            );
            await autocommit(path, (st) => st.table(1).insert(batch(...vectors), null));
            archive = await capture(source);
          } finally {
            await source.close();
          }
          async function body(chunks: readonly Uint8Array[]): Promise<Uint8Array[]> {
            const funding = new Budget(),
              reader = await SnapshotStreamReader.open(
                (async function* () {
                  yield* chunks;
                })(),
                funding,
              ),
              parts: Uint8Array[] = [];
            for await (const frame of reader.frames())
              if (frame.kind === SnapshotKind.columnPart) parts.push(frame.payload.slice());
            expect(funding.used).toBe(0);
            return parts;
          }
          const h = new Harness("strict", "memory"),
            funding = new Budget();
          await loadSnapshot(input(archive), () => target(h), funding);
          expect(funding.used).toBe(0);
          await h.open();
          let copied: Uint8Array[];
          try {
            copied = await capture(h);
          } finally {
            await h.close();
          }
          expect(await body(copied)).toEqual(await body(archive));
          expect(h.governor.usedBytes).toBe(0);
          expect(source.governor.usedBytes).toBe(0);
          reach.reach(sparse ? "sparse nulls" : "all valid");
          reach.reach("all scalar and byte layouts");
        },
      ),
      { seed, numRuns: 6 },
    );
    reach.assertReached();
  }, 60000);

for (const seed of seedsFor("next-snapshot-load-openers", [0xb267, 0xb268]))
  it(`keeps competing openers at complete generations across admission retries seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot load openers", SNAPSHOT_LOAD_OPEN_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (cancel) => {
        reach.beginHistory();
        const chunks = await fixture([12, null]),
          h = new Harness("strict", "memory"),
          b = new Budget(),
          controller = new AbortController();
        let enter!: () => void,
          unblock!: () => void,
          opened = false;
        const ready = new Promise<void>((r) => {
            enter = r;
          }),
          gate = new Promise<void>((r) => {
            unblock = r;
          });
        const loading = loadSnapshot(stalled(chunks, enter, gate), () => target(h), b, {
          signal: controller.signal,
        });
        await ready;
        const opening = contents(h).then((rows) => {
          opened = true;
          return rows;
        });
        await new Promise<void>((r) => setImmediate(r));
        expect(opened).toBe(false);
        try {
          if (cancel) {
            controller.abort(new Error("cancel while opener waits"));
            await expect(loading).rejects.toThrow("cancel while opener waits");
          } else {
            unblock();
            await loading;
          }
          const complete = [
            [12, "value12"],
            [null, null],
          ];
          // Dropping an unpublished attempt releases its fence before re-admission. A queued
          // opener may observe the old empty generation, never a partially loaded table.
          const observed = await opening;
          if (cancel) expect(observed).toEqual([]);
          else expect([[], complete]).toContainEqual(observed);
          expect(await contents(h)).toEqual(cancel ? [] : complete);
          expect(b.used).toBe(0);
          expect(h.governor.usedBytes).toBe(0);
          reach.reach(cancel ? "wait until discard" : "wait until publication");
        } finally {
          unblock();
          await loading.catch(() => undefined);
          await opening;
        }
      }),
      { seed, numRuns: 6 },
    );
    reach.assertReached();
  }, 60000);

/** A nonrepresentable ID cannot pass the canonical encoder, so mutate its wire field.
 * Reframe every MCAT length and outer CRC afterward; no malformed transport shortcut. */
function unsafeCatalogId(bytes: Uint8Array): Uint8Array {
  const input = new ByteReader(bytes),
    out = new ByteWriter();
  out.raw(input.raw(16));
  let changed = false;
  while (input.offset < bytes.length) {
    const kind = input.varuint(),
      version = input.varuint(),
      body = input.blob();
    out.varuint(kind);
    out.varuint(version);
    if (kind !== CATALOG_RECORD_KINDS.table || changed) {
      out.blob(body);
      continue;
    }
    const fields = new ByteReader(body),
      encoded = new ByteWriter();
    while (fields.offset < body.length) {
      const id = fields.varuint(),
        flags = fields.u8(),
        payload = fields.blob();
      encoded.varuint(id);
      encoded.u8(flags);
      if (id === 1) {
        encoded.blob(new Uint8Array([128, 128, 128, 128, 128, 128, 128, 16]));
        changed = true;
      } else encoded.blob(payload);
    }
    out.blob(encoded.finish());
  }
  expect(changed).toBe(true);
  return out.finish();
}

async function corruptCatalog(
  chunks: readonly Uint8Array[],
  change: (value: Record<string, unknown>) => void,
  unsafeId = false,
): Promise<Uint8Array[]> {
  const b = new Budget(),
    reader = await SnapshotStreamReader.open(
      (async function* () {
        yield* chunks;
      })(),
      b,
    ),
    out: Uint8Array[] = [];
  const writer = await SnapshotStreamWriter.open(
    reader.header,
    {
      write(bytes) {
        out.push(bytes.slice());
      },
    },
    b,
  );
  let length = 0,
    crc = 0;
  try {
    for await (const frame of reader.frames()) {
      if (frame.kind === SnapshotKind.footer) continue;
      let bytes = frame.payload;
      if (frame.kind === SnapshotKind.catalogPart) {
        const data = decodeCatalogStream(bytes);
        change(data as unknown as Record<string, unknown>);
        bytes = encodeCatalogStream(data);
        if (unsafeId) bytes = unsafeCatalogId(bytes);
        length = bytes.length;
        crc = snapshotCrc32(bytes);
      } else if (frame.kind === SnapshotKind.catalogEnd) {
        bytes = new Uint8Array(16);
        snapshotNumbers([length, 0], bytes);
        new DataView(bytes.buffer).setUint32(8, crc, true);
      }
      await writer.frame(frame.kind, frame.objectId, frame.partIndex, bytes);
    }
    await writer.finish();
  } finally {
    reader.close();
    writer.close();
  }
  expect(b.used).toBe(0);
  return out;
}
for (const seed of seedsFor("next-snapshot-catalog-refusal", [0xb269, 0xb26a]))
  it(`refuses checksum-valid catalog mutations before target acquisition seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot catalog refusal", SNAPSHOT_CATALOG_REFUSAL_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 10000 }), async (salt) => {
        reach.beginHistory();
        const chunks = await fixture([salt, null]),
          h = new Harness("strict", "memory"),
          path = await h.open();
        await autocommit(path, (st) =>
          st.changeCatalog({
            kind: "createTable",
            table: tableInput("old", [column("n", INT), column("s", { kind: "text" })], {
              primaryKey: null,
            }),
          }),
        );
        await autocommit(path, (st) =>
          st.table(1).insert(batch(ints([777]), texts(["old"])), null),
        );
        const lsn = path.appliedLsn;
        await h.close();
        for (const mutation of SNAPSHOT_CATALOG_REFUSAL_REACH) {
          const damaged = await corruptCatalog(
            chunks,
            (data) => {
              const tables = data.tables as Array<Record<string, unknown>>,
                table = tables[0];
              if (table === undefined) throw new Error("missing fixture table");
              const columns = table.columns as Array<Record<string, unknown>>,
                first = columns[0];
              if (first === undefined) throw new Error("missing fixture column");
              switch (mutation) {
                case "duplicate table name":
                  tables.push({ ...table, id: salt + 10 });
                  (data.counters as Record<string, unknown>).nextTableId = salt + 11;
                  break;
                case "counter floor":
                  (data.counters as Record<string, unknown>).nextTableId = 1;
                  break;
                case "duplicate column id":
                  columns.push({ ...first });
                  break;
                case "unsafe id":
                  // Inject an out-of-u53 varint below: the canonical encoder refuses it.
                  table.id = 1;
                  break;
                case "unresolved reference":
                  first.identitySequence = salt + 100;
                  break;
                case "unsupported trigger":
                  tables.push({ ...table, id: 2, name: "audit", columns: [] });
                  (data.counters as Record<string, unknown>).nextTableId = 3;
                  (data.counters as Record<string, unknown>).nextTriggerId = 2;
                  data.triggers = [
                    {
                      id: 1,
                      name: "copy",
                      tableId: 1,
                      event: "insert",
                      timing: "after",
                      dialectVersion: 1,
                      statements: [
                        {
                          sql: 'INSERT INTO "audit" DEFAULT VALUES',
                          kind: "insert",
                          targetTableId: 2,
                          targetName: "audit",
                          bindings: [],
                          columns: [],
                          insertColumnIds: [],
                          defaultColumnIds: [],
                          implicitArity: null,
                          assignedColumnIds: [],
                          typeIds: [],
                          sequenceIds: [],
                        },
                      ],
                    },
                  ];
                  break;
                case "unsupported fts":
                  data.fts = [ftsDef(1, 1, 2)];
                  (data.counters as Record<string, unknown>).nextFtsId = 2;
                  break;
                case "unsupported field":
                  table.managed = true;
                  break;
              }
            },
            mutation === "unsafe id",
          );
          let opens = 0;
          const b = new Budget();
          await expect(
            loadSnapshot(
              input(damaged),
              () => {
                opens++;
                return target(h, lsn, false);
              },
              b,
              { replace: true },
            ),
          ).rejects.toMatchObject({
            code: mutation.startsWith("unsupported")
              ? "MINNOW_SNAPSHOT_SCOPE"
              : "MINNOW_SNAPSHOT_FORMAT",
          });
          expect(opens).toBe(0);
          expect(b.used).toBe(0);
          reach.reach(mutation);
        }
        expect(await contents(h)).toEqual([[777, "old"]]);
        expect(h.governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 6 },
    );
    reach.assertReached();
  }, 60000);

for (const seed of seedsFor("next-snapshot-admission", [0xb26f, 0xb270]))
  it(`replays only after dropping held inputs under real governor contention seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot admission", SNAPSHOT_ADMISSION_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 100 }), async (value) => {
        reach.beginHistory();
        for (const cancel of [false, true]) {
          const chunks = await fixture([value, null]),
            h = new Harness("strict", "memory"),
            governor = h.governor;
          const startup = await governor.admit({
            label: "prepaid owner startup",
            minimumBytes: 8 << 20,
            spillable: false,
          });
          const prepared = await target(h, 0, true, undefined, startup);
          const preparedState: { owner?: SnapshotLoadTarget } = { owner: prepared };
          const baseline = governor.usedBytes;
          const [holder] = await governor.admitGroup({
            label: "temporary contention",
            includesProgress: true,
            grants: [
              {
                label: "contender",
                minimumBytes: governor.capBytes - baseline - (2 << 20),
                spillable: false,
              },
            ],
          });
          if (holder === undefined) throw new Error("missing contender");
          expect(holder.allocate(holder.held)).toBe(true);
          const controller = new AbortController();
          let queued!: () => void,
            opens = 0,
            readsAtZero = 0;
          const waiting = new Promise<void>((resolve) => {
            queued = resolve;
          });
          const admit = governor.admitGroup.bind(governor);
          governor.admitGroup = (options) => {
            const requested = options.grants.reduce((sum, part) => sum + part.minimumBytes, 0);
            if (options.label === "portable snapshot load" && requested > 2 << 20) queued();
            return admit(options);
          };
          const source = input(chunks, async (offset) => {
            if (offset === 0) readsAtZero++;
          });
          const loading = loadSnapshot(
            source,
            () => {
              opens++;
              const first = preparedState.owner;
              if (first !== undefined) {
                delete preparedState.owner;
                return Promise.resolve(first);
              }
              return target(h, 0, true, undefined, startup);
            },
            governor,
            { signal: controller.signal },
          );
          // Attach a rejection observer before generated cancellation can reject.
          void loading.catch(() => undefined);
          try {
            await Promise.race([
              waiting,
              loading.then(() => {
                throw new Error("load completed while its working step was blocked");
              }),
            ]);
            await new Promise<void>((r) => setImmediate(r));
            expect(governor.usedBytes).toBe(baseline + holder.held);
            expect(opens).toBe(0);
            reach.reach("unwind before wait");
            if (cancel) {
              controller.abort(new Error("cancel admission"));
              await expect(loading).rejects.toThrow("cancel admission");
              reach.reach("cancel queued");
            } else {
              holder.close();
              await loading;
              expect(readsAtZero).toBeGreaterThan(1);
              expect(startup.allocated).toBe(0);
              reach.reach("release and replay");
            }
          } finally {
            controller.abort();
            holder.close();
            await loading.catch(() => undefined);
            await preparedState.owner?.close();
            startup.close();
            governor.admitGroup = admit;
          }
          expect(governor.usedBytes).toBe(0);
          expect(await contents(h)).toEqual(
            cancel
              ? []
              : [
                  [value, `value${String(value)}`],
                  [null, null],
                ],
          );
          const small = new Governor(2 << 20);
          await expect(loadSnapshot(input(chunks), () => target(h), small)).rejects.toMatchObject({
            sqlState: "53200",
          });
          expect(small.usedBytes).toBe(0);
          reach.reach("only above cap refused");
        }
        // Owner-edge adaptation uses the identical core path, with private disk scratch.
        const chunks = await fixture([value]),
          disk = new SimDisk(),
          file = disk.open("snapshot-owner-scratch", true),
          scratch = new FileScratch(file, () => undefined),
          governor = new Governor(32 << 20),
          h = new Harness("strict", "memory");
        const spool = await spoolSnapshotInput(
          (async function* () {
            yield* chunks;
          })(),
          { scratch, offset: 4096, capacity: 8 << 20 },
          governor,
          { maxChunkBytes: 1 << 20 },
        );
        try {
          await loadSnapshot(spool, () => target(h), governor);
          expect(await contents(h)).toEqual([[value, `value${String(value)}`]]);
          expect(governor.usedBytes).toBe(1024); // owner source handle, until explicitly closed
          reach.reach("one shot via spool");
        } finally {
          await spool.close();
          expect(file.getSize()).toBe(4096);
          file.close();
          expect(governor.usedBytes).toBe(0);
        }
      }),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  }, 60000);

/** Construct a valid container around arbitrary value bytes: all frame/catalog/footer CRCs
 * are recomputed. These are semantic corruptions, never outer-integrity failures. */
async function structuredArchive(type: PhysicalType, value: Uint8Array): Promise<Uint8Array[]> {
  let catalog = Catalog.empty();
  const element = type.kind === "array" ? type.element : type;
  if (element.kind === "enum")
    catalog = catalog.change(
      { kind: "createType", type: { name: element.name, labels: element.labels } },
      1,
    ).catalog;
  catalog = catalog.change(
    {
      kind: "createTable",
      table: tableInput("structured", [column("v", type)], { primaryKey: null }),
    },
    1,
  ).catalog;
  const chunks: Uint8Array[] = [],
    funding = new Budget(),
    catalogBytes = catalog.encode();
  const writer = await SnapshotStreamWriter.open(
    { id: new Uint8Array(16), sourceLsn: 0 },
    {
      write(bytes) {
        chunks.push(bytes.slice());
      },
    },
    funding,
  );
  const numbers = new Uint8Array(48),
    data = new Uint8Array(26 + value.length);
  try {
    await writer.frame(SnapshotKind.catalogBegin, 1, 0, numbers.subarray(0, 0));
    await writer.frame(SnapshotKind.catalogPart, 1, 1, catalogBytes);
    snapshotNumbers([catalogBytes.length, 0], numbers.subarray(0, 16));
    new DataView(numbers.buffer).setUint32(8, snapshotCrc32(catalogBytes), true);
    await writer.frame(SnapshotKind.catalogEnd, 1, 2, numbers.subarray(0, 16));
    snapshotNumbers([1, 1, 1], numbers.subarray(0, 24));
    await writer.frame(SnapshotKind.tableBegin, 2, 0, numbers.subarray(0, 24));
    encodeSnapshotColumnHeader(type, 1, data.subarray(0, 16));
    data[16] = 1;
    new DataView(data.buffer).setUint32(17, value.length, true);
    data.set(value, 21);
    data[21 + value.length] = 2;
    snapshotNumbers([1, 1, 1, 0, 1, data.length], numbers);
    await writer.frame(SnapshotKind.columnBegin, 3, 0, numbers);
    let part = 1;
    for (let at = 0; at < data.length; at += 1 << 20)
      await writer.frame(SnapshotKind.columnPart, 3, part++, data.subarray(at, at + (1 << 20)));
    snapshotNumbers([data.length, 1], numbers.subarray(0, 16));
    await writer.frame(SnapshotKind.columnEnd, 3, part, numbers.subarray(0, 16));
    snapshotNumbers([1], numbers.subarray(0, 8));
    await writer.frame(SnapshotKind.tableEnd, 2, 1, numbers.subarray(0, 8));
    await writer.finish();
    return chunks;
  } finally {
    writer.close();
    expect(funding.used).toBe(0);
  }
}
for (const seed of seedsFor("next-snapshot-structured", [0xb271, 0xb272]))
  it(`refuses checksum-valid malformed structured payloads before publication seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot structured", SNAPSHOT_STRUCTURED_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 32 }), async (size) => {
        reach.beginHistory();
        const bytesType: PhysicalType = { kind: "text" },
          arrayType: PhysicalType = { kind: "array", element: bytesType };
        const original = encodeArrayValue(bytesType, {
          dimensions: [2],
          lowerBounds: [1],
          elements: [new Uint8Array(size), new Uint8Array([1, 2])],
        });
        const cases: Array<{
          name: (typeof SNAPSHOT_STRUCTURED_REACH)[number];
          type: PhysicalType;
          value: Uint8Array;
        }> = [];
        const mutate = (
          name: (typeof SNAPSHOT_STRUCTURED_REACH)[number],
          change: (bytes: Uint8Array) => void,
        ) => {
          const value = original.slice();
          change(value);
          cases.push({ name, type: arrayType, value });
        };
        mutate("array version", (bytes) => {
          bytes[0] = 2;
        });
        mutate("array element tag", (bytes) => {
          bytes[1] = 255;
        });
        mutate("array dimensions", (bytes) => {
          bytes[2] = 7;
        });
        mutate("array flags", (bytes) => {
          bytes[3] = 2;
        });
        mutate("array dimension length", (bytes) => {
          new DataView(bytes.buffer).setInt32(4, -1, true);
        });
        mutate("array lower bound", (bytes) => {
          new DataView(bytes.buffer).setInt32(8, 2147483647, true);
        });
        mutate("array element length", (bytes) => {
          new DataView(bytes.buffer).setUint32(12, 0xffffffff, true);
        });
        cases.push({
          name: "array truncated element",
          type: arrayType,
          value: original.slice(0, -1),
        });
        const nulls = encodeArrayValue(bytesType, {
          dimensions: [1],
          lowerBounds: [1],
          elements: [null],
        });
        nulls[12] = 128;
        cases.push({ name: "array null tail", type: arrayType, value: nulls });
        const bool = encodeArrayValue(
          { kind: "bool" },
          { dimensions: [1], lowerBounds: [1], elements: [true] },
        );
        bool[12] = 2;
        cases.push({
          name: "array boolean",
          type: { kind: "array", element: { kind: "bool" } },
          value: bool,
        });
        const decType: PhysicalType = { kind: "decimal", precision: null, scale: null };
        const decimal = encodeArrayValue(decType, {
          dimensions: [1],
          lowerBounds: [1],
          elements: [{ coefficient: 1n, scale: 2 }],
        });
        const special = decimal.slice();
        special[12] = 4;
        cases.push({
          name: "array decimal special",
          type: { kind: "array", element: decType },
          value: special,
        });
        const overflow = new Uint8Array(19 + 70000);
        overflow.set(decimal.subarray(0, 19));
        new DataView(overflow.buffer).setUint32(15, 70000, true);
        overflow[overflow.length - 1] = 64;
        cases.push({
          name: "array decimal overflow",
          type: { kind: "array", element: decType },
          value: overflow,
        });
        cases.push({
          name: "array unsupported element",
          type: { kind: "array", element: { kind: "json", binary: false } },
          value: new Uint8Array([1, 1, 0, 0]),
        });
        const scale = decimal.slice();
        new DataView(scale.buffer).setUint16(13, 0xffff, true);
        cases.push({
          name: "array decimal scale",
          type: { kind: "array", element: decType },
          value: scale,
        });
        cases.push({
          name: "json grammar",
          type: { kind: "json", binary: false },
          value: new TextEncoder().encode('{"a":[1,]}'),
        });
        cases.push({
          name: "json utf8",
          type: { kind: "json", binary: true },
          value: new Uint8Array([34, 255, 34]),
        });
        cases.push({
          name: "enum label",
          type: { kind: "enum", name: "mood", labels: ["ok", "sad"] },
          value: new TextEncoder().encode("missing"),
        });
        cases.push({ name: "text utf8", type: { kind: "text" }, value: new Uint8Array([255]) });
        cases.push({
          name: "text bom length",
          type: { kind: "text", maxLength: 1 },
          value: new TextEncoder().encode("\ufeffa"),
        });
        cases.push({
          name: "text length",
          type: { kind: "text", maxLength: 1 },
          value: new TextEncoder().encode("ab"),
        });
        const h = new Harness("strict", "memory"),
          lsn = await oldTarget(h);
        try {
          for (const example of cases) {
            const funding = new Governor(32 << 20);
            const archive = await structuredArchive(example.type, example.value);
            await expect(
              loadSnapshot(input(archive), () => target(h, lsn, false), funding, { replace: true }),
            ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_FORMAT" });
            expect(funding.usedBytes).toBe(0);
            expect(await contents(h)).toEqual([[777, "old"]]);
            reach.reach(example.name);
          }
        } finally {
          await h.close();
        }
      }),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  }, 60000);

for (const layout of ["bytes", "json", "decimal", "padded decimal"] as const)
  it(`streams large ${layout} values without materializing value graphs`, async () => {
    const element: PhysicalType = { kind: "text" };
    const decimalType: PhysicalType = { kind: "decimal", precision: null, scale: null };
    let example: { type: PhysicalType; value: Uint8Array };
    if (layout === "bytes")
      example = {
        type: { kind: "array", element },
        value: encodeArrayValue(element, {
          dimensions: [2],
          lowerBounds: [-3],
          elements: [new Uint8Array((2 << 20) + 17).fill(73), null],
        }),
      };
    else if (layout === "json")
      example = {
        type: { kind: "json", binary: false },
        value: new TextEncoder().encode(
          "[".repeat(1500) + '{"s":"' + "x".repeat((2 << 20) + 17) + '"}' + "]".repeat(1500),
        ),
      };
    else if (layout === "decimal")
      example = {
        type: { kind: "array", element: decimalType },
        value: encodeArrayValue(decimalType, {
          dimensions: [3],
          lowerBounds: [1],
          elements: [
            { coefficient: -(1n << 60000n), scale: 9 },
            { coefficient: 0n, scale: 0, special: "Infinity" },
            null,
          ],
        }),
      };
    else {
      const base = encodeArrayValue(decimalType, {
        dimensions: [1],
        lowerBounds: [1],
        elements: [{ coefficient: 1n, scale: 2 }],
      });
      const value = new Uint8Array(19 + 70000);
      value.set(base);
      new DataView(value.buffer).setUint32(15, 70000, true);
      example = { type: { kind: "array", element: decimalType }, value };
    }
    const h = new Harness("strict", "memory"),
      funding = new Governor(16 << 20),
      archive = await structuredArchive(example.type, example.value);
    let opened = false;
    try {
      await loadSnapshot(input(archive), () => target(h), funding);
      expect(funding.usedBytes).toBe(0);
      const path = await h.open();
      opened = true;
      const pin = path.pin(),
        exported: Uint8Array[] = [];
      try {
        const catalog = pin.catalog;
        if (!(catalog instanceof Catalog)) throw new Error("catalog");
        await exportPinnedSnapshot(
          pin,
          new Uint8Array(16),
          (async function* () {
            yield catalog.encode();
          })(),
          {
            write(bytes) {
              exported.push(bytes.slice());
            },
          },
          new Budget(),
        );
        const body = async (chunks: Uint8Array[]) => {
          const b = new Budget(),
            reader = await SnapshotStreamReader.open(
              (async function* () {
                yield* chunks;
              })(),
              b,
            ),
            parts: Uint8Array[] = [];
          try {
            for await (const frame of reader.frames())
              if (frame.kind === SnapshotKind.columnPart) parts.push(frame.payload.slice());
          } finally {
            reader.close();
            expect(b.used).toBe(0);
          }
          return parts;
        };
        expect(await body(exported)).toEqual(await body(archive));
      } finally {
        pin.release();
        await h.close();
      }
      expect(funding.peakBytes).toBeLessThanOrEqual(16 << 20);
    } finally {
      if (opened) await h.close();
    }
  }, 60000);

for (const seed of seedsFor("next-snapshot-load-root-credit", [0xb273, 0xb274]))
  it(`hands large native root ownership to the same governor seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot load root credit", SNAPSHOT_LOAD_ROOT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1024, max: 1028 }), async (count) => {
        reach.beginHistory();
        let catalog = Catalog.empty();
        for (let i = 0; i < count; i++)
          catalog = catalog.change(
            { kind: "createTable", table: tableInput(`t${String(i)}`, [], { primaryKey: null }) },
            1,
          ).catalog;
        const chunks: Uint8Array[] = [],
          b = new Budget(),
          numbers = new Uint8Array(24),
          bytes = catalog.encode();
        const writer = await SnapshotStreamWriter.open(
          { id: new Uint8Array(16), sourceLsn: 0 },
          {
            write(part) {
              chunks.push(part.slice());
            },
          },
          b,
        );
        try {
          await writer.frame(SnapshotKind.catalogBegin, 1, 0, numbers.subarray(0, 0));
          await writer.frame(SnapshotKind.catalogPart, 1, 1, bytes);
          snapshotNumbers([bytes.length, 0], numbers.subarray(0, 16));
          new DataView(numbers.buffer).setUint32(8, snapshotCrc32(bytes), true);
          await writer.frame(SnapshotKind.catalogEnd, 1, 2, numbers.subarray(0, 16));
          for (let id = 1; id <= count; id++) {
            snapshotNumbers([id, 1, 0], numbers);
            await writer.frame(SnapshotKind.tableBegin, id + 1, 0, numbers);
            snapshotNumbers([0], numbers.subarray(0, 8));
            await writer.frame(SnapshotKind.tableEnd, id + 1, 1, numbers.subarray(0, 8));
          }
          await writer.finish();
        } finally {
          writer.close();
          expect(b.used).toBe(0);
        }
        const h = new Harness("strict", "memory");
        let size = 0;
        await loadSnapshot(
          input(chunks),
          () =>
            target(h, 0, true, (space) => {
              const publish = space.publish.bind(space);
              space.publish = (root, change) => {
                size = root.length;
                return publish(root, change);
              };
            }),
          h.governor,
        );
        expect(size).toBeGreaterThan(18 * 1024);
        expect(h.governor.usedBytes).toBe(0);
        const path = await h.open();
        try {
          expect(path.catalog.tables).toHaveLength(count);
        } finally {
          await h.close();
        }
        expect(h.governor.usedBytes).toBe(0);
        reach.reach("oversized native root");
        reach.reach("same governor handoff");
        reach.reach("zero after reopen");
      }),
      { seed, numRuns: 2 },
    );
    reach.assertReached();
  }, 60000);
