/** Atomic load law: old/absent or complete native root, never a portable prefix. */
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
} from "../../testing/reach.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import { listedRootExtents, reachableExtents } from "../format/reachable.js";
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
import { decodeJson, encodeJson } from "./bytes.js";
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
class Budget implements ByteBudget {
  used = 0;
  peak = 0;
  constructor(readonly cap = 32 << 20) {}
  allocate(n: number): boolean {
    if (this.used + n > this.cap) return false;
    this.used += n;
    this.peak = Math.max(this.peak, this.used);
    return true;
  }
  free(n: number): void {
    this.used -= n;
    if (this.used < 0) throw new Error("double free");
  }
}
async function* input(chunks: readonly Uint8Array[]): AsyncGenerator<Uint8Array> {
  yield* chunks;
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
): Promise<SnapshotLoadTarget> {
  const lease = await h.backend.acquire(),
    session = await h.backend.open(lease),
    startup = await h.governor.admit({
      label: "load startup",
      minimumBytes: 8 << 20,
      spillable: true,
    });
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
          startup.close();
          try {
            await session.close();
          } finally {
            await lease.release();
          }
        }
      },
    };
  } catch (error) {
    startup.close();
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
  await loadSnapshot(
    (async function* () {
      yield* chunks;
      eof = true;
    })(),
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
          const stream =
            mode === "cancel"
              ? (async function* () {
                  for (let i = 0; i < chunks.length; i++) {
                    if (i === chunks.length - 1) {
                      entered();
                      await gate;
                    }
                    const chunk = chunks[i];
                    if (chunk !== undefined) yield chunk;
                  }
                })()
              : input(chunks);
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

it("recovers old or complete data after every physical publication step", async () => {
  const chunks = await fixture([10, null, 30]);
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
              reader = await SnapshotStreamReader.open(input(chunks), funding),
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
  it(`keeps competing ordinary openers behind the publication fence seed ${String(seed)}`, async () => {
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
        const loading = loadSnapshot(
          (async function* () {
            for (let i = 0; i < chunks.length; i++) {
              if (i === chunks.length - 1) {
                enter();
                await gate;
              }
              const chunk = chunks[i];
              if (chunk !== undefined) yield chunk;
            }
          })(),
          () => target(h),
          b,
          { signal: controller.signal },
        );
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
          expect(await opening).toEqual(
            cancel
              ? []
              : [
                  [12, "value12"],
                  [null, null],
                ],
          );
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

async function corruptCatalog(
  chunks: readonly Uint8Array[],
  change: (value: Record<string, unknown>) => void,
): Promise<Uint8Array[]> {
  const b = new Budget(),
    reader = await SnapshotStreamReader.open(input(chunks), b),
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
        const data = decodeJson(bytes) as Record<string, unknown>;
        change(data);
        bytes = encodeJson(data);
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
          const damaged = await corruptCatalog(chunks, (data) => {
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
                table.id = Number.MAX_SAFE_INTEGER + 1;
                break;
              case "unresolved reference":
                first.identitySequence = salt + 100;
                break;
              case "unsupported field":
                table.triggers = [{ id: salt }];
                break;
            }
          });
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
            code:
              mutation === "unsupported field" ? "MINNOW_SNAPSHOT_SCOPE" : "MINNOW_SNAPSHOT_FORMAT",
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
