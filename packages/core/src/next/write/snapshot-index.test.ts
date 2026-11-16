/** Portable load rebuilds live native indexes, checks UNIQUE/FK before its single publish. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  SNAPSHOT_INDEX_REACH,
  SNAPSHOT_INDEX_FAILURE_REACH,
  SNAPSHOT_INDEX_SCOPE_REACH,
  SNAPSHOT_LONG_FK_REACH,
  SNAPSHOT_INDEX_LEASE_REACH,
} from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { snapshotBlob } from "./snapshot-source.js";
import * as native from "./snapshot-native.js";
import { ReadDeniedError } from "../storage/extents.js";
import * as merging from "./merge.js";
import { snapshotTestTarget } from "./snapshot-test-support.js";
import { loadSnapshot } from "./snapshot-load.js";
import { decodeCatalogStream, type CatalogStreamData } from "./catalog-stream.js";
import { snapshotCatalogParts } from "./snapshot-catalog.js";
import { exportPinnedSnapshot } from "./snapshot-export.js";
import { parseCharge } from "../format/manifest-codec.js";
import { exportSnapshot } from "./snapshot-capture.js";
import {
  SnapshotKind,
  SnapshotStreamReader,
  SnapshotStreamWriter,
  snapshotU53,
} from "../format/snapshot.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import type { WritePathImpl } from "./engine.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  tableInput,
  texts,
  rowsOf,
} from "./test-support.js";

const blob = (parts: readonly Uint8Array[]) =>
  snapshotBlob(new Blob(parts.map((part) => part.slice().buffer)));
async function archive(
  values: readonly number[],
  wide: boolean,
  transform?: (data: CatalogStreamData) => CatalogStreamData,
): Promise<Uint8Array[]> {
  const h = new Harness("strict", "memory"),
    path = await h.open(),
    out: Uint8Array[] = [];
  try {
    await autocommit(path, (st) =>
      st.changeCatalog({
        kind: "createTable",
        table: tableInput(
          "parent",
          [column("id", INT, false), column("u", INT), column("label", { kind: "text" })],
          { unique: [{ name: "unique_u", columns: [1] }] },
        ),
      }),
    );
    await autocommit(path, (st) =>
      st.changeCatalog({
        kind: "createIndex",
        index: {
          name: "labels",
          tableId: 1,
          columnIds: [3],
          unique: false,
          role: "secondary",
          directions: ["desc"],
        },
      }),
    );
    const parent = path.catalog.table(1)?.primaryKey;
    if (parent === null || parent === undefined) throw new Error("primary fixture");
    await autocommit(path, (st) =>
      st.changeCatalog({
        kind: "createTable",
        table: tableInput("child", [column("pid", INT)], {
          primaryKey: null,
          foreignKeys: [
            {
              name: "child_parent",
              columns: [0],
              parentTableId: 1,
              parentIndexId: parent,
              onDelete: "cascade",
              onUpdate: "follow",
              enforced: true,
            },
          ],
        }),
      }),
    );
    await autocommit(path, (st) =>
      st
        .table(1)
        .insert(
          batch(
            ints(values),
            ints(values.map((n) => -n)),
            texts(values.map((_, i) => `${wide ? "x".repeat(3000) : "k"}${String(i % 3)}`)),
          ),
          null,
        ),
    );
    await autocommit(path, (st) =>
      st.table(2).insert(batch(ints([values[0] ?? 1, null, values.at(-1) ?? 1])), null),
    );
    await path.recycleLog();
    const grant = await h.governor.admit({
      label: "portable index fixture export",
      minimumBytes: 8 << 20,
      spillable: false,
    });
    try {
      const rootBytes = h.space.root;
      if (rootBytes === null) throw new Error("published fixture expected");
      const root = decodeManifestRoot(rootBytes, grant);
      try {
        if (wide)
          expect(
            root.value.tables[0]?.secondaryIndexes.some((index) =>
              index.runs.some((run) => run.encoding === 2),
            ),
          ).toBe(true);
      } finally {
        grant.free(root.charge);
      }
      if (transform) {
        const encoded = path.catalog.encode(),
          charge = parseCharge(encoded.length) * 4;
        expect(grant.allocate(charge)).toBe(true);
        try {
          const data = transform(decodeCatalogStream(encoded)),
            pin = path.pin();
          await exportPinnedSnapshot(
            pin,
            new Uint8Array(16),
            (async function* () {
              yield* snapshotCatalogParts(data, [], grant);
            })(),
            {
              write(bytes) {
                out.push(bytes.slice());
              },
            },
            grant,
          );
        } finally {
          grant.free(charge);
        }
      } else {
        await exportSnapshot(
          path,
          new Uint8Array(16),
          {
            write(bytes) {
              out.push(bytes.slice());
            },
          },
          grant,
        );
      }
      expect(grant.allocated).toBe(0);
    } finally {
      grant.close();
    }
  } finally {
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
  return out;
}
async function edit(
  parts: readonly Uint8Array[],
  tableId: number,
  columnId: number,
  value: number,
): Promise<Uint8Array[]> {
  const governor = new Governor(8 << 20),
    grant = await governor.admit({
      label: "fixture reframe",
      minimumBytes: 4 << 20,
      spillable: false,
    }),
    out: Uint8Array[] = [];
  const reader = await SnapshotStreamReader.open(
      (async function* () {
        yield* parts;
      })(),
      grant,
    ),
    writer = await SnapshotStreamWriter.open(
      reader.header,
      {
        write(bytes) {
          out.push(bytes.slice());
        },
      },
      grant,
    );
  let selected = false,
    first = 0,
    changed = false;
  try {
    for await (const frame of reader.frames()) {
      if (frame.kind === SnapshotKind.footer) continue;
      if (frame.kind === SnapshotKind.columnBegin) {
        selected =
          snapshotU53(frame.payload, 0) === tableId && snapshotU53(frame.payload, 8) === columnId;
        first = snapshotU53(frame.payload, 24);
      }
      let payload = frame.payload;
      if (
        selected &&
        !changed &&
        frame.kind === SnapshotKind.columnPart &&
        (tableId === 2 || first >= 256)
      ) {
        payload = payload.slice();
        expect(payload[16]).toBe(1);
        new DataView(payload.buffer, payload.byteOffset, payload.byteLength).setInt32(
          17,
          value,
          true,
        );
        changed = true;
      }
      await writer.frame(frame.kind, frame.objectId, frame.partIndex, payload);
    }
    expect(changed).toBe(true);
    await writer.finish();
  } finally {
    reader.close();
    writer.close();
    expect(grant.allocated).toBe(0);
    grant.close();
  }
  expect(governor.usedBytes).toBe(0);
  return out;
}
async function imported(
  parts: readonly Uint8Array[],
): Promise<{ h: Harness; path: WritePathImpl; peak: number }> {
  const governor = new Governor(64 << 20),
    h = new Harness("strict", "memory");
  Object.assign(h, { governor });
  const startup = await governor.admit({
    label: "prepaid load owner",
    minimumBytes: 8 << 20,
    spillable: true,
  });
  try {
    await loadSnapshot(
      blob(parts),
      () => snapshotTestTarget(h, 0, true, undefined, startup),
      governor,
    );
  } finally {
    startup.close();
  }
  const peak = governor.peakBytes;
  expect(governor.usedBytes).toBe(0);
  return { h, path: await h.open(), peak };
}
for (const seed of seedsFor("next-snapshot-native-indexes", [0xb28b, 0xb28c]))
  it(`rebuilds indexes and refuses checksum-valid constraint violations seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot native indexes", SNAPSHOT_INDEX_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.uniqueArray(fc.integer({ min: 1, max: 1000000 }), { minLength: 260, maxLength: 264 }),
        async (values) => {
          reach.beginHistory();
          for (const wide of [false, true]) {
            const original = await archive(values, wide),
              { h, path, peak } = await imported(original);
            try {
              const pin = path.pin();
              try {
                expect(await rowsOf(pin, 1)).toEqual(
                  values.map((n, i) => [n, -n, `${wide ? "x".repeat(3000) : "k"}${String(i % 3)}`]),
                );
                expect(await rowsOf(pin, 2)).toEqual([[values[0]], [null], [values.at(-1)]]);
                const table = pin.table(1, {
                  budget: { allocate: () => true, free: () => undefined },
                });
                expect(table.segments.length).toBeGreaterThan(1);
                const keyBudget = await h.governor.admit({
                  label: "native index probe",
                  minimumBytes: 1 << 20,
                  spillable: false,
                });
                const keys = new KeyBatch(keyBudget);
                try {
                  const wanted = [values[0] ?? 1, values[128] ?? 1, values.at(-1) ?? 1],
                    encoded = encodeKeyBatch(
                      "equality",
                      [{ vector: ints(wanted), direction: "asc", nulls: "first" }],
                      { length: wanted.length, selection: null },
                      keys,
                    );
                  expect(encoded.done).toBe(true);
                  const out = {
                    probes: new Int32Array(8),
                    locators: new Float64Array(8),
                    count: 0,
                  };
                  const id = path.catalog.table(1)?.primaryKey;
                  if (id === null || id === undefined) throw new Error("loaded primary");
                  expect((await pin.index(1, id).probe(keys, out)).done).toBe(true);
                  expect(out.count).toBe(wanted.length);
                  for (const [indexId, vector] of [
                    [2, ints(wanted.map((n) => -n))],
                    [3, texts([`${wide ? "x".repeat(3000) : "k"}0`])],
                  ] as const) {
                    encodeKeyBatch(
                      "equality",
                      [{ vector, direction: "asc", nulls: "first" }],
                      { length: vector.length, selection: null },
                      keys,
                    );
                    const hits = {
                      probes: new Int32Array(values.length),
                      locators: new Float64Array(values.length),
                      count: 0,
                    };
                    expect((await pin.index(1, indexId).probe(keys, hits)).done).toBe(true);
                    expect(hits.count).toBe(
                      indexId === 2 ? wanted.length : Math.ceil(values.length / 3),
                    );
                  }

                  for (let i = 0; i < out.count; i++) {
                    const locator = out.locators[i] ?? 0,
                      segment = Math.floor(locator / 65536),
                      position = locator % 65536;
                    const ordinal =
                      table.segments.slice(0, segment).reduce((n, s) => n + s.rowCount, 0) +
                      position;
                    expect(values[ordinal]).toBe(wanted[out.probes[i] ?? 0]);
                  }
                } finally {
                  keys.close();
                  keyBudget.close();
                }
              } finally {
                pin.release();
              }
              expect(peak).toBeLessThanOrEqual(64 << 20);
              await autocommit(path, (st) =>
                st
                  .table(1)
                  .insert(batch(ints([2000001]), ints([-2000001]), texts(["after"])), null),
              );
              await h.close();
              const reopened = await h.open(),
                reopenedPin = reopened.pin();
              try {
                expect((await rowsOf(reopenedPin, 1)).at(-1)).toEqual([2000001, -2000001, "after"]);
              } finally {
                reopenedPin.release();
              }
              reach.reach(wide ? "overflow keys" : "inline keys");
              reach.reach("multi block merge");
              reach.reach("native probes");
              reach.reach("continue reopen");
              reach.reach("null FK");
              reach.reach("FK modes");
            } finally {
              await h.close();
            }
            expect(h.governor.usedBytes).toBe(0);
            for (const violation of ["unique refusal", "FK refusal"] as const) {
              const invalid =
                violation === "unique refusal"
                  ? await edit(original, 1, 2, -(values[0] ?? 1))
                  : await edit(original, 2, 4, 2000001);
              const governor = new Governor(64 << 20),
                target = new Harness("strict", "memory");
              Object.assign(target, { governor });
              const startup = await governor.admit({
                label: "constraint preflight owner",
                minimumBytes: 8 << 20,
                spillable: true,
              });
              try {
                await expect(
                  loadSnapshot(
                    blob(invalid),
                    () => snapshotTestTarget(target, 0, true, undefined, startup),
                    governor,
                  ),
                ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_FORMAT" });
              } finally {
                startup.close();
              }
              expect(governor.usedBytes).toBe(0);
              const opened = await target.open();
              try {
                expect(opened.catalog.tables).toHaveLength(0);
              } finally {
                await target.close();
              }
              expect(governor.usedBytes).toBe(0);
              reach.reach(violation);
            }
          }
        },
      ),
      { seed, numRuns: 2, endOnFailure: true },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-snapshot-index-failure", [0xb28f, 0xb290]))
  it(`keeps an old target intact after partial index output failure seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot index failure", SNAPSHOT_INDEX_FAILURE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 50 }), async (salt) => {
        reach.beginHistory();
        const original = await archive(
          Array.from({ length: 260 }, (_, i) => salt + i),
          false,
        );
        for (const mode of ["quota", "cancel"] as const) {
          const governor = new Governor(64 << 20),
            h = new Harness("strict", "memory");
          Object.assign(h, { governor });
          const old = await h.open();
          await autocommit(old, (st) =>
            st.changeCatalog({
              kind: "createTable",
              table: tableInput("old", [column("n", INT)], { primaryKey: null }),
            }),
          );
          await autocommit(old, (st) => st.table(1).insert(batch(ints([777])), null));
          await old.recycleLog();
          const oldRoot = h.space.root?.slice(),
            lsn = old.appliedLsn;
          await h.close();
          expect(governor.usedBytes).toBe(0);
          const startup = await governor.admit({
              label: "prepaid replacement owner",
              minimumBytes: 8 << 20,
              spillable: true,
            }),
            controller = new AbortController();
          const real = merging.mergeSortedRuns;
          let appended = 0;
          const spy = vi.spyOn(merging, "mergeSortedRuns").mockImplementation((input) =>
            real({
              ...input,
              writer: {
                get label() {
                  return input.writer.label;
                },
                get state() {
                  return input.writer.state;
                },
                get bytes() {
                  return input.writer.bytes;
                },
                get extents() {
                  return input.writer.extents;
                },
                write: (parts) => input.writer.write(parts),
                discard: (extent) => input.writer.discard(extent),
                abort: () => input.writer.abort(),
                open(capacity) {
                  const target = input.writer.open(capacity);
                  return {
                    get capacity() {
                      return target.capacity;
                    },
                    get length() {
                      return target.length;
                    },
                    extend: (bytes) => target.extend(bytes),
                    seal: () => target.seal(),
                    async append(parts) {
                      const result = await target.append(parts);
                      appended++;
                      if (mode === "quota")
                        throw new DOMException("index quota", "QuotaExceededError");
                      controller.abort(new Error("cancel index output"));
                      return result;
                    },
                  };
                },
              },
            }),
          );
          try {
            const loading = loadSnapshot(
              blob(original),
              () => snapshotTestTarget(h, lsn, false, undefined, startup),
              governor,
              { replace: true, signal: controller.signal },
            );
            if (mode === "quota")
              await expect(loading).rejects.toMatchObject({ name: "QuotaExceededError" });
            else await expect(loading).rejects.toThrow("cancel index output");
            expect(appended).toBeGreaterThan(0);
            reach.reach("partial native output");
          } finally {
            spy.mockRestore();
            startup.close();
          }
          expect(governor.usedBytes).toBe(0);
          const lease = await h.backend.acquire(),
            session = await h.backend.open(lease);
          try {
            expect(await session.readRoot()).toEqual(oldRoot);
          } finally {
            await session.close();
            await lease.release();
          }
          const reopened = await h.open(),
            pin = reopened.pin();
          try {
            expect(pin.catalog.tableByName("old")?.id).toBe(1);
            expect(await rowsOf(pin, 1)).toEqual([[777]]);
          } finally {
            pin.release();
            await h.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach(mode);
          reach.reach("old root and rows");
        }
      }),
      { seed, numRuns: 2, endOnFailure: true },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-snapshot-index-scope", [0xb291, 0xb292]))
  it(`refuses impossible index declarations before acquiring the target seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot index scope", SNAPSHOT_INDEX_SCOPE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 20 }), async (salt) => {
        reach.beginHistory();
        for (const mode of SNAPSHOT_INDEX_SCOPE_REACH) {
          const invalid = await archive([salt, salt + 1], false, (data) => ({
            ...data,
            tables: data.tables.map((table) =>
              table.id !== 1
                ? table
                : {
                    ...table,
                    indexes: table.indexes.map((index) =>
                      index.id !== 2
                        ? index
                        : mode === "invalid unique"
                          ? { ...index, invalid: true }
                          : mode === "orphan primary"
                            ? { ...index, role: "primary" }
                            : { ...index, columnIds: [1, 2], keyLimit: "scalar" },
                    ),
                  },
            ),
          }));
          const governor = new Governor(16 << 20);
          let opens = 0;
          await expect(
            loadSnapshot(
              blob(invalid),
              () => {
                opens++;
                return Promise.reject(new Error("target opened"));
              },
              governor,
            ),
          ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_FORMAT" });
          expect(opens).toBe(0);
          expect(governor.usedBytes).toBe(0);
          reach.reach(mode);
        }
      }),
      { seed, numRuns: 2, endOnFailure: true },
    );
    reach.assertReached();
  }, 60000);

for (const seed of seedsFor("next-snapshot-long-fk", [0xb293, 0xb294]))
  it(`checks long FK keys with parent policy while retaining invalid support metadata seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot long FK", SNAPSHOT_LONG_FK_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 14000, max: 14010 }), async (width) => {
        reach.beginHistory();
        const source = new Harness("strict", "memory"),
          path = await source.open(),
          parts: Uint8Array[] = [],
          keys = ["x".repeat(width) + "a", "x".repeat(width) + "b"];
        let catalog: Uint8Array;
        try {
          await autocommit(path, (st) =>
            st.changeCatalog({
              kind: "createTable",
              table: tableInput("p", [column("id", { kind: "text" }, false)]),
            }),
          );
          const parent = path.catalog.table(1)?.primaryKey;
          if (parent === null || parent === undefined) throw new Error("parent");
          await autocommit(path, (st) =>
            st.changeCatalog({
              kind: "createTable",
              table: tableInput("c", [column("pid", { kind: "text" })], {
                primaryKey: null,
                foreignKeys: [
                  {
                    name: "long_fk",
                    columns: [0],
                    parentTableId: 1,
                    parentIndexId: parent,
                    onDelete: "setNull",
                    onUpdate: "setDefault",
                    enforced: true,
                  },
                ],
              }),
            }),
          );
          await autocommit(path, (st) => st.table(1).insert(batch(texts(keys)), null));
          await autocommit(path, (st) => st.table(2).insert(batch(texts([...keys, null])), null));
          expect(path.catalog.table(2)?.indexes[0]?.invalid).toBe(true);
          await path.recycleLog();
          catalog = path.catalog.encode();
          const grant = await source.governor.admit({
            label: "long FK export",
            minimumBytes: 8 << 20,
            spillable: false,
          });
          try {
            await exportSnapshot(
              path,
              new Uint8Array(16),
              {
                write(bytes) {
                  parts.push(bytes.slice());
                },
              },
              grant,
            );
            expect(grant.allocated).toBe(0);
          } finally {
            grant.close();
          }
        } finally {
          await source.close();
        }
        const { h, path: loaded } = await imported(parts);
        try {
          expect(loaded.catalog.encode()).toEqual(catalog);
          expect(loaded.catalog.table(2)?.indexes[0]?.invalid).toBe(true);
          const pin = loaded.pin();
          try {
            expect(await rowsOf(pin, 2)).toEqual([...keys, null].map((value) => [value]));
          } finally {
            pin.release();
          }
          await autocommit(loaded, (st) => st.table(2).insert(batch(texts([keys[0] ?? ""])), null));
          await expect(
            autocommit(loaded, (st) => st.table(2).insert(batch(texts(["missing"])), null)),
          ).rejects.toMatchObject({ name: "ForeignKeyViolationError" });
          reach.reach("long parent scalar");
          reach.reach("invalid support retained");
          reach.reach("FK actions retained");
          reach.reach("continued FK enforcement");
        } finally {
          await h.close();
        }
        expect(h.governor.usedBytes).toBe(0);
        expect(source.governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 2, endOnFailure: true },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-snapshot-index-leases", [0xb295, 0xb296]))
  it(`drops charged staging leases before retrying a denied scalar read seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot index leases", SNAPSHOT_INDEX_LEASE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 100 }), async (salt) => {
        reach.beginHistory();
        const source = new Harness("strict", "memory"),
          path = await source.open(),
          parts: Uint8Array[] = [];
        const expected = Array.from({ length: 20 }, (_, row) =>
          Array.from({ length: 8 }, (_, c) => salt + row * 16 + c),
        );
        try {
          await autocommit(path, (st) =>
            st.changeCatalog({
              kind: "createTable",
              table: tableInput(
                "wide_key",
                Array.from({ length: 8 }, (_, c) => column(`c${String(c)}`, INT, false)),
                { primaryKey: Array.from({ length: 8 }, (_, c) => c) },
              ),
            }),
          );
          await autocommit(path, (st) =>
            st
              .table(1)
              .insert(
                batch(
                  ...Array.from({ length: 8 }, (_, c) => ints(expected.map((row) => row[c] ?? 0))),
                ),
                null,
              ),
          );
          await path.recycleLog();
          const grant = await source.governor.admit({
            label: "composite fixture export",
            minimumBytes: 8 << 20,
            spillable: false,
          });
          try {
            await exportSnapshot(
              path,
              new Uint8Array(16),
              {
                write(bytes) {
                  parts.push(bytes.slice());
                },
              },
              grant,
            );
            expect(grant.allocated).toBe(0);
          } finally {
            grant.close();
          }
        } finally {
          await source.close();
        }
        const governor = new Governor(64 << 20),
          h = new Harness("strict", "memory");
        Object.assign(h, { governor });
        const startup = await governor.admit({
          label: "lease retry owner",
          minimumBytes: 8 << 20,
          spillable: true,
        });
        let filling = 0,
          active = 0,
          peak = 0,
          pressured = false,
          retried = false,
          releasePressure: (() => void) | undefined;
        const fill = native.fillSnapshotFlat,
          spy = vi.spyOn(native, "fillSnapshotFlat").mockImplementation(async (...args) => {
            filling++;
            try {
              await fill(...args);
            } finally {
              filling--;
            }
          });
        try {
          await loadSnapshot(
            blob(parts),
            () =>
              snapshotTestTarget(
                h,
                0,
                true,
                (space) => {
                  const read = space.read.bind(space);
                  space.read = async (refs, options) => {
                    const inFill = filling > 0;
                    let forced = false;
                    if (inFill && !pressured && active === 3) {
                      const credit = options.budget.ordinarySource?.();
                      if (
                        credit === undefined ||
                        !("held" in credit) ||
                        !("allocated" in credit) ||
                        typeof credit.held !== "number" ||
                        typeof credit.allocated !== "number"
                      )
                        throw new Error("expected real load grant");
                      const amount = credit.held - credit.allocated;
                      expect(credit.allocate(amount)).toBe(true);
                      releasePressure = () => credit.free(amount);
                      pressured = true;
                      forced = true;
                    }
                    let lease;
                    try {
                      lease = await read(refs, options);
                    } catch (error) {
                      if (!forced || !(error instanceof ReadDeniedError)) {
                        releasePressure?.();
                        releasePressure = undefined;
                      }
                      throw error;
                    }
                    if (inFill && releasePressure !== undefined) {
                      retried = true;
                      releasePressure();
                      releasePressure = undefined;
                    }
                    active++;
                    if (inFill) peak = Math.max(peak, active);
                    let live = true;
                    return {
                      bytes: lease.bytes,
                      release() {
                        if (!live) return;
                        live = false;
                        active--;
                        lease.release();
                      },
                    };
                  };
                },
                startup,
              ),
            governor,
          );
          expect(pressured).toBe(true);
          expect(retried).toBe(true);
          expect(peak).toBeLessThanOrEqual(4);
          expect(peak).toBe(4);
          expect(active).toBe(0);
          expect(releasePressure).toBeUndefined();
          reach.reach("real grant pressure");
          reach.reach("drop and retry");
          reach.reach("four lease bound");
          reach.reach("zero after close");
        } finally {
          spy.mockRestore();
          startup.close();
        }
        expect(governor.usedBytes).toBe(0);
        const loaded = await h.open(),
          pin = loaded.pin();
        try {
          expect(await rowsOf(pin, 1)).toEqual(expected);
        } finally {
          pin.release();
          await h.close();
        }
        expect(governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 2, endOnFailure: true },
    );
    reach.assertReached();
  }, 60000);
