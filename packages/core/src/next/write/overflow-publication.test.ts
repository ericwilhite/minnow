/** New unpublished encoding: accumulated ownership and each native root publication edge. */
import fc from "fast-check";
import { historyBody } from "../../testing/history-body.test-support.js";
import { afterAll, expect, it, vi } from "vitest";
import { createReach, OVERFLOW_PUBLICATION_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import { SimBackend, SimDisk, SimLocks } from "../storage/sim-disk.js";
import { directoryPeer } from "./directory-current.test-support.js";
import { need, UNCHARGED } from "./bytes.js";
import * as runs from "./runs.js";
import { mergeRuns, mergeWorkingBytes, DEFAULT_MERGE_POLICY } from "./merge.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  TEXT,
  INT,
  texts,
  ints,
  tableInput,
  rowsOf,
} from "./test-support.js";
const reach = createReach("overflow publication", OVERFLOW_PUBLICATION_STATES);
afterAll(() => reach.assertReached());
function key(width: number, n: number) {
  const bytes = new Uint8Array(width).fill(65);
  new DataView(bytes.buffer).setUint32(width - 4, n, false);
  return bytes;
}
function factory() {
  const Real = runs.RunBuilder;
  return vi.spyOn(runs, "RunBuilder").mockImplementation(function (
    ...args: ConstructorParameters<typeof Real>
  ) {
    return new Real(args[0], args[1], args[2], args[3] ?? 2);
  });
}
async function create(h: Harness) {
  let table = 0;
  await autocommit(h.path, async (s) => {
    table = need(
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("overflow", [column("k", TEXT, false), column("n", INT, false)]),
        })
      ).id,
    );
  });
  return table;
}
const value = (units: number, n: number) =>
  "\u0800".repeat(units - 1) + String.fromCharCode(0x0800 + n);
async function insert(h: Harness, table: number, units: number, from: number, count: number) {
  await autocommit(h.path, (s) =>
    s
      .table(table)
      .insert(
        batch(
          texts(Array.from({ length: count }, (_, i) => value(units, from + i))),
          ints(Array.from({ length: count }, (_, i) => from + i)),
        ),
        null,
      ),
  );
}
async function numbers(h: Pick<Harness, "path">, table: number) {
  const view = h.path.pin();
  try {
    return (await rowsOf(view, table)).map((r) => Number(r[1])).sort((a, b) => a - b);
  } finally {
    view.release();
  }
}
const pieceHistories = 16;
for (const seed of seedsFor("next-overflow-many-pieces", [0xb50f]))
  for (let ordinal = 0; ordinal < pieceHistories; ordinal++)
    it(`funds all retained piece summaries independently of key width (seed ${String(seed)}, history ${String(ordinal + 1)})`, async () => {
      const enter = historyBody(ordinal);
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom(1, 2, 8, 32),
          fc.constantFrom(2049, 49155),
          fc.constantFrom(32, 64),
          async (fan, width, cap) =>
            enter(async () => {
              reach.beginHistory();
              const h = new Harness("strict", "memory");
              Object.assign(h, { governor: new Governor(cap << 20) });
              await h.open();
              await h.path.close();
              const writer = h.space.beginWrite("many-piece inputs"),
                target = h.space.beginWrite("many-piece output");
              const metadata = await h.governor.admit({
                label: "input metadata",
                minimumBytes: 4 << 20,
                spillable: false,
              });
              const published = await h.governor.admit({
                label: "retained publication",
                minimumBytes: 2 << 20,
                spillable: true,
              });
              const readers: runs.RunReader[] = [];
              let grant: Awaited<ReturnType<Governor["admit"]>> | undefined,
                output: Awaited<ReturnType<typeof mergeRuns>> | undefined;
              const count = 1056;
              try {
                for (let f = 0; f < fan; f++) {
                  const builder = new runs.RunBuilder(false, false, UNCHARGED, 2);
                  try {
                    for (let i = f; i < count; i += fan) builder.add(key(width, i), i, false);
                    const summary = need(builder.finish());
                    try {
                      const ref = runs.runRef(summary, (await writer.write(summary.parts)).blobs);
                      readers.push(await runs.RunReader.open(h.space, ref, metadata, metadata));
                    } finally {
                      summary.release();
                    }
                  } finally {
                    builder.close();
                  }
                }
                grant = await h.governor.admit({
                  label: "many-piece working",
                  minimumBytes: mergeWorkingBytes(readers.map((r) => r.ref)),
                  spillable: false,
                });
                output = await mergeRuns({
                  space: h.space,
                  writer: target,
                  grant,
                  outputBudget: published,
                  readers,
                  level: 1,
                  bottom: true,
                  policy: { ...DEFAULT_MERGE_POLICY, splitEntries: 1, sliceEntries: 127 },
                  stopped: () => false,
                });
                expect(output.runs.length).toBeGreaterThanOrEqual(500);
                expect(grant.allocated).toBe(0);
                expect(published.allocated).toBeGreaterThan(output.runs.length * 64);
                expect(published.allocated).toBeLessThan(output.runs.length * 2048);
                let n = 0;
                for (const ref of output.runs) {
                  const reader = await runs.RunReader.open(h.space, ref, metadata, metadata);
                  try {
                    await reader.cursor(h.space, metadata, (bytes, rowId, tombstone) => {
                      expect(Buffer.compare(bytes, key(width, n))).toBe(0);
                      expect(rowId).toBe(n++);
                      expect(tombstone).toBe(false);
                      return false;
                    });
                  } finally {
                    metadata.free(reader.bytes);
                  }
                }
                expect(n).toBe(count);
                reach.reach("hundreds of retained summaries");
                reach.reach(width === 49155 ? "maximum text pieces" : "other piece widths");
                reach.reach(fan === 32 ? "thirty-two inputs" : "other input counts");
                reach.reach(cap === 32 ? "thirty-two MiB cap" : "sixty-four MiB cap");
              } finally {
                output?.release();
                expect(published.allocated).toBe(0);
                published.close();
                for (const reader of readers) metadata.free(reader.bytes);
                expect(metadata.allocated).toBe(0);
                metadata.close();
                grant?.close();
                await target.abort();
                await writer.abort();
                await h.close();
              }
              expect(h.governor.usedBytes).toBe(0);
            }),
        ),
        { seed, numRuns: pieceHistories },
      );
    }, 180_000);

for (const seed of seedsFor("next-overflow-publication-cuts", [0xb510]))
  it(`recovers whole acknowledged transactions at every publication edge (seed ${String(seed)})`, async () => {
    const spy = factory();
    try {
      await fc.assert(
        fc.asyncProperty(fc.constantFrom(769, 16384), fc.integer(), async (units, diskSeed) => {
          reach.beginHistory();
          for (let cut = 0; cut < 4; cut++) {
            const fault = { seed: diskSeed, crashAtStep: Infinity },
              disk = new SimDisk(fault);
            const h = new Harness("strict", "sim", 4, disk, { merges: { mode: "off" } });
            const opening = h.backend.open.bind(h.backend);
            let armed = false,
              hit = false;
            const sessionSpy = vi.spyOn(h.backend, "open").mockImplementation(async (lease) => {
              const session = await opening(lease),
                publish = session.publish.bind(session);
              vi.spyOn(session, "publish").mockImplementation(async (bytes) => {
                if (armed && !hit) {
                  const root = decodeManifestRoot(bytes, UNCHARGED).value;
                  if (root.tables.some((t) => t.keyIndex?.runs.some((r) => r.encoding === 2))) {
                    hit = true;
                    fault.crashAtStep = disk.steps + cut;
                  }
                }
                return await publish(bytes);
              });
              return session;
            });
            await h.open();
            const table = await create(h);
            await insert(h, table, units, 0, 8);
            armed = true;
            let acknowledged = false;
            try {
              await insert(h, table, units, 8, 2);
              acknowledged = true;
              await h.path.quiesce();
            } catch {
              /* The killed worker has no outcome. Recovery is the oracle. */
            }
            expect(hit, "cut must reach an actual overflow root").toBe(true);
            await h.close().catch(() => undefined);
            sessionSpy.mockRestore();
            (h.backend as SimBackend).restart();
            const recovered = new Harness("strict", "sim", 4, disk, { merges: { mode: "off" } });
            try {
              await recovered.open();
              const actual = await numbers(recovered, table);
              expect(actual.length).toBeGreaterThanOrEqual(8);
              expect(actual.length === 8 || actual.length === 10).toBe(true);
              if (acknowledged) expect(actual.length).toBe(10);
              expect(actual).toEqual(Array.from({ length: actual.length }, (_, i) => i));
              const from = actual.length;
              await insert(recovered, table, units, from, 6);
              await recovered.path.quiesce();
              await recovered.reopen();
              expect(await numbers(recovered, table)).toEqual(
                Array.from({ length: from + 6 }, (_, i) => i),
              );
              await recovered.space.audit();
              reach.reach(
                (
                  [
                    "before data flush",
                    "before root write",
                    "before root truncate",
                    "before root flush",
                  ] as const
                )[cut] ?? "before data flush",
              );
              reach.reach("recovered retry continues");
            } finally {
              await recovered.close();
            }
            expect(recovered.governor.usedBytes).toBe(0);
          }
        }),
        { seed, numRuns: 4 },
      );
    } finally {
      spy.mockRestore();
    }
  }, 180_000);

for (const seed of seedsFor("next-overflow-current-openers", [0xb511]))
  it(`serializes independent current graphs opening an overflow root (seed ${String(seed)})`, async () => {
    const spy = factory(),
      peer = await directoryPeer();
    try {
      await fc.assert(
        fc.asyncProperty(fc.constantFrom(769, 16384), async (units) => {
          reach.beginHistory();
          const h = new Harness("strict", "sim", 4, undefined, { merges: { mode: "off" } }),
            locks = new SimLocks();
          Object.assign(h, { backend: new SimBackend(h.disk, "handle", { locks }) });
          await h.open();
          const table = await create(h);
          await insert(h, table, units, 0, 8);
          await insert(h, table, units, 8, 2);
          await h.path.quiesce();
          let requested!: () => void;
          const waiting = new Promise<void>((r) => {
            requested = r;
          });
          const request = locks.request.bind(locks);
          const lockSpy = vi.spyOn(locks, "request").mockImplementation((...args) => {
            requested();
            return request(...args);
          });
          let entered = false;
          const opening = peer.open(h.disk, locks).then((p) => {
            entered = true;
            return p;
          });
          await waiting;
          await Promise.resolve();
          expect(entered).toBe(false);
          await h.close();
          const next = await opening;
          try {
            expect(await numbers(next, table)).toEqual(Array.from({ length: 10 }, (_, i) => i));
            await next.space.audit();
            reach.reach("independent current openers");
          } finally {
            await next.close();
            lockSpy.mockRestore();
          }
          expect(h.governor.usedBytes).toBe(0);
          expect(next.governor.usedBytes).toBe(0);
        }),
        { seed, numRuns: 4 },
      );
    } finally {
      spy.mockRestore();
      await peer.dispose();
    }
  }, 120_000);
