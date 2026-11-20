/** PC2/PC4 and R15.7: retaining native queries reuse validated immutable directories.
 * Work counts are observable calls, never a wall-clock deadline. PostgreSQL owns rows.
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DIRECTORY_CACHE_NATIVE_REACH } from "../../testing/reach.js";
import * as directoryCode from "../page/directory.js";
import * as sketchCode from "../page/sketch.js";
import { metadataCachesFor } from "../format/metadata-reader.js";
import { boundSource } from "../page/read-bounds.js";
import { memoryScanBufferBytes } from "../exec/table-scan.js";
import { ChunkReader } from "../page/reader.js";
import { exportSnapshot } from "../write/snapshot-capture.js";
import { loadSnapshot } from "../write/snapshot-load.js";
import { snapshotBlob } from "../write/snapshot-source.js";
import { snapshotTestTarget } from "../write/snapshot-test-support.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

const MODES = [
  "same blob",
  "different blob",
  "replaced root",
  "eviction",
  "restore",
  "once",
] as const;
type Mode = (typeof MODES)[number];
interface Row extends Record<string, number | string | null> {
  id: number;
  v: number | null;
  t: string;
}
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
});
const sql = "SELECT id,v,t FROM directory_input WHERE v < 40 ORDER BY id";
async function expected(rows: readonly Row[], statement = sql) {
  const values = rows
    .map((row) => `(${String(row.id)},${row.v === null ? "NULL::int4" : String(row.v)},'${row.t}')`)
    .join(",");
  return (await pg.query(`WITH directory_input(id,v,t) AS (VALUES ${values}) ${statement}`)).rows;
}
async function archive(core: StorageCore, salt: number) {
  const grant = await core.harness.governor.admit({
    label: "directory-cache test archive",
    minimumBytes: 8 << 20,
    spillable: false,
  });
  const parts: Array<Uint8Array<ArrayBuffer>> = [];
  try {
    await exportSnapshot(
      core.harness.path,
      new Uint8Array(16).fill(salt & 255),
      {
        write: (bytes) => {
          parts.push(bytes.slice());
        },
      },
      grant,
    );
    return new Blob(parts);
  } finally {
    grant.close();
  }
}
async function nonretainingSize(core: StorageCore) {
  const pin = core.harness.path.pin();
  try {
    const table = pin.catalog.tableByName("directory_input");
    if (!table) throw new Error("Missing directory-cache input");
    const types = table.columns.map((column) => column.type);
    await boundSource(
      pin,
      {
        id: 1,
        tableId: table.id,
        columns: table.columns.map((column) => column.id),
        types,
        windowBytes: 0,
        stagingBytes: memoryScanBufferBytes(types),
        rows: null,
        retain: false,
      },
      core.harness.governor,
      new AbortController().signal,
      8192,
    );
  } finally {
    pin.release();
  }
}
/** Exercise the actual executor's explicit relinquishing export, with the ordinary
 * StorageCore factory deliberately lacking loadTogether. Windowed IO cannot supply
 * the once flag for us; the source's non-retaining policy must survive independently.
 */
async function nonretainingQuery(core: StorageCore, rows: readonly Row[]) {
  const prepare = core.executor.prepare.bind(core.executor),
    preparing = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
      const prepared = prepare(plan);
      return {
        ...prepared,
        execute: (request) => prepared.execute({ ...request, outputBuffers: { take: () => null } }),
      };
    }),
    opens = vi.spyOn(ChunkReader, "open");
  try {
    const result = await core.execute("SELECT * FROM directory_input");
    if (!("batches" in result)) throw new Error("Expected non-retaining rows");
    const actual = oracleRows(result).sort((a, b) => Number(a.id) - Number(b.id));
    expect(actual).toEqual(await expected(rows, "SELECT * FROM directory_input ORDER BY id"));
    expect(opens.mock.calls.length).toBeGreaterThan(0);
    expect(opens.mock.calls.every((call) => call[2].once === true)).toBe(true);
  } finally {
    opens.mockClear();
    opens.mockRestore();
    preparing.mockRestore();
  }
}
it.each(seedsFor("next-directory-cache-native", [0xb501, 0xb502]))(
  "validates once while retained, then revalidates changed sources (seed %i)",
  async (seed) => {
    const reach = createReach("native directory cache", DIRECTORY_CACHE_NATIVE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...MODES),
        fc.boolean(),
        fc.integer(),
        fc.boolean(),
        async (mode, dense, salt, nullable) => {
          reach.beginHistory();
          const count = dense ? 512 + ((salt >>> 0) % 129) : 64 + ((salt >>> 0) % 65),
            core = await new StorageCore(4096).open();
          let rows: Row[] = Array.from({ length: count }, (_, id) => ({
            id,
            v:
              id === 1
                ? 1
                : nullable && id % 7 === 0
                  ? null
                  : (Math.imul(id ^ salt, 0x9e37_79b1) >>> 0) % 100,
            t: `key-${String(id % 13)}`,
          }));
          const parse = vi.spyOn(directoryCode, "parseDirectory"),
            validate = vi.spyOn(sketchCode, "validateSketch");
          const clear = () => {
            parse.mockClear();
            validate.mockClear();
          };
          const query = async () => {
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected directory-cache query rows");
            expect(oracleRows(result)).toEqual(await expected(rows));
          };
          try {
            await core.execute(
              "CREATE TABLE directory_input (id INTEGER PRIMARY KEY,v INTEGER,t TEXT)",
            );
            await core.writeBatch("insertBatch", "directory_input", rows);
            await core.harness.path.recycleLog();
            await core.harness.path.quiesce();
            const cache = metadataCachesFor(core.harness.space)?.directories;
            if (mode === "once") {
              for (let pass = 0; pass < 2; pass++) {
                clear();
                await nonretainingSize(core);
                await nonretainingQuery(core, rows);
                expect(parse).toHaveBeenCalled();
                expect(cache?.entryCount ?? 0).toBe(0);
              }
              reach.reach("once misses are not retained");
              reach.reach("once execution without windows");
            }
            clear();
            await query();
            expect(parse).toHaveBeenCalled();
            expect(validate).toHaveBeenCalled();
            for (const result of parse.mock.results)
              if (result.type === "return" && result.value.sketch)
                reach.reach(result.value.sketch[1] === 0 ? "dense sketch" : "sparse sketch");
            clear();
            await query();
            // This fails on the parent: every query reparses and validates again.
            expect(parse.mock.calls.length, "warm immutable directory parses").toBe(0);
            expect(validate.mock.calls.length, "warm immutable sketch validations").toBe(0);
            expect(cache).toBeDefined();
            if (!cache) throw new Error("Expected the native owner directory cache");
            expect(cache.residentBytes).toBeGreaterThan(0);
            expect(cache.evictableBytes).toBe(cache.residentBytes);
            reach.reach("warm query performs no validation");
            reach.reach(nullable ? "nullable input" : "all-valid input");
            if (mode === "different blob") {
              const row = { id: count, v: 1, t: `new-${String(salt >>> 0)}` };
              rows = [...rows, row];
              await core.writeBatch("insertBatch", "directory_input", [row]);
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
            } else if (mode === "replaced root") {
              await core.execute("UPDATE directory_input SET v=2,t='changed' WHERE id=1");
              rows = rows.map((row) => (row.id === 1 ? { ...row, v: 2, t: "changed" } : row));
              await core.harness.path.recycleLog();
              await core.harness.path.quiesce();
            } else if (mode === "eviction") {
              const bytes = cache.residentBytes;
              expect(cache.evict(Infinity)).toBe(bytes);
              expect(cache.residentBytes).toBe(0);
              expect(cache.evictableBytes).toBe(0);
              reach.reach("eviction returns cache credit");
            } else if (mode === "restore") {
              const image = await archive(core, salt),
                lsn = core.harness.path.appliedLsn,
                oldSpace = core.harness.space;
              await core.close();
              expect(cache.residentBytes).toBe(0);
              expect(metadataCachesFor(oldSpace)).toBeUndefined();
              expect(core.harness.governor.usedBytes).toBe(0);
              await loadSnapshot(
                snapshotBlob(image),
                () => snapshotTestTarget(core.harness, lsn, false),
                core.harness.governor,
                { replace: true },
              );
              await core.open();
              expect(metadataCachesFor(core.harness.space)?.directories).not.toBe(cache);
              reach.reach("restore creates a new cache scope");
            } else if (mode === "once") {
              const entries = cache.entryCount,
                oldest = cache.oldestUnusedAt;
              clear();
              await nonretainingSize(core);
              await nonretainingQuery(core, rows);
              expect(parse.mock.calls.length).toBe(0);
              expect(cache.entryCount).toBe(entries);
              expect(cache.oldestUnusedAt).toBe(oldest);
              reach.reach("once hits do not promote");
            }
            clear();
            await query();
            if (mode === "same blob" || mode === "once") expect(parse.mock.calls.length).toBe(0);
            else expect(parse).toHaveBeenCalled();
            clear();
            await query();
            expect(parse.mock.calls.length).toBe(0);
            expect(validate.mock.calls.length).toBe(0);
            reach.reach(mode);
            // Closing a writer does not own an older caller's read pin. Its cache
            // must nevertheless stop retaining entries released after close.
            const leaseGrant = await core.harness.governor.admit({
                label: "directory-cache late lease",
                minimumBytes: 8 << 20,
                spillable: false,
              }),
              pin = core.harness.path.pin();
            let reader: ChunkReader | undefined;
            try {
              const table = pin.catalog.tableByName("directory_input");
              if (!table) throw new Error("Missing late-lease table");
              const source = pin
                .table(table.id, { budget: leaseGrant })
                .segments.find((segment) => segment.kind === "group");
              if (source?.kind !== "group") throw new Error("Missing stored late-lease source");
              await source.root();
              const chunk = source.chunk(table.columns[0]?.id ?? -1),
                closingCache = metadataCachesFor(core.harness.space)?.directories;
              if (!chunk || !closingCache) throw new Error("Missing native directory lease");
              reader = await ChunkReader.open(pin.pages, chunk, {
                budget: leaseGrant,
                directories: closingCache,
              });
              await core.harness.path.close();
              expect(closingCache.residentBytes).toBeGreaterThan(0);
              reader.close();
              reader = undefined;
              expect(closingCache.residentBytes).toBe(0);
              reach.reach("close with live directory lease");
            } finally {
              reader?.close();
              pin.release();
              leaseGrant.close();
            }
          } finally {
            clear();
            parse.mockRestore();
            validate.mockRestore();
            await core.close();
          }
          expect(core.harness.governor.usedBytes).toBe(0);
          expect(core.spill.bytesInUse).toBe(0);
          reach.reach("close returns all credit");
        },
      ),
      {
        seed,
        numRuns: 40,
        examples: MODES.flatMap((mode, index): Array<[Mode, boolean, number, boolean]> => [
          [mode, true, index + 1, true],
          [mode, false, index + 17, false],
        ]),
      },
    );
    reach.assertReached();
  },
  180_000,
);
