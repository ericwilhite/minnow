/** Governor rule 5: one bounded spill step repays its loan before borrowing again. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { INT, TEXT, batchesOf, canonical, rowsOf, type Row } from "./join-fixtures.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinSpec } from "./join.js";
import type { Operator } from "./pipeline.js";

const reach = createReach("next-join-large-row", [
  "600000-byte reproduction",
  "generated widths",
  "NULL payload",
  "duplicate build keys",
  "multi-row input batch",
  "inner",
  "left",
  "spilled",
  "released budgets",
  "minimum grant",
  "8 MiB grant",
]);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});

for (const seed of seedsFor("next-join-large-row", [0xb4600, 0xb4601]))
  for (const tight of [false, true])
    for (const generated of [false, true])
      it(`oversized build ${generated ? "generated" : "600000-byte"} rows, ${tight ? "minimum" : "8 MiB"}, seed ${String(seed)}`, async () => {
        const random = mulberry32(seed);
        reach.beginHistory();
        const width = generated ? 500000 + Math.floor(random() * 250000) : 600000;
        const count = generated ? 60 + Math.floor(random() * 12) : 64;
        const domain = generated ? Math.floor(count / 4) : count;
        const batchRows = generated ? 2 + Math.floor(random() * 3) : 1;
        const match = generated ? Math.floor(random() * domain) : 32;
        const kind = generated ? "left" : "inner";
        const spec: HashJoinSpec = {
          kind,
          probeTypes: [INT],
          buildTypes: [INT, TEXT],
          probeKeys: [0],
          buildKeys: [0],
          nullSafe: [false],
          residual: null,
          publishes: [],
        };
        const minimum = hashJoinMinimumBytes(spec);
        const governor = new Governor(
          tight ? Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7) + 1) : 8 << 20,
        );
        const spill = new MemorySpillService();
        const grant = await governor.admit({
          label: "large-row join",
          minimumBytes: minimum,
          spillable: true,
        });
        const join = new HashJoin(spec, { grant, spill, queryId: 741 });
        const actual: Row[] = [];
        let finished = false;
        const sink: Operator = {
          push(batch, transfer) {
            actual.push(...rowsOf(batch));
            transfer?.release();
            return "blocked";
          },
          ready: () => Promise.resolve(),
          finish: () => {
            finished = true;
          },
          abort: () => undefined,
        };
        // Connect before build, as in the executor; construct only one large input batch at a time.
        const probe = join.probe(sink);
        try {
          for (let start = 0; start < count; start += batchRows) {
            const input: Row[] = [];
            for (let k = start; k < Math.min(count, start + batchRows); k++) {
              const payload =
                generated && k % 7 === 0
                  ? null
                  : String.fromCharCode(97 + (k % 26)).repeat(
                      width + (generated ? (k % 3) * 17 : 0),
                    );
              input.push([k % domain, payload]);
            }
            for (const batch of batchesOf(spec.buildTypes, input))
              if (join.build.push(batch) === "blocked") await join.build.ready();
          }
          await join.build.finish();
          for (const batch of batchesOf(spec.probeTypes, [[match], [0], [-1], [null]]))
            if (probe.push(batch) === "blocked") await probe.ready();
          await probe.finish();
          await join.closed;
          const expected = await pg.query<{
            p: number | null;
            k: number | null;
            payload: string | null;
          }>(`
          WITH b AS (
            SELECT k % ${String(domain)} AS k, CASE WHEN ${generated ? "k % 7 = 0" : "false"} THEN NULL
              ELSE repeat(chr(97 + k % 26), ${String(width)} + ${generated ? "k % 3 * 17" : "0"}) END AS payload
            FROM generate_series(0, ${String(count - 1)}) k
          ), p(p) AS (VALUES (${String(match)}), (0), (-1), (NULL::int))
          SELECT p.p, b.k, b.payload FROM p ${kind === "left" ? "LEFT" : "INNER"} JOIN b ON p.p = b.k
        `);
          expect(canonical(actual)).toEqual(
            canonical(expected.rows.map((row) => [row.p, row.k, row.payload])),
          );
          expect(finished).toBe(true);
          expect(join.profile.spilledBytes).toBeGreaterThan(0);
          expect(join.profile.largestLoan).toBeLessThanOrEqual(governor.reserveBytes);
          expect(grant.loaned).toBe(0);
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach(generated ? "generated widths" : "600000-byte reproduction");
          if (generated) {
            reach.reach("NULL payload");
            reach.reach("duplicate build keys");
            reach.reach("multi-row input batch");
          }
          reach.reach(kind);
          reach.reach("spilled");
          reach.reach("released budgets");
          reach.reach(tight ? "minimum grant" : "8 MiB grant");
        } finally {
          join.abort(new Error("large-row test cleanup"));
          await join.closed.catch(() => undefined);
          grant.close();
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
        }
      }, 120_000);
