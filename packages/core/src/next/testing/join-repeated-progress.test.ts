/** Key positions form a multiset: every occurrence needs encoded workspace under pressure. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { createReach, JOIN_REPEATED_PROGRESS_REACH, type Reach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { HashJoin, hashJoinMinimumBytes, type HashJoinSpec } from "../exec/join.js";
import { INT, TEXT, ArraySource, CollectSink, batchesOf, type Row } from "../exec/join-fixtures.js";
import type { JoinCheckpoint } from "../exec/join-checkpoint.js";
import { ReservedGrant } from "../exec/admission.js";
import { StepLoans } from "../memory/step-loans.js";
import { drive } from "../exec/pipeline.js";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
let pg: PGlite;
// A timed-out body must drain before another seed or the PG owner can close.
const active = new Map<AbortController, Promise<void>>();
afterEach(async () => {
  const pending = [...active];
  for (const [controller] of pending) controller.abort(new Error("Repeated-key history ended"));
  await Promise.all(pending.map(([, closed]) => closed));
});
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
const kinds = ["inner", "left", "right", "full", "semi", "anti", "mark"] as const;
const names = ["n", "a", "b"];
const name = (column: number): string => {
  const value = names[column];
  if (value === undefined) throw new Error("Unknown test key column");
  return value;
};
const canonical = (rows: readonly unknown[]) => rows.map((r) => JSON.stringify(r)).sort();
async function runCases(
  seed: number,
  random: () => number,
  reach: Reach<(typeof JOIN_REPEATED_PROGRESS_REACH)[number]>,
  selectedKinds: ReadonlyArray<(typeof kinds)[number]>,
  shapes: readonly number[],
): Promise<void> {
  const controller = new AbortController();
  let finish: (() => void) | undefined;
  active.set(
    controller,
    new Promise<void>((resolve) => {
      finish = resolve;
    }),
  );
  try {
    for (const kind of selectedKinds)
      for (const shape of shapes) {
        if (shape >= 5 && shape <= 7 && kind !== "inner") continue;
        controller.signal.throwIfAborted();
        reach.beginHistory();
        // Keep full-width duplicate/repeated anchors for every kind. Only the MARK
        // restore and fixed-key negative control need the >1MiB arena boundary.
        // Small semantic counterparts retain all 37 histories and both seeds.
        const boundary = shape === 6 || (shape === 4 && kind === "mark");
        const width =
          shape === 7
            ? 4000000
            : shape === 8
              ? 800000
              : boundary
                ? (1 << 20) + 1
                : shape === 2 || shape === 5
                  ? 800000
                  : shape === 1
                    ? 400000 + Math.floor(random() * 4096)
                    : 16384 + Math.floor(random() * 1024);
        const text = "x".repeat(width),
          asymmetric = shape === 3;
        const types = asymmetric || shape === 8 ? [INT, TEXT, TEXT] : [INT, TEXT];
        const probe: Row[] = [
          [1, text],
          [2, text],
          [3, null],
          [null, text],
        ];
        const build: Row[] = [
          [1, text],
          [1, text],
          [3, null],
          [4, text + "4"],
          [null, text],
        ];
        // These three control histories need one matching probe and duplicate
        // builds, rather than repeating the NULL/unmatched matrix already above.
        if (shape === 5 || shape === 6 || (shape === 4 && kind === "mark")) {
          probe.splice(1);
          build.splice(2);
        }
        if (shape === 7) {
          probe.splice(0, probe.length, [0, text], [1, text], [2, null]);
          build.splice(0, build.length, [0, text], [0, text], [1, text], [1, text], [2, null]);
        }
        if (shape === 8) {
          for (const rows of [probe, build])
            for (const [i, row] of rows.entries())
              rows[i] = [row[0] ?? null, row[1] === null ? null : row[0] === 4 ? "k4" : "k", text];
        }
        if (asymmetric) {
          for (const rows of [probe, build])
            for (const [i, row] of rows.entries()) rows[i] = [...row, row[1] ?? null];
        }
        const leftKeys =
          shape === 6
            ? Array.from({ length: 17 }, () => 0)
            : asymmetric
              ? [0, 1, 1, 2]
              : [
                  0,
                  ...Array.from(
                    { length: shape === 5 ? 17 : shape === 7 || shape === 8 ? 4 : shape + 1 },
                    () => 1,
                  ),
                ];
        const rightKeys = asymmetric ? [0, 1, 2, 2] : leftKeys;
        const correlated = kind === "mark" && shape === 4 ? 3 : 1;
        const spec: HashJoinSpec = {
          kind,
          probeTypes: types,
          buildTypes: types,
          probeKeys: leftKeys,
          buildKeys: rightKeys,
          nullSafe: leftKeys.map(() => false),
          residual: null,
          publishes: [],
          ...(kind === "mark" ? { correlated } : {}),
        };
        // The 17-position witness has a >16 MiB compound step: use a 256 MiB
        // governor (32 MiB reserve), retaining 128 MiB for every original shape.
        const governor = new Governor(
            (shape === 7 ? 512 : shape === 8 ? 32 : shape === 5 ? 256 : 128) << 20,
          ),
          spill = new MemorySpillService();
        const grant = await governor.admit({
          label: "repeated-key join",
          signal: controller.signal,
          minimumBytes: hashJoinMinimumBytes(spec),
          spillable: true,
        });
        let op = new HashJoin(spec, { grant, spill, queryId: 1, signal: controller.signal });
        const sink = new CollectSink(1);
        let saved: JoinCheckpoint | undefined;
        let blocker: Awaited<ReturnType<Governor["admit"]>> | undefined;
        try {
          blocker = await governor.admit({
            label: "competing ordinary credit",
            signal: controller.signal,
            minimumBytes: 0,
            spillable: true,
          });
          expect(
            blocker.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
          ).toBe(true);
          await drive(
            new ArraySource(batchesOf(types, build, { batchRows: 3, dictionary: true, skip: 2 })),
            op.build,
          );
          if (shape === 4) {
            // The controller awaits a complete rotation before opening the next child.
            const rotate = async () => {
              saved = await op.checkpoint();
              expect(saved.buildComplete).toBe(true);
              if (boundary) expect(saved.spilledBytes).toBeGreaterThan(0);
              op.abort(new Error("retire grant before restore"));
              await op.closed;
              blocker?.close();
              const minimum = hashJoinMinimumBytes(spec),
                progress = saved.progressBytes;
              const parts = await governor.admitGroup({
                label: "restored repeated-key join",
                signal: controller.signal,
                includesProgress: true,
                grants: [
                  { label: "fixed", minimumBytes: minimum, spillable: true },
                  { label: "progress", minimumBytes: progress, spillable: false },
                  { label: "growth", minimumBytes: 0, spillable: true },
                ],
              });
              const [fixed, lane, growth] = parts;
              if (!fixed || !lane || !growth) throw new Error("Missing restored admission parts");
              const restored = new ReservedGrant(
                fixed,
                growth,
                minimum,
                new StepLoans(lane, progress, "restored repeated keys"),
                false,
                () => lane.close(),
                true,
              );
              op = new HashJoin(
                spec,
                { grant: restored, spill, queryId: 1, signal: controller.signal },
                saved,
              );
              blocker = await governor.admit({
                label: "competing restored credit",
                signal: controller.signal,
                minimumBytes: 0,
                spillable: true,
              });
              expect(
                blocker.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
              await op.restore(saved);
              saved = undefined;
              reach.reach("retired grant and private restore lane");
            };
            await rotate();
          }
          await drive(new ArraySource(batchesOf(types, probe, { batchRows: 2 })), op.probe(sink));
          const values = (rows: readonly Row[]) =>
            rows.map((row) => (row[0] === null ? "(NULL::int)" : `(${String(row[0])})`)).join(",");
          const l = `SELECT i n,CASE WHEN i=${shape === 7 ? "2" : "3"} THEN NULL::text ELSE $1::text END a,CASE WHEN i=${shape === 7 ? "2" : "3"} THEN NULL::text ELSE $1::text END b FROM(VALUES${values(probe)})g(i)`;
          const r = `SELECT i n,CASE WHEN i=${shape === 7 ? "2" : "3"} THEN NULL::text WHEN i=4 THEN $1::text||'4' ELSE $1::text END a,CASE WHEN i=${shape === 7 ? "2" : "3"} THEN NULL::text WHEN i=4 THEN $1::text||'4' ELSE $1::text END b FROM(VALUES${values(build)})g(i)`;
          const equal = leftKeys
            .map((c, i) => `l.${name(c)}=r.${name(rightKeys[i] ?? 0)}`)
            .join(" AND ");
          const tested = leftKeys
            .slice(correlated)
            .map((c) => `l.${name(c)}`)
            .join(",");
          const returned = rightKeys
            .slice(correlated)
            .map((c) => `r.${name(c)}`)
            .join(",");
          const correlation = leftKeys
            .slice(0, correlated)
            .map((c, i) => `l.${name(c)}=r.${name(rightKeys[i] ?? 0)}`)
            .join(" AND ");
          const sql =
            kind === "mark"
              ? `SELECT l.n,ROW(${tested}) IN(SELECT ${returned} FROM(${r})r WHERE ${correlation}) m FROM(${l})l`
              : kind === "semi" || kind === "anti"
                ? `SELECT l.n FROM(${l})l WHERE ${kind === "anti" ? "NOT " : ""}EXISTS(SELECT 1 FROM(${r})r WHERE ${equal})`
                : `SELECT l.n,r.n FROM(${l})l ${kind.toUpperCase()} JOIN(${r})r ON ${equal}`;
          const expected = await pg.query(sql, [shape === 8 ? "k" : text], {
            rowMode: "array",
          } as never);
          const actual = sink.rows.map((row) =>
            kind === "mark"
              ? [row[0], row[types.length]]
              : kind === "semi" || kind === "anti"
                ? [row[0]]
                : [row[0], row[types.length]],
          );
          expect(canonical(actual), `${kind}/${String(shape)}`).toEqual(canonical(expected.rows));
          if (shape === 1 || shape === 2 || boundary || (shape >= 5 && shape <= 7)) {
            expect(op.profile.chunkPasses).toBeGreaterThan(0);
            expect(op.profile.loans).toBeGreaterThan(0);
            reach.reach("observed chunk and loan");
          }
          reach.reach(kind);
          reach.reach(
            [
              "unique positions",
              "duplicated position",
              "repeated position",
              "asymmetric repetitions",
              "retired grant and private restore lane",
              "seventeen repeats",
              "fixed repeats with large payload",
              "partitioned four MB key page",
              "small key large payload",
            ][shape] as (typeof JOIN_REPEATED_PROGRESS_REACH)[number],
          );
          if (shape === 4 && boundary) reach.reach("arena boundary");
          if (kind === "mark" && shape === 4) reach.reach("encoded repeated correlation");
          if (shape === 7) {
            expect(op.profile.forwardedPages).toBeGreaterThan(0);
            reach.reach("forwarded distinct-key partition");
          }
          if (shape === 8) expect(governor.peakBytes).toBeLessThanOrEqual(32 << 20);
          reach.reach("selected dictionary");
          reach.reach("ordinary credit occupied");
          reach.reach("PG exact");
        } catch (error) {
          throw new Error(`${String(seed)}/${kind}/shape${String(shape)}`, { cause: error });
        } finally {
          op.abort(new Error("complete"));
          await op.closed;
          await saved?.close();
          blocker?.close();
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
        }
        reach.reach("zero resources");
      }
  } finally {
    active.delete(controller);
    finish?.();
  }
}
describe.each(seedsFor("next-join-repeated-progress", [0xb734, 0xb735]))(
  "repeated native keys seed %i",
  (seed) => {
    const reach = createReach("repeated join progress", JOIN_REPEATED_PROGRESS_REACH),
      random = mulberry32(seed);
    afterAll(() => reach.assertReached());
    for (const kind of kinds)
      it(
        `keeps duplicate/repeated and small-key payload bounds for ${kind}`,
        () => runCases(seed, random, reach, [kind], [0, 1, 2, 3, 4, 8]),
        120000,
      );
    it(
      "keeps all seventeen-position controls",
      () => runCases(seed, random, reach, ["inner"], [5, 6]),
      120000,
    );
    it(
      "preflights four-MB keys before partition forwarding",
      () => runCases(seed, random, reach, ["inner"], [7]),
      120000,
    );
  },
);
