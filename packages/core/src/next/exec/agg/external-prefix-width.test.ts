/** A byte-extremum helper owns the restored value, not its host's encoded page. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, AGG_CALL_PREFIX_REACH } from "../../../testing/reach.js";
import type { AggregateCall } from "../../ir/ir.js";
import { Governor } from "../../memory/governor.js";
import { StepLoans } from "../../memory/step-loans.js";
import { MemorySpillService } from "../../spill/memory-spill.js";
import { ReservedGrant } from "../admission.js";
import { TEXT, F64, col, texts, numbers, valueAt } from "../kernels/test-support.js";
import { OutputArena } from "../kernels/vectors.js";
import { PageWriter, decodePage } from "../spill-pages.js";
import { ExternalCall, externalCallMinimum } from "./external-call.js";
import { externalCallStepBytes } from "./external-call-layout.js";
import { SortGrants } from "./sort-grants.js";
import { accumulatorFor } from "./states.js";
import { StateRow, StateReader } from "./state-row.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("aggregate-external-prefix-width", [0xb7a5, 0xb7a6]))(
  "restores nonempty byte extrema inside their exact admitted step seed%i",
  async (seed) => {
    const reach = createReach("external prefix payload width", AGG_CALL_PREFIX_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 580000, max: 810000 }),
        fc.boolean(),
        fc.boolean(),
        async (bytes, maximum, unicode) => {
          reach.beginHistory();
          const fn = maximum ? "max" : "min",
            unit = unicode ? "汉" : "m";
          const body = unit.repeat(Math.floor((bytes - 1) / (unicode ? 3 : 1)));
          const values = ["m" + body, "z" + body, "a" + body],
            width = new TextEncoder().encode(values[0]).length;
          const call: AggregateCall = {
            fn,
            args: [col(0, TEXT)],
            type: TEXT,
            distinct: false,
            filter: null,
            orderBy: [],
          };
          const minimum = externalCallMinimum(call),
            progress = externalCallStepBytes(minimum, width) - minimum;
          const governor = new Governor(16 << 20);
          const parts = await governor.admitGroup({
            label: "saved byte transition",
            includesProgress: true,
            grants: [
              { label: "state minimum", minimumBytes: minimum, spillable: false },
              { label: "state progress", minimumBytes: progress, spillable: false },
              { label: "state growth", minimumBytes: 0, spillable: true },
              { label: "snapshot IO", minimumBytes: 6 << 20, spillable: false },
            ],
          });
          const fixed = parts[0],
            lane = parts[1],
            extra = parts[2],
            io = parts[3];
          if (!fixed || !lane || !extra || !io) throw new Error("missing state parts");
          const step = new StepLoans(lane, progress, "saved byte step");
          let loans = 0;
          const borrow = step.borrow.bind(step);
          step.borrow = (bytes, signal) => {
            loans++;
            expect(bytes).toBeLessThanOrEqual(progress);
            return borrow(bytes, signal);
          };
          const grants = new SortGrants(fixed, [new ReservedGrant(lane, extra, 0, step, true)]);
          const arena = new OutputArena(fixed, 2048),
            spill = new MemorySpillService();
          const original = accumulatorFor(fn, TEXT, TEXT, io),
            row = new StateRow(io),
            writer = new PageWriter(io);
          const state = new ExternalCall(
            call,
            {
              grants,
              context: { arena, input: [TEXT], params: { slots: [] } },
              check: () => undefined,
              pace: () => Promise.resolve(),
              sortContext: (grant) => ({ grant, spill, queryId: 1 }),
            },
            0,
            { owner: 0, stateFn: fn, stateType: TEXT, reads: "own", mode: false },
          );
          let decodedCharge = 0;
          const blocker = await governor.admit({
            label: "competing holder",
            minimumBytes: 0,
            spillable: true,
          });
          try {
            original.grow(1);
            original.update(
              texts([values[0] ?? null]),
              Int32Array.of(0),
              Uint16Array.of(0),
              1,
              true,
            );
            if (!original.save) throw new Error("missing snapshot support");
            original.save(0, row);
            original.close();
            const page = writer.encode(
              { queryId: 1, run: 0, partition: 0, sequence: 0 },
              row.columns,
              1,
            );
            if (!page) throw new Error("unfunded snapshot write");
            expect(page.length).toBeGreaterThan(width);
            const decoded = decodePage(
              page,
              row.columns.map((c) => c.type),
              io,
            );
            if ("needed" in decoded) throw new Error("unfunded snapshot decode");
            decodedCharge = decoded.charged;
            expect(
              blocker.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
            ).toBe(true);
            const reader = new StateReader(decoded.columns, 0);
            await state.restoreState(reader, page.length);
            expect(reader.consumed).toBe(row.columns.length);
            for (let i = 1; i < values.length; i++)
              await state.add(
                { length: 1, columns: [texts([values[i] ?? null])], selection: null },
                0,
                1,
                numbers(F64, [i]),
              );
            const expected = await pg.query<{ v: string }>(
              `SELECT ${fn}(v) v FROM (VALUES ($1::text),($2::text),($3::text))t(v)`,
              values,
            );
            expect(valueAt(await state.emit(), 0)).toEqual(expected.rows[0]?.v);
            expect(loans).toBe(1);
            reach.reach(maximum ? "MAX" : "MIN");
            reach.reach(unicode ? "UTF8" : "ASCII");
            reach.reach("nonempty saved value");
            reach.reach("encoded framing excluded");
            reach.reach("one exact loan");
            reach.reach("PG suffix exact");
          } finally {
            await state.close();
            grants.close();
            arena.close();
            original.close();
            io.free(decodedCharge);
            writer.close();
            row.close();
            for (const part of parts) part.close();
            blocker.close();
          }
          expect(governor.usedBytes).toBe(0);
          expect(spill.bytesInUse).toBe(0);
          reach.reach("zero charges");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [800000, false, false],
          [800000, true, false],
          [600000, false, true],
          [600000, true, true],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  },
  60000,
);
