/** governor.ts accounting law: a plan retained across admission owns one object charge. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { createReach, WRITE_ROW_PLAN_ACCOUNTING_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { OBJECT_OVERHEAD_BYTES, type Grant } from "../memory/governor.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rules,
  tableInput,
} from "./test-support.js";

const reach = createReach("suspended row plan accounting", WRITE_ROW_PLAN_ACCOUNTING_REACH);
for (const seed of seedsFor("next-write-row-plan-accounting", [0xb313, 0x92]))
  it(`charges suspended plans through settlement (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          count: fc.constantFrom(0, 1, 2048, 2049, 4097),
          selected: fc.boolean(),
          outcome: fc.constantFrom("success", "error", "cancel", "sync error"),
        }),
        async ({ count, selected, outcome }) => {
          reach.beginHistory();
          reach.reach(selected ? "selected" : "range");
          reach.reach(count > 2048 ? "multiple chunks" : "single chunk");
          reach.reach(outcome);
          const h = new Harness();
          const admitGroup = h.governor.admitGroup.bind(h.governor);
          let planCredit = 0;
          const spies: Array<{ mockRestore(): void }> = [];
          spies.push(
            vi.spyOn(h.governor, "admitGroup").mockImplementation(async (options) => {
              const grants = await admitGroup(options);
              for (const grant of grants)
                if (grant.label === "write path") {
                  const allocate = grant.allocate.bind(grant);
                  const free = grant.free.bind(grant);
                  spies.push(
                    vi.spyOn(grant, "allocate").mockImplementation((bytes) => {
                      const paid = allocate(bytes);
                      if (paid && bytes === OBJECT_OVERHEAD_BYTES) planCredit += bytes;
                      return paid;
                    }),
                  );
                  spies.push(
                    vi.spyOn(grant, "free").mockImplementation((bytes) => {
                      free(bytes);
                      // Startup can refund 64 bytes from an earlier, larger allocation.
                      if (bytes === OBJECT_OVERHEAD_BYTES && planCredit > 0) planCredit -= bytes;
                    }),
                  );
                }
              return grants;
            }),
          );
          const p = await h.open();
          let id = 0;
          try {
            await autocommit(p, async (s) => {
              id =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("v", INT, false)], { primaryKey: null }),
                  })
                ).id ?? 0;
            });
            const tx = await p.begin();
            const controller = new AbortController();
            const s = tx.statement({ rules, signal: controller.signal });
            const input = {
              ...batch(ints(Array.from({ length: count }, (_, i) => i))),
              selection: selected
                ? { rows: Uint16Array.from({ length: count }, (_, i) => i), count }
                : null,
            };
            // The engine supplies its native rows grant through the narrower ByteBudget interface.
            const budget = p.shared.budget as Grant;
            const before = budget.allocated;
            const reason = new Error(outcome);
            let resolve!: () => void;
            let reject!: (reason: unknown) => void;
            const wait = new Promise<void>((yes, no) => {
              resolve = yes;
              reject = no;
            });
            const cancel = () => reject(controller.signal.reason);
            controller.signal.addEventListener("abort", cancel, { once: true });
            let admissions = 0;
            const admit = vi.spyOn(p, "admit").mockImplementation(() => {
              admissions++;
              // No rows have been allocated yet. Held headroom is not allocated scratch.
              if (admissions === 1) {
                expect(planCredit).toBe(OBJECT_OVERHEAD_BYTES);
                expect(budget.allocated).toBe(before);
              }
              if (outcome === "sync error") throw reason;
              return admissions === 1 ? wait : undefined;
            });
            let completion: Promise<unknown> | undefined;
            try {
              if (outcome === "sync error") {
                expect(() => s.table(id).insert(input, null)).toThrow(reason);
                expect(budget.allocated).toBe(before);
              } else {
                const pending = s.table(id).insert(input, null);
                // Observe both outcomes before resolving/cancelling the suspended admission.
                const result = Promise.resolve(pending).then(
                  (value) => ({ ok: true, value }),
                  (error: unknown) => ({ ok: false, error }),
                );
                completion = result;
                expect(planCredit).toBe(OBJECT_OVERHEAD_BYTES);
                expect(budget.allocated).toBe(before);
                if (outcome === "success") resolve();
                else if (outcome === "cancel") controller.abort(reason);
                else reject(reason);
                const settled = await result;
                expect(settled).toEqual(
                  outcome === "success" ? { ok: true, value: count } : { ok: false, error: reason },
                );
                if (outcome !== "success") expect(budget.allocated).toBe(before);
              }
              // A fixed allowance stays paid between chunks/calls and through every outcome.
              expect(planCredit).toBe(OBJECT_OVERHEAD_BYTES);
              reach.reach("paid after settlement");
            } finally {
              resolve();
              await completion?.catch(() => undefined);
              controller.signal.removeEventListener("abort", cancel);
              admit.mockRestore();
              tx.rollback();
            }
          } finally {
            await h.close();
            for (const spy of spies) spy.mockRestore();
          }
          expect(planCredit).toBe(0);
          reach.reach("released on close");
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 40 },
    );
  });
it("reaches the retained-plan lifecycle dimensions", () => reach.assertReached());
