import { expect, it } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../src/testing/seeds.js";
import { SEMANTIC_CLEANUP_REACH } from "../src/testing/reach.js";
import { closeSemanticResources, semanticRunFailure } from "./semantic-cleanup.js";

for (const seed of seedsFor("semantic-cleanup", [47075, 47076])) {
  it(`keeps a draining store alive and retains cleanup failures, seed ${String(seed)}`, async () => {
    const reached = new Set<string>();
    async function history(client: number, database: boolean, deletion: boolean, ticks: number) {
      const state = { alive: true, drained: false, deleted: false, terminated: false };
      let resume!: () => void;
      const gate = new Promise<void>((resolve) => {
        resume = resolve;
      });
      const clientError = new Error("client close"),
        databaseError = new Error("database close"),
        deleteError = new Error("delete");
      const worker = {
        terminate() {
          state.alive = false;
          state.terminated = true;
          if (state.deleted) reached.add("terminate after deletion");
        },
      };
      const result = await closeSemanticResources(
        worker,
        () => {
          if (client === 1) {
            reached.add("client sync failure");
            throw clientError;
          }
          if (client === 2) {
            reached.add("client async failure");
            return Promise.reject(clientError);
          }
          return Promise.resolve();
        },
        async () => {
          if (database) {
            reached.add("database close failure");
            throw databaseError;
          }
        },
        async () => {
          reached.add("native drain pending");
          // Like a native shutdown task, this continuation must still have its worker.
          if (!state.alive)
            throw new DOMException(
              "Worker terminated with checkpoint handles pending",
              "NoModificationAllowedError",
            );
          resume();
          await gate;
          for (let i = 0; i < ticks; i++) await Promise.resolve();
          state.drained = true;
          reached.add("checkpoint before delete");
          if (deletion) {
            reached.add("delete failure");
            throw deleteError;
          }
          state.deleted = true;
        },
      );
      const expected = [
        ...(client ? [clientError] : []),
        ...(database ? [databaseError] : []),
        ...(deletion ? [deleteError] : []),
      ];
      expect(result, "shutdown must drain before worker termination").toEqual(expected);
      const comparisonError = new Error(
        `Semantic worker drift: SELECT ${String(ticks)}; seed ${String(seed)}`,
      );
      const reported = semanticRunFailure([comparisonError, ...result]);
      expect(reported.errors).toEqual([comparisonError, ...expected]);
      for (const failure of [comparisonError, ...expected])
        expect(reported.message).toContain(String(failure));
      expect(state.drained).toBe(true);
      expect(state.terminated).toBe(true);
      expect(state.alive).toBe(false);
      expect(state.deleted).toBe(!deletion);
      if (client && database && deletion) reached.add("all failures retained");
    }
    for (const client of [0, 1, 2])
      for (const database of [false, true])
        for (const deletion of [false, true]) await history(client, database, deletion, 4);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 2 }),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: 0, max: 8 }),
        history,
      ),
      { seed, numRuns: 60 },
    );
    expect([...reached].sort()).toEqual([...SEMANTIC_CLEANUP_REACH].sort());
  });
}
