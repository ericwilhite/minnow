/** Original-identity captures survive rekeys without recapturing a rewritten value. */
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  createReach,
  REGISTRY_TARGET_REACH,
  REGISTRY_TARGET_IO_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { SpillService } from "../spill/contract.js";
import { RegistryDirectory, REGISTRY_DIRECTORY_BYTES } from "./registry-directory.js";
import {
  RegistryTargets,
  REGISTRY_TARGET_BYTES,
  type NativeRowIdentity,
} from "./registry-targets.js";

function fixture() {
  let held = 0,
    peak = 0,
    files = 0;
  const limit = REGISTRY_DIRECTORY_BYTES + REGISTRY_TARGET_BYTES;
  const charge = {
    allocate: (n: number) => {
      if (held + n > limit) return false;
      held += n;
      peak = Math.max(peak, held);
      return true;
    },
    free: (n: number) => {
      held -= n;
      expect(held).toBeGreaterThanOrEqual(0);
    },
  };
  const backing = new MemorySpillService(),
    root = new AbortController();
  const spill: SpillService = {
    get bytesInUse() {
      return backing.bytesInUse;
    },
    sweep: () => backing.sweep(),
    create: (id) => {
      files++;
      return backing.create(id);
    },
  };
  const directory = new RegistryDirectory(charge, spill, 1, root.signal),
    targets = new RegistryTargets({
      rootId: 27,
      directory,
      charge,
      signal: root.signal,
      checkpoint: () => undefined,
    });
  return {
    targets,
    root,
    limit,
    get held() {
      return held;
    },
    get peak() {
      return peak;
    },
    get files() {
      return files;
    },
    get spillBytes() {
      return spill.bytesInUse;
    },
    async close() {
      await targets.close();
      await directory.close();
      expect(held).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    },
  };
}

it.each(seedsFor("next-registry-targets", [0xb5e2]))(
  "keeps bounded original child targets across accepted versions seed %i",
  async (seed) => {
    const reach = createReach("native deferred target provenance", REGISTRY_TARGET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 512, max: 1536 }),
        fc.constantFrom(0, 2 ** 52),
        fc.constantFrom(7, 2 ** 52),
        async (count, base, tableId) => {
          reach.beginHistory();
          const f = fixture();
          const current = new Map<number, number>(),
            moved = new Map<number, number>();
          for (let i = 0; i < count; i++) current.set(base + i, base + i);
          const source = (group: number) =>
            async function* (): AsyncIterable<NativeRowIdentity> {
              for (let i = group; i < count; i += 2) yield { tableId, rowId: base + i };
            };
          try {
            // Both original parent key groups are captured before either follow is applied.
            const first = await f.targets.capture(source(0)),
              second = await f.targets.capture(source(1));
            const empty = await f.targets.capture(async function* () {
              /* empty child rowset */
            });
            expect(empty.count).toBe(0);
            for await (const _row of f.targets.rows(empty, f.root.signal))
              throw new Error(`An empty range yielded ${String(_row.rowId)}`);
            for (const range of [first, second])
              for await (const row of f.targets.rows(range, f.root.signal)) {
                expect(row.rowId).toBe(current.get(row.originalId));
                expect(row.originalId).toBe(row.rowId);
                const next = row.rowId + count;
                await f.targets.note(row.tableId, row.rowId, next);
                current.set(row.originalId, next);
                moved.set(row.originalId, (moved.get(row.originalId) ?? 0) + 1);
              }
            reach.reach("parent swap captures");
            expect(moved.size).toBe(count);
            expect([...moved.values()].every((n) => n === 1)).toBe(true);
            reach.reach("updated once");
            // Intervening accepted updates and deletes change physical versions, not capture identity.
            for (let i = 0; i < count; i++) {
              const original = base + i,
                before = current.get(original);
              if (before === undefined) throw new Error("Missing oracle row");
              if (i % 7 === 0) {
                await f.targets.note(tableId, before, null);
                current.delete(original);
              } else if (i % 3 === 0) {
                await f.targets.note(tableId, before, before + count);
                current.set(original, before + count);
              }
            }
            const observed = new Map<number, number>();
            for (const range of [second, first])
              for await (const row of f.targets.rows(range, f.root.signal)) {
                expect(row.tableId).toBe(tableId);
                observed.set(row.originalId, row.rowId);
                // Same row ID on another table must not inherit this table's lineage/tombstone.
                expect(await f.targets.resolve(tableId + 1, row.originalId)).toEqual({
                  tableId: tableId + 1,
                  rowId: row.originalId,
                  originalId: row.originalId,
                });
              }
            expect(observed).toEqual(current);
            reach.reach("deleted target skipped");
            reach.reach("same row different tables");
            for (let i = 0; i < count; i++) {
              const original = base + i,
                currentId = current.get(original);
              expect(await f.targets.resolve(tableId, original + count)).toEqual(
                currentId === undefined
                  ? null
                  : { tableId, rowId: currentId, originalId: original },
              );
            }
            reach.reach("multiple physical versions");
            if (base > 0 || tableId > 7) reach.reach("wide IDs");
            expect(await f.targets.resolve(tableId, base)).toBeNull();
            for await (const _row of f.targets.rows(empty, f.root.signal))
              throw new Error(`Empty descriptor yielded ${String(_row.rowId)}`);
            let foreignError: unknown;
            try {
              for await (const _row of f.targets.rows({ ...first, spoolId: 28 }, f.root.signal))
                throw new Error(`foreign range read ${String(_row.rowId)}`);
            } catch (error) {
              foreignError = error;
            }
            expect(foreignError).toMatchObject({
              message: "Invalid or retired native target range",
            });
            reach.reach("foreign root refused");
            reach.reach("empty range replay");
            expect(f.files).toBe(1);
            reach.reach("one file across ranges");
            expect(f.spillBytes).toBeGreaterThan(f.limit);
            expect(f.peak).toBe(f.limit);
            expect(f.held).toBe(f.limit);
            expect(count * 128).toBeGreaterThan(f.limit);
            reach.reach("over-cap targets");
            reach.reach("fixed metadata");
          } finally {
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [513, 0, 7],
          [512, 2 ** 52, 2 ** 52],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-target-io", [0xb5e3]))(
  "fences cancellation of a target producer seed %i",
  async (seed) => {
    const reach = createReach("native target producer lifecycle", REGISTRY_TARGET_IO_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (external) => {
        reach.beginHistory();
        const f = fixture();
        let entered!: () => void, resolve!: () => void;
        const ready = new Promise<void>((r) => {
            entered = r;
          }),
          gate = new Promise<void>((r) => {
            resolve = r;
          });
        let retired = false,
          completed = false;
        const capturing = f.targets
          .capture(async function* (signal) {
            try {
              yield { tableId: 7, rowId: 0 };
              entered();
              await new Promise((_, reject) =>
                signal.addEventListener("abort", () => reject(signal.reason as Error), {
                  once: true,
                }),
              );
            } finally {
              await gate;
              retired = true;
            }
          })
          .then(
            () => null,
            (error: unknown) => error,
          );
        try {
          await ready;
          if (external) f.root.abort(new Error("cancel native target input"));
          const closing = f.targets.close().then(() => {
            completed = true;
          });
          for (let i = 0; i < 8; i++) await Promise.resolve();
          const closedBeforeIO = completed,
            chargeBeforeIO = f.held;
          resolve();
          expect(await capturing).toBeInstanceOf(Error);
          await closing;
          expect(closedBeforeIO, "target close must fence input return/I/O").toBe(false);
          expect(chargeBeforeIO).toBe(f.limit);
          expect(retired).toBe(true);
          reach.reach(external ? "caller cancellation" : "close cancellation");
          reach.reach("input return fenced");
          reach.reach("pending input remains charged");
        } finally {
          resolve();
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 8, examples: [[true], [false]] },
    );
    reach.assertReached();
  },
  60000,
);
