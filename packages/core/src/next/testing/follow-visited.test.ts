/** Bookkeeping pressure proof for follow provenance, not a substitute for real RI tests.
 * Command/FK/original target identifies a bit across physical versions and shared spill. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { createReach, NATIVE_FOLLOW_VISITED_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { ReferentialEntry } from "../access/statement-command.js";
import { RegistryDirectory, REGISTRY_DIRECTORY_BYTES } from "../owner/registry-directory.js";
import { RegistryTargets, REGISTRY_TARGET_BYTES } from "../owner/registry-targets.js";
import { RegistryFollowing, registryFollowingBytes } from "../owner/registry-following.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { StorageCore } from "./storage-core.js";
for (const seed of seedsFor("next-follow-visited-bitmap", [0xb685, 0xb686]))
  it(`spills bounded command/FK provenance for sparse wide IDs seed ${String(seed)}`, async () => {
    const reach = createReach("follow visited bitmap", NATIVE_FOLLOW_VISITED_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 256, max: 768 }),
        fc.constantFrom(0, 2 ** 52),
        fc.constantFrom(1, 1024, 65537),
        async (count, base, stride) => {
          reach.beginHistory();
          const core = new StorageCore(256);
          await core.open();
          try {
            await core.execute("CREATE TABLE p(id INT PRIMARY KEY)");
            await core.execute(
              "CREATE TABLE c(id INT PRIMARY KEY,a INT REFERENCES p(id),b INT REFERENCES p(id))",
            );
            const catalog = core.harness.path.catalog,
              keys = catalog.tableByName("c")?.foreignKeys;
            if (keys?.length !== 2) throw new Error("FK fixture");
            const limit =
                REGISTRY_DIRECTORY_BYTES +
                REGISTRY_TARGET_BYTES +
                registryFollowingBytes(catalog, 1),
              stop = new AbortController(),
              spill = new MemorySpillService();
            let held = 0,
              peak = 0,
              files = 0;
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
              },
              directory = new RegistryDirectory(
                charge,
                {
                  get bytesInUse() {
                    return spill.bytesInUse;
                  },
                  sweep: () => spill.sweep(),
                  create: (id) => {
                    files++;
                    return spill.create(id);
                  },
                },
                1,
                stop.signal,
              ),
              targets = new RegistryTargets({
                rootId: 31,
                directory,
                charge,
                signal: stop.signal,
                checkpoint: () => undefined,
              }),
              following = new RegistryFollowing({
                rootId: 31,
                frames: 1,
                catalog,
                directory,
                targets,
                charge,
                signal: stop.signal,
                checks: {
                  captureChildren: async () => {
                    throw new Error("No child capture in ledger bookkeeping proof");
                  },
                },
                commands: {
                  active: () => true,
                  verify: async () => {
                    throw new Error("No command checkpoint in ledger bookkeeping proof");
                  },
                  rowReferentialEntries: () => {
                    throw new Error("No event scan in ledger bookkeeping proof");
                  },
                  resolveReferential: async (id): Promise<ReferentialEntry> => {
                    if (id < 1 || id > 3) throw new Error("Foreign fixture entry");
                    const fk = keys[id === 2 ? 1 : 0];
                    if (!fk) throw new Error("Missing FK");
                    return {
                      id,
                      commandId: id === 3 ? 2 ** 52 : 1,
                      frameId: 1,
                      fkId: fk.id,
                      event: "parentUpdate",
                      kind: "follow",
                      sortName: "RI_ConstraintTrigger_a",
                      images: { spoolId: 31, row: id },
                      targets: null,
                      triggerBudget: 1,
                    };
                  },
                },
              });
            try {
              expect(held).toBe(limit);
              for (let entry = 1; entry <= 3; entry++) {
                if (base)
                  expect(await following.testAndSetFollowTarget(entry, 0, stop.signal)).toBe(false);
                for (let i = 0; i < count; i++)
                  expect(
                    await following.testAndSetFollowTarget(entry, base + i * stride, stop.signal),
                  ).toBe(false);
                for (let i = count - 1; i >= 0; i--)
                  expect(
                    await following.testAndSetFollowTarget(entry, base + i * stride, stop.signal),
                  ).toBe(true);
                if (base)
                  expect(await following.testAndSetFollowTarget(entry, 0, stop.signal)).toBe(true);
                expect(held).toBe(limit);
              }
              expect(peak).toBe(limit);
              expect(files).toBeLessThanOrEqual(1);
              if (stride !== 1) {
                expect(files).toBe(1);
                expect(
                  spill.bytesInUse,
                  "sparse target history must spill through the shared registry",
                ).toBeGreaterThan(4096);
                reach.reach("one shared file");
                reach.reach("spilled replay");
              }
              reach.reach(base ? "wide IDs" : "low IDs");
              if (base) reach.reach("low and high coexist");
              reach.reach(stride === 1 ? "dense bits" : "sparse blocks");
              reach.reach("command isolation");
              reach.reach("FK isolation");
              reach.reach("fixed charged metadata");
            } finally {
              await following.close();
              await targets.close();
              await directory.close();
              expect(held).toBe(0);
              expect(spill.bytesInUse).toBe(0);
            }
            reach.reach("zero ownership");
          } finally {
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [256, 0, 1],
          [512, 2 ** 52, 1024],
          [256, 2 ** 52, 65537],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
