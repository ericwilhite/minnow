/** Registry ownership/cursor laws. These callbacks test bookkeeping, not native FK execution. */
import fc from "fast-check";
import { expect, it } from "vitest";
import {
  createReach,
  REGISTRY_COMMAND_REACH,
  REGISTRY_COMMAND_IO_REACH,
  REGISTRY_THREE_CHECKPOINT_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { CommandToken, ReferentialEntry, WriteEvent } from "../access/statement-command.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import type { SpillService } from "../spill/contract.js";
import { RegistryDirectory, REGISTRY_DIRECTORY_BYTES } from "./registry-directory.js";
import {
  RegistryCommands,
  registryCommandBytes,
  type AcceptedCommandRow,
} from "./registry-commands.js";

function fixture(rootId = 27) {
  const root = new AbortController(),
    limit = REGISTRY_DIRECTORY_BYTES + registryCommandBytes(3);
  let held = 0,
    files = 0;
  const charge = {
    allocate: (n: number) => {
      if (held + n > limit) return false;
      held += n;
      return true;
    },
    free: (n: number) => {
      held -= n;
      expect(held).toBeGreaterThanOrEqual(0);
    },
  };
  const backing = new MemorySpillService(),
    spill: SpillService = {
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
    commands = new RegistryCommands({
      rootId,
      frames: 3,
      rowEvents: 4,
      directory,
      charge,
      signal: root.signal,
      checkpoint: () => undefined,
      referentialName: (id, event) =>
        `RI_ConstraintTrigger_${event.startsWith("parent") ? "a" : "c"}_${String(id)}_${event}`,
    });
  return {
    root,
    commands,
    limit,
    get held() {
      return held;
    },
    get files() {
      return files;
    },
    get spillBytes() {
      return spill.bytesInUse;
    },
    async close() {
      await commands.close();
      await directory.close();
      expect(held).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    },
  };
}
function row(command: CommandToken, id: number, event: WriteEvent = "insert"): AcceptedCommandRow {
  return {
    command,
    tableId: 7,
    event,
    images: { spoolId: command.rootId, row: id },
    oldRowId: event === "insert" ? null : id,
    newRowId: event === "delete" ? null : id + 100000,
  };
}

/**
 * Accepted rows whose records exceed the fixed metadata (the directory's resident leaves and the
 * command controls) by 1.2x to 2.4x, so they must spill: the range follows the fixed limit.
 */
const MIN_ROWS = Math.ceil((1.2 * (REGISTRY_DIRECTORY_BYTES + registryCommandBytes(3))) / 128);

it.each(seedsFor("next-registry-commands", [0xb5e5]))(
  "checkpoints only newly accepted command work at bounded metadata seed %i",
  async (seed) => {
    const reach = createReach("native command registry", REGISTRY_COMMAND_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: MIN_ROWS, max: 2 * MIN_ROWS }),
        fc.integer({ min: 1, max: 4 }),
        fc.constantFrom(1, 2 ** 52),
        async (count, actions, base) => {
          reach.beginHistory();
          const f = fixture(base > 1 ? base : 27),
            root = f.commands.beginCommand(base + 1, 1),
            body = f.commands.beginCommand(base + 2, 0),
            terminal = f.commands.beginCommand(base + 3, -1);
          let id = base;
          const expected: number[] = [],
            observed: number[] = [];
          const signed: ReferentialEntry[] = [];
          const add = async (command: CommandToken, event: WriteEvent) => {
            const item = row(command, id++, event);
            await f.commands.accept(item);
            expect(await f.commands.accepted(command, item.images)).toEqual(item);
            return item;
          };
          const finish = async (command: CommandToken) => {
            await f.commands.verify(command, "unique", async () => undefined, f.root.signal);
            await f.commands.verify(command, "references", async () => undefined, f.root.signal);
            f.commands.retireCommand(command);
          };
          try {
            expect(Object.isFrozen(root)).toBe(true);
            reach.reach("three live frames");
            expect(() =>
              f.commands.verify(
                { ...root, rootId: root.rootId + 1 },
                "unique",
                async () => undefined,
                f.root.signal,
              ),
            ).toThrow("native command work");
            expect(() => f.commands.beginCommand(4, -1)).toThrow("native command work");
            for (const command of [root, body, terminal]) {
              expect(f.commands.budget(command)).toBe(
                command === root ? 1 : command === body ? 0 : -1,
              );
              const item = await add(command, "update");
              if (command === root) expected.push(item.images.row);
              for (const fkId of [9, 10])
                await f.commands.addReferential(command, item.images, {
                  fkId,
                  event: "parentUpdate",
                  kind: "follow",
                  targets: { spoolId: command.rootId, from: 1, count: 2 },
                });
              for await (const entry of f.commands.rowReferentialEntries(command, item.images)) {
                expect(entry.commandId).toBe(command.id);
                expect(entry.frameId).toBe(command.frameId);
                expect(entry.triggerBudget).toBe(command === root ? 1 : command === body ? 0 : -1);
                expect(entry.sortName).toBe(
                  `RI_ConstraintTrigger_a_${String(entry.fkId)}_parentUpdate`,
                );
                expect(entry.images).toEqual(item.images);
                expect(entry.targets).toEqual({ spoolId: command.rootId, from: 1, count: 2 });
                expect(await f.commands.resolveReferential(entry.id)).toEqual(entry);
                signed.push(entry);
                reach.reach(
                  command === root
                    ? "root budget 1"
                    : command === body
                      ? "body budget 0"
                      : "terminal budget -1",
                );
                reach.reach("immutable RI replay");
              }
            }
            expect(
              signed
                .filter((entry) => entry.commandId === root.id)
                .sort((a, b) => (a.sortName < b.sortName ? -1 : a.sortName > b.sortName ? 1 : 0))
                .map((entry) => entry.fkId),
            ).toEqual([10, 9]);
            await finish(terminal);
            await finish(body);
            // Retired frame IDs may be reused; command IDs remain distinct and old tokens refuse.
            const replacement = f.commands.beginCommand(body.frameId, 0);
            expect(replacement.id).not.toBe(body.id);
            expect(() =>
              f.commands.verify(body, "unique", async () => undefined, f.root.signal),
            ).toThrow("native command work");
            reach.reach("retired command refused");
            await finish(replacement);
            for (let n = 0; n < count; n++)
              expected.push(
                (await add(root, n % 3 === 0 ? "insert" : n % 3 === 1 ? "update" : "delete")).images
                  .row,
              );
            await f.commands.verify(
              root,
              "unique",
              async (item) => {
                observed.push(item.images.row);
              },
              f.root.signal,
            );
            expect(observed).toEqual(expected);
            for (let action = 0; action < actions; action++) {
              const next = await add(root, "update");
              expected.push(next.images.row);
              await f.commands.verify(
                root,
                "unique",
                async (item) => {
                  observed.push(item.images.row);
                },
                f.root.signal,
              );
              expect(observed).toEqual(expected);
              reach.reach("incremental action uniqueness");
            }
            await f.commands.verify(
              root,
              "unique",
              async () => {
                throw new Error("old accepted work was checked twice");
              },
              f.root.signal,
            );
            const references: number[] = [];
            await f.commands.verify(
              root,
              "references",
              async (item) => {
                references.push(item.images.row);
              },
              f.root.signal,
            );
            expect(references).toEqual(expected);
            reach.reach("separate reference cursor");
            const activeEntry = signed.find((entry) => entry.commandId === root.id);
            if (!activeEntry) throw new Error("Missing active RI descriptor");
            await f.commands.retireReferential(activeEntry.id);
            expect(f.held).toBe(f.limit);
            expect(f.files).toBe(1);
            expect(f.spillBytes).toBeGreaterThan(f.limit);
            await expect(f.commands.resolveReferential(activeEntry.id)).rejects.toThrow(
              "native command work",
            );
            expect(count * 128).toBeGreaterThan(f.limit);
            reach.reach("retired RI refused");
            reach.reach("spilled accepted rows");
            reach.reach("fixed metadata");
            if (base > 1) reach.reach("wide row IDs");
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
          [MIN_ROWS, 3, 1],
          [2 * MIN_ROWS, 4, 2 ** 52],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-command-io", [0xb5e6]))(
  "fences command verification callbacks before retirement seed %i",
  async (seed) => {
    const reach = createReach("native command verification lifecycle", REGISTRY_COMMAND_IO_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (external) => {
        reach.beginHistory();
        const f = fixture(),
          command = f.commands.beginCommand(1, 1);
        let entered!: () => void,
          resolve!: () => void,
          completed = false,
          retired = false;
        const ready = new Promise<void>((r) => {
            entered = r;
          }),
          gate = new Promise<void>((r) => {
            resolve = r;
          });
        await f.commands.accept(row(command, 1));
        const verifying = f.commands
          .verify(
            command,
            "unique",
            async (_, signal) => {
              try {
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
            },
            f.root.signal,
          )
          .then(
            () => null,
            (error: unknown) => error,
          );
        try {
          await ready;
          if (external) f.root.abort(new Error("cancel native verification"));
          const closing = f.commands.close().then(() => {
            completed = true;
          });
          for (let turn = 0; turn < 8; turn++) await Promise.resolve();
          const earlyClose = completed,
            paid = f.held;
          resolve();
          expect(await verifying).toBeInstanceOf(Error);
          await closing;
          expect(earlyClose, "registry close must fence command verification callback").toBe(false);
          expect(paid).toBe(f.limit);
          expect(retired).toBe(true);
          reach.reach(external ? "caller cancellation" : "close cancellation");
          reach.reach("verification IO fenced");
          reach.reach("metadata remains paid");
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

for (const seed of seedsFor("next-registry-three-checkpoints", [0xb621, 0xb622]))
  it(`keeps unique, follow input and reference watermarks independent ${String(seed)}`, async () => {
    const reach = createReach("three native checkpoints", REGISTRY_THREE_CHECKPOINT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 5 }),
        fc.integer({ min: 1, max: 48 }),
        fc.shuffledSubarray(["unique", "followTargets", "references"] as const, {
          minLength: 3,
          maxLength: 3,
        }),
        fc.boolean(),
        async (rounds, count, order, wide) => {
          reach.beginHistory();
          const base = wide ? 2 ** 52 : 1,
            f = fixture(base),
            command = f.commands.beginCommand(base + 1, 1);
          const expected: number[] = [],
            seen: { unique: number[]; followTargets: number[]; references: number[] } = {
              unique: [],
              followTargets: [],
              references: [],
            };
          let id = base;
          try {
            for (let round = 0; round < rounds; round++) {
              for (let i = 0; i < count; i++) {
                const item = row(command, id++);
                await f.commands.accept(item);
                expected.push(item.images.row);
              }
              for (const kind of order) {
                await f.commands.verify(
                  command,
                  kind,
                  async (item) => {
                    seen[kind].push(item.images.row);
                  },
                  f.root.signal,
                );
                expect(seen[kind], `${kind} sees every newly accepted row independently`).toEqual(
                  expected,
                );
                await f.commands.verify(
                  command,
                  kind,
                  async () => {
                    throw new Error("checkpoint revisited its completed prefix");
                  },
                  f.root.signal,
                );
              }
              expect(f.held).toBe(f.limit);
            }
            f.commands.retireCommand(command);
            for (const kind of order) reach.reach(kind);
            reach.reach(
              order.indexOf("followTargets") < order.indexOf("references")
                ? "follow before references"
                : "references before follow",
            );
            reach.reach(wide ? "wide IDs" : "small IDs");
            reach.reach("incremental prefixes");
            reach.reach("no completed-prefix replay");
          } finally {
            await f.close();
          }
          reach.reach("fixed metadata");
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [3, 4, ["unique", "followTargets", "references"], false],
          [4, 3, ["references", "followTargets", "unique"], true],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
