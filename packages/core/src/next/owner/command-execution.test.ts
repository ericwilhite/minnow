/** Boundary orchestration/lifetime tests. Action discovery and image reads are fixtures;
 * command-primary.test.ts separately exercises real native writes/checks against PG. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import {
  createReach,
  NATIVE_BOUNDARY_REACH,
  NATIVE_BOUNDARY_FENCE_REACH,
  NATIVE_BOUNDARY_LISTENER_REACH,
  NATIVE_BOUNDARY_NESTING_REACH,
  NATIVE_BOUNDARY_IMAGE_CANCEL_REACH,
  NATIVE_BOUNDARY_FOLLOW_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type {
  CommandToken,
  NativeRowHooks,
  ReferentialEntry,
  RowImageLease,
} from "../access/statement-command.js";
import { ROW_IMAGE_UNREAD } from "../access/statement-command.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import {
  Harness,
  autocommit,
  tableInput,
  column,
  INT,
  batch,
  ints,
} from "../write/test-support.js";
import { RegistryDirectory, REGISTRY_DIRECTORY_BYTES } from "./registry-directory.js";
import {
  RegistryCommands,
  registryCommandBytes,
  type AcceptedCommandRow,
} from "./registry-commands.js";
import {
  NativeCommandExecution,
  nativeCommandExecutionBytes,
  type NativeCommandExecutionScope,
} from "./command-execution.js";

const hooks: NativeRowHooks = {
  beforeRow: async () => {
    throw new Error("Unexpected outer BEFORE drain");
  },
  afterRow: async () => {
    throw new Error("Unexpected outer AFTER drain");
  },
};
async function fixture(frames = 3) {
  const h = new Harness("strict", "memory", 8),
    path = await h.open();
  let tableId = 0;
  await autocommit(path, async (s) => {
    tableId =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("b", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  const stop = new AbortController(),
    minimum =
      REGISTRY_DIRECTORY_BYTES + registryCommandBytes(frames) + nativeCommandExecutionBytes(frames),
    grant = await h.governor.admit({
      label: "native boundary",
      minimumBytes: minimum,
      spillable: true,
    }),
    spill = new MemorySpillService(),
    directory = new RegistryDirectory(grant, spill, 1, stop.signal),
    commands = new RegistryCommands({
      rootId: 51,
      frames,
      rowEvents: 3,
      directory,
      charge: grant,
      signal: stop.signal,
      checkpoint: () => undefined,
      referentialName: (fk) => `RI_ConstraintTrigger_a_${String(fk)}`,
    });
  let nextRow = 1,
    unique = 0,
    released = 0,
    active: RowImageLease | null = null,
    reading: (() => Promise<void>) | null = null,
    checking: (() => Promise<void>) | null = null,
    following: ((command: CommandToken, signal: AbortSignal) => Promise<void>) | null = null,
    action: NativeCommandExecutionScope["runReferential"] = async () => undefined;
  const image = async () => {
    await reading?.();
    let ended = false;
    const lease: RowImageLease = {
      tableId,
      old: null,
      new: batch(ints([1])),
      release() {
        if (ended) return;
        ended = true;
        released++;
        if (active === lease) active = null;
      },
    };
    active = lease;
    return lease;
  };
  const native = new NativeCommandExecution({
    frames,
    catalog: path.catalog,
    commands,
    charge: grant,
    signal: stop.signal,
    abortRoot: (reason) => stop.abort(reason),
    images: {
      readRowImages: image,
      readRowBindings: async (_ref, projection) => {
        const lease = await image();
        return {
          tableId,
          old: null,
          new: projection.new.length ? (lease.new?.columns ?? null) : [ROW_IMAGE_UNREAD],
          release: () => lease.release(),
        };
      },
      close: async () => {
        active?.release();
      },
    },
    checks: {
      referenceMustCheck: async () => true,
      verifyUnique: async () => {
        unique++;
        await checking?.();
      },
      verifyReference: async () => {
        throw new Error("No child FK in boundary fixture");
      },
      close: async () => undefined,
    },
    following: {
      prepare: async (command, signal) => {
        await following?.(command, signal);
      },
      close: async () => undefined,
    },
    runReferential: async (...args) => await action(...args),
  });
  return {
    native,
    commands,
    grant,
    minimum,
    stop,
    get unique() {
      return unique;
    },
    get released() {
      return released;
    },
    setRead(work: () => Promise<void>) {
      reading = work;
    },
    setCheck(work: () => Promise<void>) {
      checking = work;
    },
    setFollow(work: (command: CommandToken, signal: AbortSignal) => Promise<void>) {
      following = work;
    },
    setAction(work: NativeCommandExecutionScope["runReferential"]) {
      action = work;
    },
    async accept(command: CommandToken) {
      const row: AcceptedCommandRow = {
        command,
        tableId,
        event: "insert",
        images: { spoolId: command.rootId, row: nextRow++ },
        oldRowId: null,
        newRowId: nextRow,
      };
      await commands.accept(row);
      return row;
    },
    async entry(command: CommandToken) {
      const row = await this.accept(command),
        id = await commands.addReferential(command, row.images, {
          fkId: 1,
          event: "parentDelete",
          kind: "cascade",
          targets: { spoolId: command.rootId, from: 1, count: 2 },
        });
      return await commands.resolveReferential(id);
    },
    async close() {
      await native.close();
      await directory.close();
      expect(grant.allocated).toBe(0);
      grant.close();
      await h.close();
      expect(h.governor.usedBytes).toBe(0);
      expect(spill.bytesInUse).toBe(0);
    },
  };
}
function forged(entry: ReferentialEntry, field: string): ReferentialEntry {
  switch (field) {
    case "fk":
      return { ...entry, fkId: entry.fkId + 1 };
    case "kind":
      return { ...entry, kind: "setNull" };
    case "budget":
      return { ...entry, triggerBudget: entry.triggerBudget + 1 };
    case "image":
      return { ...entry, images: { ...entry.images, row: entry.images.row + 1 } };
    case "target":
      return { ...entry, targets: { spoolId: entry.images.spoolId, from: 2, count: 2 } };
    default:
      return { ...entry, sortName: `${entry.sortName}x` };
  }
}
it.each(seedsFor("next-native-boundary", [0xb603]))(
  "resolves immutable action IDs and retires complete commands seed %i",
  async (seed) => {
    const reach = createReach("native boundary", NATIVE_BOUNDARY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("id", "entry", "forged", "retired", "foreign"),
        fc.constantFrom("fk", "kind", "budget", "image", "target", "name"),
        fc.constantFrom(-1, 0, 1),
        async (route, field, budget) => {
          reach.beginHistory();
          const f = await fixture(),
            command = f.native.beginCommand(1, budget),
            entry = await f.entry(command);
          let calls = 0;
          f.setAction(async (canonical) => {
            expect(canonical).toEqual(entry);
            calls++;
          });
          try {
            if (route === "forged" || route === "foreign") {
              const pending =
                route === "forged"
                  ? f.native.executeReferential(forged(entry, field), hooks, f.stop.signal)
                  : f.native.executeReferentialId(entry.id + 2 ** 50, hooks, f.stop.signal);
              await expect(pending).rejects.toThrow(/native command/);
              expect(calls).toBe(0);
              reach.reach("invalid before effects");
            } else {
              if (route === "entry") await f.native.executeReferential(entry, hooks, f.stop.signal);
              else await f.native.executeReferentialId(entry.id, hooks, f.stop.signal);
              expect(calls).toBe(1);
              expect(f.unique).toBe(1);
              if (route === "retired") {
                await expect(
                  f.native.executeReferentialId(entry.id, hooks, f.stop.signal),
                ).rejects.toThrow(/native command/);
                expect(calls).toBe(1);
                reach.reach("retired action refused");
              } else {
                await f.native.verifyCommandReferences(command, f.stop.signal);
                expect(() => f.commands.budget(command)).toThrow("native command work");
                const replacement = f.native.beginCommand(1, budget);
                expect(replacement.id).not.toBe(command.id);
                reach.reach("command retired and reused");
              }
            }
            reach.reach(route);
            reach.reach(budget === -1 ? "budget-1" : budget === 0 ? "budget0" : "budget1");
          } finally {
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          ["id", "fk", -1],
          ["entry", "kind", 0],
          ["forged", "target", 1],
          ["retired", "name", 1],
          ["foreign", "image", 1],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
it.each(seedsFor("next-native-boundary-fence", [0xb604]))(
  "cancels root waits and fences late image/check/action/follow completion seed %i",
  async (seed) => {
    const reach = createReach("native boundary fence", NATIVE_BOUNDARY_FENCE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("image", "check", "action", "follow"),
        fc.boolean(),
        async (kind, callerCancel) => {
          reach.beginHistory();
          const f = await fixture(),
            command = f.native.beginCommand(1, 1),
            entry = await f.entry(command),
            caller = new AbortController();
          let enter!: () => void, resume!: () => void;
          const entered = new Promise<void>((r) => {
              enter = r;
            }),
            gate = new Promise<void>((r) => {
              resume = r;
            });
          const pause = async () => {
            enter();
            await gate;
          };
          if (kind === "image") f.setRead(pause);
          else if (kind === "check") f.setCheck(pause);
          else if (kind === "follow") f.setFollow(pause);
          else f.setAction(pause);
          let closing: Promise<void> | null = null;
          let recursive: Promise<void> | null = null;
          const reenter = () => {
            recursive = f.native.close();
          };
          f.stop.signal.addEventListener("abort", reenter, { once: true });
          try {
            const work =
              kind === "image"
                ? f.native.readRowImages(entry.images, caller.signal)
                : kind === "check" || kind === "follow"
                  ? f.native.verifyCommandUnique(command, caller.signal)
                  : f.native.executeReferentialId(entry.id, hooks, caller.signal);
            const rejected = expect(work).rejects.toThrow();
            await entered;
            if (callerCancel) {
              caller.abort(new Error("cancel native boundary"));
              expect(f.stop.signal.aborted).toBe(true);
              reach.reach("caller cancels root");
            }
            let closed = false;
            const closeWork = f.native.close();
            closing = closeWork.then(() => {
              closed = true;
            });
            for (let k = 0; k < 8; k++) await Promise.resolve();
            expect(recursive).toBe(closeWork);
            reach.reach("reentrant close shares fence");
            expect(closed).toBe(false);
            expect(f.grant.allocated).toBe(f.minimum);
            resume();
            await rejected;
            await closing;
            if (kind === "image") {
              expect(f.released).toBe(1);
              reach.reach("undelivered lease released");
            }
            reach.reach(kind);
            reach.reach("IO fence before credit return");
          } finally {
            resume();
            f.stop.signal.removeEventListener("abort", reenter);
            await closing;
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          ["image", true],
          ["check", true],
          ["action", true],
          ["image", false],
          ["follow", true],
          ["follow", false],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

for (const seed of seedsFor("next-native-boundary-follow", [0xb627, 0xb628]))
  it(`prepares original follow targets after each successful unique prefix ${String(seed)}`, async () => {
    const reach = createReach("native follow checkpoint", NATIVE_BOUNDARY_FOLLOW_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 4 }),
        fc.boolean(),
        fc.constantFrom(-1, 0, 1),
        async (count, reject, budget) => {
          reach.beginHistory();
          const f = await fixture(1),
            command = f.native.beginCommand(1, budget),
            entries: ReferentialEntry[] = [],
            trace: string[] = [];
          let preparations = 0,
            fail = false;
          const uniqueError = Object.assign(new Error("native child duplicate"), {
            sqlState: "23505",
          });
          try {
            for (let i = 0; i < count; i++) entries.push(await f.entry(command));
            f.setCheck(async () => {
              trace.push("unique");
              if (fail) throw uniqueError;
            });
            f.setFollow(async (token, signal) => {
              expect(token).toEqual(command);
              signal.throwIfAborted();
              trace.push("follow");
              preparations++;
            });
            await f.native.verifyCommandUnique(command, f.stop.signal);
            expect(trace, "unique checks precede original identity capture").toEqual([
              ...Array.from({ length: count }, () => "unique"),
              "follow",
            ]);
            expect(preparations).toBe(1);
            reach.reach("root preparation");
            for (let i = 0; i < count; i++) {
              const willFail = reject && i === count - 1;
              trace.length = 0;
              f.setAction(async () => {
                trace.push("action");
                await f.accept(command);
                fail = willFail;
              });
              const entry = entries[i];
              if (!entry) throw new Error("Missing native action");
              const error = await f.native
                .executeReferentialId(entry.id, hooks, f.stop.signal)
                .then(
                  () => null,
                  (reason: unknown) => reason,
                );
              expect(error, "incremental child uniqueness controls the continuation").toBe(
                willFail ? uniqueError : null,
              );
              expect(trace, "same-token actions prepare only after successful uniqueness").toEqual(
                willFail ? ["action", "unique"] : ["action", "unique", "follow"],
              );
              if (willFail) reach.reach("failed uniqueness skips capture");
              else reach.reach("successful action preparation");
            }
            expect(
              preparations,
              "later accepted work cannot use a token-only preparation cache",
            ).toBe(count + (reject ? 0 : 1));
            expect(f.unique).toBe(2 * count);
            if (!reject) await f.native.verifyCommandReferences(command, f.stop.signal);
            reach.reach("uniqueness before capture");
            reach.reach("same token later rows");
            if (count > 1) reach.reach("multiple actions");
            reach.reach(budget === -1 ? "budget-1" : budget === 0 ? "budget0" : "budget1");
          } finally {
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 12,
        examples: [
          [1, false, -1],
          [4, false, 0],
          [3, true, 1],
          [1, true, -1],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
it.each(seedsFor("next-native-boundary-listeners", [0xb605]))(
  "removes settled operation listeners instead of retaining row-count history seed %i",
  async (seed) => {
    const reach = createReach("native boundary listeners", NATIVE_BOUNDARY_LISTENER_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 16, max: 48 }), fc.boolean(), async (count, external) => {
        reach.beginHistory();
        const f = await fixture(),
          command = f.native.beginCommand(1, 1),
          caller = new AbortController();
        // Include the private boundary-close signal, not just the exposed caller/root.
        const active = new Map<AbortSignal, Set<unknown>>();
        const adds = vi
          .spyOn(AbortSignal.prototype, "addEventListener")
          .mockImplementation(function (this: AbortSignal, type, listener, options) {
            if (type === "abort") {
              let listeners = active.get(this);
              if (!listeners) {
                listeners = new Set();
                active.set(this, listeners);
              }
              listeners.add(listener);
            }
            EventTarget.prototype.addEventListener.call(this, type, listener, options);
          });
        const removes = vi
          .spyOn(AbortSignal.prototype, "removeEventListener")
          .mockImplementation(function (this: AbortSignal, type, listener, options) {
            if (type === "abort") {
              const listeners = active.get(this);
              listeners?.delete(listener);
              if (listeners?.size === 0) active.delete(this);
            }
            EventTarget.prototype.removeEventListener.call(this, type, listener, options);
          });
        try {
          for (let k = 0; k < count; k++) {
            await f.native.verifyCommandUnique(command, external ? caller.signal : f.stop.signal);
            expect(active.size).toBe(0);
          }
          expect(adds).toHaveBeenCalled();
          expect(f.grant.allocated).toBe(f.minimum);
          reach.reach(external ? "external signal" : "root signal");
          reach.reach("no settled listener history");
          reach.reach("constant metadata");
        } finally {
          adds.mockRestore();
          removes.mockRestore();
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      {
        seed,
        numRuns: 6,
        examples: [
          [16, false],
          [48, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-native-boundary-nesting", [0xb607]))(
  "allows reachable nested body frames but forbids synchronous outer action drain seed %i",
  async (seed) => {
    const reach = createReach("native boundary nesting", NATIVE_BOUNDARY_NESTING_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 3 }), fc.boolean(), async (depth, badDrain) => {
        reach.beginHistory();
        const f = await fixture(depth),
          root = f.native.beginCommand(1, 1),
          entry = await f.entry(root);
        const budgets: number[] = [];
        let nextFrame = 2;
        try {
          if (badDrain) {
            const id = await f.commands.addReferential(root, entry.images, {
              fkId: 2,
              event: "parentDelete",
              kind: "cascade",
              targets: null,
            });
            f.setAction(async (action, rowHooks, stop) => {
              budgets.push(action.triggerBudget);
              if (action.id === entry.id) await f.native.executeReferentialId(id, rowHooks, stop);
            });
            await expect(f.native.executeReferential(entry, hooks, f.stop.signal)).rejects.toThrow(
              "native command execution",
            );
            expect(budgets).toEqual([1]);
            reach.reach("outer action drain refused before child effects");
          } else {
            f.setAction(async (action, rowHooks, stop) => {
              budgets.push(action.triggerBudget);
              if (nextFrame <= depth) {
                const child = f.native.beginCommand(nextFrame++, action.triggerBudget - 1),
                  childEntry = await f.entry(child);
                await f.native.executeReferentialId(childEntry.id, rowHooks, stop);
                await f.native.verifyCommandReferences(child, stop);
              }
            });
            await f.native.executeReferentialId(entry.id, hooks, f.stop.signal);
            await f.native.verifyCommandReferences(root, f.stop.signal);
            expect(budgets).toEqual(Array.from({ length: depth }, (_, i) => 1 - i));
            expect(f.unique).toBe(depth);
            expect(f.grant.allocated).toBe(f.minimum);
            reach.reach(
              depth === 1
                ? "one live frame"
                : depth === 2
                  ? "two live frames"
                  : "three live frames",
            );
            if (depth === 3) reach.reach("RI preserves budget-1");
          }
        } finally {
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, false],
          [2, false],
          [3, false],
          [3, true],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-native-boundary-image-cancel", [0xb608]))(
  "releases an undelivered full or projected lease before cancellation returns seed %i",
  async (seed) => {
    const reach = createReach(
      "native boundary image cancellation",
      NATIVE_BOUNDARY_IMAGE_CANCEL_REACH,
    );
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (projected) => {
        reach.beginHistory();
        const f = await fixture(1),
          command = f.native.beginCommand(1, 1),
          row = await f.accept(command),
          caller = new AbortController();
        let enter!: () => void, resume!: () => void;
        const entered = new Promise<void>((r) => {
            enter = r;
          }),
          gate = new Promise<void>((r) => {
            resume = r;
          });
        f.setRead(async () => {
          enter();
          await gate;
        });
        try {
          const work = projected
            ? f.native.readRowBindings(row.images, { old: [], new: [1] }, caller.signal)
            : f.native.readRowImages(row.images, caller.signal);
          const rejected = expect(work).rejects.toThrow("cancel before lease delivery");
          await entered;
          caller.abort(new Error("cancel before lease delivery"));
          resume();
          await rejected;
          expect(f.released).toBe(1);
          expect(f.grant.allocated).toBe(f.minimum);
          reach.reach(projected ? "projected lease" : "full lease");
          reach.reach("released before boundary close");
        } finally {
          resume();
          await f.close();
        }
        expect(f.released).toBe(1);
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 6, examples: [[false], [true]] },
    );
    reach.assertReached();
  },
  60000,
);
