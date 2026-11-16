/** Native SQL dedup belongs to a command, not the containing trigger/RI statement.
 * These are registry bookkeeping/lifetime tests; no SQL trigger route is enabled. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import {
  createReach,
  REGISTRY_COMMAND_TARGET_REACH,
  REGISTRY_COMMAND_TARGET_PIN_REACH,
  REGISTRY_COMMAND_TARGET_FENCE_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { RegistryDirectory, REGISTRY_DIRECTORY_BYTES } from "./registry-directory.js";
import { RegistryCommands, registryCommandBytes } from "./registry-commands.js";
import { Harness } from "../write/test-support.js";

async function fixture() {
  const h = new Harness("strict", "memory", 8),
    path = await h.open(),
    pin = path.pin(),
    root = new AbortController(),
    minimum = REGISTRY_DIRECTORY_BYTES + registryCommandBytes(3),
    grant = await h.governor.admit({
      label: "command repeat metadata",
      minimumBytes: minimum,
      spillable: true,
    }),
    backing = new MemorySpillService();
  let files = 0;
  const directory = new RegistryDirectory(
    grant,
    {
      create: (id) => {
        files++;
        return backing.create(id);
      },
      sweep: () => backing.sweep(),
      get bytesInUse() {
        return backing.bytesInUse;
      },
    },
    1,
    root.signal,
  );
  const commands = new RegistryCommands({
    rootId: 31,
    frames: 3,
    rowEvents: 0,
    directory,
    charge: grant,
    signal: root.signal,
    checkpoint: () => undefined,
    referentialName: () => {
      throw new Error("No referential entry in target fixture");
    },
  });
  return {
    h,
    path,
    pin,
    root,
    minimum,
    grant,
    directory,
    commands,
    get files() {
      return files;
    },
    async close() {
      await commands.close();
      await directory.close();
      pin.release();
      expect(grant.allocated).toBe(0);
      grant.close();
      await h.close();
      expect(h.governor.usedBytes).toBe(0);
      expect(backing.bytesInUse).toBe(0);
    },
  };
}

it.each(seedsFor("next-registry-command-targets", [0xb5fe, 0xb5ff]))(
  "isolates paged repeat identities across live/reused frames seed %i",
  async (seed) => {
    const reach = createReach("command target identities", REGISTRY_COMMAND_TARGET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 20, max: 32 }),
        fc.constantFrom(0, 2 ** 52),
        fc.integer({ min: 0, max: 1023 }),
        async (count, base, offset) => {
          reach.beginHistory();
          const f = await fixture(),
            root = f.commands.beginCommand(1, 1),
            body = f.commands.beginCommand(2, 0),
            terminal = f.commands.beginCommand(3, -1);
          const first = base + offset;
          const mark = (command: typeof root, table: number, id: number) =>
            f.commands.testAndSetTarget(command, table, id, f.pin, f.root.signal);
          try {
            for (const command of [root, body, terminal]) {
              for (let k = 0; k < count; k++) {
                for (const delta of [0, 1, 7, 8, 1023]) {
                  const id = first + 16384 * k + delta;
                  expect(await mark(command, 1, id)).toBe(false);
                  expect(await mark(command, 2, id)).toBe(false);
                }
              }
              // More than eight directory leaves force a cold revisit of early row bits.
              for (let k = 0; k < count; k++) {
                for (const delta of [0, 1, 7, 8, 1023]) {
                  const id = first + 16384 * k + delta;
                  expect(await mark(command, 1, id)).toBe(true);
                  expect(await mark(command, 2, id)).toBe(true);
                }
              }
              expect(f.grant.allocated).toBe(f.minimum);
            }
            f.commands.retireCommand(terminal);
            f.commands.retireCommand(body);
            for (let k = 0; k < count; k++) {
              const next = f.commands.beginCommand(2, 0);
              expect(await mark(next, 1, first)).toBe(false);
              expect(await mark(next, 1, first)).toBe(true);
              f.commands.retireCommand(next);
              expect(await mark(root, 1, first)).toBe(true);
              expect(f.grant.allocated).toBe(f.minimum);
            }
            expect(() => mark(body, 1, first)).toThrow("native command work");
            expect(f.files).toBe(1);
            reach.reach("three live commands");
            reach.reach("table isolation");
            reach.reach("command isolation");
            reach.reach("reused frame");
            reach.reach("ancestor survives child");
            reach.reach("cold bitmap revisit");
            if (base) reach.reach("wide native ID");
            if (base === 0 && offset === 0) reach.reach("zero native ID");
            reach.reach("neighbor bits and block boundary");
            reach.reach("one shared file");
            reach.reach("constant metadata");
          } finally {
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [20, 0, 0],
          [24, 2 ** 52, 1023],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it.each(seedsFor("next-registry-command-target-pin", [0xb600]))(
  "rechecks the final target pin after every directory await seed %i",
  async (seed) => {
    const reach = createReach("command target pin", REGISTRY_COMMAND_TARGET_PIN_REACH);
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(1, 2, 3, 4), async (boundary) => {
        reach.beginHistory();
        const f = await fixture(),
          command = f.commands.beginCommand(1, 1);
        let enter!: () => void,
          resume!: () => void,
          calls = 0;
        const entered = new Promise<void>((r) => {
            enter = r;
          }),
          gate = new Promise<void>((r) => {
            resume = r;
          });
        const pause = async () => {
          if (++calls === boundary) {
            enter();
            await gate;
          }
        };

        const get = f.directory.get.bind(f.directory),
          put = f.directory.put.bind(f.directory);
        const read = vi.spyOn(f.directory, "get").mockImplementation(async function (...args) {
          const result = await get(...args);
          await pause();
          return result;
        });
        const write = vi.spyOn(f.directory, "put").mockImplementation(async function (...args) {
          await put(...args);
          await pause();
        });
        try {
          let accepted = 0;
          const attempt = f.commands
            .testAndSetTarget(command, 1, 0, f.pin, f.root.signal)
            .then(() => {
              accepted++;
            });
          // Attach rejection handling before releasing the native pin.
          const rejected = expect(attempt).rejects.toThrow(/released/i);
          await entered;
          f.pin.release();
          resume();
          await rejected;
          expect(accepted).toBe(0);
          reach.reach(
            boundary === 1
              ? "boundary1"
              : boundary === 2
                ? "boundary2"
                : boundary === 3
                  ? "boundary3"
                  : "boundary4",
          );
          reach.reach("one final target");
          reach.reach("no accepted mutation");
        } finally {
          resume();
          read.mockRestore();
          write.mockRestore();
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 8, examples: [[1], [2], [3], [4]] },
    );
    reach.assertReached();
  },
  60000,
);

it.each(seedsFor("next-registry-command-target-fence", [0xb601]))(
  "fences cancelled, retired and closed commands at the final target await seed %i",
  async (seed) => {
    const reach = createReach("command target fence", REGISTRY_COMMAND_TARGET_FENCE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("cancel", "retire", "close"),
        fc.integer({ min: 1, max: 4 }),
        async (action, boundary) => {
          reach.beginHistory();
          const f = await fixture(),
            command = f.commands.beginCommand(1, 1);
          let enter!: () => void,
            resume!: () => void,
            calls = 0;
          const entered = new Promise<void>((r) => {
              enter = r;
            }),
            gate = new Promise<void>((r) => {
              resume = r;
            });
          const pause = async () => {
            if (++calls === boundary) {
              enter();
              await gate;
            }
          };
          const get = f.directory.get.bind(f.directory),
            put = f.directory.put.bind(f.directory);
          const read = vi.spyOn(f.directory, "get").mockImplementation(async (...args) => {
            const result = await get(...args);
            await pause();
            return result;
          });
          const write = vi.spyOn(f.directory, "put").mockImplementation(async (...args) => {
            await put(...args);
            await pause();
          });
          let closing: Promise<void> | null = null;
          try {
            let accepted = 0;
            const attempt = f.commands
              .testAndSetTarget(command, 1, 0, f.pin, f.root.signal)
              .then(() => {
                accepted++;
              });
            const rejected = expect(attempt).rejects.toThrow(
              action === "cancel" ? "cancel command target" : "native command work",
            );
            await entered;
            if (action === "cancel") f.root.abort(new Error("cancel command target"));
            else if (action === "retire") f.commands.retireCommand(command);
            else {
              let closed = false;
              closing = f.commands.close().then(() => {
                closed = true;
              });
              for (let k = 0; k < 8; k++) await Promise.resolve();
              expect(closed).toBe(false);
              expect(f.grant.allocated).toBe(f.minimum);
              reach.reach("close retains metadata through IO");
            }
            resume();
            await rejected;
            await closing;
            expect(accepted).toBe(0);
            reach.reach(action);
            if (boundary === 4) reach.reach(`${action} on final await`);
            reach.reach("no accepted mutation");
          } finally {
            resume();
            read.mockRestore();
            write.mockRestore();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          ["cancel", 4],
          ["retire", 4],
          ["close", 4],
        ],
      },
    );
    reach.assertReached();
  },
  60000,
);
