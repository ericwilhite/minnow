/** Actual native OLD payload rowsets, separate from SQL trigger enablement. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_CHILD_INPUT_REACH,
  NATIVE_CHILD_INPUT_FENCE_REACH,
} from "../../testing/reach.js";
import { valueAt, decimal, decimals } from "../exec/kernels/test-support.js";
import { batch, ints, texts } from "../write/test-support.js";
import { yieldTask } from "../task.js";
import { RegistryTargets } from "./registry-targets.js";
import { NativeChildInputs, NATIVE_CHILD_INPUT_BYTES } from "./child-inputs.js";
import { prefixFixture } from "./prefix-images.test-support.js";

async function fixture(count: number, width: number, published: boolean) {
  const f = await prefixFixture(Math.max(count, 1), width, published, false),
    targets = new RegistryTargets({
      rootId: 73,
      directory: f.directory,
      charge: f.root,
      signal: f.stop.signal,
      checkpoint: () => undefined,
    }),
    range = await targets.capture(async function* () {
      for (const id of f.ids.slice(0, count)) yield { tableId: f.tableId, rowId: id };
    });
  let steps = 0,
    clock: (() => Promise<void>) | undefined;
  const before = f.root.allocated,
    inputs = new NativeChildInputs({
      rootId: 73,
      statement: f.statement,
      targets,
      images: f.reader,
      directory: f.directory,
      charge: f.root,
      signal: f.stop.signal,
      abortRoot: (reason) => f.stop.abort(reason),
      checkpoint: async () => {
        steps++;
        await clock?.();
      },
    });
  expect(f.root.allocated - before).toBe(NATIVE_CHILD_INPUT_BYTES);
  return {
    ...f,
    range,
    inputs,
    steps: () => steps,
    setClock(run: () => Promise<void>) {
      clock = run;
    },
    async close() {
      await inputs.close();
      await targets.close();
      await f.close();
    },
  };
}

for (const seed of seedsFor("next-native-child-input", [0xb62e, 0xb62f]))
  it(`seals replayable native child payloads with fixed root metadata ${String(seed)}`, async () => {
    const reach = createReach("native child input", NATIVE_CHILD_INPUT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 6 }),
        fc.constantFrom(1, 17, 33),
        fc.constantFrom(0, 24000),
        fc.boolean(),
        fc.boolean(),
        async (count, inputCount, width, published, foreign) => {
          reach.beginHistory();
          const f = await fixture(count, width, published);
          const captures = vi.spyOn(f.reader, "capture"),
            files = vi.spyOn(f.directory.spill, "create");
          try {
            const paid = f.root.allocated,
              handles = [];
            f.setCapturing(true);
            for (let n = 0; n < inputCount; n++) {
              handles.push(await f.inputs.seal(f.tableId, f.range, f.stop.signal));
              expect(f.steps(), "input is complete before a write continuation").toBe(
                count * (n + 1),
              );
              expect(captures, "all OLD payloads are sealed before writes").toHaveBeenCalledTimes(
                count * (n + 1),
              );
              expect(f.source.allocated).toBe(0);
              expect(f.source.loaned).toBe(0);
              expect(f.root.allocated, "all input descriptors have fixed prepaid metadata").toBe(
                paid,
              );
            }
            expect(
              files.mock.calls.length,
              "all child inputs share the root's two spill files",
            ).toBeLessThanOrEqual(2);
            reach.reach("shared files");
            f.setCapturing(false);
            // A real native mutation invalidates every old readOwn data pin. Replays must
            // use archived payloads, never re-read the phase pin or lazily visit the source.
            const command = { rootId: 73, id: 1, frameId: 1 },
              primary = f.statement.openNativeCommands({
                tables: [f.tableId],
                charge: f.root,
                signal: f.stop.signal,
                commandBudget: (token) => {
                  expect(token).toBe(command);
                  return 1;
                },
                finishRoot: async () => undefined,
                closeRoot: async () => undefined,
              }),
              first = handles[0];
            if (!first) throw new Error("Missing native child input");
            for (let i = 0; i < count; i++) {
              const row = await f.inputs.row(first, i, f.stop.signal);
              expect(row.rowId, "sealed native input keeps captured target identity").toBe(
                f.ids[i],
              );
              const prepared = await primary.prepare(
                  command,
                  f.tableId,
                  "update",
                  row.rowId,
                  [0, 0, 5],
                ),
                old = await f.images.readRowImages(row.images, f.stop.signal);
              try {
                await primary.apply(
                  prepared,
                  old.old,
                  batch(
                    ints([i + 1]),
                    decimals(decimal(38, 0), [[f.coefficients[i] ?? 0n, 0]], "wide"),
                    texts(["later"]),
                  ),
                );
              } finally {
                old.release();
              }
            }
            if (count === 0) {
              const prepared = await primary.prepare(command, f.tableId, "insert", null, [0, 0, 5]);
              await primary.apply(
                prepared,
                null,
                batch(ints([999]), decimals(decimal(38, 0), [[999n, 0]]), texts(["later"])),
              );
            }
            for (const handle of handles) {
              const description = await f.inputs.describe(handle, f.stop.signal);
              expect(description).toEqual({ tableId: f.tableId, count });
              for (let i = 0; i < count; i++) {
                const row = await f.inputs.row(handle, i, f.stop.signal);
                expect(row.tableId).toBe(f.tableId);
                expect(row.rowId).toBe(f.ids[i]);
                expect(row.originalId).toBe(f.ids[i]);
                const image = await f.images.readRowImages(row.images, f.stop.signal);
                try {
                  expect(image.old?.columns.map((column) => valueAt(column, 0))).toEqual([
                    i + 1,
                    [f.coefficients[i], 0],
                    f.payloads[i],
                  ]);
                  expect(image.new).toBeNull();
                } finally {
                  image.release();
                }
              }
            }
            expect(
              captures,
              "replay never rereads the mutated native source",
            ).toHaveBeenCalledTimes(count * inputCount);
            const handle = handles[0];
            if (!handle) throw new Error("Missing complete child input");
            if (foreign) {
              // Use a LIVE handle: retirement must not mask a missing root-origin guard.
              await expect(
                f.inputs.describe({ rootId: 74, id: handle.id }, f.stop.signal),
                "a live child input refuses a foreign root",
              ).rejects.toBeInstanceOf(TypeError);
              reach.reach("foreign handle");
            } else {
              const row = count ? await f.inputs.row(handle, 0, f.stop.signal) : null;
              await f.inputs.retire(handle, f.stop.signal);
              if (row) {
                const queued = await f.images.readRowImages(row.images, f.stop.signal);
                try {
                  expect(queued.old?.columns[0] && valueAt(queued.old.columns[0], 0)).toBe(1);
                } finally {
                  queued.release();
                }
                reach.reach("images survive retirement");
              }
              await expect(
                f.inputs.describe(handle, f.stop.signal),
                "a retired child input refuses replay",
              ).rejects.toBeInstanceOf(TypeError);
              reach.reach("retired handle");
            }
            reach.reach(count ? "nonempty" : "empty");
            if (inputCount === 33) reach.reach("many inputs");
            reach.reach(published ? "published" : "resident");
            reach.reach(width ? "fragmented payload" : "small payload");
            reach.reach("complete before continuation");
            reach.reach("replay after mutation");
            reach.reach("fixed metadata");
          } finally {
            captures.mockRestore();
            files.mockRestore();
            await f.statement.closeNativeCommands();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [0, 33, 0, false, true],
          [6, 33, 24000, true, false],
          [3, 17, 0, false, true],
          [2, 1, 24000, false, false],
        ],
      },
    );
    reach.assertReached();
  }, 120000);

for (const seed of seedsFor("next-native-child-input-fence", [0xb630]))
  it(`fences whole native child inputs across source cancellation and phase changes ${String(seed)}`, async () => {
    const reach = createReach("native child input fence", NATIVE_CHILD_INPUT_FENCE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("between rows", "payload IO", "phase change", "IO fault"),
        fc.boolean(),
        fc.boolean(),
        async (kind, close, published) => {
          reach.beginHistory();
          const f = await fixture(3, 24000, published),
            caller = new AbortController(),
            fault = new Error("child input cancelled");
          let entered: () => void = () => undefined,
            resume: () => void = () => undefined,
            closing: Promise<void> | undefined,
            closed = false;
          const ready = new Promise<void>((resolve) => {
              entered = resolve;
            }),
            gate = new Promise<void>((resolve) => {
              resume = resolve;
            });
          let spy: { mockRestore(): void } | undefined;
          if (kind === "payload IO" || kind === "IO fault") {
            const append = f.fragments.append.bind(f.fragments);
            spy = vi.spyOn(f.fragments, "append").mockImplementation(async (...args) => {
              const ref = await append(...args);
              entered();
              await gate;
              if (kind === "IO fault") throw fault;
              return ref;
            });
          } else
            f.setClock(async () => {
              if (f.steps() !== 2) return;
              entered();
              await gate;
            });
          const paid = f.root.allocated,
            pending = f.inputs.seal(f.tableId, f.range, caller.signal).then(
              (ref) => ({ ref, error: null }),
              (error: unknown) => ({ ref: null, error }),
            );
          try {
            await ready;
            if (kind === "phase change") f.statement.beginMutation();
            else if (kind !== "IO fault" && close)
              closing = f.inputs.close().then(() => {
                closed = true;
              });
            else if (kind !== "IO fault") caller.abort(fault);
            await yieldTask();
            const stayedPaid = f.root.allocated >= paid,
              closedEarly = closed;
            resume();
            const result = await pending;
            await closing;
            expect(result.ref, "a partial child input never becomes replayable").toBeNull();
            expect(result.error).toBeInstanceOf(Error);
            if (kind === "phase change")
              expect((result.error as Error).message).toContain("locator-only");
            if (kind === "IO fault") expect(result.error).toBe(fault);
            expect(stayedPaid, "child input metadata stays paid through pending work").toBe(true);
            expect(closedEarly, "child input close fences the pending source").toBe(false);
            expect(f.source.allocated).toBe(0);
            expect(f.source.loaned).toBe(0);
            reach.reach(kind);
            if (kind !== "phase change" && kind !== "IO fault")
              reach.reach(close ? "external close" : "caller cancellation");
            reach.reach("pending metadata");
            reach.reach("no partial handle");
            reach.reach("source retired");
          } finally {
            resume();
            await pending;
            await closing;
            spy?.mockRestore();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          ["between rows", false, false],
          ["between rows", true, true],
          ["payload IO", false, true],
          ["payload IO", true, false],
          ["phase change", false, false],
          ["phase change", false, true],
          ["IO fault", false, true],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
