/** The assembled graph fences its complete root source and end tail before it closes
 * native pins, registries and caller grants. Setup failure never closes a foreign graph. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  NATIVE_COMMAND_ROOT_LIFETIME_REACH,
  NATIVE_COMMAND_ROOT_FOREIGN_REACH,
} from "../../testing/reach.js";
import { NativeBodyHost } from "../owner/body-host.js";
import { NativeCommandRoot } from "../owner/command-root.js";
import { yieldTask } from "../task.js";
import { commandRootFixture } from "./command-root.test-support.js";
const setup = ["CREATE TABLE t(id INT PRIMARY KEY,v INT)", "INSERT INTO t VALUES(1,10),(2,20)"];
for (const seed of seedsFor("next-native-command-root-lifetime", [0xb677, 0xb678]))
  it(`fences root source and complete end before teardown seed ${String(seed)}`, async () => {
    const reach = createReach("native root lifetime", NATIVE_COMMAND_ROOT_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.boolean(), async (ending, published) => {
        reach.beginHistory();
        const f = await commandRootFixture(setup, "UPDATE t SET v=v+1", published);
        let enter: () => void = () => undefined,
          resume: () => void = () => undefined;
        const gate = new Promise<void>((r) => {
            resume = r;
          }),
          entered = new Promise<void>((r) => {
            enter = r;
          });
        let settled = false,
          closed = false;
        // eslint-disable-next-line @typescript-eslint/unbound-method -- forwarded with its original receiver via call below.
        const source = NativeBodyHost.prototype.spoolRootInput,
          originalEnd = f.statement.end.bind(f.statement);
        const spy = ending
          ? vi.spyOn(f.statement, "end").mockImplementation(async (signal) => {
              await originalEnd(signal);
              enter();
              await gate;
              signal?.throwIfAborted();
            })
          : vi.spyOn(NativeBodyHost.prototype, "spoolRootInput").mockImplementation(async function (
              this: NativeBodyHost,
              request,
            ) {
              const input = await source.call(this, request);
              enter();
              await gate;
              return input;
            });
        try {
          const operation = (
            ending
              ? (async () => {
                  expect(await f.root.run({ slots: [] })).toBe(2);
                  await f.root.end();
                })()
              : f.root.run({ slots: [] })
          ).finally(() => {
            settled = true;
          });
          void operation.catch(() => undefined);
          await entered;
          const closing = f.root.close().then(() => {
            closed = true;
          });
          void closing.catch(() => undefined);
          await yieldTask();
          expect(closed).toBe(false);
          expect(settled).toBe(false);
          resume();
          await expect(operation).rejects.toBeInstanceOf(Error);
          await closing;
          await f.retire();
          const result = await f.core.execute("SELECT id,v FROM t ORDER BY id");
          if (!("batches" in result)) throw new Error("rows");
          const { valueAt } = await import("../exec/kernels/test-support.js");
          expect(
            result.batches.flatMap((b) =>
              Array.from({ length: b.length }, (_, i) => b.columns.map((v) => valueAt(v, i))),
            ),
          ).toEqual([
            [1, 10],
            [2, 20],
          ]);
          reach.reach(ending ? "complete end tail" : "sealed source continuation");
          reach.reach(published ? "published" : "resident");
        } finally {
          resume();
          spy.mockRestore();
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [false, false],
          [true, true],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
for (const seed of seedsFor("next-native-command-root-foreign", [0xb679, 0xb67a]))
  it(`preserves the live graph after a foreign setup attempt seed ${String(seed)}`, async () => {
    const reach = createReach("native root setup ownership", NATIVE_COMMAND_ROOT_FOREIGN_REACH);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 1, max: 9 }), async (published, n) => {
        reach.beginHistory();
        const f = await commandRootFixture(setup, `UPDATE t SET v=v+${String(n)}`, published);
        try {
          const before = f.metadata.allocated;
          await expect(
            NativeCommandRoot.create({ ...f.scope, rootId: f.scope.rootId + 1 }),
          ).rejects.toThrow("Native command scope must precede statement writes");
          expect(f.metadata.allocated).toBe(before);
          expect(await f.root.run({ slots: [] })).toBe(2);
          await f.commit();
          await f.retire();
          const result = await f.core.execute("SELECT v FROM t ORDER BY id");
          if (!("batches" in result)) throw new Error("rows");
          const { valueAt } = await import("../exec/kernels/test-support.js");
          expect(
            result.batches.flatMap((b) =>
              Array.from({ length: b.length }, (_, i) =>
                valueAt(
                  b.columns[0] ??
                    (() => {
                      throw new Error("v");
                    })(),
                  i,
                ),
              ),
            ),
          ).toEqual([10 + n, 20 + n]);
          reach.reach("foreign primary refused");
          reach.reach("original graph usable");
          reach.reach(published ? "published" : "resident");
        } finally {
          await f.close();
        }
        reach.reach("zero ownership");
      }),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 120000);
