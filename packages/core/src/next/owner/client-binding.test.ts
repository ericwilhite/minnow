/** Client-side bindings neither reinterpret scalar values nor guess a terminal outcome. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import {
  createReach,
  PUBLIC_CLIENT_BINDING_REACH,
  PUBLIC_CURSOR_CONCURRENCY_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ProtocolClient } from "./client.js";
import { callTask } from "./client-call.js";
import { writeColumns } from "./client-input.js";
import { MinnowDatabaseClient } from "./database-client.js";
import { CancelledError, OutcomeUnknownError } from "./errors.js";
for (const seed of seedsFor("next-public-client-binding", [0xb73e, 0xb73f]))
  it(`preserves typed input validity and terminal authority (${String(seed)})`, async () => {
    const reach = createReach("client binding", PUBLIC_CLIENT_BINDING_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.integer({ min: -1000, max: 1000 }), { maxLength: 129 }),
        fc.integer({ min: 0, max: 2 }),
        async (values, repr) => {
          reach.beginHistory();
          reach.reach(repr === 0 ? "int32 input" : repr === 1 ? "float64 input" : "int64 input");
          if (values.length === 0) reach.reach("empty input");
          if (values.length > 32) reach.reach("validity word boundary");
          const typed =
            repr === 0
              ? Int32Array.from(values)
              : repr === 1
                ? Float64Array.from(values)
                : BigInt64Array.from(values, (value) => BigInt(value));
          const validity = Uint8Array.from({ length: Math.ceil(values.length / 8) }, () => 0x55);
          const result = writeColumns({ columns: { n: { values: typed, validity } } });
          expect(result.rowCount).toBe(values.length);
          const column = result.columns[0];
          expect(column?.kind).toBe(repr === 0 ? "i32" : repr === 1 ? "f64" : "i64");
          if (column === undefined || !("validity" in column) || !("values" in column))
            throw new Error("Missing typed column");
          expect(column.values).toBe(typed);
          for (let i = 0; i < values.length; i++)
            expect(((column.validity?.[i >>> 5] ?? 0) >>> (i & 31)) & 1).toBe(i % 2 === 0 ? 1 : 0);
          const length = values.length;
          expect(writeColumns({ columns: { n: values } }).rowCount).toBe(length);
          expect(() => writeColumns([{ n: 1 }, { different: 2 }])).toThrow("same columns");
          expect(() => writeColumns({ columns: { a: [1], b: [1, 2] } })).toThrow(
            "different lengths",
          );
          expect(() =>
            writeColumns({
              columns: { a: { values: Int32Array.of(1), validity: new Uint8Array(0) } },
            }),
          ).toThrow("validity is too short");
          const raw = new ProtocolClient(),
            controller = new AbortController(),
            reason = { terminal: "abort" };
          const cancelled = vi.spyOn(raw, "cancel");
          let resolve: (value: number) => void = () => undefined;
          const promise = new Promise<number>((done) => {
            resolve = done;
          });
          const resultTask = callTask(raw, { seq: 1, promise }, controller.signal);
          controller.abort(reason);
          resolve(7);
          await expect(resultTask).resolves.toBe(7); // a committed terminal reply wins
          expect(cancelled).toHaveBeenCalledWith(1);
          await expect(
            callTask(
              raw,
              { seq: 2, promise: Promise.reject(new CancelledError()) },
              controller.signal,
            ),
          ).rejects.toBe(reason);
          const unknown = new OutcomeUnknownError("unresponsive");
          await expect(
            callTask(raw, { seq: 3, promise: Promise.reject(unknown) }, controller.signal),
          ).rejects.toBe(unknown);
          reach.reach("terminal success beats abort");
          reach.reach("no-effect reason and unknown effect");
          cancelled.mockRestore();
          const db = new MinnowDatabaseClient(raw, { ready: new Promise<void>(() => undefined) });
          const cursorAbort = new AbortController(),
            cursorReason = { length: values.length };
          const cursor = db.cursor("SELECT 1", [], { signal: cursorAbort.signal });
          cursorAbort.abort(cursorReason);
          await expect(cursor.columns).rejects.toBe(cursorReason);
          await expect(cursor.next()).rejects.toBe(cursorReason);
          await cursor.close();
          await db.close();
          reach.reach("cursor metadata abort reason");
          raw.close();
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [[], 0],
          [Array.from({ length: 129 }, (_, i) => i), 2],
        ],
      },
    );
    reach.assertReached();
  }, 60000);

it("closing a facade before connection settles queued calls without executing them", async () => {
  const raw = new ProtocolClient();
  const db = new MinnowDatabaseClient(raw, { ready: new Promise<void>(() => undefined) });
  const task = db.execute("CREATE TABLE never_runs(id INT)");
  const failed = expect(task).rejects.toMatchObject({ code: "MINNOW_CLIENT_CLOSED" });
  const close = db.close();
  expect(raw.pendingCount).toBe(0);
  await close;
  await failed;
  await expect(db.ready()).rejects.toMatchObject({ code: "MINNOW_CLIENT_CLOSED" });
});

for (const seed of seedsFor("next-public-cursor-concurrency", [0xb746, 0xb747]))
  it(`settles overlapping cursor reads when closing or aborting (${String(seed)})`, async () => {
    const reach = createReach("public cursor concurrency", PUBLIC_CURSOR_CONCURRENCY_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 12 }), fc.boolean(), async (count, abort) => {
        reach.beginHistory();
        const raw = new ProtocolClient(),
          db = new MinnowDatabaseClient(raw, { ready: new Promise<void>(() => undefined) });
        const controller = new AbortController(),
          reason = { abort: true };
        const cursor = db.cursor("SELECT 1", [], { signal: controller.signal });
        const settled: Array<{ result: unknown } | { error: unknown }> = [];
        for (let i = 0; i < count; i++)
          void cursor.next().then(
            (result) => {
              settled.push({ result });
            },
            (error: unknown) => {
              settled.push({ error });
            },
          );
        if (abort) controller.abort(reason);
        await cursor.close();
        // A message task follows the already queued promise jobs. This is a deterministic
        // progress barrier, not a wall-time race or a timeout proving a negative.
        const barrier = new MessageChannel();
        try {
          await new Promise<void>((resolve) => {
            barrier.port1.onmessage = () => resolve();
            barrier.port2.postMessage(null);
          });
        } finally {
          barrier.port1.close();
          barrier.port2.close();
        }
        try {
          expect(settled).toHaveLength(count);
          const errors = settled.filter((entry): entry is { error: unknown } => "error" in entry);
          expect(
            errors.filter(
              (entry) =>
                entry.error instanceof TypeError &&
                entry.error.message === "Only one cursor next() may be pending",
            ),
          ).toHaveLength(count - 1);
          if (abort) expect(errors.some((entry) => entry.error === reason)).toBe(true);
          else
            expect(
              settled.some(
                (entry) =>
                  "result" in entry &&
                  typeof entry.result === "object" &&
                  entry.result !== null &&
                  "done" in entry.result &&
                  entry.result.done === true,
              ),
            ).toBe(true);
          expect(raw.pendingCount).toBe(0);
          expect(raw.hasPins).toBe(false);
          reach.reach(abort ? "abort all reads settle" : "close all reads settle");
          reach.reach(count === 2 ? "two calls" : "many calls");
        } finally {
          await db.close();
        }
      }),
      {
        seed,
        numRuns: 24,
        examples: [
          [2, false],
          [12, true],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
