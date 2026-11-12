/** ExtentBuilder.append is the documented encoded-buffer consumption boundary. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_RUN_OUTPUT_STATES } from "../../testing/reach.js";
import { RunBuilder, type RunSummary } from "./runs.js";
import { RunSink } from "./run-output.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rules,
  tableInput,
  watchGrants,
} from "./test-support.js";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const reach = createReach("encoded storage handoff", WRITE_RUN_OUTPUT_STATES);
for (const seed of seedsFor("next-write-run-output", [0xb5a4]))
  it(`retains actual append buffers through acknowledgement or failure (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.boolean(), fc.integer({ min: 128, max: 512 }), async (fail, count) => {
        reach.beginHistory();
        const h = new Harness("strict", "memory", 8, undefined, { merges: { mode: "off" } });
        await h.open();
        let paid = 0;
        const builder = new RunBuilder(false, false, {
          allocate(n) {
            paid += n;
            return true;
          },
          free(n) {
            paid -= n;
          },
        });
        const writer = h.space.beginWrite("run output ownership");
        const open = writer.open.bind(writer);
        const barrier = deferred();
        const stopped = new Error("append failed");
        let held = 0,
          inputBytes = 0;
        writer.open = (capacity) => {
          const target = open(capacity),
            append = target.append.bind(target);
          target.append = async (parts) => {
            held = paid;
            inputBytes = parts.reduce((sum, part) => sum + part.buffer.byteLength, 0);
            expect(paid).toBeGreaterThanOrEqual(inputBytes);
            await barrier.promise;
            expect(paid, "no release while storage still borrows the parts").toBe(held);
            if (fail) throw stopped;
            return append(parts);
          };
          return target;
        };
        const outputs: RunSummary[] = [];
        let pending: Promise<void> | null = null;
        try {
          const bytes = new Uint8Array(129);
          for (let i = 0; i < count; i++) {
            new DataView(bytes.buffer).setUint32(0, i);
            builder.add(bytes, i, false);
          }
          const sink = new RunSink(builder, writer, 1024);
          const before = paid;
          pending = Promise.resolve(sink.drain(true));
          expect(paid, "append must retain the caller's charges before acknowledgement").toBe(
            before,
          );
          expect(inputBytes).toBeGreaterThan(0);
          expect(paid).toBe(held);
          barrier.resolve();
          if (fail) {
            await expect(pending).rejects.toBe(stopped);
            expect(paid, "failed consumption remains owned until abort cleanup").toBe(held);
            reach.reach("append failure");
          } else {
            await pending;
            expect(paid).toBeLessThan(held);
            const ref = await sink.finish(outputs);
            expect(ref?.entryCount).toBe(count);
            expect(paid).toBe(2 * (129 + 192));
            reach.reach("append acknowledgement");
            reach.reach("boundary ownership");
          }
        } finally {
          barrier.resolve();
          await pending?.catch(() => undefined);
          for (const output of outputs) output.release();
          builder.close();
          await writer.abort();
          await h.close();
        }
        expect(paid).toBe(0);
        reach.reach("abort or publication cleanup");
      }),
      { seed, numRuns: 20 },
    );
    reach.assertReached();
  });

it("the production staged encoder uses the metadata account", async () => {
  const h = new Harness("strict", "memory", 8, undefined, { merges: { mode: "off" } });
  let charged = 0;
  watchGrants(h.governor, (grant) => {
    if (grant.label !== "write path metadata") return;
    const allocate = grant.allocate.bind(grant);
    grant.allocate = (bytes) => {
      if (new Error().stack?.includes("RunBuffers.create")) charged++;
      return allocate(bytes);
    };
  });
  const path = await h.open();
  try {
    let table = 0;
    await autocommit(path, async (s) => {
      table =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false)]),
          })
        ).id ?? 0;
    });
    const tx = await path.begin();
    for (let from = 0; from < 64; from += 8) {
      const s = tx.statement({ rules });
      await s.table(table).insert(batch(ints(Array.from({ length: 8 }, (_, i) => from + i))), null);
      await s.end();
    }
    await (
      await tx.commit(null)
    ).durable;
    await path.quiesce();
    expect(path.maintenance.stages).toBeGreaterThan(0);
    expect(charged, "encoder allocations reach the real owner account").toBeGreaterThan(0);
  } finally {
    await h.close();
  }
});
