/** Ordinary Results keep caller-owned buffers across local delivery, native reuse and teardown. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_RESULT_OWNERSHIP_REACH } from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { frameTransferList } from "../exec/frame-buffers.js";
import type { FramePayload } from "../owner/frame.js";
import type { ReturnPool } from "../exec/return-pool.js";
import { publicOwner } from "./public-owner.test.fixture.js";

function viewBytes(value: unknown, out: Uint8Array[] = []): Uint8Array[] {
  if (ArrayBuffer.isView(value))
    out.push(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  else if (value !== null && typeof value === "object")
    for (const member of Object.values(value as Record<string, unknown>)) viewBytes(member, out);
  return out;
}

for (const seed of seedsFor("next-public-result-ownership", [0xb760, 0xb761]))
  it(`releases engine credit while held Results survive pooled reuse (${String(seed)})`, async () => {
    const reach = createReach("public Result ownership", PUBLIC_RESULT_OWNERSHIP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2, max: 9 }),
        fc.constantFrom(8, 16384),
        async (count, width) => {
          reach.beginHistory();
          const owner = await publicOwner({ planCacheBytes: 0 });
          const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
          const pools: ReturnPool[] = [];
          const governor = owner.core.harness.governor,
            createPool = owner.engine.createReturnPool.bind(owner.engine);
          // Mutate the owner-side dedicated buffers after postMessage copied/transferred them.
          // A local synchronous alias would corrupt the Result immediately.
          const post = owner.channel.port2.postMessage.bind(owner.channel.port2);
          let probe = true,
            copied = 0,
            moved = 0;
          const delivery = vi
            .spyOn(owner.channel.port2, "postMessage")
            .mockImplementation(
              (message: unknown, options?: Transferable[] | StructuredSerializeOptions) => {
                const buffers = probe
                  ? ((message as { frames?: FramePayload[] }).frames ?? []).flatMap(
                      frameTransferList,
                    )
                  : [];
                if (Array.isArray(options)) post(message, options);
                else post(message, options);
                for (const buffer of buffers) {
                  if (buffer.byteLength === 0) moved++;
                  else {
                    copied++;
                    new Uint8Array(buffer).fill(0xa5);
                  }
                }
              },
            );
          const pooled = vi.spyOn(owner.engine, "createReturnPool").mockImplementation((cap) => {
            const pool = createPool(cap);
            pools.push(pool);
            return pool;
          });
          const query = width === 8 ? "SELECT 1::bigint n,$1::text v" : "SELECT n,v FROM t";
          const params = width === 8 ? ["x".repeat(width)] : [];
          try {
            probe = false;
            await db.execute("CREATE TABLE t(n BIGINT,v TEXT)");
            for (let i = 0; i < count; i++)
              await db.execute("INSERT INTO t VALUES($1,$2)", [BigInt(i + 1), "x".repeat(width)]);
            await owner.core.harness.path.recycleLog();
            await db.query(query, params);
            await owner.client.send({ t: "admin", op: "catalog", args: [] }).promise;
            const baseline = governor.usedBytes;
            probe = true;
            const result = await db.query(query, params);
            probe = false;
            const tuples = result.tuples(),
              view = result.column("n");
            expect(view.kind).toBe("int64");
            const heldViews = [...viewBytes(view), ...viewBytes(result.column("v"))];
            const heldBytes = heldViews.map((bytes) => bytes.slice());
            const checkViews = () => {
              for (let i = 0; i < heldViews.length; i++) expect(heldViews[i]).toEqual(heldBytes[i]);
            };
            // An admin reply emits no result frame and flushes all prior release credits in order.
            await owner.client.send({ t: "admin", op: "catalog", args: [] }).promise;
            expect(owner.server.stats.outstandingFrames).toBe(0);
            expect(governor.usedBytes).toBe(baseline);
            expect(width === 8 ? copied : moved).toBeGreaterThan(0);
            reach.reach("delivered frame charge released");
            const expected = Array.from({ length: width === 8 ? 1 : count }, (_, i) => [
              BigInt(i + 1),
              "x".repeat(width),
            ]);
            expect(tuples).toEqual(expected);
            // Explicit recycling uses its own buffers; an ordinary held Result is never returned.
            for (let round = 0; round < 3; round++) {
              const stream = owner.client.exportStream("SELECT n,v FROM t", [], {
                windowBytes: 1,
              });
              try {
                for await (const frame of stream.frames) frame.release();
                await stream.done;
              } finally {
                stream.close();
              }
              await owner.client.send({ t: "admin", op: "catalog", args: [] }).promise;
              expect(result.tuples()).toEqual(expected);
              expect(result.column("n")).toBe(view);
              checkViews();
            }
            expect(pools.reduce((sum, pool) => sum + pool.hits, 0)).toBeGreaterThan(0);
            reach.reach("actual native return-pool reuse");
            await db.close();
            await owner.close();
            expect(governor.usedBytes).toBe(0);
            checkViews();
            expect(owner.core.spill.bytesInUse).toBe(0);
            expect(result.rows().map((row) => [row.n, row.v])).toEqual(expected);
            expect(result.values("n")).toEqual(expected.map((row) => row[0]));
            expect(result.column("n")).toBe(view);
            reach.reach("held Results survive zero owner charge");
            reach.reach(width === 8 ? "small cloned delivery" : "large transferred delivery");
          } finally {
            await db.close();
            await owner.close();
            delivery.mockRestore();
            pooled.mockRestore();
          }
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [2, 8],
          [9, 16384],
        ],
      },
    );
    reach.assertReached();
  }, 60000);
