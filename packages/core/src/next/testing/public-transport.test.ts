/** Native protocol metadata is not lost at the thin-client boundary. Ordinary Results own
 * their frames; only explicit exports may detach transferred views. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_TRANSPORT_REACH } from "../../testing/reach.js";
import { ProtocolClient } from "../owner/client.js";
import { StreamDictionaries, type FramePayload } from "../owner/frame.js";
import { PROTOCOL_VERSION, type StatementSummary } from "../owner/protocol.js";
import { decodeResult } from "../owner/result.js";
import { publicOwner } from "./public-owner.test.fixture.js";

const summary: StatementSummary = {
  command: "SELECT",
  rowCount: 1,
  counts: null,
  lsn: null,
  replayed: false,
  closedEarly: false,
  transaction: "idle",
  catalogVersion: 1,
};
function dictionaryFrame(sequence: number, value: string): FramePayload {
  const bytes = new TextEncoder().encode(value);
  return {
    sequence,
    rowCount: 1,
    columns: [
      {
        form: "dictionary",
        length: 1,
        codes: Int32Array.of(sequence),
        validity: null,
        dictionary: 7,
      },
    ],
    dictionaries: [
      {
        id: 7,
        from: sequence,
        values: {
          form: "flat",
          length: 1,
          validity: null,
          data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
        },
      },
    ],
  };
}
const columns = [
  { name: "v", sqlType: "text", nullable: false, render: null, type: { kind: "text" as const } },
];

for (const seed of seedsFor("next-public-transport", [0xb736, 0xb737]))
  it(`preserves native metadata, ordinary ownership and unsent cancellation (${String(seed)})`, async () => {
    const reach = createReach("public transport", PUBLIC_TRANSPORT_REACH);
    const owner = await publicOwner();
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("bigint", "integer", "text", "numeric"),
          fc.string({ maxLength: 80 }),
          fc.integer({ min: 1, max: 5 }),
          async (sqlType, label, length) => {
            reach.beginHistory();
            const prepared = await owner.client.prepare(`SELECT $1::${sqlType} v`);
            expect(prepared.parameters).toHaveLength(1);
            expect(prepared.parameters[0]).toEqual(prepared.columns[0]?.type);
            expect(prepared.catalogVersion).toBeGreaterThanOrEqual(0);
            owner.client.unprepare(prepared.id);
            reach.reach("prepared parameter types");
            const admin = await owner.client.send({ t: "admin", op: "catalog", args: [] }).promise;
            expect(admin.value).toBeDefined();
            reach.reach("structured admin reply");
            const raw = owner.client.cursor("SELECT i::bigint v FROM generate_series(1,$1) s(i)", [
              length,
            ]);
            expect(await raw.columns).toMatchObject([{ name: "v", type: { kind: "int64" } }]);
            let rows = 0;
            const held = [];
            for await (const frame of raw.frames) {
              const result = decodeResult({
                columns: await raw.columns,
                frames: [frame],
                summary,
                dictionaries: raw.snapshotDictionaries(),
              });
              rows += result.rowCount;
              held.push(result);
            }
            expect(rows).toBe(length);
            expect((await raw.done).command).toBe("SELECT");
            raw.close();
            raw.close();
            expect(held.flatMap((result) => result.values(0))).toEqual(
              Array.from({ length }, (_, i) => BigInt(i + 1)),
            );
            reach.reach("ordinary cursor metadata and completion");
            reach.reach("views live after close");
            const early = owner.client.cursor(
              "SELECT i FROM generate_series(1,$1) s(i)",
              [length + 3],
              { frameRows: 1, windowBytes: 1 },
            );
            const prefixFrame = await early.frames.next();
            expect(prefixFrame.done).toBe(false);
            if (prefixFrame.done) throw new Error("Expected prefix before close");
            const prefixResult = decodeResult({
              columns: await early.columns,
              frames: [prefixFrame.value],
              summary,
              dictionaries: early.snapshotDictionaries(),
            });
            await early.frames.return?.();
            expect((await early.done).closedEarly).toBe(true);
            expect(prefixResult.values(0)).toEqual([1]);
            expect(owner.client.hasPins).toBe(false);
            reach.reach("early return terminal fence");

            const dictionaries = new StreamDictionaries();
            const first = dictionaryFrame(0, label);
            dictionaries.apply(first);
            const prefix = dictionaries.snapshot();
            dictionaries.apply(dictionaryFrame(1, label + "suffix"));
            expect(prefix.size(7)).toBe(1);
            expect(dictionaries.size(7)).toBe(2);
            const entry = prefix.locate(7, 0);
            expect(entry.entries).toBe(first.dictionaries[0]?.values);
            expect(
              decodeResult({ columns, frames: [first], summary, dictionaries: prefix }).values(0),
            ).toEqual([label]);
            reach.reach("immutable dictionary prefix");

            const queued = new ProtocolClient();
            const stream = queued.cursor("SELECT 1");
            stream.close();
            expect(queued.pendingCount).toBe(0);
            expect(await stream.columns).toEqual([]);
            await expect(stream.done).resolves.toMatchObject({ closedEarly: true });
            expect(await stream.frames.next()).toEqual({ done: true, value: undefined });
            const task = queued.send({ t: "prepare", id: 1, text: "SELECT 1" });
            queued.cancel(task.seq);
            await expect(task.promise).rejects.toMatchObject({ code: "MINNOW_CANCELLED" });
            expect(queued.hasPins).toBe(false);
            const channel = new MessageChannel();
            const requests: string[] = [];
            channel.port2.onmessage = (event: MessageEvent<{ t: string }>) => {
              requests.push(event.data.t);
              if (event.data.t === "hello")
                channel.port2.postMessage({
                  t: "welcome",
                  ownerId: "unsent",
                  lockHolder: "unsent",
                  protocol: PROTOCOL_VERSION,
                  epoch: 1,
                });
              if (event.data.t === "ping") channel.port2.postMessage({ t: "alive" });
            };
            try {
              await queued.connect(channel.port1);
              // Both request sequences are already terminal. Connecting cannot issue them.
              expect(requests).toEqual(["hello"]);
              expect(queued.pendingCount).toBe(0);
            } finally {
              queued.close();
              channel.port1.close();
              channel.port2.close();
            }
            reach.reach("unsent close and cancellation");
          },
        ),
        { seed, numRuns: 16 },
      );
      reach.assertReached();
    } finally {
      await owner.close();
      expect(owner.core.harness.governor.usedBytes).toBe(0);
      expect(owner.core.spill.bytesInUse).toBe(0);
    }
  }, 60000);
