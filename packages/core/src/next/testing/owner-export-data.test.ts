import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, OWNER_EXPORT_DATA_REACH } from "../../testing/reach.js";
import { ProtocolClient, type ExportFrame } from "../owner/client.js";
import { OwnerServer } from "../owner/server.js";
import { composeEngine } from "../owner/compose.js";
import { PROTOCOL_VERSION } from "../owner/protocol.js";
import { ResultSink } from "../exec/result-sink.js";
import {
  frameTransferList,
  type FramePayload,
  type WireFlat,
  type WireVector,
} from "../exec/result.js";
import { ReturnPool, type ReturnedBuffer } from "../exec/return-pool.js";
import { Governor } from "../memory/governor.js";
import { chunkReads } from "../exec/scan.js";
import { decodeUtf8 } from "../vector/varlen.js";
import { allocateValidity, setValid, isValid } from "../vector/validity.js";
import type { VectorData } from "../vector/vector.js";
import { StorageCore } from "./storage-core.js";

function value(frame: ExportFrame, vector: WireVector, row: number): unknown {
  let column: WireFlat;
  if (vector.form === "constant") {
    column = vector.value;
    row = 0;
  } else if (vector.form === "dictionary") {
    if (!isValid(vector.validity, row)) return null;
    const located = frame.dictionaries.locate(vector.dictionary, vector.codes[row] ?? 0);
    column = located.entries;
    row = located.index;
  } else column = vector;
  if (!isValid(column.validity, row)) return null;
  const d = column.data;
  if (d.layout === "i32") return d.values[row];
  if (d.layout === "int64") return d.values.values[row];
  if (d.layout === "varlen") return decodeUtf8(d, row);
  throw new Error(`Unexpected fixture type ${d.layout}`);
}

for (const seed of seedsFor("owner-export-real-data", [0xb2ea, 0xb2eb]))
  it(`real owner/executor data equality (${String(seed)})`, async () => {
    const reach = createReach("owner export real", OWNER_EXPORT_DATA_REACH.real);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 65, max: 96 }),
        fc.integer({ min: 1, max: 19 }),
        fc.boolean(),
        async (count, salt, sparse) => {
          reach.beginHistory();
          const core = await new StorageCore(32).open();
          const engine = composeEngine({
            planner: core.lowerer,
            writePath: core.harness.path,
            governor: core.harness.governor,
            spill: core.spill,
            reads: chunkReads,
          });
          const server = new OwnerServer(engine, { ownerId: "real", epoch: 1, build: "test" });
          const ports = new MessageChannel(),
            client = new ProtocolClient();
          const target = vi.spyOn(ResultSink.prototype, "terminalBatchTarget");
          server.accept(ports.port1);
          await client.connect(ports.port2);
          try {
            await core.execute("CREATE TABLE t (id INTEGER, amount BIGINT, tag TEXT, body TEXT)");
            const rows = Array.from({ length: count }, (_, id) => ({
              id,
              amount: (1n << 60n) + BigInt(id),
              tag: sparse && id % 7 === 0 ? null : `水${String(id % 3)}`,
              body: id === 3 ? "z".repeat(65536 + salt) : `payload ${String(id)}`,
            }));
            const table = core.harness.path.catalog.tableByName("t");
            if (!table) throw new Error("missing fixture table");
            const tagValidity = allocateValidity(count);
            rows.forEach((row, i) => setValid(tagValidity, i, row.tag !== null));
            const data: VectorData[] = [
              { layout: "i32", values: Int32Array.from(rows, (r) => r.id) },
              {
                layout: "int64",
                values: { repr: "int64", values: BigInt64Array.from(rows, (r) => r.amount) },
              },
              entries(rows.map((r) => r.tag ?? "")).data,
              entries(rows.map((r) => r.body)).data,
            ];
            await core.insertVectors(table, {
              length: count,
              selection: null,
              columns: table.columns.map((column, i) => {
                const storage = data[i];
                if (!storage) throw new Error("missing fixture column");
                return {
                  form: "flat",
                  type: column.type,
                  length: count,
                  validity: i === 2 ? tagValidity : null,
                  data: storage,
                };
              }),
            });
            await core.harness.path.quiesce();
            const ordinary = await client.query("SELECT * FROM t");
            expect(target).not.toHaveBeenCalled();
            const stream = client.exportStream("SELECT * FROM t", [], { windowBytes: 128 });
            const actual: unknown[][] = [];
            for await (const frame of stream.frames) {
              for (let r = 0; r < frame.payload.rowCount; r++)
                actual.push(frame.payload.columns.map((c) => value(frame, c, r)));
              frame.release();
              expect(frameTransferList(frame.payload).every((b) => b.byteLength === 0)).toBe(true);
            }
            await stream.done;
            expect(actual).toEqual(rows.map((r) => [r.id, r.amount, r.tag, r.body]));
            expect(actual).toEqual(ordinary.rows());
            expect(target).toHaveBeenCalledOnce();
            reach.reach("real factory and SQL");
            reach.reach("wide int64 and large text");
            reach.reach(sparse ? "sparse source" : "all-valid source");
          } finally {
            target.mockRestore();
            client.close();
            const stop = server.quiesce("closing");
            await stop.drained;
            stop.finish();
            ports.port1.close();
            ports.port2.close();
            await engine.close();
            await core.close();
            expect(core.harness.governor.usedBytes).toBe(0);
          }
        },
      ),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  }, 60_000);

function peer(
  onMessage: (
    message: { t: string; seq: number; buffers?: readonly ReturnedBuffer[]; through: number },
    port: MessagePort,
  ) => void,
) {
  const ports = new MessageChannel();
  ports.port1.onmessage = (event) => {
    const message = event.data as Parameters<typeof onMessage>[0];
    if (message.t === "hello")
      ports.port1.postMessage({
        t: "welcome",
        protocol: PROTOCOL_VERSION,
        ownerId: "fixture",
        epoch: 1,
        lockHolder: null,
      });
    else onMessage(message, ports.port1);
  };
  return ports;
}
const summary = {
  command: "SELECT",
  rowCount: 4,
  counts: null,
  lsn: null,
  replayed: false,
  closedEarly: false,
  transaction: "idle",
  catalogVersion: 1,
};
function entries(strings: string[]): WireFlat {
  const bytes = new TextEncoder().encode(strings.join(""));
  let at = 0;
  const offsets = Int32Array.from([
    0,
    ...strings.map((s) => (at += new TextEncoder().encode(s).length)),
  ]);
  return {
    form: "flat",
    length: strings.length,
    validity: null,
    data: { layout: "varlen", bytes, offsets },
  };
}

for (const seed of seedsFor("owner-export-dictionaries", [0xb2ec, 0xb2ed]))
  it(`dictionary deltas outlive recycled frame storage (${String(seed)})`, async () => {
    const reach = createReach("owner export dictionaries", OWNER_EXPORT_DATA_REACH.dictionary);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 29 }), async (salt) => {
        reach.beginHistory();
        const governor = new Governor(8 * 1048576),
          pool = new ReturnPool(governor, 4 * 1048576);
        const client = new ProtocolClient();
        let target = 0,
          nextSent = false;
        const firstValues = [`a${String(salt)}`, `水${String(salt)}`],
          last = `last${String(salt)}`;
        const ports = peer((message, port) => {
          const send = (frame: FramePayload, done: boolean) => {
            const transfer = frameTransferList(frame),
              ids = pool.issue(frame.sequence, transfer);
            port.postMessage(
              {
                t: done ? "done" : "rows",
                seq: target,
                columns: [],
                frames: [frame],
                bufferIds: [ids],
                ...(done ? { summary } : {}),
              },
              transfer,
            );
          };
          if (message.t === "statement") {
            target = message.seq;
            send(
              {
                sequence: 0,
                rowCount: 2,
                columns: [
                  {
                    form: "dictionary",
                    dictionary: 7,
                    codes: Int32Array.of(0, 1),
                    validity: null,
                    length: 2,
                  },
                ],
                dictionaries: [{ id: 7, from: 0, values: entries(firstValues) }],
              },
              false,
            );
          } else if (message.t === "returnBuffers") {
            pool.release(message.through, message.buffers ?? []);
            const reused = pool.take(8);
            if (reused) new Uint8Array(reused).fill(0x7f);
            if (!nextSent) {
              nextSent = true;
              send(
                {
                  sequence: 1,
                  rowCount: 2,
                  columns: [
                    {
                      form: "dictionary",
                      dictionary: 7,
                      codes: Int32Array.of(1, 2),
                      validity: null,
                      length: 2,
                    },
                  ],
                  dictionaries: [{ id: 7, from: 2, values: entries([last]) }],
                },
                true,
              );
            }
          }
        });
        await client.connect(ports.port2);
        try {
          const stream = client.exportStream("SELECT tag FROM t", [], { windowBytes: 1 });
          const first = await stream.frames.next();
          if (first.done) throw new Error("missing dictionary frame");
          expect(first.value.payload.columns.map((c) => value(first.value, c, 0))).toEqual([
            firstValues[0],
          ]);
          first.value.release();
          const second = await stream.frames.next();
          if (second.done) throw new Error("missing dictionary append");
          const c = second.value.payload.columns[0];
          if (!c) throw new Error("missing codes");
          expect([value(second.value, c, 0), value(second.value, c, 1)]).toEqual([
            firstValues[1],
            last,
          ]);
          second.value.release();
          await stream.done;
          reach.reach("compacted dictionary delta");
          reach.reach("later dictionary append");
        } finally {
          client.close();
          ports.port1.close();
          ports.port2.close();
          pool.close();
          expect(governor.usedBytes).toBe(0);
        }
      }),
      { seed, numRuns: 32 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("owner-export-view-validation", [0xb2ee, 0xb2ef]))
  it(`validates every received view (${String(seed)})`, async () => {
    const reach = createReach("owner export views", OWNER_EXPORT_DATA_REACH.views);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2, max: 32 }), fc.boolean(), async (n, partialFirst) => {
        reach.beginHistory();
        const client = new ProtocolClient();
        const ports = peer((message, port) => {
          if (message.t !== "statement") return;
          const whole = new Int32Array(n),
            partial = whole.subarray(0, n - 1);
          port.postMessage(
            {
              t: "done",
              seq: message.seq,
              columns: [],
              bufferIds: [[1]],
              frames: [
                {
                  sequence: 0,
                  rowCount: n,
                  dictionaries: [],
                  columns: (partialFirst ? [partial, whole] : [whole, partial]).map((values) => ({
                    form: "flat",
                    length: n,
                    validity: null,
                    data: { layout: "i32", values },
                  })),
                },
              ],
              summary,
            },
            [whole.buffer],
          );
        });
        await client.connect(ports.port2);
        try {
          const stream = client.exportStream("SELECT * FROM t");
          await expect(stream.done).rejects.toMatchObject({ code: "MINNOW_FRAME_BUFFER" });
          reach.reach(partialFirst ? "first view partial" : "later alias partial");
        } finally {
          client.close();
          ports.port1.close();
          ports.port2.close();
        }
      }),
      { seed, numRuns: 32 },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("owner-export-preconnect", [0xb2f0, 0xb2f1]))
  it(`cancels an unsent export locally (${String(seed)})`, async () => {
    const reach = createReach("owner export preconnect", OWNER_EXPORT_DATA_REACH.preconnect);
    await fc.assert(
      fc.asyncProperty(fc.boolean(), async (close) => {
        reach.beginHistory();
        const client = new ProtocolClient();
        let statements = 0;
        const ports = peer((message, port) => {
          if (message.t === "statement") {
            statements++;
            port.postMessage({ t: "done", seq: message.seq, columns: [], frames: [], summary });
          }
        });
        try {
          const stream = client.exportStream("SELECT * FROM t");
          const pending = stream.frames.next();
          await expect(stream.frames.next()).rejects.toThrow("Only one export next");
          reach.reach("concurrent next refused");
          reach.reach(close ? "close before connect" : "cancel before connect");
          if (close) stream.close();
          else stream.cancel();
          await expect(stream.done).rejects.toMatchObject({ code: "MINNOW_CANCELLED" });
          await expect(pending).rejects.toMatchObject({ code: "MINNOW_CANCELLED" });
          await client.connect(ports.port2);
          await client.query("SELECT 1");
          expect(statements).toBe(1);
        } finally {
          client.close();
          ports.port1.close();
          ports.port2.close();
        }
      }),
      { seed, numRuns: 16 },
    );
    reach.assertReached();
  });
