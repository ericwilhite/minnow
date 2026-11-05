/**
 * Cross-track: frames built by the executor's own result channel (exec/result.ts) cross a real
 * MessagePort by transfer and are decoded on the client through `StreamDictionaries` (ruling
 * R16.5).
 */
import { describe, expect, it } from "vitest";
import { FrameBufferError, ResultChannel, frameTransferList } from "../exec/result.js";
import type { OutputColumn } from "../ir/ir.js";
import { decodeUtf8 } from "../vector/varlen.js";
import { StreamDictionaries, type FramePayload, type WireFlat } from "./frame.js";
import type { Rows } from "./protocol.js";

const text = (...values: string[]): WireFlat => {
  const encoder = new TextEncoder();
  const parts = values.map((value) => encoder.encode(value));
  const bytes = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  const offsets = new Int32Array(values.length + 1);
  parts.forEach((part, index) => {
    const start = offsets[index] ?? 0;
    bytes.set(part, start);
    offsets[index + 1] = start + part.length;
  });
  return {
    form: "flat",
    length: values.length,
    validity: null,
    data: { layout: "varlen", bytes, offsets },
  };
};

const columns: OutputColumn[] = [
  { name: "colour", type: { kind: "text" }, nullable: false },
  { name: "n", type: { kind: "int32", bits: 32 }, nullable: false },
];

/** A frame: dictionary-coded colours plus an int column, with optional new dictionary entries. */
function offer(
  channel: ResultChannel,
  codes: number[],
  delta: { from: number; values: string[] } | null,
) {
  return channel.offer({
    payload: {
      rowCount: codes.length,
      columns: [
        {
          form: "dictionary",
          length: codes.length,
          codes: Int32Array.from(codes),
          validity: null,
          dictionary: 7,
        },
        {
          form: "flat",
          length: codes.length,
          validity: null,
          data: { layout: "i32", values: Int32Array.from(codes, (code) => code * 10) },
        },
      ],
      dictionaries:
        delta === null ? [] : [{ id: 7, from: delta.from, values: text(...delta.values) }],
    },
    requiresLsn: 0,
    free: () => undefined,
  });
}

/** Client side: decodes the colour column of a received payload. */
function colours(payload: FramePayload, dictionaries: StreamDictionaries): string[] {
  const column = payload.columns[0];
  if (column?.form !== "dictionary") throw new TypeError("expected a dictionary column");
  return [...column.codes].map((code) => {
    const { entries, index } = dictionaries.locate(column.dictionary, code);
    if (entries.data.layout !== "varlen") throw new TypeError("expected text entries");
    return decodeUtf8(entries.data, index);
  });
}

describe("executor frames through the port", () => {
  it("transfers every buffer and decodes frame-level dictionary deltas, including frames with none", async () => {
    const channel = new ResultChannel(columns);
    offer(channel, [0, 1, 0], { from: 0, values: ["red", "green"] });
    offer(channel, [2, 1], { from: 2, values: ["blue"] });
    offer(channel, [1, 2, 0], null);
    channel.end();

    // Owner: drain what is queued without awaiting, post it in one message, by transfer.
    const posted = [];
    for (let frame = channel.poll(); frame !== undefined && frame !== null; frame = channel.poll())
      posted.push(frame);
    expect(channel.poll()).toBeNull();
    expect(posted.map((frame) => frame.payload.sequence)).toEqual([0, 1, 2]);
    expect(channel.outstandingBytes).toBeGreaterThan(0);
    const transfer = posted.flatMap((frame) => [...frame.transfer]);
    for (const frame of posted) expect(frame.transfer).toEqual(frameTransferList(frame.payload));

    const { port1, port2 } = new MessageChannel();
    const received = new Promise<Rows>((resolve) => {
      port2.onmessage = (event: MessageEvent<Rows>) => {
        resolve(event.data);
      };
    });
    const message: Rows = {
      t: "rows",
      seq: 4,
      columns: null,
      frames: posted.map((frame) => frame.payload),
    };
    port1.postMessage(message, transfer);
    const rows = await received;
    port1.close();
    port2.close();
    // Zero added copies: the owner's buffers moved.
    for (const buffer of transfer) expect(buffer.byteLength).toBe(0);
    // Still charged until the client's release reaches the owner (R3).
    expect(channel.outstandingBytes).toBeGreaterThan(0);

    // Client: apply each frame's deltas in order, then read it.
    const dictionaries = new StreamDictionaries();
    const decoded = rows.frames.map((payload) => {
      dictionaries.apply(payload);
      return colours(payload, dictionaries);
    });
    expect(decoded).toEqual([
      ["red", "green", "red"],
      ["blue", "green"],
      ["green", "blue", "red"],
    ]);
    expect(dictionaries.size(7)).toBe(3);

    // The client's `release { through: 2 }` reaches the owner, which releases those frames.
    for (const frame of posted) frame.release();
    expect(channel.outstandingBytes).toBe(0);
  });

  it("rejects a delta that skips entries and a frame out of sequence", () => {
    const dictionaries = new StreamDictionaries();
    const payload = (sequence: number, from: number): FramePayload => ({
      sequence,
      rowCount: 0,
      columns: [],
      dictionaries: [{ id: 1, from, values: text("a") }],
    });
    dictionaries.apply(payload(0, 0));
    expect(() => {
      dictionaries.apply(payload(1, 2));
    }).toThrow(RangeError);
    expect(() => {
      dictionaries.apply(payload(5, 1));
    }).toThrow(RangeError);
    expect(() => dictionaries.locate(1, 1)).toThrow(RangeError);
  });

  it("refuses payload views that do not own their whole buffer", () => {
    expect(() =>
      frameTransferList({
        rowCount: 2,
        dictionaries: [],
        columns: [
          {
            form: "flat",
            length: 2,
            validity: null,
            data: { layout: "f64", values: new Float64Array(16).subarray(0, 2) },
          },
        ],
      }),
    ).toThrow(FrameBufferError);
  });
});
