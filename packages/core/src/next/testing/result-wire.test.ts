/**
 * Cross-track wire test (ruling R16.5): B4's `ResultFrame` payload (exec/result.ts) is the one
 * wire definition. Frames go through `ResultChannel`, are transferred as `postMessage` would
 * (structured clone with the transfer list), and are decoded on the client side by
 * `StreamDictionaries`, applying frame-level dictionary deltas in frame order, including frames
 * that reuse earlier entries without sending new ones.
 *
 * `StreamDictionaries` below is a faithful copy of B5's client contract (rebuild/owner e460360,
 * owner/frame.ts): deltas appended in frame order, entries kept as the chunks that arrived (no
 * copy), a delta that does not start at the dictionary's current size is a protocol error. It
 * consumes the frame-level `dictionaries` deltas of R16.5 instead of B5's per-column ones. B5
 * replaces this copy with its own class (importing the payload type from exec/result.ts) and
 * keeps this test.
 */
import { describe, expect, it } from "vitest";
import type { OutputColumn } from "../ir/ir.js";
import {
  ResultChannel,
  type FramePayload,
  type FrameOffer,
  type ResultFrame,
  type WireFlat,
} from "../exec/result.js";
import { decodeUtf8 } from "../vector/varlen.js";

/** B5's client-side dictionaries, consuming frame-level deltas. */
class StreamDictionaries {
  readonly #dictionaries = new Map<number, { size: number; readonly parts: WireFlat[] }>();

  /** Applies a frame's deltas, in order, before its columns are read. */
  apply(frame: FramePayload): void {
    for (const delta of frame.dictionaries) {
      let dictionary = this.#dictionaries.get(delta.id);
      if (dictionary === undefined) {
        dictionary = { size: 0, parts: [] };
        this.#dictionaries.set(delta.id, dictionary);
      }
      if (delta.from !== dictionary.size)
        throw new RangeError(
          `Dictionary ${String(delta.id)} delta starts at ${String(delta.from)}, expected ${String(dictionary.size)}`,
        );
      dictionary.parts.push(delta.values);
      dictionary.size += delta.values.length;
    }
  }

  size(id: number): number {
    return this.#dictionaries.get(id)?.size ?? 0;
  }

  /** The chunk holding `code`, and the code's index inside it. */
  locate(id: number, code: number): { readonly entries: WireFlat; readonly index: number } {
    let index = code;
    for (const entries of this.#dictionaries.get(id)?.parts ?? []) {
      if (index < entries.length) return { entries, index };
      index -= entries.length;
    }
    throw new RangeError(`Code ${String(code)} is outside dictionary ${String(id)}`);
  }
}

const TEXT = { kind: "text" } as const;
const INT = { kind: "int32", bits: 32 } as const;
const COLUMNS: readonly OutputColumn[] = [
  { name: "colour", type: TEXT, nullable: true },
  { name: "n", type: INT, nullable: false },
];

const encoder = new TextEncoder();

function varlen(values: readonly string[]): WireFlat {
  const encoded = values.map((value) => encoder.encode(value));
  const bytes = new Uint8Array(encoded.reduce((sum, value) => sum + value.length, 0));
  const offsets = new Int32Array(values.length + 1);
  encoded.forEach((value, k) => {
    bytes.set(value, offsets[k] ?? 0);
    offsets[k + 1] = (offsets[k] ?? 0) + value.length;
  });
  return {
    form: "flat",
    length: values.length,
    validity: null,
    data: { layout: "varlen", bytes, offsets },
  };
}

function ints(values: readonly number[]): WireFlat {
  return {
    form: "flat",
    length: values.length,
    validity: null,
    data: { layout: "i32", values: Int32Array.from(values) },
  };
}

/** A frame whose colour column is dictionary codes (null where `codes[k]` is −1). */
function dictionaryFrame(
  codes: readonly number[],
  n: readonly number[],
  deltas: FramePayload["dictionaries"],
): FrameOffer {
  const validity = new Uint32Array(1);
  codes.forEach((code, k) => {
    if (code >= 0) validity[0] = (validity[0] ?? 0) | (1 << k);
  });
  return {
    payload: {
      rowCount: codes.length,
      columns: [
        {
          form: "dictionary",
          length: codes.length,
          codes: Int32Array.from(codes, (code) => Math.max(0, code)),
          validity,
          dictionary: 0,
        },
        ints(n),
      ],
      dictionaries: deltas,
    },
    requiresLsn: 0,
    free: () => undefined,
  };
}

/** What the client does with one posted frame: apply deltas, then read rows. */
function decode(
  payload: FramePayload,
  dictionaries: StreamDictionaries,
): Array<[string | null, number]> {
  dictionaries.apply(payload);
  const [colour, n] = payload.columns;
  const rows: Array<[string | null, number]> = [];
  for (let row = 0; row < payload.rowCount; row++) {
    let text: string | null = null;
    if (colour?.form === "dictionary") {
      const valid =
        colour.validity === null || ((colour.validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
      if (valid) {
        const { entries, index } = dictionaries.locate(colour.dictionary, colour.codes[row] ?? 0);
        if (entries.data.layout !== "varlen") throw new TypeError("text dictionary");
        text = decodeUtf8(entries.data, index);
      }
    } else if (colour?.form === "flat" && colour.data.layout === "varlen")
      text = decodeUtf8(colour.data, row);
    const value = n?.form === "flat" && n.data.layout === "i32" ? (n.data.values[row] ?? 0) : 0;
    rows.push([text, value]);
  }
  return rows;
}

/** `postMessage(frame.payload, frame.transfer)` and the client's receipt. */
function transfer(frame: ResultFrame): FramePayload {
  const received = structuredClone(frame.payload, { transfer: [...frame.transfer] });
  frame.release(); // on the client's credit message
  return received;
}

describe("B4 frames decoded by B5's StreamDictionaries", () => {
  it("applies frame-level deltas in order, including frames with no new entries", async () => {
    const channel = new ResultChannel(COLUMNS);
    channel.offer(
      dictionaryFrame(
        [0, 1, -1, 0],
        [1, 2, 3, 4],
        [{ id: 0, from: 0, values: varlen(["red", "green"]) }],
      ),
    );
    channel.offer(dictionaryFrame([1, 1, 0], [5, 6, 7], [])); // no new entries
    channel.offer(dictionaryFrame([2, 0], [8, 9], [{ id: 0, from: 2, values: varlen(["blue"]) }]));
    // Past the stream's dictionary budget the column goes plain.
    channel.offer({
      payload: { rowCount: 1, columns: [varlen(["violet"]), ints([10])], dictionaries: [] },
      requiresLsn: 0,
      free: () => undefined,
    });
    channel.end();

    const client = new StreamDictionaries();
    const rows: Array<[string | null, number]> = [];
    const sequences: number[] = [];
    for (let frame = await channel.next(); frame !== null; frame = await channel.next()) {
      const transferred = frame.transfer.length;
      const payload = transfer(frame);
      expect(frame.transfer.every((buffer) => buffer.byteLength === 0)).toBe(transferred > 0); // moved, not copied
      sequences.push(payload.sequence);
      rows.push(...decode(payload, client));
    }
    expect(sequences).toEqual([0, 1, 2, 3]);
    expect(client.size(0)).toBe(3);
    expect(rows).toEqual([
      ["red", 1],
      ["green", 2],
      [null, 3],
      ["red", 4],
      ["green", 5],
      ["green", 6],
      ["red", 7],
      ["blue", 8],
      ["red", 9],
      ["violet", 10],
    ]);
    await channel.delivered;
  });

  it("refuses a delta that skips entries (a lost or reordered frame)", () => {
    const client = new StreamDictionaries();
    const later = dictionaryFrame([2], [1], [{ id: 0, from: 2, values: varlen(["blue"]) }]);
    expect(() => client.apply({ ...later.payload, sequence: 2 })).toThrow(RangeError);
  });
});
