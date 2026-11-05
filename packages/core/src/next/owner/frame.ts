/**
 * Columnar data on the wire: result frames (owner → client) and batch-write input (client →
 * owner).
 *
 * ## Result frames (rulings R3, R16.5)
 * - The executor's payload (exec/result.ts `FramePayload`) is the single definition, re-exported
 *   here. The owner posts `postMessage(frame.payload, frame.transfer)` as the executor built it:
 *   plain data, every typed array covering its whole buffer, no second compaction, no copy. The
 *   release handle and `requiresLsn` stay outside the payload, on exec's `ResultFrame`.
 * - The port adds no copy: every buffer in `transfer` moves (exec `frameTransferList` checks a
 *   payload owns exactly what it transfers). Wide decimal coefficients are cloned. This module
 *   imports exec/ for types only, so a client bundle carries no executor code.
 * - Vectors keep their executor form (`VectorData`), so the client sees exact physical values. JS
 *   values are built on the client, on request (api.ts).
 * - **Accounting.** A frame stays charged to its execution's result grant until the client's
 *   `release` signal covers it (protocol.ts), and is released by the owner when the connection
 *   ends.
 * - **Dictionaries are per stream, sent as frame-level deltas.** `payload.dictionaries` lists
 *   `{ id, from, values }`: entries appended to stream dictionary `id` at index `from`, applied
 *   before the frame's columns are read. A dictionary column names its stream dictionary by id;
 *   codes index the whole dictionary so far. Later frames carry only new entries, or none. The
 *   executor stops at `STREAM_DICTIONARY_BYTES` per stream and sends plain values after it. The
 *   client keeps a result's dictionaries for the life of the result (`StreamDictionaries`).
 *
 * ## Batch-write input (ruling R9)
 * Input columns are **structured-cloned, not transferred**: the client keeps them until the
 * request's terminal reply so it can resend them after a reconnect. One clone costs ~4–7 ms per
 * MiB (probe), paid once per write.
 */
import type { DictionaryDelta, FramePayload, WireFlat } from "../exec/result.js";
import type { WireValue } from "./protocol.js";

export type {
  DictionaryDelta,
  FramePayload,
  WireConstant,
  WireDictionary,
  WireFlat,
  WireVector,
} from "../exec/result.js";

/** Client-encoded input for a batch write: one per input column, `rowCount` slots each. Cloned. */
export type WireInputColumn =
  | { readonly kind: "bool"; readonly values: Uint8Array; readonly validity: Uint32Array | null }
  | { readonly kind: "i32"; readonly values: Int32Array; readonly validity: Uint32Array | null }
  | { readonly kind: "f64"; readonly values: Float64Array; readonly validity: Uint32Array | null }
  | {
      readonly kind: "i64";
      readonly values: BigInt64Array;
      readonly validity: Uint32Array | null;
    }
  | {
      readonly kind: "utf8";
      /** Value i is `bytes[offsets[i] … offsets[i + 1])`. */
      readonly bytes: Uint8Array;
      readonly offsets: Int32Array;
      readonly validity: Uint32Array | null;
    }
  | { readonly kind: "values"; readonly values: readonly WireValue[] };

/**
 * The client's per-stream dictionaries: deltas applied in frame order, entries kept as the chunks
 * that arrived (no copy). A delta whose `from` is not the dictionary's current size, or a frame
 * out of sequence, is a protocol error (a lost or reordered frame): `RangeError`.
 */
export class StreamDictionaries {
  readonly #dictionaries = new Map<number, { size: number; readonly parts: WireFlat[] }>();
  #nextSequence = 0;

  /** Applies `payload.dictionaries`. Call once per frame, in sequence order, before reading it. */
  apply(payload: FramePayload): void {
    if (payload.sequence !== this.#nextSequence)
      throw new RangeError(
        `Frame ${String(payload.sequence)} arrived, expected ${String(this.#nextSequence)}`,
      );
    this.#nextSequence++;
    for (const delta of payload.dictionaries) this.#append(delta);
  }

  #append(delta: DictionaryDelta): void {
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

  /** Entries of dictionary `id` so far. */
  size(id: number): number {
    return this.#dictionaries.get(id)?.size ?? 0;
  }

  /** The chunk holding `code`, and the code's index inside it. */
  locate(id: number, code: number): { readonly entries: WireFlat; readonly index: number } {
    let index = code;
    for (const entries of this.#dictionaries.get(id)?.parts ?? []) {
      const count = entries.length;
      if (index < count) return { entries, index };
      index -= count;
    }
    throw new RangeError(`Code ${String(code)} is outside dictionary ${String(id)}`);
  }
}
