/** MATCH is non-strict: NULL cells do not hide a match in another field. */
import {
  tokenize,
  termMatches,
  fullTermsMask,
  tokenizeQuery,
  validateFtsQuery,
  type FtsQueryTerm,
} from "../../full-text.js";
import { Slice, runToEnd, type SlicedJob } from "../slices.js";
import type { QueryRuntime } from "../run.js";
import type { Batch, Selection, Vector } from "../../vector/vector.js";
import { decodeUtf8 } from "../../vector/varlen.js";
import { FTS_WORKSPACE_BYTES, ftsValue } from "../fts-value.js";
import {
  countIn,
  rowsIn,
  flatOf,
  slotIndex,
  isValidSlot,
  type Compiled,
  type OutputArena,
} from "./vectors.js";

const charged = new WeakSet<OutputArena>();
const capacities = new WeakMap<OutputArena, number>();
export const ftsRuntimeBytes = new WeakMap<QueryRuntime, number>();
export function configureFtsWorkspace(arena: OutputArena, bytes: number): void {
  capacities.set(arena, bytes);
}
export function inheritFtsWorkspace(parent: OutputArena, child: OutputArena): void {
  configureFtsWorkspace(child, capacities.get(parent) ?? FTS_WORKSPACE_BYTES);
}
export function ftsWorkspace(arena: OutputArena): void {
  if (charged.has(arena)) return;
  arena.take(capacities.get(arena) ?? FTS_WORKSPACE_BYTES);
  charged.add(arena);
}
export function queryText(vector: Vector): string {
  if (!isValidSlot(vector, 0) || vector.type.kind !== "text")
    throw new TypeError("A full-text search query parameter must be a string");
  const data = flatOf(vector).data;
  if (data.layout !== "varlen")
    throw new TypeError("A full-text search query parameter must be a string");
  const slot = slotIndex(vector, 0);
  if ((data.offsets[slot + 1] ?? 0) - (data.offsets[slot] ?? 0) > 4 * 16384)
    throw new RangeError("Full-text query is too long");
  return decodeUtf8(data, slot);
}
/** Copy UTF-16 units, including a v1 truncation at a surrogate boundary. Substrings
 * must not pin an entire normalized/decoded 16 KiB query for each cached term. */
export const FTS_QUERY_STATE_BYTES = 8192;
export function queryTerms(text: string): readonly FtsQueryTerm[] {
  validateFtsQuery(text);
  const units = new Uint16Array(32);
  return tokenizeQuery(text).map((value) => {
    for (let i = 0; i < value.term.length; i++) units[i] = value.term.charCodeAt(i);
    return {
      prefix: value.prefix,
      term: String.fromCharCode(...units.subarray(0, value.term.length)),
    };
  });
}
const hosts = new WeakMap<OutputArena, (node: FtsMatchNode) => void>();
export function installFtsHost(
  arena: OutputArena,
  host: ((node: FtsMatchNode) => void) | null,
): void {
  if (host) hosts.set(arena, host);
  else hosts.delete(arena);
}
/** The relational owner precomputes inert column-only FTS, keeping errors deferred to demand.
 * Synchronous residual callers retain the same value law and fixed workspace. */
export class FtsMatchNode implements Compiled {
  readonly type = { kind: "bool" } as const;
  readonly #out;
  #termsReady = false;
  #terms: readonly FtsQueryTerm[] = [];
  #prepared = false;
  #failed = false;
  #error: unknown;
  constructor(
    readonly args: readonly Compiled[],
    arena: OutputArena,
  ) {
    if (!args[0]) throw new TypeError("MATCH requires a query");
    ftsWorkspace(arena);
    arena.take(FTS_QUERY_STATE_BYTES + args.length * 16);
    this.#out = arena.flat(this.type);
  }
  *compute(batch: Batch, selection: Selection | null, slice: Slice): SlicedJob<void> {
    this.#prepared = false;
    this.#failed = false;
    this.#error = undefined;
    if (!this.#termsReady) {
      const query = this.args[0];
      if (!query) throw new TypeError("MATCH requires a query");
      this.#terms = queryTerms(queryText(query.evaluate(batch, selection)));
      this.#termsReady = true;
    }
    const inputs = this.args.slice(1).map((arg) => arg.evaluate(batch, selection));
    this.#out.validity.fill(0);
    const output = this.#out.vector;
    if (output.data.layout !== "bool") throw new TypeError("Expected MATCH output");
    const rows = rowsIn(selection),
      count = countIn(selection, batch.length),
      full = fullTermsMask(this.#terms.length);
    for (let k = 0; k < count; k++) {
      const row = rows[k] ?? 0;
      let any = false,
        mask = 0;
      for (const input of inputs) {
        const rendered = ftsValue(input, row);
        if (rendered !== undefined) {
          any = true;
          const tokens = tokenize(rendered);
          for (let t = 0; t < tokens.length; t++) {
            const token = tokens[t];
            if (token === undefined) throw new Error("FTS token vanished");
            for (let term = 0; term < this.#terms.length; term++) {
              const query = this.#terms[term];
              if (!query) throw new Error("FTS query term vanished");
              if (!(mask & (1 << term)) && termMatches(token, query)) mask |= 1 << term;
            }
            if ((t & 63) === 0 && slice.due()) yield;
          }
          if (this.#terms.length > 0 && mask === full) break;
        }
        if (slice.due()) yield;
      }
      if (any) {
        this.#out.validity[row >>> 5] = (this.#out.validity[row >>> 5] ?? 0) | (1 << (row & 31));
        output.data.values[row] = Number(this.#terms.length > 0 && mask === full);
      }
      if (slice.due()) yield;
    }
    output.length = batch.length;
    output.validity = this.#out.validity;
    this.#prepared = true;
  }
  fail(error: unknown): void {
    this.#failed = true;
    this.#error = error;
    this.#prepared = true;
  }
  clear(): void {
    this.#prepared = false;
    this.#failed = false;
    this.#error = undefined;
  }
  evaluate(batch: Batch, selection: Selection | null): Vector {
    if (!this.#prepared) runToEnd(this.compute(batch, selection, new Slice(Infinity)));
    if (this.#failed) throw this.#error;
    const output = this.#out.vector;
    // A synchronous caller evaluates each new batch; hosted callers clear per row.
    if (!this.hosted) this.#prepared = false;
    return output;
  }
  hosted = false;
}
export function compileFtsMatch(args: readonly Compiled[], arena: OutputArena): Compiled {
  const node = new FtsMatchNode(args, arena),
    host = hosts.get(arena);
  if (host) {
    node.hosted = true;
    host(node);
  }
  return node;
}
