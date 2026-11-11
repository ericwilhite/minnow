/** A bounded, exact key-domain plan for snapshot replay. No sketches or floating state merges.
 * Planning visits keys on the pinned snapshot before completing any aggregate group. A pass
 * owns a contiguous range of mixed-radix key IDs, so at most groupsPerPass possible tuples
 * can finish in it. The retained domains and scratch stay inside one admitted fixed budget.
 */
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import { BATCH_ROWS, type Selection, type FlatVector, type Vector } from "../../vector/vector.js";
import { IDENTITY } from "../kernels/vectors.js";
import { GroupTable } from "./groups.js";
import { nullConstant } from "../column-store.js";

export const MAX_AGGREGATE_PASSES = 32;
export const GROUP_PROBE_BYTES = 1 << 20;
export interface PartitionBounds {
  readonly rows: number;
  readonly domains: readonly number[];
  readonly groups: number;
  readonly groupsPerPass: number;
  readonly passes: number;
  readonly keyBytes: readonly number[];
  readonly dictionaryEntries: readonly number[];
  readonly payloadBytes: readonly number[];
}

export class GroupPartitionProbe {
  readonly #tables: GroupTable[] = [];
  readonly #ids: Int32Array[] = [];
  readonly #one: Vector[] = [];
  readonly #dictionaryEntries: number[];
  readonly #payloadBytes: number[];
  readonly #budget: ByteCharge;
  #used = 0;
  #scratch = 0;
  #limited = false;
  #closed = false;
  #sealed = false;
  #rows = 0;
  #bounds: PartitionBounds | null = null;
  constructor(
    readonly types: readonly PhysicalType[],
    readonly payloads: number,
    readonly charge: ByteCharge,
    readonly groupsPerPass = 1024,
    readonly maxPasses = MAX_AGGREGATE_PASSES,
    readonly limitBytes = GROUP_PROBE_BYTES,
  ) {
    if (
      types.length === 0 ||
      !Number.isSafeInteger(groupsPerPass) ||
      groupsPerPass < 1 ||
      !Number.isSafeInteger(maxPasses) ||
      maxPasses < 1 ||
      maxPasses > MAX_AGGREGATE_PASSES
    )
      throw new RangeError("Invalid bounded aggregation partition plan");
    this.#dictionaryEntries = Array.from({ length: types.length + payloads }, () => 0);
    this.#payloadBytes = Array.from({ length: payloads }, () => 0);
    this.#budget = {
      allocate: (bytes) => {
        if (this.#used + bytes > this.limitBytes) {
          this.#limited = true;
          return false;
        }
        if (!charge.allocate(bytes)) return false;
        this.#used += bytes;
        return true;
      },
      free: (bytes) => {
        this.#used -= bytes;
        charge.free(bytes);
      },
    };
    try {
      for (const type of types) {
        this.#tables.push(new GroupTable([type], this.#budget));
        const bytes = 4 * BATCH_ROWS;
        if (!this.#budget.allocate(bytes))
          throw new RangeError("Group probe scratch was not admitted");
        this.#scratch += bytes;
        this.#ids.push(new Int32Array(BATCH_ROWS));
      }
    } catch (error) {
      this.#release();
      // A probe whose own bounded workspace cannot represent the domains selects
      // the external plan. A native grant denial inside that admitted workspace
      // is a separate accounting error and must not masquerade as a certificate.
      if (this.#limited) return;
      this.#closed = true;
      throw error;
    }
  }
  /** Exact codebooks may conservatively include unused/deleted entries. No row-code scan.
   * A nullable chunk contributes the SQL NULL class even when no dictionary entry is NULL.
   */
  offerDomain(column: number, dictionary: FlatVector | null, nullable: boolean): boolean {
    if (this.#closed || this.#sealed) throw new Error("Group probe is not accepting domains");
    if (this.#limited) return false;
    const table = this.#tables[column],
      ids = this.#ids[column],
      type = this.types[column];
    if (!table || !ids || !type) throw new RangeError("Missing group domain");
    try {
      if (dictionary !== null) {
        if (dictionary.length > BATCH_ROWS) {
          this.#limited = true;
          return false;
        }
        this.#dictionaryEntries[column] = Math.max(
          this.#dictionaryEntries[column] ?? 0,
          dictionary.length,
        );
        this.#one[0] = dictionary;
        table.assign(this.#one, dictionary.length, null, ids);
      }
      if (nullable) {
        this.#one[0] = nullConstant(type, 1);
        table.assign(this.#one, 1, null, ids);
      }
      if (this.#tables.reduce((n, t) => n * t.groupCount, 1) > this.groupsPerPass * this.maxPasses)
        this.#limited = true;
      return !this.#limited;
    } catch (error) {
      if (this.#limited) return false;
      throw error;
    } finally {
      this.#one.length = 0;
    }
  }
  offerPayload(column: number, dictionary: FlatVector): boolean {
    if (this.#closed || this.#sealed)
      throw new Error("Group probe is not accepting payload bounds");
    if (this.#limited) return false;
    if (column < 0 || column >= this.payloads) throw new RangeError("Missing payload bound");
    if (dictionary.length > BATCH_ROWS) {
      this.#limited = true;
      return false;
    }
    if (dictionary.data.layout === "fixed16") {
      this.#payloadBytes[column] = 16;
      return true;
    }
    if (dictionary.data.layout !== "varlen")
      throw new TypeError("Group probe payload must be bytes");
    const offsets = dictionary.data.offsets;
    this.#dictionaryEntries[this.types.length + column] = Math.max(
      this.#dictionaryEntries[this.types.length + column] ?? 0,
      dictionary.length,
    );
    for (let row = 0; row < dictionary.length; row++)
      this.#payloadBytes[column] = Math.max(
        this.#payloadBytes[column] ?? 0,
        (offsets[row + 1] ?? 0) - (offsets[row] ?? 0),
      );
    return true;
  }
  /** Exact snapshot live-row count; called once when planning from codebooks only. */
  sourceRows(rows: number): void {
    if (this.#closed || this.#sealed || this.#rows !== 0)
      throw new Error("Group probe row bound already set");
    if (!Number.isSafeInteger(rows) || rows < 0 || rows > 2 ** 21) this.#limited = true;
    this.#rows = rows;
  }
  /** False selects the external plan; it never certifies an incomplete scan. */
  offer(
    keys: readonly Vector[],
    payloads: readonly Vector[],
    length: number,
    selection: Selection | null,
    rowsAlreadyBounded = false,
  ): boolean {
    if (this.#closed || this.#sealed) throw new Error("Group probe is not accepting input");
    if (this.#limited) return false;
    if (!rowsAlreadyBounded) this.#rows += selection?.count ?? length;
    // This is the numeric-bank prefix certificate, not an assumption about future batches.
    if (this.#rows > 2 ** 21) {
      this.#limited = true;
      return false;
    }
    try {
      for (let c = 0; c < this.types.length; c++) {
        const key = keys[c],
          table = this.#tables[c],
          ids = this.#ids[c];
        if (!key || !table || !ids) throw new Error("Missing group probe key");
        if (key.form === "dictionary") {
          if (key.dictionary.length > BATCH_ROWS) {
            this.#limited = true;
            return false;
          }
          this.#dictionaryEntries[c] = Math.max(
            this.#dictionaryEntries[c] ?? 0,
            key.dictionary.length,
          );
          this.#one[0] = key.dictionary;
          table.assign(this.#one, key.dictionary.length, null, ids);
          // Include a row NULL even if its dictionary has no NULL entry. Including unused
          // entries is conservative; a non-null dictionary row need not scan its codes.
          if (key.validity !== null) {
            this.#one[0] = key;
            table.assign(this.#one, length, selection, ids);
          }
        } else {
          this.#one[0] = key;
          table.assign(this.#one, length, selection, ids);
        }
        let product = 1;
        for (const t of this.#tables) {
          product *= t.groupCount;
          if (product > this.groupsPerPass * this.maxPasses) {
            this.#limited = true;
            return false;
          }
        }
      }
      for (let c = 0; c < this.payloads; c++) {
        const vector = payloads[c];
        if (!vector) throw new Error("Missing group probe payload");
        const flat =
          vector.form === "dictionary"
            ? vector.dictionary
            : vector.form === "constant"
              ? vector.value
              : vector;
        if (vector.form === "dictionary")
          this.#dictionaryEntries[this.types.length + c] = Math.max(
            this.#dictionaryEntries[this.types.length + c] ?? 0,
            flat.length,
          );
        if (flat.length > BATCH_ROWS) {
          this.#limited = true;
          return false;
        }
        if (flat.data.layout === "fixed16") {
          this.#payloadBytes[c] = 16;
          continue;
        }
        if (flat.data.layout !== "varlen") throw new TypeError("Group probe payload must be bytes");
        const offsets = flat.data.offsets;
        for (let row = 0; row < flat.length; row++)
          this.#payloadBytes[c] = Math.max(
            this.#payloadBytes[c] ?? 0,
            (offsets[row + 1] ?? 0) - (offsets[row] ?? 0),
          );
      }
      return true;
    } catch (error) {
      if (this.#limited) return false;
      throw error;
    } finally {
      this.#one.length = 0;
    }
  }
  /** Called only after the source reaches EOF on the same pinned snapshot. */
  seal(): PartitionBounds | null {
    if (this.#closed) throw new Error("Group probe is closed");
    this.#sealed = true;
    if (this.#limited) return null;
    const domains = this.#tables.map((t) => t.groupCount),
      groups = domains.reduce((a, b) => a * b, 1);
    const passes = Math.max(1, Math.ceil(groups / this.groupsPerPass));
    if (passes > this.maxPasses) throw new Error("Group probe exceeded its planned pass bound");
    this.#bounds = {
      rows: this.#rows,
      domains,
      groups,
      groupsPerPass: this.groupsPerPass,
      passes,
      keyBytes: this.#tables.map((t) => t.keyBytes(0, 0, t.groupCount)),
      dictionaryEntries: [...this.#dictionaryEntries],
      payloadBytes: [...this.#payloadBytes],
    };
    return this.#bounds;
  }
  /** Preserves the input selection's order; a key belongs to exactly one planned pass. */
  select(
    keys: readonly Vector[],
    length: number,
    selection: Selection | null,
    pass: number,
    out: Selection,
  ): void {
    const bound = this.#bounds;
    if (!bound || this.#closed) throw new Error("Group domains are not sealed");
    if (!Number.isInteger(pass) || pass < 0 || pass >= bound.passes)
      throw new RangeError("Group partition outside plan");
    try {
      for (let c = 0; c < this.types.length; c++) {
        const table = this.#tables[c],
          key = keys[c],
          ids = this.#ids[c];
        if (!table || !key || !ids) throw new Error("Missing replay key");
        this.#one[0] = key;
        table.assign(this.#one, length, selection, ids);
        if (table.groupCount !== bound.domains[c])
          throw new Error("Pinned snapshot changed its certified key domain");
      }
      const rows = selection?.rows ?? IDENTITY,
        count = selection?.count ?? length;
      let kept = 0;
      for (let k = 0; k < count; k++) {
        const row = rows[k] ?? 0;
        let ordinal = 0,
          stride = 1;
        for (let c = 0; c < this.types.length; c++) {
          ordinal += (this.#ids[c]?.[row] ?? 0) * stride;
          stride *= bound.domains[c] ?? 0;
        }
        if (Math.floor(ordinal / bound.groupsPerPass) === pass) out.rows[kept++] = row;
      }
      out.count = kept;
    } finally {
      this.#one.length = 0;
    }
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#release();
  }
  #release(): void {
    for (const t of this.#tables) t.close();
    this.#tables.length = 0;
    if (this.#scratch > 0) this.#budget.free(this.#scratch);
    this.#scratch = 0;
    this.#ids.length = 0;
    this.#one.length = 0;
  }
}
