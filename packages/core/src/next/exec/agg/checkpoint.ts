/** Exact prefix state precedes every suffix row. Lanes store native transition fields and
 * logical DISTINCT membership, never partial aggregate results or combined float states.
 * One lane per transition owner bounds its row independently of the number of aggregates.
 */
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { FlatVector } from "../../vector/vector.js";
import type { AggregateOperator, CheckpointCall } from "./aggregate.js";
import { HashTape, type TapeDirectory } from "./hash-tape.js";
import { StateRow, StateReader } from "./state-row.js";

interface Lane {
  readonly tape: HashTape;
  types: readonly PhysicalType[] | null;
}

export class GroupCheckpoint {
  readonly #states = new Map<number, Lane>();
  readonly #members = new Map<number, Lane>();
  readonly #values = new Map<number, Lane>();
  #groups = 0;
  #written = false;
  calls: readonly CheckpointCall[] = [];
  constructor(
    readonly directory: TapeDirectory,
    readonly keyCount: number,
  ) {}
  #lane(map: Map<number, Lane>, call: number): Lane {
    let lane = map.get(call);
    if (!lane) {
      lane = { tape: new HashTape(this.directory), types: null };
      map.set(call, lane);
    }
    return lane;
  }
  #saveKeys(hash: AggregateOperator, group: number, row: StateRow): void {
    const keys = hash.groupTable;
    if (this.keyCount > 0 && !keys) throw new Error("Missing checkpoint keys");
    if (keys)
      for (let c = 0; c < this.keyCount; c++)
        row.value(keys.keyBytes(c, group, 1), () => keys.emitKey(c, group, 1));
  }
  async #append(lane: Lane, row: StateRow): Promise<void> {
    const io = this.directory.io;
    lane.types ??= row.columns.map((column) => column.type);
    if (lane.types.length !== row.columns.length) throw new Error("Checkpoint schema changed");
    let page = io.pages.encode(
      { queryId: io.context.queryId, run: 0, partition: 0, sequence: lane.tape.pageCount },
      row.columns,
      1,
    );
    if (!page) {
      await io.need(io.pages.growthFor(row.columns, 1));
      page = io.pages.encode(
        { queryId: io.context.queryId, run: 0, partition: 0, sequence: lane.tape.pageCount },
        row.columns,
        1,
      );
      if (!page) throw new Error("Checkpoint row exceeded its admitted I/O step");
    }
    if (!lane.tape.write) throw new Error("Missing checkpoint writer");
    await lane.tape.write(page, 1);
  }
  async #row(charge: ByteCharge, fill: (row: StateRow) => void, lane: Lane): Promise<void> {
    const io = this.directory.io;
    // Release the previous page's temporary capacity BEFORE measuring the next row.
    io.pages.shrinkIf(true);
    io.pool.settle();
    const measured = new StateRow(charge, true);
    let bytes: number;
    try {
      fill(measured);
      bytes = measured.byteLength;
    } finally {
      measured.close();
    }
    // Complete native row + encoding/page growth + field/buffer headers. The numeric
    // inspection scratch was charged even during measurement; emitters were not called.
    await io.need(3 * bytes + 65536);
    const row = new StateRow(charge);
    try {
      fill(row);
      await this.#append(lane, row);
    } finally {
      row.close();
      io.pages.shrinkIf(true);
      io.pool.settle();
    }
  }

  stateTypes(call: number): readonly PhysicalType[] {
    const types = this.#states.get(call)?.types;
    if (!types) throw new Error("Missing saved transition schema");
    return types.slice(this.keyCount);
  }
  /** Sorted restoration needs exact fields and logical membership, not global IDs. */
  async *records(): AsyncGenerator<
    { kind: "state" | "member"; call: number; columns: readonly FlatVector[] },
    void,
    void
  > {
    for (const [call, lane] of this.#states)
      if (lane.types)
        for await (const batch of lane.tape.read(lane.types)) {
          await this.#pace();
          if (batch.length !== 1) throw new Error("Expected one saved state row");
          yield { kind: "state", call, columns: batch.columns };
        }
    for (const [call, lane] of this.#members)
      if (lane.types)
        for await (const batch of lane.tape.read(lane.types)) {
          await this.#pace();
          if (batch.length !== 1) throw new Error("Expected one saved member row");
          yield { kind: "member", call, columns: batch.columns };
        }
  }

  async #pace(): Promise<void> {
    const io = this.directory.io;
    io.checkAborted();
    const wait = io.pace?.();
    if (wait) await wait;
  }
  /** Call only at a transition boundary. No borrowed input is retained or re-evaluated. */
  async save(hash: AggregateOperator, groups: number, charge: ByteCharge): Promise<void> {
    if (this.#written) throw new Error("A checkpoint is immutable");
    this.#written = true;
    this.#groups = groups;
    this.calls = hash.checkpointCalls();
    const calls = hash.stateCalls;
    for (const call of calls.length === 0 ? [-1] : calls) {
      const lane = this.#lane(this.#states, call);
      for (let group = 0; group < groups; group++) {
        await this.#pace();
        await this.#row(
          charge,
          (row) => {
            this.#saveKeys(hash, group, row);
            if (call >= 0) hash.saveCall(call, group, row);
          },
          lane,
        );
      }
    }
    for (const value of hash.distinctValues()) {
      await this.#pace();
      await this.#row(
        charge,
        (row) => hash.saveMember(value.call, value.id, row),
        this.#lane(this.#values, value.call),
      );
    }
    for (const member of hash.distinctMembers()) {
      await this.#pace();
      if (!member || member.group >= groups) continue;
      await this.#row(
        charge,
        (row) => {
          this.#saveKeys(hash, member.group, row);
          hash.saveMember(member.call, member.id, row);
        },
        this.#lane(this.#members, member.call),
      );
    }
  }
  #group(hash: AggregateOperator, columns: readonly FlatVector[], row: number): number {
    if (this.keyCount === 0) return 0;
    // GROUP BY input positions are not necessarily contiguous; reconstruct only those slots.
    const keys = columns.slice(0, this.keyCount);
    return hash.restoreKey(keys, row);
  }
  /** The caller admits the entire restore workspace before this method. Membership has no
   * transitions: all lanes drain before the caller may supply even one continuation row. */
  async resume(hash: AggregateOperator, continuation: () => Promise<void>): Promise<void> {
    await this.#restore(hash);
    await continuation();
  }
  async #restore(hash: AggregateOperator): Promise<void> {
    if (!this.#written) throw new Error("Unwritten checkpoint");
    hash.restoreStateGroups(this.#groups);
    for (const [call, lane] of this.#states) {
      if (!lane.types) continue;
      for await (const batch of lane.tape.read(lane.types)) {
        await this.#pace();
        const columns = batch.columns,
          group = this.#group(hash, columns, 0),
          reader = new StateReader(columns, 0);
        for (let c = 0; c < this.keyCount; c++) reader.value();
        if (call >= 0) hash.restoreCall(call, group, reader);
        if (reader.consumed !== columns.length) throw new Error("Checkpoint field mismatch");
      }
    }
    if (hash.groupCount !== this.#groups) throw new Error("Checkpoint group cardinality changed");
    for (const [call, lane] of this.#values) {
      if (!lane.types) continue;
      for await (const batch of lane.tape.read(lane.types)) {
        await this.#pace();
        const value = batch.columns[0];
        if (!value) throw new Error("Missing DISTINCT registry value");
        hash.restoreDistinctValue(call, value, 0);
      }
    }
    for (const [call, lane] of this.#members) {
      if (!lane.types) continue;
      for await (const batch of lane.tape.read(lane.types)) {
        await this.#pace();
        const columns = batch.columns,
          group = this.#group(hash, columns, 0),
          value = columns[this.keyCount];
        if (!value) throw new Error("Missing DISTINCT checkpoint value");
        if (hash.groupCount !== this.#groups) throw new Error("Unknown membership group");
        hash.restoreMember(call, group, value, 0);
      }
    }
  }
  async close(): Promise<void> {
    const results = await Promise.allSettled(
      [...this.#states.values(), ...this.#values.values(), ...this.#members.values()].map((lane) =>
        lane.tape.release(),
      ),
    );
    this.#states.clear();
    this.#members.clear();
    this.#values.clear();
    const errors = results
      .filter((r): r is PromiseRejectedResult => r.status === "rejected")
      .map((r) => r.reason as unknown);
    if (errors.length) throw new AggregateError(errors, "Checkpoint close failed");
  }
}
