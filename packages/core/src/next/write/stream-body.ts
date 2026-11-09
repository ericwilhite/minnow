/** Existing version-1 commit bytes, projected a frame at a time without materialized columns. */
import type { CommitRequest } from "../access/write-path.js";
import type { FlatVector } from "../vector/vector.js";
import type { LoggedChange } from "./catalog.js";
import { encodeColumn, MEMORY_CHUNK_ROWS } from "./columns.js";
import type { Memtable } from "./memtable.js";
import type { PublishedRows, TransactionImpl, Undo } from "./transaction.js";
import { need } from "./bytes.js";
import { taskClock, yieldTask } from "../exec/pipeline.js";
import { ByteWindow } from "./window.js";

async function pause(check: () => void): Promise<void> {
  check();
  if (taskClock.due()) {
    await yieldTask();
    check();
  }
}

const uintLength = (value: number): number => {
  let bytes = 1;
  while (value >= 128) {
    bytes++;
    value = Math.floor(value / 128);
  }
  return bytes;
};

/** Exact column length, using offsets/layout metadata rather than traversing values. */
export function columnLength(vector: FlatVector, from: number, count: number): number {
  let bytes =
    1 + uintLength(count) + 1 + (vector.validity === null ? 0 : Math.ceil(count / 32) * 4);
  const data = vector.data;
  switch (data.layout) {
    case "bool":
      return bytes + count;
    case "i32":
    case "f32":
      return bytes + 4 * count;
    case "f64":
      return bytes + 8 * count;
    case "int64":
      return bytes + 1 + 8 * count;
    case "fixed16":
      return bytes + 16 * count;
    case "interval":
      return bytes + 1 + 16 * count;
    case "varlen":
      return (
        bytes + 4 * (count + 1) + (data.offsets[from + count] ?? 0) - (data.offsets[from] ?? 0)
      );
    case "decimal":
      bytes += 1 + (data.scale === null ? 2 * count : uintLength(data.scale));
      bytes += 1 + (data.specials === null ? 0 : count);
      if (data.coefficients.repr === "wide")
        throw new RangeError("Decimal coefficients past 128 bits are not stored yet");
      return bytes + 1 + count * (data.coefficients.repr === "int128" ? 16 : 8);
  }
}

type Delete = Extract<Undo, { kind: "memDelete" | "groupDelete" }>;
const isDelete = (entry: Undo): entry is Delete =>
  entry.kind === "memDelete" || entry.kind === "groupDelete";
type Effect =
  | { kind: "inserts"; memtable: Memtable; from: number; count: number }
  | { kind: "published"; memtable: Memtable; names: PublishedRows }
  | {
      kind: "deletes";
      tableId: number;
      schemaVersion: number;
      start: number;
      end: number;
      count: number;
    }
  | { kind: "catalog"; change: LoggedChange };

/** No transaction-sized effect/column/deletion copy. Catalog boundaries preserve the old order. */
async function* effects(
  tx: TransactionImpl,
  staged: (tableId: number) => PublishedRows | null,
  check: () => void,
): AsyncGenerator<Effect> {
  const undo = tx.undo;
  for (let start = 0; start < undo.length;) {
    let end = start;
    while (end < undo.length && undo[end]?.kind !== "catalog") {
      if ((end & 1023) === 0) await pause(check);
      end++;
    }
    for (let i = start; i < end; i++) {
      if ((i & 1023) === 0) await pause(check);
      const entry = need(undo[i]);
      if (entry.kind !== "rows") continue;
      let seen = false;
      for (let k = start; k < i; k++) {
        if ((k & 1023) === 0) await pause(check);
        const previous = need(undo[k]);
        if (previous.kind === "rows" && previous.memtable === entry.memtable) {
          seen = true;
          break;
        }
      }
      if (seen) continue;
      const memtable = entry.memtable;
      if (memtable.staged !== null) {
        if (memtable.rowCount === 0) continue;
        for (let k = 0; k < i; k++) {
          if ((k & 1023) === 0) await pause(check);
          const previous = need(undo[k]);
          if (
            previous.kind === "rows" &&
            previous.memtable.tableId === memtable.tableId &&
            previous.memtable.staged !== null &&
            previous.memtable.rowCount > 0
          ) {
            seen = true;
            break;
          }
        }
        if (!seen) yield { kind: "published", memtable, names: need(staged(memtable.tableId)) };
        continue;
      }
      let to = entry.before + entry.count;
      for (let k = i + 1; k < end; k++) {
        if ((k & 1023) === 0) await pause(check);
        const next = need(undo[k]);
        if (next.kind === "rows" && next.memtable === memtable) to = next.before + next.count;
      }
      for (let from = entry.before; from < to;) {
        const count = Math.min(to - from, MEMORY_CHUNK_ROWS - (from % MEMORY_CHUNK_ROWS));
        yield { kind: "inserts", memtable, from, count };
        from += count;
      }
    }
    for (let i = start; i < end; i++) {
      if ((i & 1023) === 0) await pause(check);
      const entry = need(undo[i]);
      if (!isDelete(entry)) continue;
      let seen = false;
      for (let k = start; k < i; k++) {
        if ((k & 1023) === 0) await pause(check);
        const previous = need(undo[k]);
        if (isDelete(previous) && previous.tableId === entry.tableId) {
          seen = true;
          break;
        }
      }
      if (seen) continue;
      let count = 0;
      let version: number | undefined;
      for (let k = start; k < end; k++) {
        if ((k & 1023) === 0) await pause(check);
        const next = need(undo[k]);
        if (isDelete(next) && next.tableId === entry.tableId) count++;
        if (next.kind === "rows" && next.tableId === entry.tableId)
          version = next.memtable.schemaVersion;
      }
      yield {
        kind: "deletes",
        tableId: entry.tableId,
        schemaVersion: version ?? need(tx.tableVersion(entry.tableId)),
        start,
        end,
        count,
      };
    }
    const catalog = undo[end];
    if (catalog?.kind === "catalog")
      for (const change of catalog.logged) yield { kind: "catalog", change };
    start = end + 1;
  }
}

/** Stable row-id order, with no transaction-sized sort buffer; ordered input is one linear pass. */
async function* deletes(
  undo: readonly Undo[],
  effect: Extract<Effect, { kind: "deletes" }>,
  check: () => void,
): AsyncGenerator<Delete> {
  let ordered = true;
  let last = -Infinity;
  for (let k = effect.start; k < effect.end; k++) {
    if ((k & 1023) === 0) await pause(check);
    const entry = need(undo[k]);
    if (!isDelete(entry) || entry.tableId !== effect.tableId) continue;
    if (entry.rowId < last) ordered = false;
    last = entry.rowId;
  }
  if (ordered) {
    for (let k = effect.start; k < effect.end; k++) {
      if ((k & 1023) === 0) await pause(check);
      const entry = need(undo[k]);
      if (isDelete(entry) && entry.tableId === effect.tableId) yield entry;
    }
    return;
  }
  let rowId = -Infinity;
  let position = -1;
  for (let emitted = 0; emitted < effect.count; emitted++) {
    let best: Delete | null = null;
    let at = -1;
    for (let k = effect.start; k < effect.end; k++) {
      if ((k & 1023) === 0) await pause(check);
      const entry = need(undo[k]);
      if (
        !isDelete(entry) ||
        entry.tableId !== effect.tableId ||
        entry.rowId < rowId ||
        (entry.rowId === rowId && k <= position)
      )
        continue;
      if (best === null || entry.rowId < best.rowId) {
        best = entry;
        at = k;
      }
    }
    yield need(best);
    rowId = need(best).rowId;
    position = at;
  }
}

/** JSON.stringify/encodeJson spelling, streamed in bounded string pieces. */
async function json(
  writer: ByteWindow,
  value: unknown,
  check: () => void,
  tagged = true,
): Promise<void> {
  if (typeof value === "string") {
    writer.u8(34);
    for (let from = 0; from < value.length;) {
      await pause(check);
      let to = Math.min(value.length, from + 256);
      const last = value.charCodeAt(to - 1);
      if (to < value.length && last >= 0xd800 && last <= 0xdbff) to--;
      const quoted = JSON.stringify(value.slice(from, to));
      writer.utf8(quoted.slice(1, -1));
      from = to;
    }
    writer.u8(34);
  } else if (typeof value === "bigint") {
    writer.utf8('{"$bigint":');
    await json(writer, value.toString(), check);
    writer.u8(125);
  } else if (
    tagged &&
    typeof value === "number" &&
    (!Number.isFinite(value) || Object.is(value, -0))
  ) {
    writer.utf8('{"$number":');
    await json(writer, Object.is(value, -0) ? "-0" : String(value), check);
    writer.u8(125);
  } else if (value instanceof Uint8Array) {
    writer.utf8('{"$bytes":[');
    for (let k = 0; k < value.length; k++) {
      if ((k & 1023) === 0) await pause(check);
      if (k > 0) writer.u8(44);
      writer.utf8(String(value[k]));
    }
    writer.utf8("]}");
  } else if (Array.isArray(value)) {
    writer.u8(91);
    for (let k = 0; k < value.length; k++) {
      if (k > 0) writer.u8(44);
      await json(writer, value[k] ?? null, check, tagged);
    }
    writer.u8(93);
  } else if (typeof value === "object" && value !== null) {
    writer.u8(123);
    let first = true;
    for (const key in value) {
      await pause(check);
      if (!Object.hasOwn(value, key)) continue;
      const child = (value as Record<string, unknown>)[key];
      if (child === undefined || typeof child === "function" || typeof child === "symbol") continue;
      if (!first) writer.u8(44);
      first = false;
      await json(writer, key, check);
      writer.u8(58);
      await json(writer, child, check, tagged);
    }
    writer.u8(125);
  } else writer.utf8(JSON.stringify(value));
}

async function jsonBlob(
  writer: ByteWindow,
  value: unknown,
  check: () => void,
  tagged = true,
): Promise<void> {
  const counter = new ByteWindow(writer.target.subarray(0, 0), 0, writer.scratch);
  await json(counter, value, check, tagged);
  writer.varuint(counter.length);
  if (writer.intersects(counter.length)) await json(writer, value, check, tagged);
  else writer.skip(counter.length);
}

export async function streamBody(
  tx: TransactionImpl,
  request: CommitRequest | null,
  staged: (tableId: number) => PublishedRows | null,
  writer: ByteWindow,
  check: () => void = () => undefined,
  order?: Int32Array,
): Promise<number> {
  writer.u8(1);
  if (request === null) writer.u8(0);
  else {
    writer.u8(1);
    writer.blob(request.clientId);
    writer.varuint(request.sequence);
    writer.f64(request.sentAtMs);
    // counts uses ordinary JSON.stringify, not encodeJson's tagged values. Counts are numbers.
    await jsonBlob(writer, request.counts, check, false);
  }
  let count = 0;
  for await (const effect of effects(tx, staged, check)) {
    void effect;
    count++;
    await pause(check);
  }
  writer.varuint(count);
  let orderOffset = 0;
  for await (const effect of effects(tx, staged, check)) {
    await pause(check);
    switch (effect.kind) {
      case "catalog": {
        const change = effect.change;
        if (change.op === "dropTable") {
          writer.u8(3);
          writer.varuint(change.tableId);
        } else {
          writer.u8(change.op === "createTable" ? 1 : 4);
          if (change.op === "createTable") writer.varuint(change.table.id);
          await jsonBlob(writer, change, check);
        }
        break;
      }
      case "published":
        writer.u8(7);
        writer.varuint(effect.memtable.tableId);
        writer.varuint(effect.memtable.schemaVersion);
        writer.varuint(effect.names.transaction);
        writer.varuint(effect.names.rootSequence);
        writer.varuint(effect.names.indexIds.length);
        for (const id of effect.names.indexIds) writer.varuint(id);
        break;
      case "inserts": {
        const { memtable, from, count } = effect;
        writer.u8(6);
        writer.varuint(memtable.tableId);
        writer.varuint(memtable.schemaVersion);
        writer.u8(1);
        writer.varuint(memtable.firstRowId + from);
        writer.varuint(count);
        writer.varuint(memtable.chunks.length);
        for (const stored of memtable.chunks) {
          await pause(check);
          const vector = need(stored[Math.floor(from / MEMORY_CHUNK_ROWS)]).view();
          const slot = from % MEMORY_CHUNK_ROWS;
          const length = columnLength(vector, slot, count);
          writer.varuint(length);
          if (writer.intersects(length)) encodeColumn(vector, slot, count, writer);
          else writer.skip(length);
        }
        writer.u8(0);
        break;
      }
      case "deletes": {
        writer.u8(6);
        writer.varuint(effect.tableId);
        writer.varuint(effect.schemaVersion);
        writer.u8(0);
        writer.u8(1);
        writer.varuint(effect.count);
        const ordered = async function* (): AsyncGenerator<Delete> {
          if (order !== undefined) {
            for (let k = 0; k < effect.count; k++) {
              if ((k & 1023) === 0) await pause(check);
              const entry = need(tx.undo[order[orderOffset + k] ?? -1]);
              if (!isDelete(entry)) throw new Error("Invalid deletion order");
              yield entry;
            }
          } else yield* deletes(tx.undo, effect, check);
        };
        const rowBytes = effect.count * 8;
        if (writer.intersects(rowBytes))
          for await (const entry of ordered()) writer.f64(entry.rowId);
        else writer.skip(rowBytes);
        let keyBytes = 0;
        for (let k = effect.start; k < effect.end; k++) {
          if ((k & 1023) === 0) await pause(check);
          const entry = need(tx.undo[k]);
          if (!isDelete(entry) || entry.tableId !== effect.tableId) continue;
          keyBytes += uintLength(entry.keys.length);
          for (const key of entry.keys) keyBytes += uintLength(key.length) + key.length;
        }
        if (writer.intersects(keyBytes))
          for await (const entry of ordered()) {
            writer.varuint(entry.keys.length);
            for (const key of entry.keys) writer.blob(key);
          }
        else writer.skip(keyBytes);
        orderOffset += effect.count;
        break;
      }
    }
  }
  return count;
}

/** Charged row-order index, sorted in place with no native sort's hidden temporary arrays. */
export async function sortDeleteOrder(
  tx: TransactionImpl,
  staged: (tableId: number) => PublishedRows | null,
  order: Int32Array,
  check: () => void,
): Promise<void> {
  let offset = 0;
  const less = (a: number, b: number): boolean => {
    const left = need(tx.undo[a]);
    const right = need(tx.undo[b]);
    if (!isDelete(left) || !isDelete(right)) throw new Error("Invalid deletion order");
    return left.rowId < right.rowId || (left.rowId === right.rowId && a < b);
  };
  for await (const effect of effects(tx, staged, check)) {
    if (effect.kind !== "deletes") continue;
    let length = 0;
    for (let k = effect.start; k < effect.end; k++) {
      if ((k & 1023) === 0) await pause(check);
      const entry = need(tx.undo[k]);
      if (isDelete(entry) && entry.tableId === effect.tableId) order[offset + length++] = k;
    }
    const sift = (root: number, size: number): void => {
      const value = order[offset + root] ?? 0;
      while (root * 2 + 1 < size) {
        let child = root * 2 + 1;
        if (child + 1 < size && less(order[offset + child] ?? 0, order[offset + child + 1] ?? 0))
          child++;
        if (!less(value, order[offset + child] ?? 0)) break;
        order[offset + root] = order[offset + child] ?? 0;
        root = child;
      }
      order[offset + root] = value;
    };
    for (let k = (length >>> 1) - 1; k >= 0; k--) {
      if ((k & 1023) === 0) await pause(check);
      sift(k, length);
    }
    for (let end = length - 1; end > 0; end--) {
      if ((end & 1023) === 0) await pause(check);
      const root = order[offset] ?? 0;
      order[offset] = order[offset + end] ?? 0;
      order[offset + end] = root;
      sift(0, end);
    }
    offset += length;
  }
  if (offset !== order.length) throw new Error("Deletion order count differs from transaction");
}
