/**
 * The commit-body codec (format/log-frame.ts `CommitFrameBody`, "codec: write-path track").
 *
 *   u8 registered commitBody version; catalog effects contain MCHG1
 *   u8 hasRequest; [blob clientId, varuint sequence, f64 sentAtMs, string countsJson]
 *   varuint effectCount; effects, each `u8 kind` then:
 *     1 createTable   varuint tableId, blob definition
 *     2 alterTable    varuint tableId, varuint schemaVersion, blob definition
 *     3 dropTable     varuint tableId, blob definition
 *     4 catalog       blob definition
 *     5 sequence      varuint sequenceId, bigint reservedThrough
 *     6 rows          varuint tableId, varuint schemaVersion,
 *                     u8 hasInserts; [varuint firstRowId, varuint rowCount, varuint columns,
 *                                     u8 hasColumnIds, [varuint ids, varuint columnId…],
 *                                     blob column…]   (columns.ts frame column codec)
 *                     u8 hasDeletes; [u8 hasIndexIds, [varuint ids, varuint indexId…],
 *                                     varuint count, f64 rowId…, per row: varuint keys, blob key…]
 *   Native ROW effects always carry IDs. Absent IDs are strict positional codec controls,
 *   not a compatible predecessor encoding. Body9 is gated by reader8/manifest11.
 *     7 publishedRows varuint tableId, varuint schemaVersion, varuint transaction,
 *                     varuint rootSequence, varuint indexes, varuint indexId…
 *     8 fts           varuint stagedTransaction, varuint protectedRootSequence, u8 invalidated
 *     9 ftsMutation   varuint tableId, varuint schemaVersion (reference-free keyed history)
 */
import { DURABLE_CODECS, DURABLE_FORMATS, requireTag } from "../durable-format.js";
import type { BodyEffect, CommitFrameBody } from "../format/log-frame.js";
import { LogCorruptionError } from "../format/log-frame.js";
import { SMALL_COMMIT_BYTES } from "./commit-buffers.js";
import { ByteReader, ByteWriter } from "./bytes.js";

const VERSION = DURABLE_FORMATS.commitBody.version;
const KIND = DURABLE_CODECS.effect;

/**
 * Encodes a commit body. Into a given `into` writer the result is a view of its bytes, valid until
 * the writer is used again (the log appender copies it into frames at once); else a fresh copy.
 */
export function encodeBody(body: CommitFrameBody, into?: ByteWriter): Uint8Array {
  const writer = into ?? new ByteWriter();
  writer.reset();
  writer.u8(VERSION);
  const request = body.request;
  if (request === null) writer.u8(0);
  else {
    writer.u8(1);
    writer.blob(request.clientId);
    writer.varuint(request.sequence);
    writer.f64(request.sentAtMs);
    const release = writer.allocator?.reserve?.(24 + 2 * (SMALL_COMMIT_BYTES + 16));
    try {
      writer.string(JSON.stringify(request.counts));
    } finally {
      release?.();
    }
  }
  writer.varuint(body.effects.length);
  for (const effect of body.effects) {
    writer.u8(KIND[effect.kind]);
    switch (effect.kind) {
      case "createTable":
        writer.varuint(effect.tableId);
        writer.blob(effect.definition);
        break;
      case "alterTable":
        writer.varuint(effect.tableId);
        writer.varuint(effect.schemaVersion);
        writer.blob(effect.definition);
        break;
      case "dropTable":
        writer.varuint(effect.tableId);
        writer.blob(effect.definition);
        break;
      case "catalog":
        writer.blob(effect.definition);
        break;
      case "sequence":
        writer.varuint(effect.sequenceId);
        writer.bigint(effect.reservedThrough);
        break;
      case "rows": {
        writer.varuint(effect.tableId);
        writer.varuint(effect.schemaVersion);
        const inserts = effect.inserts;
        if (inserts === null) writer.u8(0);
        else {
          writer.u8(1);
          writer.varuint(inserts.firstRowId);
          writer.varuint(inserts.rowCount);
          writer.varuint(inserts.columns.length);
          writeIds(writer, inserts.columnIds);
          for (const column of inserts.columns) writer.blob(column);
        }
        const deletes = effect.deletes;
        if (deletes === null) writer.u8(0);
        else {
          writer.u8(1);
          writeIds(writer, deletes.indexIds);
          writer.varuint(deletes.rowIds.length);
          for (const rowId of deletes.rowIds) writer.f64(rowId);
          for (let k = 0; k < deletes.rowIds.length; k++) {
            const keys = deletes.keys[k] ?? [];
            writer.varuint(keys.length);
            for (const key of keys) writer.blob(key);
          }
        }
        break;
      }
      case "ftsMutation":
        writer.varuint(effect.tableId);
        writer.varuint(effect.schemaVersion);
        break;
      case "fts":
        writer.varuint(effect.transaction);
        writer.varuint(effect.rootSequence);
        writer.u8(effect.invalidated ? 1 : 0);
        break;
      case "publishedRows":
        writer.varuint(effect.tableId);
        writer.varuint(effect.schemaVersion);
        writer.varuint(effect.transaction);
        writer.varuint(effect.rootSequence);
        writer.varuint(effect.indexIds.length);
        for (const id of effect.indexIds) writer.varuint(id);
        break;
    }
  }
  return into === undefined ? writer.finish() : writer.bytes.subarray(0, writer.length);
}

/** Decodes a commit body; copies every byte it keeps. `LogCorruptionError` if malformed. */
export function decodeBody(bytes: Uint8Array): CommitFrameBody {
  try {
    const reader = new ByteReader(bytes);
    if (reader.u8() !== VERSION) throw new LogCorruptionError("Unknown commit body version");
    let request: CommitFrameBody["request"] = null;
    if (readFlag(reader)) {
      const clientId = reader.blob().slice();
      const sequence = reader.varuint();
      const sentAtMs = reader.f64();
      const counts = JSON.parse(reader.string()) as Record<string, number>;
      request = { clientId, sequence, sentAtMs, counts };
    }
    const effects: BodyEffect[] = [];
    const count = reader.varuint();
    for (let k = 0; k < count; k++) effects.push(decodeEffect(reader));
    reader.end();
    return { request, effects };
  } catch (error) {
    if (error instanceof LogCorruptionError) throw error;
    throw new LogCorruptionError(`A commit body is malformed: ${String(error)}`);
  }
}

function readFlag(reader: ByteReader): boolean {
  const value = reader.u8();
  requireTag(value, DURABLE_CODECS.boolean, "commit flag", LogCorruptionError);
  return value === 1;
}

function decodeEffect(reader: ByteReader): BodyEffect {
  const kind = reader.u8();
  switch (kind) {
    case KIND.createTable:
      return { kind: "createTable", tableId: reader.varuint(), definition: reader.blob().slice() };
    case KIND.alterTable:
      return {
        kind: "alterTable",
        tableId: reader.varuint(),
        schemaVersion: reader.varuint(),
        definition: reader.blob().slice(),
      };
    case KIND.dropTable:
      return { kind: "dropTable", tableId: reader.varuint(), definition: reader.blob().slice() };
    case KIND.catalog:
      return { kind: "catalog", definition: reader.blob().slice() };
    case KIND.sequence:
      return { kind: "sequence", sequenceId: reader.varuint(), reservedThrough: reader.bigint() };
    case KIND.rows: {
      const tableId = reader.varuint();
      const schemaVersion = reader.varuint();
      let inserts: Extract<BodyEffect, { kind: "rows" }>["inserts"] = null;
      if (readFlag(reader)) {
        const firstRowId = reader.varuint();
        const rowCount = reader.varuint();
        const columnCount = reader.varuint();
        const columnIds = readIds(reader);
        if (columnIds !== undefined && columnIds.length !== columnCount)
          throw new LogCorruptionError("Logged column IDs differ from column count");
        const columns = Array.from({ length: columnCount }, () => reader.blob().slice());
        inserts = {
          firstRowId,
          rowCount,
          columns,
          ...(columnIds === undefined ? {} : { columnIds }),
        };
      }
      let deletes: Extract<BodyEffect, { kind: "rows" }>["deletes"] = null;
      if (readFlag(reader)) {
        const indexIds = readIds(reader);
        const n = reader.varuint();
        const rowIds = reader.f64Array(n);
        const keys = Array.from({ length: n }, () => {
          const keyCount = reader.varuint();
          if (indexIds !== undefined && keyCount !== indexIds.length)
            throw new LogCorruptionError("Logged keys differ from index ID count");
          return Array.from({ length: keyCount }, () => reader.blob().slice());
        });
        deletes = { rowIds, keys, ...(indexIds === undefined ? {} : { indexIds }) };
      }
      return { kind: "rows", tableId, schemaVersion, inserts, deletes };
    }
    case KIND.ftsMutation: {
      const tableId = reader.varuint(),
        schemaVersion = reader.varuint();
      if (tableId < 1 || schemaVersion < 1)
        throw new LogCorruptionError("Invalid FTS mutation identity");
      return { kind: "ftsMutation", tableId, schemaVersion };
    }
    case KIND.fts: {
      const transaction = reader.varuint(),
        rootSequence = reader.varuint();
      if (transaction < 1 || rootSequence !== transaction)
        throw new LogCorruptionError("Invalid FTS claim identity");
      return { kind: "fts", transaction, rootSequence, invalidated: readFlag(reader) };
    }
    case KIND.publishedRows: {
      const tableId = reader.varuint();
      const schemaVersion = reader.varuint();
      const transaction = reader.varuint();
      const rootSequence = reader.varuint();
      const n = reader.varuint();
      const indexIds = Array.from({ length: n }, () => reader.varuint());
      return { kind: "publishedRows", tableId, schemaVersion, transaction, rootSequence, indexIds };
    }
    default:
      throw new LogCorruptionError(`Unknown commit effect kind ${String(kind)}`);
  }
}

/** A presence flag preserves strict positional codec controls; native effects always name IDs. */
function writeIds(writer: ByteWriter, ids: readonly number[] | undefined): void {
  writer.u8(ids === undefined ? 0 : 1);
  if (ids !== undefined) {
    writer.varuint(ids.length);
    for (const id of ids) writer.varuint(id);
  }
}
function readIds(reader: ByteReader): readonly number[] | undefined {
  if (!readFlag(reader)) return undefined;
  const ids = Array.from({ length: reader.varuint() }, () => reader.varuint());
  if (ids.some((id) => id < 1) || new Set(ids).size !== ids.length)
    throw new LogCorruptionError("Logged identities must be positive and unique");
  return ids;
}
