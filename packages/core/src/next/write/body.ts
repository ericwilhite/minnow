/**
 * The commit-body codec (format/log-frame.ts `CommitFrameBody`, "codec: write-path track").
 *
 *   u8 version (1)
 *   u8 hasRequest; [blob clientId, varuint sequence, f64 sentAtMs, string countsJson]
 *   varuint effectCount; effects, each `u8 kind` then:
 *     1 createTable   varuint tableId, blob definition
 *     2 alterTable    varuint tableId, varuint schemaVersion, blob definition
 *     3 dropTable     varuint tableId
 *     4 catalog       blob definition
 *     5 sequence      varuint sequenceId, bigint reservedThrough
 *     6 rows          varuint tableId, varuint schemaVersion,
 *                     u8 hasInserts; [varuint firstRowId, varuint rowCount, varuint columns,
 *                                     blob column…]   (columns.ts frame column codec)
 *                     u8 hasDeletes; [varuint count, f64 rowId…, per row: varuint keys, blob key…]
 *     7 publishedRows varuint tableId, varuint schemaVersion, varuint transaction,
 *                     varuint rootSequence, varuint indexes, varuint indexId…
 */
import type { BodyEffect, CommitFrameBody } from "../format/log-frame.js";
import { LogCorruptionError } from "../format/log-frame.js";
import { ByteReader, ByteWriter } from "./bytes.js";

const VERSION = 1;
const KIND = {
  createTable: 1,
  alterTable: 2,
  dropTable: 3,
  catalog: 4,
  sequence: 5,
  rows: 6,
  publishedRows: 7,
} as const;

export function encodeBody(body: CommitFrameBody, writer = new ByteWriter()): Uint8Array {
  writer.reset();
  writer.u8(VERSION);
  const request = body.request;
  if (request === null) writer.u8(0);
  else {
    writer.u8(1);
    writer.blob(request.clientId);
    writer.varuint(request.sequence);
    writer.f64(request.sentAtMs);
    writer.string(JSON.stringify(request.counts));
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
          for (const column of inserts.columns) writer.blob(column);
        }
        const deletes = effect.deletes;
        if (deletes === null) writer.u8(0);
        else {
          writer.u8(1);
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
  return writer.finish();
}

/** Decodes a commit body; copies every byte it keeps. `LogCorruptionError` if malformed. */
export function decodeBody(bytes: Uint8Array): CommitFrameBody {
  try {
    const reader = new ByteReader(bytes);
    if (reader.u8() !== VERSION) throw new LogCorruptionError("Unknown commit body version");
    let request: CommitFrameBody["request"] = null;
    if (reader.u8() === 1) {
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
      return { kind: "dropTable", tableId: reader.varuint() };
    case KIND.catalog:
      return { kind: "catalog", definition: reader.blob().slice() };
    case KIND.sequence:
      return { kind: "sequence", sequenceId: reader.varuint(), reservedThrough: reader.bigint() };
    case KIND.rows: {
      const tableId = reader.varuint();
      const schemaVersion = reader.varuint();
      let inserts: Extract<BodyEffect, { kind: "rows" }>["inserts"] = null;
      if (reader.u8() === 1) {
        const firstRowId = reader.varuint();
        const rowCount = reader.varuint();
        const columnCount = reader.varuint();
        const columns = Array.from({ length: columnCount }, () => reader.blob().slice());
        inserts = { firstRowId, rowCount, columns };
      }
      let deletes: Extract<BodyEffect, { kind: "rows" }>["deletes"] = null;
      if (reader.u8() === 1) {
        const n = reader.varuint();
        const rowIds = reader.f64Array(n);
        const keys = Array.from({ length: n }, () => {
          const keyCount = reader.varuint();
          return Array.from({ length: keyCount }, () => reader.blob().slice());
        });
        deletes = { rowIds, keys };
      }
      return { kind: "rows", tableId, schemaVersion, inserts, deletes };
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
