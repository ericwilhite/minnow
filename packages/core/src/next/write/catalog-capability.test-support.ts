/** Independent field-ID/flags/length mutations; no production schema drives the expectations. */
import { ByteReader, ByteWriter } from "./bytes.js";
import type { CatalogChange } from "../access/catalog.js";
import { Catalog } from "./catalog.js";
import { column, INT, TEXT, tableInput } from "./test-support.js";
import { ftsDef } from "../access/fts-def.js";
import { catalogStream } from "./catalog-stream.js";
export const capabilityCases = [
  "optional field",
  "required field",
  "unknown flags",
  "duplicate field",
  "out-of-order field",
  "unknown feature",
  "duplicate feature",
  "out-of-order feature",
  "feature disagreement",
  "unknown family",
  "record version",
  "stream version",
  "expression dialect",
  "semantic downgrade",
  "section order",
] as const;
export type CapabilityCase = (typeof capabilityCases)[number];
export interface WireField {
  id: number;
  flags: number;
  bytes: Uint8Array;
}
export function fields(source: Uint8Array): {
  family: number;
  version: number;
  fields: WireField[];
} {
  const r = new ByteReader(source),
    family = r.varuint(),
    version = r.varuint(),
    body = new ByteReader(r.blob());
  r.end();
  const result: WireField[] = [];
  while (!body.done) result.push({ id: body.varuint(), flags: body.u8(), bytes: body.blob() });
  return { family, version, fields: result };
}
export function record(family: number, version: number, items: readonly WireField[]): Uint8Array {
  const body = new ByteWriter();
  for (const f of items) {
    body.varuint(f.id);
    body.u8(f.flags);
    body.blob(f.bytes);
  }
  const out = new ByteWriter();
  out.varuint(family);
  out.varuint(version);
  out.blob(body.finish());
  return out.finish();
}
function edit(source: Uint8Array, change: (items: WireField[]) => void): Uint8Array {
  const r = fields(source);
  change(r.fields);
  return record(r.family, r.version, r.fields);
}
function field(
  source: Uint8Array,
  id: number,
  change: (bytes: Uint8Array) => Uint8Array,
): Uint8Array {
  return edit(source, (items) => {
    const f = items.find((f) => f.id === id);
    if (!f) throw new Error("missing fixture field");
    f.bytes = change(f.bytes);
  });
}
function firstArrayRecord(
  source: Uint8Array,
  change: (bytes: Uint8Array) => Uint8Array,
): Uint8Array {
  const r = new ByteReader(source),
    count = r.varuint(),
    out = new ByteWriter();
  out.varuint(count);
  for (let i = 0; i < count; i++) {
    const at = r.offset;
    r.varuint();
    r.varuint();
    r.blob();
    const bytes = source.subarray(at, r.offset);
    out.raw(i === 0 ? change(bytes) : bytes);
  }
  r.end();
  return out.finish();
}
function uints(values: readonly number[]): Uint8Array {
  const out = new ByteWriter();
  out.varuint(values.length);
  for (const value of values) out.varuint(value);
  return out.finish();
}
export function capabilityChanges(): readonly CatalogChange[] {
  const { id, ...fts } = ftsDef(1, 1, 2);
  void id;
  return [
    {
      kind: "createTable",
      table: tableInput("source", [column("id", INT, false), column("text", TEXT)]),
    },
    { kind: "createTable", table: tableInput("sink", [column("id", INT)], { primaryKey: null }) },
    { kind: "createFts", fts },
    {
      kind: "createTrigger",
      trigger: {
        name: "user_after",
        tableId: 1,
        event: "insert",
        timing: "after",
        dialectVersion: 1,
        statements: [
          {
            sql: "INSERT INTO sink DEFAULT VALUES",
            kind: "insert",
            targetTableId: 2,
            targetName: "sink",
            bindings: [],
            columns: [],
            insertColumnIds: [],
            defaultColumnIds: [],
            implicitArity: null,
            assignedColumnIds: [],
            typeIds: [],
            sequenceIds: [],
          },
        ],
      },
    },
  ];
}
export function capabilityCatalog(): Catalog {
  let c = Catalog.empty();
  for (const change of capabilityChanges()) c = c.change(change, c.version + 1).catalog;
  return c;
}
export function futureRecord(
  source: Uint8Array,
  which: CapabilityCase,
  tag: number,
  payload: Uint8Array,
  flags: number,
): Uint8Array {
  if (which === "unknown family" || which === "record version") {
    const r = fields(source);
    return record(
      which === "unknown family" ? tag : r.family,
      which === "record version" ? 2 : r.version,
      r.fields,
    );
  }
  return edit(source, (items) => {
    if (which === "optional field" || which === "required field")
      items.push({ id: tag, flags: which === "required field" ? 1 : 0, bytes: payload });
    else if (which === "unknown flags") {
      const first = items[0];
      if (first) first.flags = flags;
    } else if (which === "semantic downgrade") {
      const last = items.at(-1);
      if (last) last.flags = 0;
    } else if (which === "duplicate field") {
      const first = items[0];
      if (first) items.push(first);
    } else if (which === "out-of-order field") items.reverse();
    else throw new Error("not a record mutation");
  });
}
export function futureCatalog(
  catalog: Catalog,
  which: CapabilityCase,
  level: number,
  tag: number,
  payload: Uint8Array,
  flags: number,
): Uint8Array {
  const parts: Uint8Array[] = Array.from(catalogStream(catalog), (part) => part.slice());
  const top = parts[1],
    table = parts[2];
  if (!top || !table) throw new Error("missing MCAT fixture");
  if (which === "stream version" || which === "expression dialect") {
    const h = parts[0];
    if (!h) throw new Error("missing MCAT header");
    const view = new DataView(h.buffer, h.byteOffset, h.length);
    if (which === "stream version") view.setUint16(4, 2, true);
    else view.setUint32(12, 2, true);
  } else if (which.includes("feature")) {
    const values =
      which === "unknown feature"
        ? [1, 2, tag]
        : which === "duplicate feature"
          ? [1, 1, 2]
          : which === "out-of-order feature"
            ? [2, 1]
            : [];
    parts[1] = field(top, 3, () => uints(values));
  } else if (which === "section order") {
    parts[1] = field(top, 4, (bytes) =>
      firstArrayRecord(bytes, (source) =>
        edit(source, (items) => {
          const kind = items[0];
          if (kind) kind.bytes = uints([tag]).subarray(1);
        }),
      ),
    );
  } else if (level === 0) parts[1] = futureRecord(top, which, tag, payload, flags);
  else if (level === 1)
    parts[1] = field(top, 2, (b) => futureRecord(b, which, tag, payload, flags));
  else
    parts[2] = field(table, 4, (bytes) =>
      firstArrayRecord(bytes, (column) =>
        field(column, 3, (b) => futureRecord(b, which, tag, payload, flags)),
      ),
    );
  const out = new ByteWriter();
  for (const p of parts) out.raw(p);
  return out.finish();
}
export function refusalCode(which: CapabilityCase): string {
  return [
    "optional field",
    "required field",
    "unknown feature",
    "unknown family",
    "record version",
    "stream version",
    "expression dialect",
  ].includes(which)
    ? "MINNOW_CATALOG_UNSUPPORTED"
    : "MINNOW_STORE_CORRUPT";
}
