/** Inspection may discard unknown optional bytes; it must never regain write capability. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  CATALOG_INSPECTION_INPUT_REACH,
  CATALOG_INSPECTION_NESTED_REACH,
  createReach,
} from "../../testing/reach.js";
import type { CatalogChange } from "../access/catalog.js";
import { ftsDef } from "../access/fts-def.js";
import { Catalog, type LoggedChange } from "./catalog.js";
import { decodeCatalogStream } from "./catalog-stream.js";
import {
  decodeCatalogRecord,
  encodeCatalogRecord,
  type CatalogRecordKind,
} from "./catalog-records.js";
import { capabilityCatalog, futureRecord } from "./catalog-capability.test-support.js";
import { column, INT, TEXT, tableInput } from "./test-support.js";

const code = "MINNOW_CATALOG_UNSUPPORTED";
type InputChange = Exclude<CatalogChange, { kind: `drop${string}` }>;
interface Entry {
  kind: CatalogRecordKind;
  operand: string;
  change: InputChange;
}
function inputs(suffix: number): { base: Catalog; entries: Entry[] } {
  let base = capabilityCatalog();
  for (const change of [
    {
      kind: "createSequence",
      sequence: {
        name: "s",
        start: 1n,
        min: 1n,
        max: 1000n,
        increment: 1n,
        cycle: false,
        ownedBy: null,
      },
    },
    { kind: "createView", view: { name: "v", bodySql: "SELECT 1 AS n" } },
  ] satisfies CatalogChange[])
    base = base.change(change, base.version + 1).catalog;
  const table = tableInput("source", [column("id", INT, false), column("text", TEXT)]),
    sequence = base.sequences[0],
    view = base.views[0],
    trigger = base.triggers[0];
  if (!sequence || !view || !trigger) throw new Error("missing inspection control");
  return {
    base,
    entries: [
      {
        kind: "table",
        operand: "table",
        change: { kind: "createTable", table: { ...table, name: `new_${String(suffix)}` } },
      },
      {
        kind: "table",
        operand: "table",
        change: {
          kind: "alterTable",
          tableId: 1,
          table: {
            ...table,
            columns: table.columns.map((column, at) => ({ ...column, id: at + 1 })),
          },
        },
      },
      {
        kind: "index",
        operand: "index",
        change: {
          kind: "createIndex",
          index: { name: "ix", tableId: 1, columnIds: [2], unique: false, role: "secondary" },
        },
      },
      {
        kind: "sequence",
        operand: "sequence",
        change: { kind: "createSequence", sequence: { ...sequence, name: "new_s" } },
      },
      {
        kind: "sequence",
        operand: "sequence",
        change: { kind: "alterSequence", sequence: { ...sequence, cycle: true } },
      },
      {
        kind: "view",
        operand: "view",
        change: { kind: "createView", view: { ...view, name: "new_v" } },
      },
      {
        kind: "view",
        operand: "view",
        change: {
          kind: "replaceView",
          view: { ...view, bodySql: `SELECT ${String(suffix)} AS n` },
        },
      },
      {
        kind: "namedType",
        operand: "type",
        change: { kind: "createType", type: { name: "new_type", labels: ["a", String(suffix)] } },
      },
      { kind: "fts", operand: "fts", change: { kind: "createFts", fts: ftsDef(2, 2, 3) } },
      { kind: "fts", operand: "fts", change: { kind: "upsertFts", fts: ftsDef(1, 1, 2) } },
      {
        kind: "trigger",
        operand: "trigger",
        change: { kind: "createTrigger", trigger: { ...trigger, name: "new_trigger" } },
      },
    ],
  };
}
function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object") throw new Error("missing record operand");
  return value as Record<string, unknown>;
}
function inspect(
  kind: CatalogRecordKind,
  value: unknown,
  tag: number,
  payload: Uint8Array,
): unknown {
  const encoded = encodeCatalogRecord(kind, value);
  try {
    return decodeCatalogRecord(
      kind,
      futureRecord(encoded.bytes, "optional field", tag, payload, 0),
      "read",
    );
  } finally {
    encoded.close();
  }
}
function refuses(work: () => unknown): void {
  expect(work).toThrow(expect.objectContaining({ code }));
}
for (const seed of seedsFor("next-catalog-inspection-input", [0xb808, 0xb809]))
  it(`refuses inspection operands before every planning/replay copy seed ${String(seed)}`, () => {
    const reach = createReach("catalog inspection operands", CATALOG_INSPECTION_INPUT_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: 10 }),
        fc.integer({ min: 1, max: 10000 }),
        fc.integer({ min: 100, max: Number.MAX_SAFE_INTEGER }),
        fc.uint8Array({ maxLength: 40 }),
        (at, suffix, tag, payload) => {
          reach.beginHistory();
          const { base, entries } = inputs(suffix),
            entry = entries[at];
          if (!entry) throw new Error("missing change input");
          const before = base.encode(),
            version = base.version + 1,
            control = base.change(entry.change, version),
            logged = control.logged.find((l) => entry.operand in l);
          if (!logged) throw new Error("missing logged operand");
          // The control proves validity independently of provenance; encode/reopen must still work.
          expect(Catalog.decode(control.catalog.encode()).version).toBe(version);
          const wireValue = object(logged)[entry.operand],
            inspected = object(inspect(entry.kind, wireValue, tag, payload));
          refuses(() => encodeCatalogRecord(entry.kind, inspected));
          // TableInput uses column positions, whereas stored TableDef uses IDs. Adapt the known
          // fields in place, retaining the decoder's identity/provenance (not any opaque bytes).
          for (const name of Object.keys(inspected)) Reflect.deleteProperty(inspected, name);
          Object.assign(inspected, object(entry.change)[entry.operand]);
          refuses(() => base.change({ ...entry.change, [entry.operand]: inspected }, version));
          reach.reach(entry.change.kind);
          const replay: LoggedChange = {
            ...logged,
            [entry.operand]: inspect(entry.kind, wireValue, tag, payload),
          };
          refuses(() => base.apply(replay, version));
          reach.reach("fresh replay wrapper");
          expect(base.encode()).toEqual(before);
          reach.reach("source unchanged");
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: Array.from(
          { length: 11 },
          (_, at) => [at, at + 1, 100, Uint8Array.of(at)] as [number, number, number, Uint8Array],
        ),
      },
    );
    reach.assertReached();
  });

const nestedKinds = ["physical", "declared", "ftsConfig", "triggerBody", "viewOutput"] as const;
for (const seed of seedsFor("next-catalog-inspection-nested", [0xb80a, 0xb80b]))
  it(`refuses nested inspection aliases in unmarked inputs seed ${String(seed)}`, () => {
    const reach = createReach("nested catalog inspection", CATALOG_INSPECTION_NESTED_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 0, max: nestedKinds.length - 1 }),
        fc.integer({ min: 100, max: Number.MAX_SAFE_INTEGER }),
        fc.uint8Array({ maxLength: 40 }),
        (at, tag, payload) => {
          reach.beginHistory();
          const { base, entries } = inputs(7),
            kind = nestedKinds[at];
          if (!kind) throw new Error("missing nested shape");
          const change =
            kind === "ftsConfig"
              ? entries[9]?.change
              : kind === "triggerBody"
                ? entries[10]?.change
                : kind === "viewOutput"
                  ? ({
                      kind: "createView",
                      view: {
                        name: "nested",
                        bodySql: "SELECT 1 AS n",
                        outputs: [
                          {
                            name: "n",
                            type: INT,
                            declared: {
                              type: "number",
                              integer: true,
                              declaredTypeName: "INTEGER",
                            },
                            nullable: false,
                          },
                        ],
                      },
                    } satisfies CatalogChange)
                  : entries[0]?.change;
          if (!change) throw new Error("missing nested input");
          base.change(change, base.version + 1);
          if (change.kind === "createTable") {
            const field = kind === "physical" ? "type" : "declared",
              first = change.table.columns[0];
            if (!first) throw new Error("missing column");
            object(first)[field] = inspect(kind, first[field], tag, payload);
          } else if (change.kind === "upsertFts")
            object(change.fts).config = inspect(kind, change.fts.config, tag, payload);
          else if (change.kind === "createTrigger")
            object(change.trigger).statements = [
              inspect(kind, change.trigger.statements[0], tag, payload),
            ];
          else if (change.kind === "createView")
            object(change.view).outputs = [inspect(kind, change.view.outputs?.[0], tag, payload)];
          refuses(() => base.change(change, base.version + 1));
          reach.reach(kind);
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: nestedKinds.map(
          (_, at) => [at, 100, Uint8Array.of(at)] as [number, number, Uint8Array],
        ),
      },
    );
    reach.assertReached();
  });

it("refuses nested inspection before constructing or allocating a stored catalog", () => {
  const { base } = inputs(1),
    data = decodeCatalogStream(base.encode()),
    view = data.views[0];
  if (!view) throw new Error("missing view");
  const input = { ...data, views: [inspect("view", view, 100, Uint8Array.of(1))] } as typeof data;
  let calls = 0;
  refuses(() => new Catalog(input));
  refuses(() =>
    Catalog.fromStored(input, {
      allocate() {
        calls++;
        return true;
      },
      free() {
        calls++;
      },
    }),
  );
  expect(calls).toBe(0);
});
