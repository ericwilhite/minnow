/** Independent MCAT20/30/31/32 writer freezes numeric field IDs and separate flag bytes. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_WIRE_REACH } from "../../testing/reach.js";
import { ByteWriter } from "./bytes.js";
import { encodeCatalogRecord, decodeCatalogRecord } from "./catalog-records.js";
import { validateTriggerDef, type TriggerDef } from "../access/trigger-def.js";
const u = (n: number) => {
  const out = new ByteWriter();
  out.varuint(n);
  return out.finish();
};
const s = (text: string) => {
  const out = new ByteWriter();
  out.string(text);
  return out.finish();
};
const wire = (kind: number, fields: ReadonlyArray<readonly [number, Uint8Array, number?]>) => {
  const body = new ByteWriter();
  for (const [id, value, flags = 1] of fields) {
    body.varuint(id);
    body.u8(flags);
    body.blob(value);
  }
  const out = new ByteWriter();
  out.varuint(kind);
  out.varuint(1);
  out.blob(body.finish());
  return out.finish();
};
const list = (values: readonly Uint8Array[]) => {
  const out = new ByteWriter();
  out.varuint(values.length);
  for (const v of values) out.raw(v);
  return out.finish();
};
for (const seed of seedsFor("next-trigger-wire", [0xb7c6, 0xb7c7]))
  it(`keeps trigger fields, extensions and semantic versions exact seed ${String(seed)}`, () => {
    const reach = createReach("trigger wire", TRIGGER_WIRE_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 500 }),
        fc.constantFrom("tr", "Case.a", '汉."'),
        fc.constantFrom("insert", "update", "delete"),
        fc.boolean(),
        fc.integer({ min: 1, max: 4 }),
        fc.boolean(),
        (id, name, event, before, count, required) => {
          reach.beginHistory();
          const source = event === "delete" ? "old" : "new",
            timing = before ? "before" : "after";
          const type = wire(5, [
              [1, s("int32")],
              [2, u(32), 1],
            ]),
            target = wire(32, [
              [1, u(2)],
              [2, s("n")],
              [3, type],
            ]),
            binding = wire(31, [
              [1, u(1)],
              [2, s("n")],
              [3, type],
              [4, s(source)],
            ]);
          const bodies = Array.from({ length: count }, (_, i) =>
            wire(30, [
              [1, s(`INSERT INTO audit(n) VALUES(?+${String(i)})`)],
              [2, s("insert")],
              [3, u(2)],
              [4, s("audit")],
              [5, list([binding])],
              [6, list([target])],
              [7, list([u(2)])],
              [8, list([])],
              [9, Uint8Array.of(0)],
              [10, list([])],
              [11, list([])],
              [12, list([])],
            ]),
          );
          const fields: Array<readonly [number, Uint8Array, number?]> = [
            [1, u(id)],
            [2, s(name)],
            [3, u(1)],
            [4, s(event)],
            [5, s(timing)],
            [6, u(1)],
            [7, list(bodies)],
          ];
          const exact = wire(20, fields),
            decoded = decodeCatalogRecord("trigger", exact) as TriggerDef;
          validateTriggerDef(decoded);
          expect(decoded.statements).toHaveLength(count);
          expect(decoded.statements[0]?.bindings[0]?.source).toBe(source);
          const encoded = encodeCatalogRecord("trigger", decoded);
          try {
            expect(encoded.bytes).toEqual(exact);
          } finally {
            encoded.close();
          }
          const extended = wire(20, [...fields, [100, Uint8Array.of(0xff), required ? 1 : 0]]);
          expect(() => decodeCatalogRecord("trigger", extended)).toThrow(
            expect.objectContaining({ code: "MINNOW_CATALOG_UNSUPPORTED" }),
          );
          if (required) {
            expect(() => decodeCatalogRecord("trigger", extended, "read")).toThrow(
              expect.objectContaining({ code: "MINNOW_CATALOG_UNSUPPORTED" }),
            );
            reach.reach("required refused");
          } else {
            const inspected = decodeCatalogRecord("trigger", extended, "read");
            expect(inspected).toMatchObject(decoded);
            expect(() => encodeCatalogRecord("trigger", inspected)).toThrow(
              expect.objectContaining({ code: "MINNOW_CATALOG_UNSUPPORTED" }),
            );
            reach.reach("optional reader tolerated");
            reach.reach("optional writer refused");
          }
          expect(() => validateTriggerDef({ ...decoded, dialectVersion: 2 })).toThrow(
            expect.objectContaining({ code: "MINNOW_FORMAT_UNSUPPORTED" }),
          );
          reach.reach("dialect refused");
          expect(() =>
            validateTriggerDef({ ...decoded, name: `RI_ConstraintTrigger_${name}` }),
          ).toThrow(expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }));
          reach.reach("reserved refused");
          const badBinding = { ...decoded.statements[0]?.bindings[0], source: "other" };
          expect(() =>
            validateTriggerDef({
              ...decoded,
              statements: decoded.statements.map((b) => ({
                ...b,
                bindings: [badBinding as never],
              })),
            }),
          ).toThrow(expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }));
          reach.reach("source refused");
          reach.reach(event);
          reach.reach(before ? "before" : "after");
          reach.reach(count === 1 ? "one body" : "many bodies");
          reach.reach(name === "tr" ? "plain name" : "UTF8 name");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [1, "tr", "insert", true, 1, true],
          [200, '汉."', "delete", false, 4, false],
          [20, "Case.a", "update", true, 2, true],
        ],
      },
    );
    reach.assertReached();
  });
