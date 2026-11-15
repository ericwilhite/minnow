/** Catalog validation is input-linear and internal construction does not repeat capability walks. */
import fc from "fast-check";
import { afterEach, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_WORK_REACH, createReach } from "../../testing/reach.js";
import * as capabilities from "../access/catalog-capabilities.js";
import { Catalog } from "./catalog.js";
import { decodeCatalogStream } from "./catalog-stream.js";
import { capabilityCatalog } from "./catalog-capability.test-support.js";
import { INT } from "./test-support.js";
afterEach(() => vi.restoreAllMocks());
for (const seed of seedsFor("next-catalog-validation-work", [0xb80e, 0xb80f]))
  it(`visits shared operands once seed ${String(seed)}`, () => {
    const reach = createReach("catalog validation work", CATALOG_WORK_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 4, max: 10 }),
        fc.integer({ min: 2, max: 4 }),
        fc.integer({ min: 2, max: 24 }),
        (depth, fanout, outputs) => {
          reach.beginHistory();
          let reads = 0;
          const node = (children: object[]) =>
            Object.defineProperty({}, "children", {
              get() {
                reads++;
                return children;
              },
            });
          let root = node([]);
          for (let i = 0; i < depth; i++) root = node(Array.from({ length: fanout }, () => root));
          capabilities.requireWritableCatalogInput(root);
          expect(reads).toBe(depth + 1);
          reach.reach("shared DAG linear");
          const shared = { ...INT },
            view = {
              name: "v",
              bodySql:
                "SELECT " +
                Array.from({ length: outputs }, (_, i) => `1 AS n${String(i)}`).join(", "),
              outputs: Array.from({ length: outputs }, (_, i) => ({
                name: `n${String(i)}`,
                type: shared,
                declared: {
                  type: "number" as const,
                  integer: true as const,
                  declaredTypeName: "INTEGER",
                },
                nullable: false,
              })),
            };
          const original = Object.getOwnPropertyNames;
          let walks = 0;
          const spy = vi.spyOn(Object, "getOwnPropertyNames").mockImplementation((value) => {
            if (value === shared) walks++;
            return original(value);
          });
          try {
            capabilities.requireWritableCatalogInput({ kind: "createView", view });
          } finally {
            spy.mockRestore();
          }
          expect(walks).toBe(1);
          const c = Catalog.empty().change({ kind: "createView", view }, 1).catalog;
          expect(Catalog.decode(c.encode()).views[0]?.outputs).toHaveLength(outputs);
          reach.reach("shared view operand");
          const inspected = capabilities.catalogInspection({});
          expect(() =>
            capabilities.requireWritableCatalogInput({ left: root, right: root, last: inspected }),
          ).toThrow(expect.objectContaining({ code: "MINNOW_CATALOG_UNSUPPORTED" }));
          reach.reach("late inspection refuses");
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          [4, 2, 2],
          [10, 4, 24],
        ],
      },
    );
    reach.assertReached();
  });
it("validates external inputs once and uses shallow internal construction", () => {
  const c = capabilityCatalog(),
    data = decodeCatalogStream(c.encode()),
    spy = vi.spyOn(capabilities, "requireWritableCatalogInput");
  c.withVersion(c.version + 1);
  expect(spy).not.toHaveBeenCalled();
  const change = { kind: "createView" as const, view: { name: "v", bodySql: "SELECT 1" } };
  const result = c.change(change, c.version + 1);
  expect(spy.mock.calls.map((args) => args[0])).toEqual([change]);
  spy.mockClear();
  const logged = result.logged[0];
  if (!logged) throw new Error("missing logged control");
  c.apply(logged, c.version + 1);
  expect(spy.mock.calls.map((args) => args[0])).toEqual([logged]);
  spy.mockClear();
  new Catalog(data);
  expect(spy.mock.calls.map((args) => args[0])).toEqual([data]);
  spy.mockClear();
  Catalog.fromStored(data);
  expect(spy.mock.calls.map((args) => args[0])).toEqual([data]);
  spy.mockClear();
  // The internal capability is module-private; a caller-created symbol cannot bypass validation.
  Reflect.construct(Catalog, [data, Symbol("internal catalog construction")]);
  expect(spy.mock.calls.map((args) => args[0])).toEqual([data]);
});
it("rejects cycles rather than treating them as completed shared nodes", () => {
  const value: { next?: object } = {};
  value.next = value;
  expect(() => capabilities.requireWritableCatalogInput(value)).toThrow(
    expect.objectContaining({ code: "MINNOW_STORE_CORRUPT" }),
  );
});
