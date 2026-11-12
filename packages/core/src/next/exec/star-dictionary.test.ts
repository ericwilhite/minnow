/** PG text equality is the authority; canonical IDs are private, never caller page codes. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import type { Rel } from "../ir/ir.js";
import type { Batch, Vector } from "../vector/vector.js";
import { StorageCore } from "../testing/storage-core.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  STAR_DICTIONARY_REACH,
  STAR_DICTIONARY_FUNDING_REACH,
} from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { vectorOf } from "./join-fixtures.js";
import { valueAt } from "./kernels/test-support.js";
import { isValidSlot } from "./kernels/vectors.js";
import { starDictionaryShape } from "./star-shape.js";
import { StarDictionaryBooks } from "./star-dictionary.js";

const SQL = `SELECT a.t,b.t,c.label,count(*),sum(f.v) FROM fact f
 JOIN a ON a.k=f.ka JOIN b ON b.k=f.kb JOIN c ON c.k=f.kc
 GROUP BY a.t,b.t,c.label`;
const modes = ["flat", "dictionary", "constant"] as const;
let pg: PGlite;
let core: StorageCore;
let aggregate: Extract<Rel, { op: "aggregate" }>;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore(8192).open();
  await core.execute("CREATE TABLE fact(ka TEXT,kb TEXT,kc TEXT,v DOUBLE PRECISION)");
  for (const name of ["a", "b", "c"])
    await core.execute(`CREATE TABLE ${name}(k TEXT PRIMARY KEY,t TEXT,label TEXT)`);
  const find = (rel: Rel): void => {
    if (rel.op === "aggregate") aggregate = rel;
    if ("input" in rel) find(rel.input);
    if ("left" in rel && "right" in rel) {
      find(rel.left);
      find(rel.right);
    }
  };
  find(core.prepare(SQL).plan.root);
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
it.each(seedsFor("next-star-dictionary", [0xb761, 0xb762]))(
  "owns exact TEXT classes across sources and epochs (seed %i)",
  async (seed) => {
    const reach = createReach("star dictionaries", STAR_DICTIONARY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...modes),
        fc.boolean(),
        fc.integer({ min: 0, max: 999 }),
        async (mode, selected, salt) => {
          reach.beginHistory();
          const shape = starDictionaryShape(aggregate, false);
          expect(shape).not.toBeNull();
          if (!shape) throw new Error("Missing generated star shape");
          expect(shape.books.filter((book) => book.sources.length > 1)).toHaveLength(3);
          const gov = new Governor(16 << 20),
            grant = await gov.admit({ label: "books", minimumBytes: 12 << 20, spillable: false }),
            books = new StarDictionaryBooks(shape, grant);
          const values = [null, "", "same", `汉😀${String(salt)}`, "a.b", "same", "x", "y"];
          const original = new Map<number, Array<Array<string | number | null>>>(),
            encoded = new Map<number, Batch>();
          try {
            // Change dictionary identities and code order while each semantic domain is shared.
            for (let epoch = 0; epoch < 2; epoch++) {
              for (const scan of shape.scans) {
                const rows = scan.output.map((column, c) =>
                  values.map((_, i) =>
                    column.type.kind === "text"
                      ? mode === "constant"
                        ? null
                        : (values[(i + c + scan.id + epoch) % values.length] ?? null)
                      : i + 0.25,
                  ),
                );
                original.set(scan.id, rows);
                const columns = rows.map((v, c): Vector => {
                  const type = scan.output[c]?.type;
                  if (!type) throw new Error("Missing source type");
                  if (mode === "constant" && type.kind === "text") {
                    const value = vectorOf(type, [null]);
                    if (value.form !== "flat") throw new Error("Expected flat constant");
                    return { form: "constant", type, value, length: values.length };
                  }
                  return vectorOf(type, v, { dictionary: mode === "dictionary" });
                });
                const batch: Batch = {
                  length: values.length,
                  columns,
                  selection: selected
                    ? { rows: Uint16Array.from([0, 2, 3, 5, 7]), count: 5 }
                    : null,
                };
                encoded.set(scan.id, books.encode(scan.id, batch));
                // Owned canonical keys must survive arbitrary caller buffer mutation.
                for (const vector of columns) {
                  const flat =
                    vector.form === "flat"
                      ? vector
                      : vector.form === "constant"
                        ? vector.value
                        : vector.dictionary;
                  if (flat.data.layout === "varlen") flat.data.bytes.fill(0x71);
                }
              }
            }
            const comparisons: Array<readonly [string | null, string | null, boolean]> = [];
            for (const book of shape.books) {
              const a = book.sources[0],
                b = book.sources[1];
              if (!a || !b) continue;
              const av = encoded.get(a.scan.id)?.columns[a.index],
                bv = encoded.get(b.scan.id)?.columns[b.index];
              if (!av || !bv) throw new Error("Missing encoded equality source");
              const active = selected ? [0, 2, 3, 5, 7] : values.map((_, i) => i);
              for (const x of active)
                for (const y of active) {
                  const left = original.get(a.scan.id)?.[a.index]?.[x],
                    right = original.get(b.scan.id)?.[b.index]?.[y];
                  if (
                    !(typeof left === "string" || left === null) ||
                    !(typeof right === "string" || right === null)
                  )
                    throw new Error("Expected original text");
                  const equal =
                    isValidSlot(av, x) && isValidSlot(bv, y) && valueAt(av, x) === valueAt(bv, y);
                  comparisons.push([left, right, equal]);
                  if (equal) reach.reach("cross-source equality");
                }
            }
            const expected = await pg.query<{ equal: boolean }>(
              `SELECT (a=b) IS TRUE AS equal FROM (VALUES ${comparisons.map((_, i) => `($${String(2 * i + 1)}::text,$${String(2 * i + 2)}::text)`).join(",")}) v(a,b)`,
              comparisons.flatMap(([a, b]) => [a, b]),
            );
            expect(comparisons.map((x) => x[2])).toEqual(expected.rows.map((r) => r.equal));
            const inputColumns: Vector[] = shape.groupBooks.map((book) => {
              const source = shape.books[book]?.sources[0];
              const vector = source
                ? encoded.get(source.scan.id)?.columns[source.index]
                : undefined;
              if (!vector) throw new Error("Missing group input");
              return vector;
            });
            const batch = books.decode({
              length: values.length,
              columns: inputColumns,
              selection: selected ? { rows: Uint16Array.from([0, 2, 3, 5, 7]), count: 5 } : null,
            });
            for (const [c, book] of shape.groupBooks.entries()) {
              const source = shape.books[book]?.sources[0];
              const vector = batch.columns[c];
              if (!source) throw new Error("Missing original group source");
              if (!vector) throw new Error("Missing decoded group column");
              for (const row of selected ? [0, 2, 3, 5, 7] : values.map((_, i) => i))
                expect(valueAt(vector, row)).toBe(
                  original.get(source.scan.id)?.[source.index]?.[row],
                );
            }
            reach.reach(mode);
            if (selected) reach.reach("selection");
            reach.reach("NULL");
            reach.reach("dictionary epochs");
            reach.reach("owned input");
            reach.reach("decoded groups");
            reach.reach("PGlite equality");
          } finally {
            books.close();
            expect(grant.allocated).toBe(0);
            grant.close();
            expect(gov.usedBytes).toBe(0);
            reach.reach("zero resources");
          }
        },
      ),
      {
        seed,
        numRuns: 20,
        examples: modes.flatMap(
          (mode) =>
            [
              [mode, true, 3],
              [mode, false, 7],
            ] as Array<[(typeof modes)[number], boolean, number]>,
        ),
      },
    );
    reach.assertReached();
  },
  60000,
);

it("keeps volatile and non-inner shapes on the original recipe", () => {
  expect(starDictionaryShape(aggregate, true)).toBeNull();
  const mutate = (node: Rel): Rel => {
    if (node.op === "join") return { ...node, kind: "left" };
    if (node.op === "project") return { ...node, input: mutate(node.input) };
    return node;
  };
  expect(starDictionaryShape({ ...aggregate, input: mutate(aggregate.input) }, false)).toBeNull();
});

it.each(seedsFor("next-star-dictionary-funding", [0xb763, 0xb764]))(
  "funds every backing and unwinds a refused construction (seed %i)",
  (seed) => {
    const reach = createReach("star dictionary funding", STAR_DICTIONARY_FUNDING_REACH);
    const shape = starDictionaryShape(aggregate, false);
    if (!shape) throw new Error("Missing star funding shape");
    fc.assert(
      fc.property(fc.boolean(), fc.integer({ min: 1, max: 200 }), (deny, at) => {
        reach.beginHistory();
        let pending = 0,
          held = 0,
          attempts = 0,
          constructed = 0;
        const charge = {
          allocate(bytes: number) {
            if (deny && ++attempts === at) return false;
            pending += bytes;
            held += bytes;
            return true;
          },
          free(bytes: number) {
            held -= bytes;
          },
        };
        const types = [Uint8Array, Uint16Array, Uint32Array, Int32Array, Float64Array] as const;
        let books: StarDictionaryBooks | undefined;
        try {
          for (const Type of types)
            (globalThis as Record<string, unknown>)[Type.name] = new Proxy(Type, {
              construct(target, args: unknown[]) {
                const n = args[0];
                if (typeof n !== "number") throw new Error("Unexpected star backing view");
                const bytes = n * target.BYTES_PER_ELEMENT;
                expect(pending, "unfunded star backing").toBeGreaterThanOrEqual(bytes);
                pending -= bytes;
                constructed++;
                return Reflect.construct(target, args) as object;
              },
            });
          try {
            books = new StarDictionaryBooks(shape, charge);
          } catch (error) {
            if (!(error instanceof RangeError) || !deny || attempts !== at) throw error;
            reach.reach("denied construction");
          }
          if (books) {
            expect(constructed).toBeGreaterThan(0);
            expect(pending).toBe(0);
            reach.reach("charged before construction");
          }
        } finally {
          for (const Type of types) (globalThis as Record<string, unknown>)[Type.name] = Type;
          books?.close();
          expect(held).toBe(0);
          reach.reach("zero charges");
        }
      }),
      {
        seed,
        numRuns: 20,
        examples: [
          [false, 1],
          [true, 1],
          [true, 17],
          [true, 200],
        ],
      },
    );
    reach.assertReached();
  },
);
