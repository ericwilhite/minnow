import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSONB_ERROR_ORDER_REACH } from "../../testing/reach.js";
import { Slice, runToEnd } from "../exec/slices.js";
import { JsonOutput } from "../exec/kernels/json-output.js";
import { StorageCore } from "./storage-core.js";
import { canonicalJsonb } from "../exec/kernels/jsonb-canonical.js";

let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});

const defects = [
  '"\\u0000"',
  '"\\q"',
  "1e131072",
  "1e131072x",
  "1e131072.0",
  '"\\uD800"',
  '"\\u00G0"',
  '"unfinished',
  '"\\u0000\\q"',
  '"\\q\\u0000"',
  "1e-16384",
  "1e131072true",
  "1e131072_",
] as const;

function nativeState(text: string): string | undefined {
  const slice = new Slice(Infinity);
  let used = 0;
  try {
    runToEnd(
      canonicalJsonb(
        new TextEncoder().encode(text),
        new JsonOutput(null, slice),
        {
          // eslint-disable-next-line require-yield -- pre-admitted synchronous unit-test owner
          *take(n: number) {
            used += n;
          },
          free(n: number) {
            used -= n;
            expect(used).toBeGreaterThanOrEqual(0);
          },
        },
        slice,
      ),
    );
  } catch (error) {
    return (error as { sqlState?: string }).sqlState;
  } finally {
    expect(used).toBe(0);
  }
}

// Fragments are independently invalid; positions, container/key placement and intervening
// tokens vary. PostgreSQL's lexer reads one token ahead before numeric conversion, so
// input order means its lexer/parser action order, not simply the leftmost bad byte.
it.each(seedsFor("next-jsonb-error-order", [0xb692, 0xb693]))(
  "keeps PostgreSQL multi-defect SQLSTATE order (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB error order", JSONB_ERROR_ORDER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.record({
            defect: fc.integer({ min: 0, max: defects.length - 1 }),
            position: fc.nat(20),
          }),
          { minLength: 2, maxLength: 5 },
        ),
        fc.integer({ min: 0, max: 5 }),
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom(",", " ", "", ":", ",,"),
        async (edits, depth, object, keys, separator) => {
          reach.beginHistory();
          reach.reach(object ? "object" : "array");
          reach.reach(edits.length === 2 ? "two defects" : "three or more defects");
          if (depth > 0) reach.reach("nested");
          if (object && keys) reach.reach("defective keys");
          if (separator !== ",") reach.reach("token lookahead");
          const parts = ["0", "null", '"valid"'];
          for (const edit of edits) {
            parts.splice(edit.position % (parts.length + 1), 0, defects[edit.defect] ?? "");
            if (edit.defect === 3 || edit.defect === 4 || edit.defect >= 11)
              reach.reach("malformed numeric token");
            if (edit.defect === 8 || edit.defect === 9) reach.reach("same string defects");
          }
          const zero = parts.indexOf(defects[0]),
            lex = parts.indexOf(defects[1]),
            num = parts.indexOf(defects[2]);
          if (zero >= 0 && lex > zero) reach.reach("Unicode before lexical");
          if (num >= 0 && lex > num) reach.reach("numeric before lexical");
          if (lex >= 0 && zero > lex) reach.reach("lexical before Unicode");
          let doc = object
            ? "{" +
              parts
                .map((part, i) => (keys ? part + ":0" : JSON.stringify(String(i)) + ":" + part))
                .join(separator) +
              "}"
            : "[" + parts.join(separator) + "]";
          for (let i = 0; i < depth; i++)
            doc = i % 2 === 0 ? "[" + doc + "]" : '{"nested":' + doc + "}";
          let state: string | undefined;
          try {
            await pg.query("SELECT $1::jsonb", [doc]);
          } catch (error) {
            state = (error as { code: string }).code;
          }
          expect(state, doc).toBeDefined();
          reach.reach("SQLSTATE " + String(state));
          expect(nativeState(doc), doc).toBe(state);
          await expect(
            core.execute("SELECT '" + doc.replace(/'/g, "''") + "'::jsonb::text AS j"),
          ).rejects.toMatchObject({ sqlState: state });
        },
      ),
      {
        seed,
        numRuns: 120,
        examples: [
          [
            [
              { defect: 0, position: 0 },
              { defect: 1, position: 1 },
            ],
            0,
            false,
            false,
            ",",
          ],
          [
            [
              { defect: 2, position: 0 },
              { defect: 1, position: 1 },
            ],
            1,
            false,
            false,
            ",",
          ],
          [
            [
              { defect: 1, position: 0 },
              { defect: 0, position: 1 },
            ],
            2,
            false,
            false,
            ",",
          ],
          [
            [
              { defect: 3, position: 0 },
              { defect: 0, position: 1 },
              { defect: 8, position: 2 },
            ],
            3,
            true,
            false,
            ",",
          ],
          [
            [
              { defect: 8, position: 0 },
              { defect: 1, position: 1 },
            ],
            0,
            true,
            true,
            ",",
          ],
          [
            [
              { defect: 2, position: 0 },
              { defect: 1, position: 1 },
            ],
            0,
            false,
            false,
            " ",
          ],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it.each([
  '["\\u0000","\\q"]',
  '[1e131072,"\\q"]',
  '[1e131072,"unfinished',
  '"\\u0000\\q"',
  '"\\q\\u0000"',
  '"\\u0000',
  '"\\ud800\\u0000"',
  "1e131072x",
  "[1e131072x]",
  "1e131072.0",
  "1e131072true",
  '{"a":1e131072x}',
  '1e131072 "\\q"',
  '1e131072 "\\u0000"',
  "1e131072 true",
  "1e131072:",
  '{"a" "\\u0000":1}',
])("scalar/lookahead precedence: %s", async (doc) => {
  let state: string | undefined;
  try {
    await pg.query("SELECT $1::jsonb", [doc]);
  } catch (error) {
    state = (error as { code: string }).code;
  }
  expect(state).toBeDefined();
  expect(nativeState(doc)).toBe(state);
});
