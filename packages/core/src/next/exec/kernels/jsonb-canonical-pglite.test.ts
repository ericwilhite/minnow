import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, JSONB_CANONICAL_REACH } from "../../../testing/reach.js";
import { Slice, runToEnd } from "../slices.js";
import { JsonOutput } from "./json-output.js";
import { canonicalJsonb } from "./jsonb-canonical.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
function convert(text: string): string {
  const bytes = new TextEncoder().encode(text),
    slice = new Slice(Infinity);
  let used = 0;
  const memory = {
    // eslint-disable-next-line require-yield -- synchronous, pre-admitted unit-test owner
    *take(n: number) {
      used += n;
    },
    free(n: number) {
      used -= n;
      expect(used).toBeGreaterThanOrEqual(0);
    },
  };
  const counter = new JsonOutput(null, slice);
  runToEnd(canonicalJsonb(bytes, counter, memory, slice));
  const output = new Uint8Array(counter.position),
    writer = new JsonOutput(output, slice);
  runToEnd(canonicalJsonb(bytes, writer, memory, slice));
  expect(used).toBe(0);
  return new TextDecoder().decode(output);
}
it.each(seedsFor("next-jsonb-canonical", [0xb684, 0xb685]))(
  "matches PG canonical bytes without a JS tree (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB canonical", JSONB_CANONICAL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: -999999, max: 999999 }),
        fc.integer({ min: -20, max: 20 }),
        fc.integer({ min: 0, max: 12 }),
        async (n, exponent, depth) => {
          reach.beginHistory();
          reach.reach(
            depth === 0 ? "flat object" : depth % 2 === 1 ? "nested array" : "nested object",
          );
          if (depth >= 10) reach.reach("deep nesting");
          for (const label of ["duplicate aliases", "decoded UTF8 keys", "beyond float precision"])
            reach.reach(label);
          if (exponent < 0) reach.reach("negative exponent");
          if (exponent > 0) reach.reach("positive exponent");
          let doc = `{"zz":1.00,"a":"汉/\\n","bb":9007199254740993.0100,"a":"\\uD83D\\uDE03","b\\u0062":${String(n)}.00100e${String(exponent)},"null":null,"bool":true}`;
          for (let i = 0; i < depth; i++)
            doc = i % 2 === 0 ? `[${doc},false,[],{}]` : `{"z":${doc},"aa":[null,1.00]}`;
          expect(convert(doc)).toBe(
            (await pg.query<{ t: string }>("SELECT $1::jsonb::text AS t", [doc])).rows[0]?.t,
          );
        },
      ),
      {
        seed,
        numRuns: 50,
        examples: [
          [0, 0, 0],
          [-12, -20, 1],
          [999999, 20, 12],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
it.each([
  "0",
  "-0.00",
  "1e131071",
  "1e-16383",
  "0e1000000",
  "0e1073741823",
  "null",
  "[]",
  "{}",
  '"hello"',
  '{"a":1,"a":2}',
])("canonical edge: %s", async (text) => {
  expect(convert(text)).toBe(
    (await pg.query<{ t: string }>("SELECT $1::jsonb::text AS t", [text])).rows[0]?.t,
  );
});
it.each([
  "1e131072",
  "1e-16384",
  "0e-1000000",
  "0e1073741824",
  "0e99999999999999999999999",
  '{"a":bad,"b":"\\u0000"}',
  '{"a":1e131072,"a":1}',
  '"\\u0000"',
  '"\\uD800"',
  "[1,]",
  '{"x":true,}',
])("keeps PG error: %s", async (text) => {
  let state: string | undefined;
  try {
    await pg.query("SELECT $1::jsonb", [text]);
  } catch (e) {
    state = (e as { code: string }).code;
  }
  expect(state).toBeDefined();
  expect(() => convert(text)).toThrow(expect.objectContaining({ sqlState: state }));
});

it.each([false, true])(
  "funds bounded canonical traversal without full-document key copies (refuse=%s)",
  (refuse) => {
    const bytes = new TextEncoder().encode('{"n":"' + "x".repeat(1000) + '"}');
    const Native = globalThis.Uint8Array,
      NativeFrames = globalThis.Int32Array;
    let used = 0,
      peak = 0,
      largeAllocations = 0,
      frameAllocations = 0;
    const denied = new Error("JSON scratch credit refused");
    const memory = {
      // eslint-disable-next-line require-yield -- synchronous pre-allocation boundary
      *take(n: number) {
        if (refuse && n >= 512) throw denied;
        used += n;
        peak = Math.max(peak, used);
      },
      free(n: number) {
        used -= n;
        expect(used).toBeGreaterThanOrEqual(0);
      },
    };
    globalThis.Uint8Array = new Proxy(Native, {
      construct(target, args) {
        if (typeof args[0] === "number" && args[0] >= bytes.length) largeAllocations++;
        return Reflect.construct(target, args, target) as Uint8Array;
      },
    });
    globalThis.Int32Array = new Proxy(NativeFrames, {
      construct(target, args) {
        if (used < 512) throw new Error("Traversal allocated before admission");
        frameAllocations++;
        return Reflect.construct(target, args, target) as Int32Array;
      },
    });
    try {
      const slice = new Slice(Infinity),
        sink = new JsonOutput(null, slice);
      const run = () => runToEnd(canonicalJsonb(bytes, sink, memory, slice));
      if (refuse) expect(run).toThrow(denied);
      else run();
      expect(largeAllocations).toBe(0);
      expect(frameAllocations).toBe(refuse ? 0 : 1);
      expect(peak).toBeLessThanOrEqual(1024);
      expect(used).toBe(0);
    } finally {
      globalThis.Uint8Array = Native;
      globalThis.Int32Array = NativeFrames;
    }
  },
);
