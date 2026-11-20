/** Stored UTF8 bytes retain their exact values across WAL replay, publication and continued writes. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, UTF8_DURABILITY_REACH } from "../../testing/reach.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  ints,
  INT,
  TEXT,
  tableInput,
  rowsOf,
  rules,
} from "../write/test-support.js";
import type { FlatVector } from "../vector/vector.js";
import type { WritePath } from "../access/write-path.js";
const encoder = new TextEncoder(),
  decoder = new TextDecoder("utf8", { fatal: true, ignoreBOM: true });
const scenario = fc.record({
  atom: fc.constantFrom("Ж", "中", "😀", "\ufeff"),
  width: fc.integer({ min: 258, max: 1024 }),
  long: fc.boolean(),
  nullable: fc.boolean(),
});
type Scenario = typeof scenario extends fc.Arbitrary<infer T> ? T : never;
const examples: Array<[Scenario]> = [
  [{ atom: "Ж", width: 512, long: false, nullable: false }],
  [{ atom: "中", width: 258, long: true, nullable: true }],
  [{ atom: "😀", width: 511, long: true, nullable: false }],
  [{ atom: "\ufeff", width: 258, long: false, nullable: true }],
];
function vector(value: string | null): FlatVector {
  const bytes = value === null ? new Uint8Array(0) : encoder.encode(value);
  return {
    form: "flat",
    type: TEXT,
    length: 1,
    validity: value === null ? Uint32Array.of(0) : null,
    data: { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) },
  };
}
for (const seed of seedsFor("next-utf8-durability", [49523, 49524]))
  for (const backend of ["memory", "sim"] as const)
    it("preserves native UTF8 through WAL and pages " + backend + " " + String(seed), async () => {
      const reach = createReach("UTF8 durability " + backend, UTF8_DURABILITY_REACH);
      await fc.assert(
        fc.asyncProperty(scenario, async (data) => {
          reach.beginHistory();
          const h = new Harness("strict", backend, 2, undefined, { merges: { mode: "off" } });
          let path = await h.open(),
            table = 0;
          const name = data.atom.repeat(data.width) + "table",
            columnName = data.atom.repeat(data.width) + "column";
          const raw = data.long
            ? "a".repeat(8191) + "😀" + data.atom.repeat(data.width) + "\ud800Z\udc00"
            : data.atom.repeat(data.width);
          const value = decoder.decode(encoder.encode(raw)),
            defaultSql = "'" + value.replaceAll("'", "''") + "'";
          const counts = { [data.atom.repeat(data.width)]: 1 };
          const expected: Array<[number, string | null]> = [];
          const check = async (p: WritePath, published: boolean) => {
            expect(p.catalog.table(table)?.name).toBe(name);
            expect(p.catalog.table(table)?.columns[1]?.name).toBe(columnName);
            expect(p.catalog.table(table)?.columns[1]?.defaultSql).toBe(defaultSql);
            const view = p.pin();
            try {
              const found = await rowsOf(view, table);
              found.sort((a, b) => Number(a[0]) - Number(b[0]));
              expect(found, "stored rows in the right columns").toEqual(expected);
              for (let i = 0; i < found.length; i++) {
                const text = found[i]?.[1],
                  wanted = expected[i]?.[1];
                if (typeof text === "string" && typeof wanted === "string") {
                  const bytes = encoder.encode(text),
                    oracle = encoder.encode(wanted);
                  expect(bytes.length).toBe(oracle.length);
                  expect(
                    bytes.every((b, k) => b === oracle[k]),
                    "stored byte round-trip",
                  ).toBe(true);
                }
              }
              const source = await view.table(table, {
                budget: { allocate: () => true, free: () => undefined },
              });
              expect(
                source.segments.some((s) => s.kind === "group"),
                "observed physical publication",
              ).toBe(published);
            } finally {
              view.release();
            }
            const outcome = p.ledger
              .checkpoint()
              .flatMap((c) => c.outcomes)
              .find((o) => o.sequence === 1);
            expect(outcome?.counts, "durable JSON ledger keys").toEqual(counts);
          };
          try {
            await autocommit(path, async (s) => {
              table =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput(name, [
                      column("id", INT, false),
                      { ...column(columnName, TEXT), defaultSql },
                    ]),
                  })
                ).id ?? 0;
            });
            const tx = await path.begin(),
              statement = tx.statement({ rules });
            try {
              await statement.table(table).insert(batch(ints([1]), vector(value)), null);
              await statement.end();
              const receipt = await tx.commit({
                clientId: new Uint8Array(16).fill(0x51),
                sequence: 1,
                sentAtMs: Date.now(),
                counts,
              });
              await receipt.durable;
            } catch (error) {
              statement.rollback();
              tx.rollback();
              throw error;
            }
            expected.push([1, value]);
            await path.flush();
            await check(path, false);
            path = await h.reopen();
            await check(path, false);
            reach.reach("WAL reopen");
            reach.reach("catalog names and defaults");
            reach.reach("JSON ledger");

            const second = data.nullable ? null : value + "😀";
            await autocommit(path, (s) =>
              s.table(table).insert(batch(ints([2]), vector(second)), null),
            );
            expected.push([2, second]);
            // The following commit freezes the full memtable, making the complete
            // earlier prefix publishable. Filling it alone does not freeze it.
            await autocommit(path, (s) =>
              s.table(table).insert(batch(ints([3]), vector(value + "中")), null),
            );
            expected.push([3, value + "中"]);
            await path.quiesce();
            await check(path, true);
            path = await h.reopen();
            await check(path, true);
            reach.reach("published pages reopen");

            await autocommit(path, (s) =>
              s.table(table).insert(batch(ints([4]), vector(value + "Ж")), null),
            );
            expected.push([4, value + "Ж"]);
            await path.flush();
            path = await h.reopen();
            await check(path, true);
            reach.reach("continued writing");
            reach.reach(data.nullable ? "NULL payload" : "non-NULL payload");
            if (data.long) reach.reach("chunk-boundary pair and lone surrogates");
            if (data.atom === "Ж") reach.reach("two bytes");
            if (data.atom === "中") reach.reach("three bytes");
            if (data.atom === "😀") reach.reach("four bytes");
            if (data.atom === "\ufeff") reach.reach("BOM catalog and payload");
          } finally {
            await h.close();
            expect(h.governor.usedBytes, "reader/writer ownership released").toBe(0);
          }
          reach.reach("zero retained");
        }),
        { seed, numRuns: 8, examples, endOnFailure: true },
      );
      reach.assertReached();
    });
