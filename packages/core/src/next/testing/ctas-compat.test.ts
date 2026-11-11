/** Valid CTAS header compatibility; PostgreSQL, not this legacy regex, owns invalid DDL. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { ctasDataClause } from "../lower/ctas.js";

// Oracle inputs stay below 2 KiB. Never run this quadratic regex on the large-input campaign.
const OLD = /(?<=^\s*CREATE\s+TABLE\b[\s\S]*\bAS\b[\s\S]*)\s+WITH\s+(NO\s+)?DATA\s*;?\s*$/iu;
const legacy = (sql: string) => {
  const match = OLD.exec(sql);
  return match === null ? null : { index: match.index, noData: match[1] !== undefined };
};

it.each([
  'CREATE TABLE "s"."t" AS SELECT 1 WITH DATA',
  "CREATE TABLE t (a, b) AS SELECT 1, 2 WITH DATA",
])("keeps the previously accepted CTAS clause: %s", (sql) => {
  expect(ctasDataClause(sql)).toEqual(legacy(sql));
  expect(ctasDataClause(sql)).not.toBeNull();
});

interface Name {
  quoted: boolean;
  kind: number;
  tag: number;
}
interface Header {
  parts: Name[];
  columns: Name[] | null;
  ifNotExists: boolean;
  prefix: string;
  createGap: string;
  gap: string;
  clauseGap: string;
  clause: number;
  trailing: string;
  semicolon: boolean;
  casing: number;
  literalAs: boolean;
}
const name = fc.record({
  quoted: fc.boolean(),
  kind: fc.integer({ min: 0, max: 5 }),
  tag: fc.nat(999),
});
const headers: fc.Arbitrary<Header> = fc.record({
  parts: fc.array(name, { minLength: 1, maxLength: 3 }),
  columns: fc.option(fc.array(name, { minLength: 1, maxLength: 4 }), { nil: null }),
  ifNotExists: fc.boolean(),
  prefix: fc.constantFrom("", "\n", "/* prefix AS */ "),
  createGap: fc.constantFrom(" ", "\t", "\n", " /* gap */ "),
  gap: fc.constantFrom(" ", "\t", "\n", " /* AS */ ", " /* outer /* AS */ tail */ ", " -- AS\n"),
  clauseGap: fc.constantFrom(" ", "\t", "\n", " /* clause */ "),
  clause: fc.integer({ min: 0, max: 2 }),
  trailing: fc.constantFrom("", "\n", " /* tail */", " -- tail\n"),
  semicolon: fc.boolean(),
  casing: fc.integer({ min: 0, max: 2 }),
  literalAs: fc.boolean(),
});
const unquoted = (tag: number): Name => ({ quoted: false, kind: 0, tag });
const quoted = (kind: number, tag: number): Name => ({ quoted: true, kind, tag });
const base: Header = {
  parts: [unquoted(1)],
  columns: null,
  ifNotExists: false,
  prefix: "",
  createGap: " ",
  gap: " ",
  clauseGap: " ",
  clause: 1,
  trailing: "",
  semicolon: false,
  casing: 0,
  literalAs: false,
};

it.each(seedsFor("next-ctas-header-compatibility", [0xb4ed, 0xb4ee]))(
  "matches the legacy clause decision for valid CTAS headers (seed %i)",
  (seed) => {
    const reach = createReach("CTAS header compatibility", [
      "qualified quoted",
      "mixed qualification",
      "unquoted",
      "quoted dot",
      "quoted space",
      "doubled quote",
      "quoted punctuation",
      "name AS",
      "literal AS",
      "column list",
      "no columns",
      "IF NOT EXISTS",
      "comments",
      "nested comments",
      "line comments",
      "whitespace only",
      "lowercase",
      "mixed case",
      "semicolon",
      "DATA",
      "NO DATA",
      "legacy no match",
    ]);
    fc.assert(
      fc.property(headers, (header) => {
        reach.beginHistory();
        const word = (text: string) =>
          header.casing === 0
            ? text
            : header.casing === 1
              ? text.toLowerCase()
              : text.replace(/[A-Z]/gu, (c, i: number) => (i % 2 === 0 ? c.toLowerCase() : c));
        const render = (n: Name) => {
          if (!n.quoted) {
            reach.reach("unquoted");
            return "part_" + String(n.tag);
          }
          const value = ["part", "a.b", "a b", 'a"b', "a(b,c)", "AS name"][n.kind] ?? "part";
          const state = (
            [
              "",
              "quoted dot",
              "quoted space",
              "doubled quote",
              "quoted punctuation",
              "name AS",
            ] as const
          )[n.kind];
          if (state) reach.reach(state);
          return '"' + (value + String(n.tag)).replaceAll('"', '""') + '"';
        };
        const table = header.parts.map(render).join(header.gap + "." + header.gap);
        const columns =
          header.columns === null
            ? ""
            : "(" + header.columns.map(render).join(header.gap + "," + header.gap) + ")";
        const values = Array.from(
          { length: header.columns?.length ?? 1 },
          (_, i) =>
            (header.literalAs ? "'AS literal'" : String(i + 1)) +
            " " +
            word("AS") +
            ' "value' +
            String(i) +
            '"',
        );
        const sql =
          header.prefix +
          word("CREATE") +
          header.createGap +
          word("TABLE") +
          header.gap +
          (header.ifNotExists
            ? ["IF", "NOT", "EXISTS"].map(word).join(header.gap) + header.gap
            : "") +
          table +
          header.gap +
          columns +
          header.gap +
          word("AS") +
          header.gap +
          word("SELECT") +
          " " +
          values.join(", ") +
          (header.clause === 0
            ? ""
            : header.gap +
              word("WITH") +
              header.clauseGap +
              (header.clause === 2 ? word("NO") + header.clauseGap : "") +
              word("DATA")) +
          (header.semicolon ? ";" : "") +
          header.trailing;
        expect(sql.length).toBeLessThan(2048);
        expect(ctasDataClause(sql), sql).toEqual(legacy(sql));
        if (header.parts.length > 1 && header.parts.every((p) => p.quoted))
          reach.reach("qualified quoted");
        if (header.parts.some((p) => p.quoted) && header.parts.some((p) => !p.quoted))
          reach.reach("mixed qualification");
        reach.reach(header.columns === null ? "no columns" : "column list");
        if (header.ifNotExists) reach.reach("IF NOT EXISTS");
        if (sql.includes("/*") || sql.includes("--")) reach.reach("comments");
        else reach.reach("whitespace only");
        if (header.gap.includes("outer")) reach.reach("nested comments");
        if (header.gap.includes("--")) reach.reach("line comments");
        if (header.literalAs) reach.reach("literal AS");
        if (header.casing === 1) reach.reach("lowercase");
        if (header.casing === 2) reach.reach("mixed case");
        if (header.semicolon) reach.reach("semicolon");
        const result = legacy(sql);
        reach.reach(result === null ? "legacy no match" : result.noData ? "NO DATA" : "DATA");
      }),
      {
        seed,
        numRuns: 128,
        examples: [
          [{ ...base, parts: [quoted(0, 1), quoted(0, 2)] }],
          [{ ...base, columns: [unquoted(1), unquoted(2)] }],
          [
            {
              ...base,
              parts: [unquoted(1), quoted(3, 2)],
              columns: [quoted(4, 1), quoted(5, 2)],
              ifNotExists: true,
              gap: " /* outer /* AS */ tail */ ",
              clause: 2,
              casing: 2,
              literalAs: true,
              semicolon: true,
            },
          ],
          [{ ...base, parts: [quoted(1, 1), quoted(2, 2)], clause: 2, casing: 1 }],
          [{ ...base, gap: " -- AS\n", clause: 0 }],
        ],
      },
    );
    reach.assertReached();
  },
  30000,
);
