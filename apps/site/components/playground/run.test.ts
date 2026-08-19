import { describe, expect, it } from "vitest";
import { runSnippet, toRunnableBody } from "./run";

/**
 * The rewriter is the one place in the console where a snippet stops being the reader's code and
 * becomes something else. Everything it produces has to run, and it has to keep the line numbers
 * it was given — a runtime error that reports the wrong line is worse than no line at all.
 */
describe("rewriting a compiled snippet", () => {
  it("keeps every line where it was", () => {
    const compiled = 'import { sql } from "@minnowdb/client";\n\nconsole.log(sql);\n';
    expect(toRunnableBody(compiled).split("\n")).toHaveLength(compiled.split("\n").length);
  });

  it("rewrites every import form", () => {
    const cases: Array<[string, string]> = [
      ['import "x";', '__require("x");'],
      ['import * as ns from "x";', 'const ns = __require("x");'],
      ['import { a } from "x";', 'const { a } = __require("x");'],
      ['import { a as b } from "x";', 'const { a: b } = __require("x");'],
      ['import { a, b as c } from "x";', 'const { a, b: c } = __require("x");'],
      ['import d from "x";', 'const { default: d } = __require("x");'],
      ['import d, { a } from "x";', 'const { default: d, a } = __require("x");'],
      ["import { a } from 'x';", 'const { a } = __require("x");'],
    ];
    for (const [source, expected] of cases) expect(toRunnableBody(source)).toBe(expected);
  });

  it("leaves an import-looking string alone", () => {
    const compiled = 'console.log("import { a } from \\"x\\";");';
    expect(toRunnableBody(compiled)).toBe(compiled);
  });

  it("drops the exports a pasted module carries", () => {
    expect(toRunnableBody("export const a = 1;")).toBe("const a = 1;");
    expect(toRunnableBody("const a = 1;\nexport { a };\n")).toBe("const a = 1;\n\n");
  });
});

describe("running a snippet", () => {
  const scope = { modules: { "@minnowdb/client": { sql: "tag" } }, globals: { db: { rows: 2 } } };

  it("awaits at the top level and collects what it printed", async () => {
    const result = await runSnippet(
      'const value = await Promise.resolve(db.rows);\nconsole.log("rows", value);',
      scope,
    );
    expect(result.failure).toBeUndefined();
    expect(result.output).toEqual([{ level: "log", values: ["rows", 2] }]);
    expect(result.elapsedMs).toBeGreaterThanOrEqual(0);
  });

  it("resolves an import against the page's own modules", async () => {
    const result = await runSnippet(
      'import { sql } from "@minnowdb/client";\nconsole.log(sql);',
      scope,
    );
    expect(result.output).toEqual([{ level: "log", values: ["tag"] }]);
  });

  it("names the modules it has when a specifier misses", async () => {
    const result = await runSnippet('import x from "react";', scope);
    expect(result.failure).toBe('The console has no "react". It can import "@minnowdb/client".');
  });

  it("reports a throw as a result rather than throwing", async () => {
    const result = await runSnippet('console.log("before");\nthrow new Error("nope");', scope);
    expect(result.failure).toBe("nope");
    expect(result.output).toEqual([{ level: "log", values: ["before"] }]);
  });

  it("reports what the snippet returned", async () => {
    expect((await runSnippet("return 41 + 1;", scope)).returned).toBe(42);
    expect((await runSnippet("void 0;", scope)).returned).toBeUndefined();
  });

  it("reports a syntax error the compiler let through", async () => {
    expect((await runSnippet("const = ;", scope)).failure).toMatch(/Unexpected token/);
  });
});
