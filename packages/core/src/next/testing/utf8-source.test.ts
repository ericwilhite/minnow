/** All direct encoder calls share the progress/byte-boundary contract. */
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import ts from "typescript";
import { expect, it } from "vitest";

const root = join(import.meta.dirname, ".."),
  helper = join(root, "utf8.ts");
function visit(directory: string, file: (path: string) => void): void {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) visit(path, file);
    else if (path.endsWith(".ts") && !/\.(test|bench)\.ts$/.test(path)) file(path);
  }
}
it("centralizes every next encodeInto access in the UTF8 helper", () => {
  const escaped: string[] = [];
  visit(root, (path) => {
    if (path === helper) return;
    const text = readFileSync(path, "utf8");
    if (!text.includes("encodeInto")) return; // Parse candidates only; keep the guard cheap under coverage.
    const source = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true);
    function walk(node: ts.Node): void {
      if (
        (ts.isPropertyAccessExpression(node) && node.name.text === "encodeInto") ||
        (ts.isElementAccessExpression(node) &&
          ts.isStringLiteral(node.argumentExpression) &&
          node.argumentExpression.text === "encodeInto")
      )
        escaped.push(
          relative(root, path) +
            ":" +
            String(source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1),
        );
      ts.forEachChild(node, walk);
    }
    walk(source);
  });
  expect(escaped, "direct encoder access must use the shared progress helper").toEqual([]);
});
it("keeps the helper free of copied suffixes and temporary byte buffers", () => {
  const source = ts.createSourceFile(
      helper,
      readFileSync(helper, "utf8"),
      ts.ScriptTarget.Latest,
      true,
    ),
    allocations: string[] = [];
  function walk(node: ts.Node): void {
    if (
      ts.isNewExpression(node) &&
      /^(?:Array|ArrayBuffer|SharedArrayBuffer|(?:Uint|Int|Float|BigInt|BigUint)\d+(?:Clamped)?Array)$/.test(
        node.expression.getText(source),
      )
    )
      allocations.push(node.expression.getText(source));
    if (
      ts.isCallExpression(node) &&
      ts.isPropertyAccessExpression(node.expression) &&
      [
        "slice",
        "substring",
        "substr",
        "subarray",
        "encode",
        "fromCharCode",
        "fromCodePoint",
      ].includes(node.expression.name.text)
    )
      allocations.push(node.expression.getText(source));
    ts.forEachChild(node, walk);
  }
  walk(source);
  expect(
    allocations,
    "only native prefix progress and scalar locals; no extra copy or workspace",
  ).toEqual([]);
});
