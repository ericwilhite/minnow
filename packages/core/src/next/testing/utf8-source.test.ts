/** All direct encoder calls share the progress/byte-boundary contract. */
import { readdirSync, readFileSync } from "node:fs";
import { join, relative, sep } from "node:path";
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
/** Syntax guard, not a sandbox: track local encoder aliases and typed parameters.
 * Ordinary data indexing stays legal; unknown keys on encoder/prototype values do not.
 * Named encodeInto access and destructuring are forbidden regardless of the receiver. */
function encoderAccesses(path: string, text: string): string[] {
  const source = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true),
    escaped: string[] = [],
    bindings = new Map<string, ts.Expression[]>(),
    constants = new Map<string, ts.Expression[]>(),
    typed = new Map<string, Set<string>>(),
    accesses: ts.Node[] = [];
  const unwrap = (value: ts.Expression): ts.Expression => {
    while (
      ts.isParenthesizedExpression(value) ||
      ts.isAsExpression(value) ||
      ts.isTypeAssertionExpression(value) ||
      ts.isNonNullExpression(value) ||
      ts.isSatisfiesExpression(value)
    )
      value = value.expression;
    return value;
  };
  function literal(value: ts.Expression | undefined, seen = new Set<string>()): string | undefined {
    if (!value) return undefined;
    value = unwrap(value);
    if (
      ts.isStringLiteral(value) ||
      ts.isNoSubstitutionTemplateLiteral(value) ||
      ts.isNumericLiteral(value)
    )
      return value.text;
    if (ts.isIdentifier(value) && !seen.has(value.text)) {
      seen.add(value.text);
      const values = constants.get(value.text);
      if (values?.length === 1) return literal(values[0], seen);
    }
    if (ts.isBinaryExpression(value) && value.operatorToken.kind === ts.SyntaxKind.PlusToken) {
      const left = literal(value.left, new Set(seen)),
        right = literal(value.right, new Set(seen));
      if (left !== undefined && right !== undefined) return left + right;
    }
    return undefined;
  }
  const property = (value: ts.PropertyName): string | undefined =>
    ts.isComputedPropertyName(value)
      ? literal(value.expression)
      : ts.isIdentifier(value) || ts.isStringLiteral(value) || ts.isNumericLiteral(value)
        ? value.text
        : undefined;
  const member = (value: ts.Expression): string | undefined =>
    ts.isPropertyAccessExpression(value)
      ? value.name.text
      : ts.isElementAccessExpression(value)
        ? literal(value.argumentExpression)
        : undefined;
  function receiver(
    value: ts.Expression | undefined,
    name: string,
    seen = new Set<ts.Node>(),
  ): boolean {
    if (!value) return false;
    if (
      ts.isAsExpression(value) ||
      ts.isTypeAssertionExpression(value) ||
      ts.isSatisfiesExpression(value)
    )
      return (
        new RegExp(`\\b${name}\\b`).test(value.type.getText(source)) ||
        receiver(value.expression, name, seen)
      );
    if (ts.isParenthesizedExpression(value) || ts.isNonNullExpression(value))
      return receiver(value.expression, name, seen);
    if (seen.has(value)) return false;
    seen.add(value);
    if (ts.isIdentifier(value))
      return (
        value.text === name ||
        (typed.get(name)?.has(value.text) ?? false) ||
        (bindings.get(value.text)?.some((v) => receiver(v, name, seen)) ?? false)
      );
    if (ts.isNewExpression(value))
      return name === "TextEncoder" && receiver(value.expression, name, seen);
    if (ts.isPropertyAccessExpression(value) || ts.isElementAccessExpression(value)) {
      const key = member(value);
      return (
        (key === name &&
          ts.isIdentifier(value.expression) &&
          value.expression.text === "globalThis") ||
        (name === "TextEncoder" && key === "prototype" && receiver(value.expression, name, seen))
      );
    }
    return false;
  }
  function assigned(node: ts.Node): boolean {
    while (
      ts.isObjectLiteralExpression(node.parent) ||
      ts.isPropertyAssignment(node.parent) ||
      ts.isParenthesizedExpression(node.parent)
    )
      node = node.parent;
    return (
      ts.isBinaryExpression(node.parent) &&
      node.parent.operatorToken.kind === ts.SyntaxKind.EqualsToken &&
      node.parent.left === node
    );
  }
  function collect(node: ts.Node): void {
    if ((ts.isVariableDeclaration(node) || ts.isParameter(node)) && ts.isIdentifier(node.name)) {
      if (node.type)
        for (const type of ["TextEncoder", "Reflect"]) {
          if (new RegExp(`\\b${type}\\b`).test(node.type.getText(source))) {
            const names = typed.get(type) ?? new Set<string>();
            names.add(node.name.text);
            typed.set(type, names);
          }
        }
      if (ts.isVariableDeclaration(node) && node.initializer) {
        bindings.set(node.name.text, [...(bindings.get(node.name.text) ?? []), node.initializer]);
        if (ts.isVariableDeclarationList(node.parent) && node.parent.flags & ts.NodeFlags.Const)
          constants.set(node.name.text, [
            ...(constants.get(node.name.text) ?? []),
            node.initializer,
          ]);
      }
    }
    if (
      ts.isPropertyAccessExpression(node) ||
      ts.isElementAccessExpression(node) ||
      (ts.isBindingElement(node) && ts.isObjectBindingPattern(node.parent)) ||
      ts.isPropertyAssignment(node) ||
      ts.isShorthandPropertyAssignment(node)
    )
      accesses.push(node);
    ts.forEachChild(node, collect);
  }
  collect(source);
  for (const node of accesses) {
    let refused = false;
    if (ts.isPropertyAccessExpression(node) || ts.isElementAccessExpression(node)) {
      const key = member(node),
        reflective = receiver(node.expression, "Reflect");
      refused =
        key === "encodeInto" ||
        (key === undefined && (reflective || receiver(node.expression, "TextEncoder")));
      if (reflective && key === "get") {
        // A borrowed get/bind/call/apply must not hide which property it reads.
        let use: ts.Node = node;
        while (
          ts.isParenthesizedExpression(use.parent) ||
          ts.isAsExpression(use.parent) ||
          ts.isNonNullExpression(use.parent)
        )
          use = use.parent;
        const call = use.parent;
        if (!ts.isCallExpression(call) || call.expression !== use) refused = true;
        else {
          const selected = literal(call.arguments[1]);
          refused ||=
            selected === "encodeInto" ||
            (selected === undefined && receiver(call.arguments[0], "TextEncoder"));
        }
      }
    } else if (ts.isBindingElement(node) && !node.dotDotDotToken) {
      const key = node.propertyName
        ? property(node.propertyName)
        : ts.isIdentifier(node.name)
          ? node.name.text
          : undefined;
      const declaration = node.parent.parent;
      refused =
        key === undefined ||
        key === "encodeInto" ||
        (key === "get" &&
          ts.isVariableDeclaration(declaration) &&
          receiver(declaration.initializer, "Reflect"));
    } else if (
      (ts.isPropertyAssignment(node) || ts.isShorthandPropertyAssignment(node)) &&
      assigned(node)
    ) {
      const key = property(node.name);
      refused = key === undefined || key === "encodeInto";
    }
    if (refused)
      escaped.push(String(source.getLineAndCharacterOfPosition(node.getStart(source)).line + 1));
  }
  return escaped;
}
// Partition the repository scan without dropping files: coverage must not put
// the entire next/ AST walk under one local per-test deadline.
const groups = new Map<string, string[]>();
visit(root, (path) => {
  if (path === helper) return;
  const parts = relative(root, path).split(sep),
    group = parts.length > 1 ? (parts[0] ?? ".") : ".";
  const files = groups.get(group) ?? [];
  files.push(path);
  groups.set(group, files);
});
const scans: Array<[string, string[]]> = [];
for (const [group, files] of groups)
  for (let at = 0; at < files.length; at += 16)
    scans.push([`${group}/${String(at)}`, files.slice(at, at + 16)]);
it.each(scans)(
  "centralizes every next encodeInto access in the UTF8 helper: %s",
  (_group, files) => {
    const escaped: string[] = [];
    for (const path of files)
      for (const line of encoderAccesses(path, readFileSync(path, "utf8")))
        escaped.push(relative(root, path) + ":" + line);
    expect(escaped, "direct encoder access must use the shared progress helper").toEqual([]);
  },
);

const refusedAccesses = [
  ["direct", "encoder.encodeInto(text, dest)"],
  ["quoted", 'encoder["encodeInto"](text, dest)'],
  ["template", "encoder[`encodeInto`](text, dest)"],
  ["destructured", "const { encodeInto: write } = encoder; write.call(encoder, text, dest)"],
  ["destructured shorthand", "const { encodeInto } = object; encodeInto(text, dest)"],
  ["destructured quoted", 'const { "encodeInto": write } = object'],
  ["destructured escaped", 'const { "encode\\u0049nto": write } = object'],
  ["computed escaped", 'encoder["encode\\u0049nto"](text, dest)'],
  ["computed concatenation", 'const key = "encode" + "Into"; encoder[key](text, dest)'],
  ["computed cast", "(value as TextEncoder)[key](text, dest)"],
  ["destructured computed", 'const key = "encodeInto"; const { [key]: write } = object'],
  ["destructured unknown", "const { [key]: write } = object"],
  ["assigned destructuring", "({ encodeInto: write } = object)"],
  ["computed constant", 'const key = "encodeInto"; encoder[key](text, dest)'],
  ["computed dynamic", "encoder[key](text, dest)"],
  ["computed alias", "const other = encoder; other[key](text, dest)"],
  [
    "computed typed parameter",
    "function f(value: TextEncoder, key: string) { value[key](text, dest) }",
  ],
  ["computed prototype", "TextEncoder.prototype[key](text, dest)"],
  ["prototype reference", "const write = TextEncoder.prototype.encodeInto"],
  ["bound", "encoder.encodeInto.bind(encoder)(text, dest)"],
  ["called", "encoder.encodeInto.call(encoder, text, dest)"],
  ["applied", "encoder.encodeInto.apply(encoder, [text, dest])"],
  ["reflected literal", 'Reflect.get(object, "encodeInto")(text, dest)'],
  ["reflected dynamic", "Reflect.get(encoder, key)(text, dest)"],
  ["reflected computed get", 'Reflect["get"](encoder, key)(text, dest)'],
  ["reflected get alias", "const get = Reflect.get; get(encoder, key)(text, dest)"],
  ["reflected destructured get", "const { get: read } = Reflect; read(encoder, key)"],
  ["reflected concatenation", 'Reflect.get(object, "encode" + "Into")'],
  ["reflected alias", "const R = Reflect; R.get(encoder, key)"],
  ["reflected bound get", "Reflect.get.bind(Reflect)(encoder, key)(text, dest)"],
  ["reflected called get", 'Reflect.get.call(Reflect, object, "encodeInto")'],
  ["reflected applied get", 'Reflect.get.apply(Reflect, [object, "encodeInto"])'],
] as const;
it.each(refusedAccesses)("rejects hidden encoder access: %s", (_name, text) => {
  expect(encoderAccesses("fixture.ts", "const encoder = new TextEncoder();\n" + text)).not.toEqual(
    [],
  );
});
it.each([
  'encoder["encode"](text)',
  'const key = "encode"; encoder[key](text)',
  "const { encode: write } = encoder",
  'const { ["encode"]: write } = encoder',
  'const key = "encode"; const { [key]: write } = encoder',
  "const values = new Uint8Array(8); values[index] = 1",
  "function read(values: Record<string, unknown>, key: string) { return values[key] }",
  "Reflect.get(target, key, target)",
  'Reflect.get(encoder, "encode")',
])("permits non-encodeInto access: %s", (text) => {
  expect(encoderAccesses("fixture.ts", "const encoder = new TextEncoder();\n" + text)).toEqual([]);
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
