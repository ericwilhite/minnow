import { stringArgument } from "./sql-semantics.js";
import { externalSqlDomainValue } from "./sql-domains.js";

/**
 * SQL/JSON scalar support (T801 family). Documents may be ordinary JSON text or native JSON/JSONB
 * domain values carried by the string vector representation. Scalar paths take one unambiguous
 * result: `$`, member steps, and array subscripts. JSON_TABLE owns its separate `$`/`$[*]`
 * row-producing subset in the parser.
 */
interface JsonPathStep {
  kind: "member" | "index";
  name: string;
  index: number;
}

export function parseJsonPath(path: unknown, caller: string): JsonPathStep[] {
  const text = stringArgument(caller, path).trim();
  if (!text.startsWith("$")) throw new TypeError(`${caller} paths start at $`);
  const steps: JsonPathStep[] = [];
  let cursor = 1;
  while (cursor < text.length) {
    if (text[cursor] === ".") {
      cursor += 1;
      const start = cursor;
      while (cursor < text.length && /[A-Za-z0-9_]/.test(text[cursor] ?? "")) cursor += 1;
      if (cursor === start) throw new TypeError(`${caller} path expects a member name after .`);
      steps.push({ kind: "member", name: text.slice(start, cursor), index: 0 });
      continue;
    }
    if (text[cursor] === "[") {
      const close = text.indexOf("]", cursor);
      if (close === -1) throw new TypeError(`${caller} path has an unclosed subscript`);
      const inner = text.slice(cursor + 1, close).trim();
      cursor = close + 1;
      if (/^\d+$/.test(inner)) {
        steps.push({ kind: "index", name: "", index: Number(inner) });
        continue;
      }
      const quoted = /^'(.*)'$/.exec(inner) ?? /^"(.*)"$/.exec(inner);
      if (quoted?.[1] !== undefined) {
        steps.push({ kind: "member", name: quoted[1], index: 0 });
        continue;
      }
      throw new TypeError(`${caller} paths take array indexes and member names, not: ${inner}`);
    }
    throw new TypeError(`${caller} path is not understood: ${text}`);
  }
  return steps;
}

/** Walks one path over a document, reporting whether it selected anything. */
export function jsonAtPath(
  document: unknown,
  path: unknown,
  caller: string,
): { found: boolean; value?: unknown } {
  const steps = parseJsonPath(path, caller);
  let current: unknown;
  try {
    current = JSON.parse(stringArgument(caller, externalSqlDomainValue(document)));
  } catch {
    // A document that is not JSON selects nothing rather than failing the whole statement,
    // matching the standard's default ON ERROR behaviour for these functions.
    return { found: false };
  }
  for (const step of steps) {
    if (current === null || current === undefined) return { found: false };
    if (step.kind === "index") {
      if (!Array.isArray(current)) return { found: false };
      if (step.index >= current.length) return { found: false };
      current = current[step.index];
      continue;
    }
    if (typeof current !== "object" || Array.isArray(current)) return { found: false };
    const members = current as Record<string, unknown>;
    if (!Object.hasOwn(members, step.name)) return { found: false };
    current = members[step.name];
  }
  return { found: true, value: current };
}

/** Whether a value is JSON text of the requested shape (T825). */
export function jsonIsValid(document: unknown, kind: string): boolean {
  document = externalSqlDomainValue(document);
  if (typeof document !== "string") return false;
  let parsed: unknown;
  try {
    parsed = JSON.parse(document);
  } catch {
    return false;
  }
  const isObject = typeof parsed === "object" && parsed !== null && !Array.isArray(parsed);
  switch (kind) {
    case "object":
      return isObject;
    case "array":
      return Array.isArray(parsed);
    case "scalar":
      return !isObject && !Array.isArray(parsed);
    default:
      return true;
  }
}

/** A SQL value as its JSON counterpart: datetimes serialize as ISO text, like every cast. */
export function jsonValueOf(value: unknown): unknown {
  const external = externalSqlDomainValue(value);
  return external instanceof Date ? external.toISOString() : external;
}

/**
 * JSON_ARRAY(v, ...) and JSON_OBJECT(k, v, ...) (T811/T812). The omitted null-handling clause is
 * `NULL ON NULL`: SQL NULL becomes a JSON null. `ABSENT ON NULL` is a separate spelling rather
 * than the default. Constructors return JSON text at the JavaScript boundary.
 */
export function jsonConstructor(
  name: "JSON_ARRAY" | "JSON_OBJECT",
  values: readonly unknown[],
): string {
  if (name === "JSON_ARRAY") {
    return JSON.stringify(values.map((value) => jsonValueOf(value ?? null)));
  }
  const members: string[] = [];
  for (let index = 0; index + 1 < values.length; index += 2) {
    const rawKey = values[index];
    if (rawKey === null || rawKey === undefined) {
      throw new TypeError("JSON_OBJECT keys cannot be NULL");
    }
    let key: string;
    if (rawKey instanceof Date) key = rawKey.toISOString();
    else if (typeof rawKey === "string") key = rawKey;
    else if (typeof rawKey === "number" || typeof rawKey === "boolean") key = String(rawKey);
    else throw new TypeError("JSON_OBJECT keys must be scalar values");
    const member = values[index + 1];
    // Build JSON text directly. WITHOUT UNIQUE KEYS is the default, so duplicate names must be
    // preserved; assigning through a JavaScript object would collapse them and mishandle
    // special names such as "__proto__".
    members.push(`${JSON.stringify(key)}:${JSON.stringify(jsonValueOf(member ?? null))}`);
  }
  return `{${members.join(",")}}`;
}
