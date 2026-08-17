import { stringArgument } from "./sql-semantics.js";

/**
 * The SQL/JSON support the engine keeps (T801 family). Documents are UTF-8 text in ordinary
 * string columns, exactly as SQLite stores them: no new logical type, no new storage format,
 * and every function below is a scalar over that text. The path language is the subset with an
 * unambiguous single result — `$`, member steps, and array subscripts — because the multi-value
 * forms need a row-producing operator (JSON_TABLE) the executors do not have.
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
    current = JSON.parse(stringArgument(caller, document));
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
  return value instanceof Date ? value.toISOString() : value;
}

/**
 * JSON_ARRAY(v, ...) and JSON_OBJECT(k, v, ...) (T811/T812). Both skip NULL members, which is
 * the standard's default (ABSENT ON NULL for objects, NULL ON NULL for arrays is the other
 * spelling), and both return JSON text.
 */
export function jsonConstructor(
  name: "JSON_ARRAY" | "JSON_OBJECT",
  values: readonly unknown[],
): string {
  if (name === "JSON_ARRAY") {
    return JSON.stringify(
      values.filter((value) => value !== null && value !== undefined).map(jsonValueOf),
    );
  }
  const entries: Record<string, unknown> = {};
  for (let index = 0; index + 1 < values.length; index += 2) {
    const key = values[index];
    if (typeof key !== "string") throw new TypeError("JSON_OBJECT keys must be strings");
    const member = values[index + 1];
    if (member === null || member === undefined) continue;
    entries[key] = jsonValueOf(member);
  }
  return JSON.stringify(entries);
}
