import { type QueryRow, type QueryValue } from "@minnowdb/core/plan";

/**
 * Raw-SQL escape hatch: sql<Row>`SELECT ... WHERE x = ${value}`. Interpolated values become
 * `$n` parameter placeholders bound at execution — never text spliced into the statement.
 * Arrays expand to parenthesized placeholder lists for IN, and nested `sql` fragments splice
 * their text with parameters renumbered under the outer statement. The result executes through
 * the ordinary statement pipeline, so it supports everything the SQL surface supports and
 * nothing more.
 */

export interface SqlExecutable {
  $executeRaw(sql: string, params?: readonly QueryValue[]): Promise<QueryRow[]>;
}

type FragmentPart =
  | { kind: "text"; text: string }
  | { kind: "value"; value: QueryValue }
  | { kind: "fragment"; fragment: RawSqlFragment<unknown> };

export type RawSqlValue =
  QueryValue | ReadonlyArray<QueryValue | RawSqlFragment<unknown>> | RawSqlFragment<unknown>;

export interface RenderedSql {
  readonly sql: string;
  readonly params: readonly QueryValue[];
}

export class RawSqlFragment<out TRow> {
  readonly #parts: readonly FragmentPart[];
  #rendered: RenderedSql | undefined;

  constructor(parts: readonly FragmentPart[]) {
    this.#parts = parts;
  }

  /** Type-only row shape; this property does not exist at runtime. */
  declare readonly __row?: TRow;

  /** Type-only: the fragment's row, e.g. `type Row = typeof fragment.$inferRow`. Undefined at runtime. */
  declare readonly $inferRow: TRow;

  /** The statement text with `$n` placeholders, numbered as a standalone statement. */
  get sql(): string {
    return this.render().sql;
  }

  /** The parameter values referenced by this fragment's placeholders, in order. */
  get params(): readonly QueryValue[] {
    return this.render().params;
  }

  render(): RenderedSql {
    if (this.#rendered === undefined) {
      const params: QueryValue[] = [];
      const sql = renderParts(this.#parts, params);
      this.#rendered = { sql, params };
    }
    return this.#rendered;
  }

  /** @internal Appends this fragment's text to `out`, registering parameters into `params`. */
  appendTo(out: string[], params: QueryValue[]): void {
    out.push(renderParts(this.#parts, params));
  }

  async execute(db: SqlExecutable): Promise<TRow[]> {
    const { sql: text, params } = this.render();
    return (await db.$executeRaw(text, params)) as TRow[];
  }
}

function validateValue(value: QueryValue): QueryValue {
  if (typeof value === "number" && !Number.isFinite(value)) {
    throw new TypeError("sql parameters must be finite numbers");
  }
  if (value instanceof Date && !Number.isFinite(value.getTime())) {
    throw new TypeError("sql parameters must be valid dates");
  }
  return value;
}

function renderParts(parts: readonly FragmentPart[], params: QueryValue[]): string {
  const out: string[] = [];
  for (const part of parts) {
    if (part.kind === "text") {
      out.push(part.text);
    } else if (part.kind === "value") {
      params.push(part.value);
      out.push(`$${String(params.length)}`);
    } else {
      part.fragment.appendTo(out, params);
    }
  }
  return out.join("");
}

function valueParts(value: RawSqlValue): FragmentPart[] {
  if (value instanceof RawSqlFragment) return [{ kind: "fragment", fragment: value }];
  if (Array.isArray(value)) {
    if (value.length === 0) throw new TypeError("sql IN lists require at least one value");
    const parts: FragmentPart[] = [{ kind: "text", text: "(" }];
    value.forEach((item: unknown, index) => {
      if (Array.isArray(item)) throw new TypeError("sql lists cannot nest arrays");
      if (index > 0) parts.push({ kind: "text", text: ", " });
      parts.push(...valueParts(item as RawSqlValue));
    });
    parts.push({ kind: "text", text: ")" });
    return parts;
  }
  // Validation is eager so a bad value fails at the interpolation site, not at execution.
  return [{ kind: "value", value: validateValue(value as QueryValue) }];
}

export interface SqlTag {
  <TRow = QueryRow>(strings: TemplateStringsArray, ...values: RawSqlValue[]): RawSqlFragment<TRow>;
  /** Quotes one identifier path, for the rare case where a table or column name is dynamic. */
  identifier(...parts: string[]): RawSqlFragment<never>;
}

function createFragment<TRow = QueryRow>(
  strings: TemplateStringsArray,
  ...values: RawSqlValue[]
): RawSqlFragment<TRow> {
  const parts: FragmentPart[] = [{ kind: "text", text: strings[0] ?? "" }];
  values.forEach((value, index) => {
    parts.push(...valueParts(value));
    parts.push({ kind: "text", text: strings[index + 1] ?? "" });
  });
  return new RawSqlFragment<TRow>(parts);
}

export const sql: SqlTag = Object.assign(createFragment, {
  identifier: (...parts: string[]): RawSqlFragment<never> => {
    if (parts.length === 0 || parts.some((part) => part.length === 0)) {
      throw new TypeError("sql.identifier() requires one or more non-empty names");
    }
    return new RawSqlFragment([
      {
        kind: "text",
        text: parts.map((part) => `"${part.replaceAll('"', '""')}"`).join("."),
      },
    ]);
  },
});
