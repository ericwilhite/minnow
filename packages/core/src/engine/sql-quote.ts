/** Quotes an identifier for SQL the engine generates about its own catalog. */
export function quoteSqlIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

/** PostgreSQL 18 keywords that need quoting (reserved, column-name, and type/function names).
 * Source: src/include/parser/kwlist.h, REL_18_STABLE. Kept in sync by differential tests. */
const quotedKeywords = new Set(
  "all analyse analyze and any array as asc asymmetric authorization between bigint binary bit boolean both case cast char character check coalesce collate collation column concurrently constraint create cross current_catalog current_date current_role current_schema current_time current_timestamp current_user dec decimal default deferrable desc distinct do else end except exists extract false fetch float for foreign freeze from full grant greatest group grouping having ilike in initially inner inout int integer intersect interval into is isnull join json json_array json_arrayagg json_exists json_object json_objectagg json_query json_scalar json_serialize json_table json_value lateral leading least left like limit localtime localtimestamp merge_action national natural nchar none normalize not notnull null nullif numeric offset on only or order out outer overlaps overlay placing position precision primary real references returning right row select session_user setof similar smallint some substring symmetric system_user table tablesample then time timestamp to trailing treat trim true union unique user using values varchar variadic verbose when where window with xmlattributes xmlconcat xmlelement xmlexists xmlforest xmlnamespaces xmlparse xmlpi xmlroot xmlserialize xmltable".split(
    " ",
  ),
);

export function minimallyQuoteSqlIdentifier(identifier: string): string {
  return /^[a-z_][a-z0-9_]*$/.test(identifier) && !quotedKeywords.has(identifier)
    ? identifier
    : quoteSqlIdentifier(identifier);
}
