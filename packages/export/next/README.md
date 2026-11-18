# Internal canonical cursor exports

This separate build consumes the native facade's cursor directly. It remains
outside the published package dist and exports. Each pull serializes one native
frame; cancellation and encoding errors close the cursor. The engine keeps its
ordinary result-window cap; the consumer owns the encoded output it retains.

1.0 differences from the 0.x adapter, per the public API plan and D1/D2/C1–C6:

- `frameRows` replaces `batchRows`; parameters and cancellation pass through.
- BIGINT emits exact decimal digits (CSV and JSON number tokens), never a Number
  conversion. NDJSON consumers must use a parser that preserves large integers;
  JavaScript's ordinary JSON.parse may round them. This exporter preserves bytes,
  not an arbitrary consumer's number representation.
- NUMERIC stays a string with its scale, including in JSON. Temporal values stay
  native ISO strings with microseconds, without conversion to Date.
- BYTEA emits PostgreSQL-style `\x` hexadecimal text. SQL arrays emit nested JSON
  arrays (JSON text inside CSV); their native element values keep the same rules.
- CSV/NDJSON use positional tuples, preserving each repeated column's own value.
  NDJSON repeats duplicate keys in SQL order; a consumer that builds an object may
  choose last-wins semantics. SQL NULL is JSON null; CSV's nullValue defaults empty.
- Non-finite float values remain a typed JavaScript RangeError for NDJSON, as in
  0.x. CSV keeps their text. Empty CSV queries still produce the requested header.

These are ordinary SQL exports, not snapshot bytes or B8 live stores. No public
entry/default, snapshot format or receiving-credit exemption is introduced.

Qualification limits: this base's SQL front end refuses BYTEA casts and ARRAY
construction. Those scalar serialization paths use real canonical-frame/C9 codec
fixtures; native SQL end-to-end tests cover enabled scalars and cursor lifecycle.
SQL duplicate aliases currently acquire suffixes before wire delivery (tracked
OPEN-ISSUES, B6). An expected-failure test pins that separate metadata defect;
frame tests prove this serializer retains supplied duplicate labels and values.
