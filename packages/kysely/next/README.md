# Internal native Kysely qualification

This separate build imports the existing compiler and the canonical native SQL
facade. It is outside the package's published `dist` and exports. Build it with
`tsc -b packages/kysely/next` after core and the existing adapter. Tests boot the
real native owner; no 0.x engine fallback, SQL evaluator or second dispatcher.

Differences approved by the 1.0 API plan and DECISIONS D1/D2/C1–C6:

- BIGINT is bigint; NUMERIC is exact text by default; temporal values are strings
  with native microsecond precision. No implicit Date/Number conversion.
- Kysely transactions use isolated facade callbacks, not the default SQL session.
  Connection holds remain FIFO. Queries issued through this Kysely instance wait
  behind its transaction, while unrelated native default-session calls stay isolated.
- Optional NUMERIC/JSON decoding remains explicit and may lose decimal precision.
- Streaming maps Kysely chunk rows to the native cursor's frameRows; early return
  closes the native cursor. Mutation RETURNING uses buffered committed results.
- Introspection uses the existing catalog projection for defaults/identity and
  zero-row SQL metadata for exact SQL types. It rejects observable catalog shape
  changes during its reads; it does not promise an atomic metadata snapshot.
- The old schema-inference/createKysely facade is not switched. New type-channel
  inference awaits the public schema DSL decision; callers supply their DB type.
- Live wrappers throw "not yet bound" until B8's receiving-credit contract lands.

The existing adapter suite continues qualifying 0.x; native contract tests separately
exercise these bindings. Running the old suite unchanged does not constitute running
its whole old Result/Date/schema DSL contract against the new native facade.
