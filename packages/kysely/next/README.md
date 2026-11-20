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
- `nativeLiveQuery({ driver, db?, resultDecoding?, memoryBytes? })` creates the
  private live factory. Call it with a SELECT builder or use `$call(live)`. The
  native owner executes; the client only decodes. Pass the Kysely `db` so its
  result plugins and dialect decoding also apply to live results. Builder-only
  plugins are not applied to deliveries; without `db`, rows use canonical decoding
  plus any explicit `resultDecoding` configuration.
- `changes(query, { key, table? })` adds stable keyed rows and patches;
  `window(query, { key, limit, offset?, table? })` requires top-level ORDER BY.
  Covered scalar primary-key history produces rekeys. History gaps, owner changes
  and unsupported composite identities produce explicit resets. Refresh
  and close operate on the factory's stores, never the caller's database connection.
- Client stores share a finite 64 MiB account per client, separate from the
  worker governor; `memoryBytes` can impose a stricter per-store cap. Slow async result plugins backpressure native delivery; reconnect
  work coalesces. Teardown waits for active result plugins to finish; their own allocations and any
  snapshots retained by callers remain caller-owned.

The existing adapter suite continues qualifying 0.x; native contract tests separately
exercise these bindings. Running the old suite unchanged does not constitute running
its whole old Result/Date/schema DSL contract against the new native facade.

Duplicate SQL result labels use the last occurrence in object rows, as in node-postgres.

For table-qualified rekeys, the selected key must preserve the source primary-key
value. Omit `table` when a result plugin changes that key value; ordinary keyed diffs
still describe the resulting rows.

Async iterators coalesce snapshots and allow one pending `next()` per iterator;
a second concurrent call rejects instead of building a request queue.

An iterator starts with a reset/full snapshot and resets after coalescing unread
states. It clears earlier patches and rekey notices when that consumer has no
continuous baseline; no per-iterator row history is retained.

The retained `result` stays canonical, including raw SQL labels and exact values,
even when `rows` uses result decoding/plugins. Parameters/options are captured
once. Source key/table names and complete binary backing capacities consume credit.

Native decoder/plugin output uses enumerable data fields with SQL scalars, Dates, binary
values, arrays and plain records. Materialize custom models before returning them; accessors
and custom host objects are rejected before cloning. Adapter inputs are isolated from the
canonical Result, including mutable binary values.

Binary decoder values use non-shared ArrayBuffers. Materialize SharedArrayBuffer views
with `Uint8Array.from(view)` before returning them, so later writes cannot change a snapshot.
