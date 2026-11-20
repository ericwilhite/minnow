# Internal native React entry

The hooks use the same structural external-store implementation as 0.x. They
preserve whatever exact snapshot B8/B6 supplies; they do not decode or coerce
BIGINT, NUMERIC or temporal values. This separate project is outside the packed
dist and exports. The existing hook suites qualify the unchanged implementation;
the native entry test verifies function identity and exact-value rendering.

`nativeLiveStore(driver, sql, params?, options?)` creates the native external store.
The driver is a native database/client with `liveQuery()`. Pass that store to the
same hooks, including Suspense and selectors. Store creation is lazy; subscribing
or refreshing starts the owner's live read. `close()` cancels and drains decoding.
The store keeps one charged snapshot, shares pending refreshes, and cancels stale
async transforms on reconnect. Stores share a 64 MiB client account; callers can
set a stricter per-store `memoryBytes` limit. Caller-retained snapshots and private adapter-transform allocations
are caller-owned. No polling or mutation replay substitutes for subscriptions.

Use `key` for keyed changes and patches, `table` for table-qualified rekeys, and
`maxRows` for a bounded result. Covered in-memory UPDATE history produces rekeys;
eviction, owner changes and unsupported composite key continuity produce
an explicit reset/full snapshot. Consumers must handle `reset` before patches.

For table-qualified rekeys, the selected key must preserve the source primary-key
value. Omit `table` when a result plugin changes that key value; ordinary keyed diffs
still describe the resulting rows.

Async iterators coalesce snapshots and allow one pending `next()` per iterator;
a second concurrent call rejects instead of building a request queue.

An iterator starts with a reset/full snapshot and resets after coalescing unread
states. It clears earlier patches and rekey notices when that consumer has no
continuous baseline; no per-iterator row history is retained.

Parameters and options are captured once. Array getters are read once, and binary
copies reserve their actual backing capacity before cloning. A source born during
client retirement is drained before the client close completes.

Native decoder/plugin output uses enumerable data fields with SQL scalars, Dates, binary
values, arrays and plain records. Materialize custom models before returning them; accessors
and custom host objects are rejected before cloning. Adapter inputs are isolated from the
canonical Result, including mutable binary values.

Binary decoder values use non-shared ArrayBuffers. Materialize SharedArrayBuffer views
with `Uint8Array.from(view)` before returning them, so later writes cannot change a snapshot.
