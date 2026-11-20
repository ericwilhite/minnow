# Internal live runtime integration

This directory is private. Native cancellation uses the lazy executor resource fence described
below. B6 owns the facade, lossless result decoder and adapter binding;
package exports stay unchanged until cutover. The public behavior change is described in the
site changelog's unreleased section.

- `LiveRuntime` belongs to one native owner. `composeEngine().live` creates it lazily from the
  native writer's `ChangeFeed`. Ordinary committed changes use the table dependency index;
  catalog, replacement, unknown and missed notifications widen invalidation. Failed planning
  or dependency admission invalidates the old proof: later commits retry conservatively until
  a fresh proof succeeds, then unaffected tables are excluded again. A cohort pins
  synchronously, executes at most eight queries concurrently, then delivers in one turn.
  Native observer dispatch runs in one deferred task after receipt/ledger bookkeeping and
  writer release. Two funded fixed frontiers coalesce installed effects and isolate a borrowed
  event from reentrant publication; a registration epoch excludes callback-created replacements.
- `ProtocolClient.subscribeLive()` returns synchronous `refresh()` and `close()` handles.
  Refresh registers a fresh read and explicitly resets the result. It captures parameters
  privately, admitting complete backing-buffer capacity before cloning a byte view. The private
  owner protocol refuses shared buffers. Retirement during admission/capture cannot publish a
  new subscription or leave an index link behind. Reconnect retires old request IDs and registers fresh subscriptions; it never
  resends a subscription through mutation replay. A loading callback may synchronously close
  or refresh its handle; its token is established before the callback, so an outer superseded
  job cannot register another read. Peers retiring during admission close the late handle. Initial `done` settles the ordinary request
  slot. Subsequent `live` updates use a separate handle and acknowledgement token.
- `onResult` receives columnar `ClientResult`, acknowledgement version, connection generation,
  and `{ reset, keyChanges }`. Use `result.summary.lsn` for snapshot versions; the callback
  version is only an acknowledgement token. Its promise backpressures delivery. Resolve after
  atomic decode/install; reject before installation. Rejection returns credit without advancing
  the last accepted LSN and discards the unusable wire baseline, preserving covered rekey history
  and permitting the same rows to retry on a later invalidation. Isolate downstream listener
  errors separately. Each client store has one
  active decoder and one coalesced replacement across reconnects. B6 must cancel stale decoder
  jobs on `onLoading`, check their generation/job token before publishing, and keep their
  allocations charged until those jobs drain. A timeout ends the owner handle after 60 seconds;
  the client re-subscribes. Native callbacks and error hooks are isolated. `onError` may also return a promise: resolve
  after installing the error snapshot, keep its payload charged until replacement/disposal,
  and isolate downstream listener failures. Error and
  result deliveries share the same active job and replacement slot. Error acknowledgements
  release credit without accepting data or advancing the rekey watermark. The next successful
  read delivers ready even when its rows equal the last good rows.
- `NativeLiveState` supplies stable loading/ready/error snapshot cells and charged listeners.
  Loading and error keep the last good rows. B6 supplies the row payload's account, refresh
  promise sharing and adapter lifecycle. `LiveRows` supplies unique-key validation, stable row
  identity, window result-bound enforcement, keyed changes, and reset/retained-position patches.
  Its budget belongs to the receiving decoded store. Snapshots retained by application code
  after replacement belong to that application, not an owner delivery queue. Reconciliation
  charges complete cloned ArrayBuffer backing capacity, including previous rows held by diffs.
  A narrow view over a large buffer therefore needs admission for the backing capacity.

## Rekey continuity

Rekey evidence comes from accepted native UPDATE OLD/NEW primary-key images. Non-key UPDATE
identity hops and DELETE retirement markers prevent a removed and reused key from acquiring
another row's identity. They are rollback-owned undo metadata, not commit-codec effects.

The writer captures at most 128 KiB per transaction. The feed retains at most 64 image records
and 256 KiB across commits, charged before publication. Oversized keys and optional capture
refusal mark a gap without failing a legal mutation. B6 decodes the existing native scalar
column images using its normal lossless decoder. An enum key image has TEXT representation;
its value is still the enum label. Forward `kind`, `fromRowId`, `toRowId`, table and decoded
from/to values to `LiveRows`. Identity-only records are not public rekey notices. Composite key notices remain in snapshots;
only scalar notices pair keyed result rows, as in the shipped contract.

Covered gaps deliver exact ordered notices. Catalog publication resets identity proofs because
table names and primary-key definitions may change. Uncovered gaps, owner changes and snapshot
replacement deliver an explicit full reset with no stale notices. Never infer a rekey by
pairing an unrelated delete and insert. Each previous identity is matched once; moved or retired
keys cannot reuse that previous row when a new row acquires the key, and an old destination's
delete remains visible when another row rekeys into its value. These guarantees do not require durable rekey history
or a stored-format change.

## Memory and limits

The owner charges subscription/parameter/dependency/cohort metadata, native frame capacities,
retained baselines, previous-row payloads held by diffs, clone credit until acknowledgement,
history copies, bounded diagnostic scratch and error-copy overlap. Live retained
state uses at most one quarter of ordinary owner capacity. A slow subscriber owns one delivery
(result or error) and one dirty watermark. Acknowledged baselines remain evictable during
an error delivery and their next execution;
no pin remains between sweeps. Terminal disposal drops private parameter captures, dependency
vectors and queued replacements, even if application code retains the closed handle. Refresh
and reconnect preserve the captured parameters. Active consumer jobs retain their receiving
charges until they drain. Dispose cancels admission/execution and owner close drains it. Internal `Run.cleaned`
forwards `Execution.settled()`, including deferred launch, program, delivery and sizing-job
cleanup. The cohort launcher installs its work fence before synchronous planning/admission,
so reentrant close cannot resolve with diagnostic scratch still held. Retirement stays fenced
through result-copy admission and delivery too: a reentrant disposal returns newly acquired
copy credit without installing or delivering the staged result. `Run.done` alone is the producer/outcome boundary and does not guarantee retirement.

Live diagnostics prepay 16 KiB of bounded sizing/fallback scratch per subscription. Before
copying diagnostic properties, the owner sizes them and admits three-copy overlap credit.
Diagnostics are limited to 64 KiB, 32 graph nodes, 64 fields and depth eight (including eight
cause links). They preserve bounded cloneable own data, codes and SQLSTATE, but omit stacks
and do not invoke custom diagnostic getters. Oversized, unsupported or refused diagnostics
become an explicit `MemoryAdmissionError` with SQLSTATE `53200`; the previous good rows remain.
This bounds failing-query notifications as well as successful updates.

Result retention adopts ordinary fixed allocations through the governor's existing cache
transfer contract. Progress-backed and mixed growth allocations cannot be adopted. Refusal
reports SQLSTATE 53200, retaining the consumer's previous good result. Execution minima still
obey the native governor; live retention cannot make a native query that cannot progress under
its cap fit. A 32 MiB pressure test covers refusal/cancellation; rekey histories use 64 MiB
because pending native keyed-write/FTS minima can leave a 32 MiB read waiting for maintenance.
That native minimum interaction is not claimed fixed here.

The first runtime re-executes affected SELECTs. It does not promise 0.x incremental-maintenance
performance or merge adapter result transforms. Native table-level proofs are conservative;
column/key-range exclusion requires additional planner and accepted-effect proof. The private Kysely
window helper validates top-level ORDER BY and applies LIMIT/OFFSET before registration; ordinary
SQL LIMIT/OFFSET executes natively and `LiveRows.maxRows` rejects oversized decoded windows.
Trigger/RI writers enter the same surviving undo stream. Full trigger firing qualification
remains with the tracks enabling it; the FTS test here qualifies the native scan/fallback path.

Run every live test, including transport, lifecycle and the main runtime/pressure campaign:

```sh
npx vitest run packages/core/src/next/live \
  packages/core/src/next/testing/live-*.test.ts \
  packages/core/src/next/write/changes*.test.ts \
  packages/core/src/next/exec/result-retain.test.ts \
  packages/core/src/next/exec/retirement.test.ts \
  packages/core/src/testing/seeds.test.ts --maxWorkers=2
```

Discover all `live/` and `testing/live-*.test.ts` files when assembling a programmatic command;
never substitute a cached selection. The main runtime file owns the rejected-delivery and
missed-notification reach as well as the pressure histories. Publication idle checks cross the
deferred notification task first, retaining their original assertions and deadlines.
Binary decoded values compare visible byte lengths and contents for raw ArrayBuffers, typed
views and DataViews. Equal bytes preserve the snapshot; changed or resized bytes emit updates.

Every generated history uses `seedsFor`; replay with `MINNOW_SEED`. Declared campaign reach lives
in `testing/reach.ts`. The parent-API commit oracle fails on the parent, and mutants reject a
missed dirty table, clearing the raced final watermark, retaining one queue record per commit,
and fabricating rekey provenance. The handoff records commands, observed failures and full-gate
results. No browser or performance qualification is implied by the Node evidence.

The page decoder uses the same column grammar as native replay through
`vector/column-codec.ts`. Native replay keeps its `ByteReader` and `StoreCorruptionError`;
`owner/live-column.ts` supplies page-only byte access and `ProtocolError`. Shared immutable
sizing functions and `MemoryAdmissionError` live in `memory-values.ts`, re-exported by the
governor. The page therefore imports neither a native writer nor governor state. This extraction
changes no stored encoding, memory admission policy or published export.

## Snapshot import and durable identity

Successful portable import replaces the database incarnation. The replacement barrier closes
all connection live handles, drains `engine.retireLive()` before import admission/path close,
and discards the old feed attachment. A new live runtime is lazy and uses the recovered path's
feed. Even an unacknowledged delivery cannot retain owner live charge across replacement.

The existing `databaseReplaced` message has terminal live-reset semantics.
`ClientLiveOptions.onReset("databaseReplaced")` runs synchronously before the typed error,
independently of an outstanding `onResult` promise. Receiving stores must abort decoding and
clear rows, patches and rekey baselines immediately; they must not present last-good rows from
the old database as current. The protocol drops old registration/parameter/queued-delivery
state. Reset-hook failures are isolated. Old calls, live refresh and stale-identity reconnects
fail with `MINNOW_DATABASE_REPLACED`; explicitly reopen the client to read the replacement.
No mutation is retried under a new identity. Its first ready snapshot is a full reset with no
old rekey notices. This terminal reset differs from transparent owner-generation reconnect,
which keeps the durable database identity and re-subscribes with a fresh read.

## Private receiving bindings

`owner/live-store.ts` connects canonical `Result` decoding to `NativeLiveState` and `LiveRows`.
`MinnowDatabaseClient.liveQuery` owns store shutdown; the private React helper supplies this
store to the existing structural hooks, and Kysely compiles SELECTs and applies configured
native decoding and the supplied database's result plugins. No package exports are switched.
The selected key must preserve the source primary-key value for table-qualified scalar rekeys;
composite identities reset when they cannot be paired with a scalar result key.

Stores share a finite 64 MiB receiving account per ProtocolClient; memoryBytes can impose a
stricter local cap. Source capture and diagnostics are prepaid. Result decode/copies and retained
rows/diffs are admitted before ownership. Adapter-private allocations and caller-retained
snapshots are caller-owned. One active decoder and coalesced protocol replacement bound pending
work, including reconnect and unsubscribe/re-subscribe. Close cancels then drains before freeing
credit. Async iteration has one latest dirty state and one pending next; concurrent next rejects.

Native final-update seeds8894/8895 pass in the current stack above84d10cff with their original
64MiB caps and final-update assertions. The earlier bare8c8385b8 admission-stall evidence remains
recorded with the B7 writer/retention and B9 sizing owners; no general native-admission fix is
claimed here. The bindings do not poll or alter admission.

Each receiving iterator establishes its own full-snapshot baseline. If unread states
coalesce, its next ready value is an explicit reset with no stale changes/rekey notices.
Only baseline/gap flags and one pending next are retained, never a row-history queue.
React renders use full rows because batching can skip source publications.

The receiving source captures wire parameter arrays once, with admitted scalar/Date/
array/typed-buffer copies. Intrinsic backing capacity includes caller-shadowed views.
Key/table metadata is charged. Keyed no-op publications clear prior differences while
retaining a fresh raw canonical Result independently of adapter rows.

Receiving bindings attach the terminal reset hook: they abort the active decoder, retire
rows/patches/rekey baselines, and publish a stable `MINNOW_DATABASE_REPLACED` error with
empty rows and no old version. Late decoding cannot reinstall the old baseline. A fresh
store on an explicitly reopened client begins with a full snapshot. Decoder credit remains
charged until cancellation drains; `close()` fences that cleanup before returning.

Row sizing and binary equality read intrinsic backing capacity, offsets and lengths. A decoder
can shadow `.buffer` or a buffer's `.byteLength`; those fields cannot hide owned clone capacity
or make changed binary contents compare equal. Copies of supported binary values, including
previous rows retained by a diff, remain admitted before ownership.

Canonical result headers are part of the receiving admission even for zero-row results.
Column labels and SQL type metadata cannot bypass a store's cap through an empty payload.

Decoded row graphs use enumerable data properties with SQL scalars, Dates, binary values,
arrays and plain records. Accessor properties and custom host/class objects are rejected
before cloning; a getter cannot substitute a larger value after sizing. Materialize custom
models in the adapter transform. Its private allocations remain caller-owned.

Adapter decoding receives a separate admitted copy of metadata and frame buffers. Mutating
a plugin row or binary value cannot change the independently retained canonical `Result`.
The raw result keeps native labels, types and exact values; adapted rows use their own baseline.

Binary receiver data uses non-shared ArrayBuffers. `structuredClone` aliases shared storage,
so SharedArrayBuffer views are rejected before copying; materialize them with
`Uint8Array.from(view)` in the decoder when needed. Ordinary SQL wire buffers are non-shared.
