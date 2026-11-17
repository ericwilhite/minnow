# Internal live runtime integration

This directory is private. Native cancellation uses the lazy executor resource fence described
below. B6 owns the facade, lossless result decoder and adapter binding;
package exports stay unchanged until cutover. The public behavior change is described in the
site changelog's unreleased section.

- `LiveRuntime` belongs to one native owner. `composeEngine().live` creates it lazily from the
  native writer's `ChangeFeed`. Ordinary committed changes use the table dependency index;
  catalog, replacement, unknown and missed notifications widen invalidation. A cohort pins
  synchronously, executes at most eight queries concurrently, then delivers in one turn.
- `ProtocolClient.subscribeLive()` returns synchronous `refresh()` and `close()` handles.
  Refresh registers a fresh read and explicitly resets the result. It captures parameters
  privately. Reconnect retires old request IDs and registers fresh subscriptions; it never
  resends a subscription through mutation replay. Initial `done` settles the ordinary request
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
  after replacement belong to that application, not an owner delivery queue.

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
pairing an unrelated delete and insert. These guarantees do not require durable rekey history
or a stored-format change.

## Memory and limits

The owner charges subscription/parameter/dependency/cohort metadata, native frame capacities,
retained baselines, previous-row payloads held by diffs, clone credit until acknowledgement,
history copies, bounded diagnostic scratch and error-copy overlap. Live retained
state uses at most one quarter of ordinary owner capacity. A slow subscriber owns one delivery
(result or error) and one dirty watermark. Acknowledged baselines remain evictable during
an error delivery and their next execution;
no pin remains between sweeps. Dispose cancels admission/execution and owner close drains it. Internal `Run.cleaned`
forwards `Execution.settled()`, including deferred launch, program, delivery and sizing-job
cleanup. The cohort launcher installs its work fence before synchronous planning/admission,
so reentrant close cannot resolve with diagnostic scratch still held. `Run.done` alone is the producer/outcome boundary and does not guarantee retirement.

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
column/key-range exclusion requires additional planner and accepted-effect proof. B6's window
helper must validate top-level ORDER BY and apply LIMIT/OFFSET before registration; ordinary
SQL LIMIT/OFFSET executes natively and `LiveRows.maxRows` rejects oversized decoded windows.
Trigger/RI writers enter the same surviving undo stream. Full trigger firing qualification
remains with the tracks enabling it; the FTS test here qualifies the native scan/fallback path.

Run the focused evidence with:

```sh
npx vitest run packages/core/src/next/live \
  packages/core/src/next/testing/live-runtime.test.ts \
  packages/core/src/next/write/changes.test.ts \
  packages/core/src/next/testing/live-commit-oracle.test.ts \
  packages/core/src/next/exec/result-retain.test.ts --maxWorkers=2
```

Every generated history uses `seedsFor`; replay with `MINNOW_SEED`. Declared campaign reach lives
in `testing/reach.ts`. The parent-API commit oracle fails on the parent, and mutants reject a
missed dirty table, clearing the raced final watermark, retaining one queue record per commit,
and fabricating rekey provenance. The handoff records commands, observed failures and full-gate
results. No browser or performance qualification is implied by the Node evidence.
