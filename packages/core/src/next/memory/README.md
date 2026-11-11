# Native credit groups

`MemoryGovernor.admitGroup(options)` acquires a complete minimum with one FIFO
admission and splits it into named native grants. `options.grants` lists each
part's `label`, `minimumBytes` and `spillable` flag. The result has the same order.
Labels must be unique and minima must be nonnegative safe integers. The request
is copied before waiting. No part holds credit while the total waits; cancellation
returns the whole admission, including a cancellation after atomic service but before
the async caller adopts the named grants. A zero-total group creates empty budgets without queueing: it acquires
no capacity and cannot displace a waiting request.

By default, the total uses ordinary capacity, like `admit`. A group whose total
already includes all its private progress credit can set `includesProgress: true`
and use the whole owner cap. Those consumers must progress inside their own
credit instead of requesting a shared-reserve loan. Existing `admit`, `tryAdmit`,
loan and grant methods keep their semantics. This private credit is unavailable
to legacy shared loans; the ordinary-admission loan progress proof does not cover
mixing a private-progress minimum with a consumer that still needs shared loans.

`MemoryGovernor.transferCredit(source, target, bytes)` moves unused ordinary
credit between live native grants of this same governor. It changes only their
held balances. Allocations, owner used bytes and peak bytes stay unchanged. It
returns `false` for foreign or closed grants, or insufficient idle ordinary credit.
Loan-backed bytes and live allocations cannot move. Invalid byte counts throw
`RangeError`. A zero-byte move between live native grants is inert; so is a
permitted move back to the same grant.

These are native grants, not suballocator wrappers. A retained-cache `takeFrom`
can move an allocated buffer's charge out of a grant, updating that grant's real
held and allocation counts. Subsequent idle transfers therefore see the current
balances. A grant's normal release or close returns exactly the credit still in
that grant; it does not return credit now owned by another grant or the cache.
The caller still owns every buffer's allocation, transfer and release lifetime.

## Liveness

A positive group waits once for its whole minimum, holding no partial grants.
Transfers never release into the global admission queue or ask it to reacquire
bytes. An admitted operation can fund its next bounded step even while another
admission waits. The caller must preserve each active operation's required
minimum; this primitive cannot infer a plan's protected credit from its buffers.
It does not authorize taking another operation's minimum, retaining borrowed
progress in a cache, or growing beyond an admitted bound.

`native-credit.test.ts` generates real buffer ownership, transfers in both
directions, cache takeovers, loans, close order, queued admission and cancellation.
Native cache takeovers check every post-transfer allocation and close balance.
`MINNOW_NATIVE_CREDIT_CACHE=1` additionally requires that capability: a governor
without native takeovers cannot pass this integration mode.

## Write-owner minimum

The write path admits its row/I/O, metadata, commit and merge accounts together.
The metadata allowance follows the current schema and root, and is revised before larger work; it is not a reservation for the largest allowed format blob.
A merge takes its already admitted credit into a temporary native grant and returns it
when the step settles. Credit is not released into global admission between merge steps.
Idle write estimates may fund that step directly, while live allocations and loan-backed
bytes stay with their owner. Storage can take retained buffers from these native accounts;
there is no second allocation counter that can become stale after a takeover.

Additional growth first transfers idle row estimates, then requests ordinary admission
with cancellation. Closing the writer cancels pending growth before releasing its inputs.
Metadata allocation distinguishes an individual object larger than the whole cap from
transient pressure: only the former is a typed size refusal. A metadata parse still has
to fit as one step; this does not add a new stored format or a streaming manifest parser.
The owner of an extent-space startup grant returns its unused estimate when handing off
to write admission, retaining any live allocation or outstanding loan.

# Execution admission and bounded read steps

Production memory reads use `MemorySegment.readHint` and the bounded `chunk` path.
The provider fills caller-owned native buffers and cannot widen or replace them. The
metadata envelope is conservative: the uncompressed 2,048-row, 200-column reference
uses about 25.6 MiB including I/O, before outputs and downstream buffers. A query at a smaller cap can
therefore have a minimum above that cap even when its present resident data is tiny.

The cap covers held capacity, including buffers in flight. Temporary pressure is not a
query-size error. A complete execution minimum greater than the whole cap is the typed
`MemoryAdmissionError` at admission. Oversized parameter inputs fail initial admission. Only already-admitted owned copies may
checkpoint during a pressure handoff (see below).

Memory refusals carry `code: "MINNOW_MEMORY_ADMISSION"`, SQLSTATE `53200`, and a
`cause` with `requestedBytes` and `admittedBytes`. A private progress step larger
than its admitted lane raises the same error before allocating or waiting; it
does not consume the lane or hide a still-active-loan ownership violation.
This structured refusal does not repair an underestimated minimum. In particular,
repeating a wide source column across sort output aliases can still exceed its
admitted progress lane; the planner must account for the expanded output or keep
one copy until final projection.

An executor first sizes the widest reachable scan step, including encoded leases,
decoded pages, dictionaries, directories and decode overlap. It adds every active
operator's fixed buffers, each breaker's private progress space, parameter storage, and
one result frame. A zero LIMIT validates its bounds and parameters without opening its
input. The entire minimum is one FIFO admission (`includesProgress: true`). Waiting
admission owns no partial input, result or operator reservation. Metadata sizing attempts
are separate complete admissions: all readers and holds close before a bigger attempt
waits. Optional late-sort recipes keep the generic recipe when their minimum will not fit.

`ReadStep` serializes bounded I/O inside the execution's private read credit. It moves a
completed lease's charge into the source's admitted resident-page budget before returning
it. Transfer charges the destination before freeing the I/O account, so resize/transfer
overlap stays counted. This move does not copy bytes. A source budget cannot grow into
shared headroom for pinned pages. At each downstream push, no pending read or I/O-backed
lease may remain. Resident page leases can remain only in the separately admitted source
budget; consumed values and retired group metadata are released as the scan advances.

Each parameter, read, operator, progress and result phase gets a named native grant.
Queued admission uses `admitGroup` for the entire minimum. R6's uncontended path
first `tryAdmit`s a total that fits ordinary capacity, creates zero-sized native grants and uses
`transferCredit` to divide **idle** ordinary credit before any buffer is allocated.
If any part cannot be installed synchronously, every part and the temporary total
are closed before entering the native FIFO group wait. A partial split never crosses
a wait or lets another execution observe unowned capacity. Complete minima that need
private progress capacity use `admitGroup` directly. The private-progress flag belongs
to the group contract; ordinary `admit`/`tryAdmit` keep their reserve boundary. Each
independent sizing scratch reservation is a one-part private group, returned before
an execution or larger scratch attempt waits.

`ReservedGrant` protects each phase's admitted minimum. Its fixed and progress accounts
are native grants, separate from spillable growth. It returns those accounts only when
the phase's buffers close; result frames and retained source leases can outlive the
producer. Allocation never waits: data-dependent state spills or runs at its minimum.
Breakers and the result sink borrow from their own `StepLoans` credit, including unused
fixed space, rather than queueing for the owner's shared reserve. A local loan must be
freed and repaid before the next local loan. The read lease's ownership conversion is
separate: its destination is charged before its I/O allocation is freed. `transferCredit`
is never used on a live lease or loan-backed charge. There is no owner-level restart or
spill service that can rescue a cycle involving pinned inputs.

## Liveness argument

Assuming every source supplies a bound that remains valid across publication, the memory
wait graph has no edge from one admitted execution to another execution's
minimum or pinned leases. Admission waits FIFO while holding nothing. Once admitted,
every active operator, its next bounded read, and downstream's next bounded step already
have space. A source either releases progress-backed leases or moves them to its admitted
resident-page charge before downstream can ask for memory. Growth does not wait for
unreclaimable space: it spills or continues within the minimum. Thus two executions can
each retain half the cap in resident leases and still take their own next step without
acquiring any byte from the other. They cannot form the proposed hold-and-wait cycle.

Cache eviction precedes spill requests. Cooperative callbacks are serialized per holder;
waiting for a callback is permitted only for reclaimable state, never its protected
minimum. Returning an unused amount of zero does not restart the governor drain.
Result-credit waits depend on the consumer returning owned frames, not on another
execution allocating memory. I/O waits depend on the storage operation settling, not on
another execution's grant. Those external progress assumptions are necessary: a consumer
that never returns credit, or storage that never settles, can still stop its execution.

Cancellation wakes queued admission and read callers immediately. An aborted in-flight
read keeps its private lane until it drains and releases a late lease; cleanup waits for
that lane before returning the execution's reservation. Task pacing uses the shared
`next/task.ts` clock; no read step starts an operator-local slice.

## Legacy isolated grants

Existing direct operator fixtures can still admit incomplete minima and use the shared
progress reserve. They must repay a loan without requesting another memory resource.
They are not the executor's concurrent-execution admission protocol. The compatibility
`makeRoom` read path waits/reclaims when the caller holds no allocated input; a caller
retaining input must use an admitted read step. `ReadStepBoundaryError` identifies a
violated internal protocol, rather than treating ordinary pressure as `ReadDeniedError`.

`read-step.test.ts` generates two to four executions filling the cap, retained leases,
further reads, downstream allocation, cancellation and FIFO admission. It also generates
room for one part but not the whole minimum and asserts that the waiter holds no partial credit. Its lease-release
mutant must be rejected at downstream allocation, not by a timeout. `reclaim.test.ts`
includes pinned competing holders that release later and abortable full-cap waits.
The stored executor pressure campaign also fills competing breaker grants before
admission and again when scanning starts.

## Publication-stable memory-source steps

Sizing visits every reachable chunk and common byte prefix through `readHint`, using
its own complete FIFO scratch admission. It reads lengths and layout metadata, not
borrowed row payloads. A failed sizing attempt closes before increasing its reservation.
The execution's source minimum includes the provider's resident/scratch bounds,
initial staging, old/new native output overlap, headers, and the private I/O lane.

Ordinary output prefixes fit the byte target. A row larger than the target forms a
one-row prefix and is visited separately. Thus `max(target, largest oversized row)`
bounds output even when a LIMIT, deletion or predicate changes a later prefix's start.
Every hint covers all selected columns; the scan prepares every validity mask, code
array, native int64/interval lane and int128 decimal/scale/special lane before calling
`chunk(..., {hint, context})`. Reusing sufficient buffers allocates nothing. Growth
charges both old and new arrays; closing after a suspended read drains returns them.
A sparse predicate still reads a vectorized bounded prefix before limiting active rows.

Hints remain sufficient after publication; their byte bounds may only shrink for the
same request. Whole logical values are counted, including values spanning multiple
1 MiB format blobs. B3 streams their fragments into the prepared output instead of
assembling an additional whole value. A source minimum plus downstream minima above
the owner cap raises `MemoryAdmissionError` before input execution.

The generated production check covers real memory views, publication after sizing,
observed large payloads, concurrent executions, cancellation between decode and delivery,
exact caller output identity, and zero retained grants. A mutant omitting the bounded
step fails its API-boundary assertion; a mutant omitting output charge fails before the
caller observes uncharged buffers. Provider-level layout, I/O cancellation and publication
invariance tests live in `write/memory-read.test.ts`.

Direct legacy operator fixtures and the opt-in diagnostic key-order fetch can omit
the bounded option and retain the old borrow API. The latter is off in ordinary SQL and
is outside this production proof until its query-owned read admission is integrated. The structural page-sizing helper keeps its conservative legacy fallback for
such callers without `readHint`; production access segments always provide hints.
That fallback is not a production proof, and no semantic oracle or R21 waiver is used.

## Aggregation workspaces

A regular aggregation chooses its memory strategy before applying any transition. A complete
fixed-width scalar bound, exact direct-address bounds, or a complete codebook-domain certificate
can authorize the hash path. The hash minimum covers table/translation growth, every old/new
buffer overlap, ordinary-state promotion and output. An allocation is synchronous inside that
admitted workspace; it does not borrow shared headroom. A short synthetic budget at the raw
transition-engine seam is an accounting error, not a resumable `push`.

The optional domain probe owns one complete temporary native admission. It reads the pinned
snapshot's codebooks, or a publication-stable bounded key/payload scan for a memory tail. It
drops every reader and grant before requesting the execution minimum. Only bounded numeric
certificate metadata survives. Thus planning cannot retain one part of the execution minimum
while waiting for its other parts, and competing planning executions cannot form a credit cycle.

Without a complete certificate, eligible ordinary column aggregates start with a bounded
hash workspace. They request optional headroom asynchronously at transition boundaries.
A pinned competitor causes the hash pass to stop admitting new groups: resident groups
continue in place and other rows enter stable hash-partitioned tapes. Each group finishes
in one pass. Recursive partitions preserve that group's source order; no partial floating
states are combined. A bounded resident job directory collapses children onto one tape
when it would otherwise grow, preserving each group's order.

When a resident variable state cannot grow, its exact transition fields and DISTINCT
membership are checkpointed before any unapplied input is spooled. The producer then
spools its remaining child once and closes its readers, leases, buffers and grants. The
execution retains its original snapshot pin. The runtime moves quiescent result credit
into the shared idle pool and atomically admits a restore phase: state, bounded I/O,
remaining operators, owned parameters and result credit. A pressure-only stable merge
sorts state, membership and suffix records by group key, record kind and input ordinal.
It restores one group's fields and membership before its suffix rows, so a combined
resident set above the restore cap cannot force refusal or endless hash repartitioning
under skew. Native scalar copies retain -0/Sxx, exact NUMERIC coefficients/scales and
byte extrema. Compound checkpoint rows are measured without copies, then fully funded
before native materialization. The input scan is not re-executed. The saved progress
bound is carried intact, without applying its compound multiplier a second time.
Cancellation of an in-flight nested spill belongs to its holder: the aggregate wrapper
drains it without rejecting another execution's FIFO admission. Active-owner spill
failures still propagate.

Plentiful-memory executions neither checkpoint nor spool. Fixed COUNT/float states with
bounded fixed-width keys use the admitted I/O budget for emission, so an unchanged
resident group does not need fresh hash headroom merely to produce its result. Waits
remain cancellable and use the shared task clock; cleanup drains all started I/O.

The phase handoff reconstructs unary parents and checkpoints retained EXCEPT counts,
first-left ordinals and representatives. A completed EXCEPT left arm is not reopened.
Each sealed cut stays in an execution-owned cleanup slot across producer suspension,
restore admission, parameter binding and the resumed recipe. Installing the next owner
precedes clearing the previous slot. Cancellation drains both pending and resumed cuts
before the execution settles; tests check spill bytes before the shared service closes.
Current limits: retaining join parents and computed arguments without a proven
materialization boundary use the external fallback; direct-column FILTER transitions
have a conservative full-batch growth bound. Varlen CASE output in an ordinary projection
has a separate transient-pressure allocation gap, reproduced on main; this hash-state
protocol does not make that expression buffer resumable. Genuinely ordered DISTINCT and
ordered-set transitions retain external sorting with independent private progress lanes.
The raw synchronous hash helpers still require a complete workspace; retrying a partly
applied raw push after refusal is invalid.

The operator tape's bounded disk-linked directory does not bound the shared scratch
service's per-file extent list. That metadata is presently uncharged; B1 owns the routed
fix. Pressure tests cover operator/native-buffer accounting, not a universal whole-service
hard RAM bound. This limitation also applies to scratch-backed generated SQL cases.

## Idle credit at a pressure handoff

`admitGroup` accepts `idleCredit`, a live native account containing no allocated or
loaned bytes. It registers an owner-wide idle pool. All earlier FIFO heads can use
that credit, including ordinary admission, read and loan requests; the requesting
phase gets no private reservation while waiting. `transferCredit` donations wake
the existing drain, and closing the pool unregisters it. Registered pools cannot
allocate or borrow: their credit is idle until the drain installs it in a consumer.

Service adopts pooled credit and splits named grants synchronously, without a
callback or await between them. Only the deficit increases used/peak capacity.
Unused pooled credit still counts against the physical cap; it does not consume
an ordinary head's active allowance. Adopted loan credit follows the existing loan
repayment accounting. Cancellation after service closes every installed named part.
Invalid options and already-cancelled requests register nothing.

The generated `idle-credit.test.ts` campaign covers two to four paused executions,
random complete minima that fit alone, the 8 MiB two-execution case, fresh FIFO
heads, donation wakeup, full-cap pools, cancellation and zero final charges.

The parameter input rule has one exception: after pressure forces an execution
phase handoff, already-admitted owned parameter copies may be checkpointed. Initial
oversized-parameter refusal remains unchanged. Byte payloads are written in bounded
fragments and restored only inside the new admitted input account. Caller buffers
are never reread; their contents may already have changed.
