# Native credit groups

`MemoryGovernor.admitGroup(options)` acquires a complete minimum with one FIFO
admission and splits it into named native grants. `options.grants` lists each
part's `label`, `minimumBytes` and `spillable` flag. The result has the same order.
Labels must be unique and minima must be nonnegative safe integers. The request
is copied before waiting. No part holds credit while the total waits; cancellation
returns the whole admission, including a cancellation after service but before
splitting. A zero-total group creates empty budgets without queueing: it acquires
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
