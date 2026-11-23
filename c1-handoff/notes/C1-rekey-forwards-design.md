# Rekey forwards on next/ — design note

C1, 2026-11-22. Design only; nothing built. Base: main 284f32ccd.

## 0. Decision needed first: D14 says the opposite

The 2026-11-21 item treats missing forwards as a 1.0 bug. The 1.0 plan removed them on purpose:

- **D14** (`digest.md:238`, `PROPOSAL-v1-compat.md:45`, marked unanimous): "Move 10-day aliases
  into an explicit extension … Core keeps stable row IDs and FK semantics. **API change.**"
- **R21** (`reviews/PHASE-B-joint-review-1.md:272-274`): "the reset removes key forwarding (D14);
  PostgreSQL accepts the insert — registered intended divergence."
- **In code:** `next/testing/divergences.ts:315-327` registers `key-forwarding` as an intended
  divergence (authority: D14, plus PGlite). `lower/lowerer.ts:16` and `lower/frontend.ts:13` say
  "key forwarding (gone)". B8's live design assumes it is gone
  (`handoff/B8-live-publication-seam.md:102-104`: "0.x prose claiming durable cross-tab rekey
  forwarding must be rewritten in the held cutover docs").
- **Against it:** the docs still promise it (`sql/dml.mdx:134-143, 262-263`,
  `engine/rekeying.mdx` throughout, `engine/index.mdx:151` `keyForwardRetentionMs`). The
  simulator's next/ run lists it as an open finding (`next-parity-gaps.ts:93-103`, owner C1).

Both registrations can't stand. The options:

- **(A) Reverse D14: forwards are core 1.0 behaviour.** Build sections 1–8 below. Remove the
  `key-forwarding` divergence and the open finding. Rework B8's assumption (section 6). Format
  change before the freeze.
- **(B) Keep D14: no forwards in 1.0.** No engine work. A docs and test change:
  - rewrite `sql/dml.mdx` "Changing a primary key", `engine/rekeying.mdx`, the
    `keyForwardRetentionMs` option (`engine/index.mdx:151`), the agent rules and the changelog
    (a 1.0 behaviour change with migration notes: a rekeyed-away key is free at once, as in
    PostgreSQL);
  - turn the simulator's open finding into the registered divergence it already is (the next/
    shadow model frees a retired key);
  - the extension D14 names stays future work.
  - About 1–2 days.

My recommendation is a product call, so I only list the trade:
- **(A)** keeps a documented 0.x behaviour with no migration story to write. It costs about
  7–9 days plus a format bump, and adds a per-statement check on keyed writes.
- **(B)** matches PostgreSQL and the 1.0 plan as reviewed. It is a documented behaviour change.

Sections 1–8 assume (A).

## 1. What must hold (from the docs)

- **Reading and writing:** after `UPDATE t SET id = new WHERE id = old`, the old key "keeps
  reading and writing the row for `keyForwardRetentionMs` (10 days by default)". Equality and its
  opposite with a constant or a parameter, under `AND`/`OR`/`NOT`/`IS NULL`, in subqueries,
  CTEs and views (`rekeying.mdx:113-123`). A composite key only when every column is pinned with
  `=` on one table reference. Results show the current key.
- **Writes through an old key:** `UPDATE`, `DELETE` and the batch calls act on the row. An upsert
  or `ON CONFLICT` naming it updates the row. A child row's foreign key naming it is stored as the
  current key.
- **Taken keys:** a plain `INSERT` of a retired key, and a rekey onto one (including back to an
  earlier key), are refused with `UniqueConstraintError`. next/'s is `UniqueViolationError`, 23505
  (`access/modify.ts:265`).
- **Swaps:** a key that another moved row now holds gets no forward (`rekeying.mdx:129-130`).
- **Chains:** forwards are never rewritten. A→B then B→C: A follows both, and each hop expires on
  its own.
- **Deleted rows:** a forward outlives its row. If a new row later takes the target key, the old
  key names that row until the forward expires (`rekeying.mdx:145-147`).
- **Expiry:** fixed per forward when it is written (`now + keyForwardRetentionMs`). Changing the
  option affects only later forwards. After expiry the old key is free. Maintenance and
  `collectGarbage()` delete expired forwards.
- **Atomicity and durability:** the forward publishes with the row in the same commit, survives a
  reload, is visible to other tabs and connections after commit, and is local (not synced).
- **Triggers:** a rekey fires no `INSERT`/`DELETE` trigger. next/ already fires `UPDATE` only.
- **Cost:** "one in-memory lookup, and nothing at all until the first rekey" (`rekeying.mdx:127`).

## 2. Storage: a reserved internal table (recommended)

**The shape.** One per-database internal table, `minnow.key_forwards`, created on the first rekey:
- `table_id INT`;
- `from_key BYTEA`: the encoded primary-key bytes, the same encoding the key index uses;
- `to_key BYTEA`;
- `expires_at INT8`: ms, engine clock;
- primary key `(table_id, from_key)`.

**What it reuses.** It is ordinary data, so the existing path covers it unchanged:
- the memtable `KeyDelta` and its runs, flush, level merges and tombstone purge;
- WAL `rows` effects, recovery replay and snapshots.

**What's new:**
- **`TableDef.internal?: "key-forwards"`.** A catalog field. SQL name resolution, the catalog
  listing, `DROP`/`ALTER`, the schema DSL and export never see an internal table. A user table
  can't take the name.
- **The format bump.** It's a catalog codec field, so: vocabulary fingerprint change, a new
  `NATIVE_READER/WRITER_PROFILES` entry, and `MANIFEST_FORMAT_VERSION` and the minimum reader
  raised (`durable-format.ts:11-15, 20-29, 1104-1130`). Writers before the bump refuse a root
  that carries the field, which is the existing "older writer refuses after upgrade" mechanism.
  next/ has no released format yet, so there's no upgrade path to write. But it must land
  **before the freeze**, or it becomes a supported-format conversion.
- **Rejected alternative: a dedicated forwards structure.** A new root field with its own sorted
  runs, flush and merge. It's more format surface and a second copy of the run machinery, for
  nothing the internal table lacks.

**Costs:**
- Disk: about 40–60 bytes per forward plus key bytes (0.x: about 100 B).
- Memory: none resident beyond the ordinary memtable and runs. See section 3 for the gate.

## 3. Lookups

**The gate (what "nothing until the first rekey" means).** A statement consults forwards only
when the forwards table exists and holds a live row. That's read from the snapshot's catalog and
the table's live-row count: no I/O, no resident cache.

**Resolution.** One point lookup into the forwards table's primary key per key value, through
the ordinary `ViewIndex.#resolveFrom` (a memtable hash, then runs with Bloom filters).
- Follow `to_key` while a non-expired forward exists, at most 64 hops, stopping at a cycle (as
  0.x's `followForwards`).
- Effective expiry is the earliest along the chain.
- A forward is used only if `expires_at > now` at statement start (one clock read per
  statement, so a statement sees one consistent set).
- It reads at the statement's snapshot, so uncommitted forwards from other connections are
  invisible, and the statement's own transaction sees its own (ordinary MVCC).

**Where it applies:**
1. **Bind time, for SQL with constants and parameters.** A key-equality predicate (`=`, `IN`,
   `IS NOT DISTINCT FROM`, and their negations) on a primary key, or on a composite key fully
   pinned with `=`, has each bound value replaced by its resolved current key. That's after
   parameters bind (exec/run.ts binding), before sizing.
   - **No re-planning:** the plan shape is the same; only the slot values change.
   - It must be per bind, not per plan (the plan cache serves many parameter sets).
   - It covers subqueries, CTEs and views, since they're lowered into the same plan.
   - The lowering marks which slots are "key-equality on table T's primary key", so binding
     knows what to resolve. That's the analogue of 0.x `forwardPlanKeyLiterals`, but on slots,
     not on SQL text.
2. **Write paths** (`write/transaction.ts` older path and `owner/command-*` native path):
   - **Plain `INSERT`:** after the ordinary uniqueness check, a non-expired forward for
     (table, key) means `UniqueViolationError` 23505.
   - **Upsert / `ON CONFLICT`:** redirect the conflict target to the resolved key.
   - **Rekey:** the new key is refused if another row holds it or it has a live forward. It is
     allowed when a row moved in the same statement held it (a swap); that key gets no forward.
   - **Foreign-key child writes:** a referencing value is resolved before the parent check and
     stored as the current key (`#parentChecksForChange` / native follow).
   - **Batch API keys:** the same resolution at the owner's batch entry.
3. **Writing the forward.** The rekey's `#deleteRows`/`#insertRows` pair (`write/transaction.ts`
   1625/1632, 2552/2553) also writes a `(table, old_key → new_key, now + retention)` row to the
   forwards table, in the same statement and transaction, so it's atomic by construction.
   - Re-retiring a key after its forward expired overwrites it (upsert on the forwards PK).
   - Existing forwards are never rewritten. A chain is followed, never collapsed.

**Cost per keyed statement once forwards exist:** one or more point lookups, cheap with
memtable-resident forwards, plus Bloom-filtered run probes. Not a per-row cost, except per value
in an `IN` list and per batch row on the write paths.

## 4. The memtable key index, flush and merge

- **No change.** The forwards table is an ordinary keyed table.
- **Purging expired forwards:**
  - an ordinary `DELETE … WHERE expires_at <= now` on the forwards table, in pages (0.x uses
    2,048), run by idle maintenance and by `collectGarbage()`;
  - skipped when nothing has expired: a per-table minimum `expires_at` kept in memory, rebuilt
    lazily on open, or simply one cheap check of the forwards table's live count;
  - the tombstones then flow through flush, and bottom merges drop them.
- **Funding:** the purge is a normal write statement under the writer turn, admitted like any
  other. No new holder chain.

## 5. Crash recovery

- **Nothing new:** forwards are rows. The rekey's row move and its forward are one commit's
  `rows` effects, so replay restores both or neither.
- **Tests must cover:**
  - an interruption at every publication step of a commit carrying a rekey: the forward and the
    move appear together or not at all;
  - a reopen with forwards across flush and merge;
  - expiry across a reopen with an injected clock (`WritePathOptions.now`).

## 6. Live queries and the client API

- **Live queries:** next/'s B8 rekey notices are owner-session state, never stored
  (`write/live-rekeys.ts:1-2`). With forwards back:
  - a live query that names a retired key by constant or parameter resolves it per evaluation,
    like any statement;
  - B8's "continuity is owner-session-bounded, reset when it can't be proved" stays valid;
    forwards don't need to feed live continuity.
  - The docs' sentence "A live query that named an expired key keeps its last result until one
    of its tables changes or maintenance deletes the forward" needs the purge to count as a
    change to the forwards table. That's true if live invalidation sees the internal table's
    writes. **To check in the build.**
- **Client API:** `resolveKey`/`resolveKeys`/`onKeyChanges`/`mergeKey` (`rekeying.mdx:196-292`)
  are 0.x client APIs. Whether next/'s client exposes them is a separate parity question.
  - Under (A): `resolveKey(s)` is a direct use of section 3.
  - `onKeyChanges`/`mergeKey` sit on B8's notices and are listed but not designed here.

## 7. Option and configuration

- `keyForwardRetentionMs` becomes an owner option (default 864,000,000), validated as a positive
  safe integer.
- It is passed to the write path. It's not stored: each forward stores its own expiry.
- The clock is the write path's `now` (`write/engine.ts:821`), injectable in tests.

## 8. Tests and evidence

- **The simulator:** remove `rekey-forwards` from `NEXT_OPEN_FINDINGS`; the rekeying plans then
  declare reach states (`interaction-simulator-next.test.ts:31-37`). Remove the `key-forwarding`
  divergence. The shadow model already encodes the docs (`interaction-simulator.ts:2280-2700`),
  but it has **no expiry**. Add a clock dimension (advance past retention), with a reach.
- **Port 0.x's pins:** `engine/rekey.test.ts` (reserved key, rekey back, chain, expiry, swap,
  writes through SQL and batch, another connection, read and write audits, savepoints),
  `key-forwards-chain.test.ts` (chain, partial expiry, reuse, reopen), and the FK child write.
- **New:**
  - the gate: no forwards-table lookups before the first rekey, as a count assertion;
  - the purge: expired rows deleted, tombstones merged away;
  - crash and reopen at each commit publication step;
  - an older writer refused after the format bump;
  - concurrency: an uncommitted forward is invisible to another connection; a committed one is
    visible.
- **Format:** frozen-fixture discipline begins at the first released 1.0 format. Until then: the
  bump, the profile entry and a refusal test.

## 9. Size

**About 7–9 days:**
- 2 days: storage, catalog flag and format bump;
- 2–3 days: bind-time resolution and plan slot marking;
- 2 days: the write paths (insert refusal, upsert, rekey refusal and swap, FK, batch);
- 1 day: purge and the option;
- 1–2 days: the simulator clock dimension and ported tests.

**Excluded:** the `onKeyChanges`/`mergeKey` client APIs.

Option (B) is about 1–2 days, all docs and test registration.
