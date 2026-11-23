# C1 handoff (2026-11-23, moving from puck to the laptop)

Everything is on origin. Nothing of C1's is left only on puck. Main at handoff: b57a18cf9 (#52 merged).

## Where things are

| Work | Branch | SHA | State |
|---|---|---|---|
| sql-write-batch-size speed-up | `c1/batch-size-speed` | ab9d8b23a | **Draft PR #61**, on main b57a18cf9. No review yet; CI not run (draft). |
| Fix 4 (#52 follow-up) | `c1/fix4-retain` | d7c61c01d | Pushed, no PR. Needs a rebase onto main and its gate. |
| #55 registry-only | `c1/trigger-perf-registry-only` | 9f8ba017a | Pushed, no PR. PR #55's own branch `c1/trigger-perf` still points at b5c470ea3 (the withdrawn step-signal version). |
| Step signals (withdrawn) | `c1/step-signal-wip` | 45cc6bd06 | Kept for reference only. |
| Note 1: registry path compression | `c1/registry-compress` | 092665714 | Two WIP commits on 9f8ba017a. No PR. |
| Handoff scratch | `c1/handoff-scratch` | (this branch) | `c1-handoff/`: design notes, tools, drafts and this note. Not for merge. |

Backup refs, safe to delete once the above land: `c1/wide-row-readback-pre57` (f4ff95949), `c1/wide-row-readback-pre58` (c964ac205), `c1/registry-compress-wip-old` (ddedc3f01).

Older WIP pushed only so nothing is lost: `c1/abort-fanout` (221f363de, profiling probes from the 14 ms/row trigger profile), `c1/memtable-sizing` (2136b92ed, "WIP design A", 2026-11-21), `c1/point-reads` (64c8d25b3, 2026-11-19), `c1/writer-floors-wip` (ced8f7484, 2026-11-20). None is needed for the queue. Check whether their content already landed before reviving any.

## Queue, in order

### 1. PR #61: the sql-write-batch-size speed-up

**What it is.** `next/testing/sql-write-batch-size.test.ts` timed out at 300 s on #52's CI. Profiling showed the time is the engine's, in `INSERT … ON CONFLICT DO UPDATE` over 131,073 conflicting rows: 94.1 s of 100 s (UPDATE over the same rows takes 4.3 s). The upsert fetches each conflicting row with its own one-row scan, deliberately one proposal at a time. Each fetch repeated work:
1. `write/memory-read.ts`: memory reads now use the owner's metadata caches (`metadataCachesFor`). Before, every fetch re-parsed the group root and every column's page directory, including a register walk of its sketch, twice (31%).
2. `write/rows.ts`: `readGroupRows` opens readers through the directory cache.
3. `write/runs.ts`: a run block is validated once per cached bytes object (a `WeakSet`), not on every lookup (7.5%). This is sound by `storage/verify-cached.ts`'s contract, enforced by `MINNOW_VERIFY_CACHED_BYTES=1` in every test build.
4. `exec/table-scan.ts`: one shared `AbortError` `DOMException` for scan aborts. I checked that nothing compares abort reasons by identity or mutates errors.

**Evidence.**
- Main 7f0a6277f vs the fix, back to back on puck: plain 87.7 s → 39.3 s (upsert 84.3 → 34.4 s); `vitest run --coverage` 225.2 s → 95.8 s.
- Full next/ suite on main 7f0a6277f: 1,140 files, 7,621 tests passed. After the rebase onto b57a18cf9: 228 overlapping files, 1,226 tests passed.
- New `write/run-lookup-validation.test.ts`. It rejects both mutants: validation skipped, and a block recorded before its checks. `corrupt-maintenance` didn't see either mutant, because merge publication refuses its damage cases, not a lookup.
- tsc on the rebased head caught that test importing `StoreCorruptionError` from the wrong module. It's fixed and re-verified, and said in the PR body.

**Next steps.**
- Lead review; the lead marks it ready, and CI runs then.
- If CI on the merged head is slow anywhere, compare with the timings above.

**Limits.**
- The upsert is still about 14× the UPDATE.
- Timings are wall-clock on a shared machine.

### 2. Reader memo: the follow-up perf PR (not started)

**Goal (lead).**
- The upsert within 3× of the UPDATE on this test. Today it's 34.4 s vs 2.4 s.
- Keep the one-proposal-at-a-time semantics.
- Add a scaling test: rows × 2 → time ≈ × 2, so a quadratic regression can't return.

**Where the time is now** (profile after #61):
- `#fetch` in `exec/modify-upsert.ts` is about 43%. Each conflicting row builds a `TableScan` over a `PointMemorySegment` (`exec/point-segment.ts`). That reaches `write/view.ts` `readHint`/`chunk`, then `write/memory-read.ts` `hint` and `chunk`.
- Each call goes through `#during`: an `AbortSignal.any` composite, a source watch, and a 1 KiB budget charge. Then `#withReader` per column opens the group root and a `ChunkReader`, now through the caches: pin/unpin and `fullBlobKey`.
- So there are about 4 reader opens per fetch: sizing (hint) and reading (chunk), times 2 columns.
- The rest: write-path insert/settle about 11%, conflicts about 6%, key lookups about 8%.

**Plan (lead).**
- Cache the opened group root and the column readers across consecutive fetches against the same published chunk within one statement. Invalidate on publication change; the source's `watch` already fires on that.
- **Open design question: budget ownership.** Readers are charged to each call's `context.budget` and closed at the end of the call. A memo that outlives a call needs an owner whose lifetime is the statement, for example the upsert's `pool` and grant, and it must be released on statement end and on abort.
- Alternatives to weigh:
  - give `UpsertWriter` one reusable point fetcher instead of a new `TableScan` per row;
  - let `PointMemorySegment` carry a memo.

**Measure with** `c1-handoff/tools/zz-batch-time.test.ts`: per-statement timings, writes to `ZZ_OUT`; copy it into `packages/core/src/next/testing/` and delete it after. In-process profiling: `zz-batch-timing.test.ts`, then `tools/top.mjs` and `tools/callers.mjs` on the `.cpuprofile`. Vitest's `--execArgv` doesn't reach the unit project's workers, and `--cpu-prof` in `NODE_OPTIONS` misses the worker because it's killed, so profile in-process with `node:inspector/promises`.

### 3. Fix 4 (#52 follow-up): retain a key read's selected memtable chunk

**What it is** (d7c61c01d).
- A one-row read of a resident, unretained memtable chunk funded `MEMORY_READ_RESIDENT_BYTES` (19,279,872 bytes): the worst-case metadata of a publication that might happen before it opened. Its real peak is about 2.8 MB.
- Sizing now retains the selected chunk when all locators lie in one memtable chunk (`QueryRuntime.retainSourceRow`, `prepareRead({ budget, locator })`). Sizing and execution read the same retained source.
- There's a test-only hook, `QueryRuntime.afterReservation`.
- `wide-row-read-need` drops its `MEMORY_READ_RESIDENT_BYTES` allowance.
- New test: `testing/retained-key-read.test.ts`. The changelog and the next/ README are updated.

**Evidence in the commit.**
- RED: 7 of 14 with the allowance removed.
- Mutants: no retention; the recipe never releasing its borrow.
- Peak holdings beside a memtable value at 64 MiB: 30,333,162 → 4,626,696 bytes.

**Next steps (lead: rebase, run its gate as one claim run alone, open a draft PR against main).**
1. `git rebase --onto origin/main 3cd9b1279 c1/fix4-retain`. 3cd9b1279 was #52's head; main has #52 as a merge.
2. tsc, eslint, prettier, dependency-rules, `api:check`, `version:check`, then the full next/ suite with `--maxWorkers=2`.
3. `gh pr create --draft`.

**Review history.** None yet. It's the design the lead accepted in #52's Limits ("retain the selected chunk at sizing").

**Limits.**
- Reads spanning chunks or segments, and scans, keep the resident envelope.
- The hook covers publication, not a merge or compaction during the read.

### 4. #55 registry-only, with re-measured throughput

**What it is** (9f8ba017a, on main 47e7bf7e).
- The native root's registry directory keeps 16 leaves resident instead of 8 (`REGISTRY_DIRECTORY_LEAVES` in `exec/row-image-layout.ts`).
- `REGISTRY_DIRECTORY_BYTES` is derived from it (47,144 bytes).
- `REGISTRY_DIRECTORY_WORK` counters.
- `testing/registry-directory-work.test.ts`: RED 9.72 misses per updated row against a bound of 4.5; GREEN 3.93 misses, 115 page reads and 64 appends per row; mutant: 8 leaves.
- Two tests' setups are derived from the constants.

**History.**
- PR #55 first also carried "step signals", which replaced per-row `AbortSignal.any` composites with fanned-out controllers (about 12% faster).
- Five Astra review rounds each found a divergence from native `AbortSignal.any`: reentrant reason, flattening, nested pending events, observer reads in a listener, mixed dispatch order.
- The cause is structural. Native abort marks every dependent before any listener runs; a listener-based fan-out can't, short of a composite per step (about 13% cost).
- The lead dropped step signals. They're kept on `c1/step-signal-wip`, with `step-signal-review/README.md` and all probes.

**State.** Full suite on 9f8ba017a: 1,136 files, 7,609 tests passed, no heap errors.

**Next steps.**
1. Rebase onto main.
2. Re-measure the trigger UPDATE throughput at 100, 1k and 2k rows: main vs registry-only vs 0.x, on a quiet machine.
   - Use `c1-handoff/tools/zz-throughput.test.ts` (`ENGINE=next|0x`, `SIZES`): copy it into `packages/core/src/next/testing/`, run it, delete it.
   - Previous numbers: `c1-handoff/drafts/pr55-throughput-previous.txt`.
3. Fill `THROUGHPUT_TABLE` and `FULL_SUITE_RESULT` in `c1-handoff/drafts/pr55-body-registry-only.md`.
4. Run the full suite.
5. Push to PR #55's branch: `git push --no-verify --force-with-lease=c1/trigger-perf:b5c470ea3 origin <local>:c1/trigger-perf`.
6. `gh pr edit 55` with a new title (for example "Keep 16 registry leaves resident on the native root") and the body.
7. The lead marks it ready after a quick check.

**Limits.**
- The work bounds cover one trigger shape, a keyed UPDATE body.
- The counters are process-wide.

### 5. Note 1: registry path compression (`c1/registry-compress`, 092665714)

**Design.** `c1-handoff/notes/C1-registry-adaptive-depth.md`.
- A path-compressed radix walk, flush and split in `owner/registry-directory.ts`: 296-byte nodes, 40-byte header, `(REGISTRY_LEVELS+1)*NODE_BYTES` path slots, a `splits` counter.
- The uncompressed directory is kept as the reference (`owner/registry-directory-uncompressed.test-support.ts`).
- A fast-check property, compressed vs uncompressed (`owner/registry-directory-compression.test.ts`, `REGISTRY_COMPRESSION_REACH`).
- Work bounds tightened to 15 page reads and 10 appends per updated row. Measured: 3.9 misses, 12.8 reads, 8.3 appends.

**Done on puck.**
- Rebased onto 9f8ba017a.
- tsc, eslint and prettier are clean; the 4 registry test files pass 12/12.
- Mutant, a split losing the remainder digits: the property fails both seeds with "expected false to be true".

**Left.**
- Throughput vs #55 at 100, 1k, 2k and 20k rows.
- Squash the two WIP commits into one with Law/RED/GREEN/Mutant/Reach/Limits.
- Rebase onto #55's final head.
- Full suite; draft PR after #55 merges.

### 6. Note 2: fewer native re-stabilizations

**Design.** `c1-handoff/notes/C1-fewer-native-restabilizations.md`, re-scoped to plain `AbortSignal.any`.
- `NativeRootCredits.#fund` (`owner/native-root-credits.ts`) opens a native write step, and the engine re-stabilizes, between producers even when no new credit is needed.
- Measured per updated row (2,000-row trigger UPDATE, on an older main): 14.05 stabilizations, 7.93 of them finding the writer exactly as the previous one left it.
- Proposal: a "stabilization witness" (state object, catalog, minimum state, protected bytes, root bytes and capacity, native step version, loan and merge state). A funding pass needing no credit skips stabilization when the witness proves the writer current.
- A test-build dry-mode check runs the full stabilization on every skip and asserts it would change nothing.

**First step:** re-measure the counters on current main (#56 changed write-side funding). Build only if most stabilizations per row are still unchanged.

**Size:** about 1.5 days, no stored-format change.

### 7. Sentinel follow-up: likely drop, confirm with the lead

This was a planned reduction of the per-parent sentinel `AbortSignal.any([parent])` composites that the step-signal design used (about 13 per row). With step signals withdrawn there are no sentinels, so it's moot. Ask the lead to confirm dropping it.

## Open decisions for the lead

- **Reader memo:** who owns the memo's budget (statement pool/grant vs per call), and whether to reuse one point fetcher per `UpsertWriter`.
- **Sentinel follow-up:** drop (recommended).
- **Follow-ups named in #52's Limits, not scheduled:**
  - the stored selected-value factor: a stored one-row read bound counts a selected large value four times (`4 * values` in `page/read-bounds.ts`) while the fetch allocates two;
  - the scan and sort lanes' four-copy envelope: scheduled with P1's sort sizing;
  - a format-freeze candidate: record each page's raw length in `PageEntry`.

## Things learned the hard way (puck)

- **Vitest 4 with `isolate` on** (the default) gives every test file a fresh fork. A worker's heap can't leak across files. #52's "OOM" was one file spinning in microtasks: `write/engine.ts` `#waitForRows`, main's bug, fixed by #57.
- **To see a microtask spin:** start the worker with `NODE_OPTIONS=--inspect=127.0.0.1:0` and pause it over CDP with `c1-handoff/tools/pause.mjs <port> <samples>`. Timers and in-thread inspector sessions never run during the spin; the WebSocket inspector does.
- **`setsid` without `-w` returns at once.** Use `setsid -w` when the next command depends on the run.
- **Vitest doesn't typecheck tests:** an import from the wrong module turned `toThrow(Class)` into `toThrow(undefined)`, which accepts any error. Run tsc before trusting a new test.
- **Branch pushes run no CI.** `ci.yml` runs on pushes to main and on PRs only.

Tool paths in `c1-handoff/tools` assume `/home/eric/.cache/c1-tmp/...` output locations; adjust on the laptop.
