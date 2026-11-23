## What

The native root's registry directory now keeps 16 leaves resident instead of 8.

This was profiled on a 2,000-row trigger `UPDATE`, whose AFTER UPDATE body runs `UPDATE mirror SET v = NEW.v WHERE id = NEW.id` for each row. Each row's working set spans about a dozen record blocks, so 8 resident leaves thrashed. Per updated row, the directory:
- missed 9.7 leaves;
- paid 377 page reads;
- paid 181.5 spill appends.

**The change:**
- `REGISTRY_DIRECTORY_LEAVES = 16` is a named constant in `exec/row-image-layout.ts`, the image-registry budget module, which both `owner/` and `exec/` can import.
- `REGISTRY_DIRECTORY_BYTES` is derived from it: 4,096 control + 29 × 264 path + 64 + 16 × (2,080 leaf + 2 × 64 objects) = 47,144 bytes. It was a bare 30,720 for 8 leaves; the same derivation gives 29,480 for 8.
- `ROW_IMAGE_FIXED_BYTES` is derived from it as well.
- `REGISTRY_DIRECTORY_WORK` counts leaf misses, flushes, page reads and appends. These are plain integer increments on the spill path only.

## Throughput

THROUGHPUT_TABLE

## Evidence

- **Law:** a statement whose per-row working set fits the resident leaves misses only the blocks it opens for the first time (`owner/registry-directory.ts`).
- **The test** (`testing/registry-directory-work.test.ts`): 400 rows, 200 of them updated through the trigger.
- **RED (8 leaves, as on main):** "leafMisses per updated row: expected 9.72 to be less than or equal to 4.5" (9.72 misses, 377 page reads, 181.5 appends per row).
- **GREEN:** 3.93 misses, 115 page reads and 64 appends per row, against bounds of 4.5, 130 and 70.
- **Mutant:** `REGISTRY_DIRECTORY_LEAVES = 8` fails, for the reason above.
- **Two tests' setup changed; their assertions didn't:**
  - `registry-directory.test.ts`'s I/O-fence and fault cases filled 8 blocks to force an eviction. They now fill `REGISTRY_DIRECTORY_LEAVES` blocks.
  - `registry-commands.test.ts`'s row range (320–640, with examples at both ends) was sized to exceed the old fixed metadata by 1.2–2.4×. It's now derived from `REGISTRY_DIRECTORY_BYTES` the same way: 476–952 rows.
- **Full next/ unit suite (2 workers):** FULL_SUITE_RESULT
- **Gates:** tsc, eslint, prettier, dependency-rules, `api:check` and `version:check` are clean.

## Withdrawn: step signals

This PR first also replaced per-row `AbortSignal.any` composites on the native write path with "step signals": plain controllers fanned out from long-lived parents. They cut composites from 57 to 0.01 per updated row, and about 12% of this statement's time.

**Why it was withdrawn:** five review rounds (Astra) each found a way a step differed from `AbortSignal.any`:
1. a reentrant abort replaced the caller's reason;
2. an aborted step's recorded reason was lost through flattening; a direct abort, and a close, inside a parent's abort misbehaved;
3. a step opened over a step with two nested pending aborts fired an extra event, which could abort an independent step;
4. an observer reading a step's state inside the parent's own listener saw it unaborted, where native shows it aborted;
5. parents mixing native composites and step signals dispatched in a different order.

The cause is structural. Native "signal abort" marks every dependent aborted before any listener of the source runs. A fan-out runs as a listener, so it can't, short of a composite per step, which is the cost being removed. A prototype with a hidden composite per step was exact but about 13% slower. The lead decided that exact cancellation semantics are worth more than this gain.

**Kept on `c1/step-signal-wip`:**
- the implementation;
- the fast-check differential against native composites;
- the dispatch guard;
- the ten converted engine sites;
- all of Astra's probes and their output, with a note on each round (`step-signal-review/README.md`).

A later design could create native composites lazily, only for steps that are observed during a parent's abort.

## Limits

- **The work bounds cover one trigger shape:** a keyed `UPDATE` body. A body with a larger per-row working set can still miss.
- **The counters are process-wide.** The test reads a difference taken around one statement.
- **No public behaviour or stored-format change,** so there's no changelog entry. The next/ README documents the resident leaves.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
