# P1 handoff (puck → laptop), 2026-11-23

Everything below is on origin. Nothing of P1's is left only on puck.
Standing rules still apply:
- push with `--no-verify`, and open PRs only as drafts;
- give every change Law / RED / GREEN / Mutant / Reach / Limits;
- run one gate at a time, and vitest with `--maxWorkers=2` for full `next/` runs;
- for `src/engine/` changes, run `tree-shaking.test.ts` measured on your own tree, plus the 0.x compiler suites;
- run `api:check`, `version:check`, `scripts/package-shape.test.ts` and `next/dependency-rules.test.ts` before every push;
- if a size cap would need raising, stop and ask the lead.

## 1. #59: read-only snapshot timing and re-pin
Branch `fix/p1-repin`, PR https://github.com/ericwilhite/minnow/pull/59 (draft), head **`1fc8c730d`**. This is `d2bf552f0` with its GREEN line amended, pushed with `--force-with-lease` over `2badb7ee0`.

**State:** pushed, still a **draft**, and **not fully gated**. The gate was interrupted when work moved to the laptop.
- **Green so far:** build, typecheck, `api:check`, `version:check`, shape/deps, the grammar suites.
- **Not run:** the whole `next/` suite (it was running), the 0.x compiler suites, tree-shaking.
- **Run alone before the gate, all green:**
  - `grammar-verdict-pglite`;
  - `snapshot-statement-kind`, `front-end-states`, `seeds` and `dependency-rules` (5 files, 19 tests);
  - tree-shaking (core and kysely) and `package-shape` with the pre-#59 caps (3 files, 39 tests).

**Commits (base main):**
- `c57d384e6`: a read-only transaction takes its snapshot at its first statement.
- `12068f48c`: a refused query that has observed nothing moves to a fresh snapshot, once.
- `c59669caa`: the snapshot is taken before analysis (`Lowerer.takesSnapshot`).
- `ded7b14bb`: at most one re-pin per execution.
- `e827b24c5` and `2badb7ee0`: parser grammar marks and an audited allowlist. Both are superseded by the next commit.
- `d2bf552f0`: token rules in `next/lower/raw-parse.ts`, gated by a PostgreSQL corpus.

**How the last commit works (the lead chose option 1):**
- A failed statement counts as parsed, and takes the snapshot by its kind. The exception is when `rawParseRejects` finds text that gram.y or scan.l reads nowhere. It checks the tokenizer's output against rules, each probed in PGlite (77 statements rejected and 44 parsed, all as claimed).
- `engine/query.ts` is back to its pre-#59 text plus two `export` keywords (`tokenize`, `Token`), which cost 0 bundle bytes.
- `engine/grammar-failure.ts` and the audit guard are deleted.
- The three cap raises from `2badb7ee0` are reverted. `tree-shaking.test.ts` and `package-shape.test.ts` are back to their `ded7b14bb` text and pass. Sizes equal the pre-#59 baselines:
  - main entry gzip 354,700;
  - IndexedDB worker gzip 459,066;
  - OPFS gzip 461,652 (172 bytes of headroom);
  - packed 1,030,709.

**The gate test** is `next/testing/grammar-verdict-pglite.test.ts`:
- **Corpus** (`grammar-corpus.ts`, built per seed): the SQLLogicTest profile, the feature matrix, the docs and console SQL, 20 PostgreSQL forms and one interaction-simulator plan, plus 7 mutations. About 3,800 unique statements per seed.
- **PostgreSQL's verdict:** `exec("SELECT 1/0;\n"+sql)` failing with 22012 means the statement parsed. PGlite 0.5.5 faults after about 1,470 parse errors in one instance, so the test recycles it every 300 statements.
- **A** (PostgreSQL parses, next/ rejects) must be 0.
- **B** is the share of PostgreSQL's rejections that next/ reads. It must be at most 0.609 (`LATE_SYNTAX_ERROR_SNAPSHOT.ceiling` in `testing/divergences.ts`).
  - Measured: 0.6083 and 0.5910 (1,241 of 2,040, and 1,192 of 2,017).
  - Of those, 78 and 79 are next/'s extensions, which it runs.
  - The ceiling may only go down. A tightness check fails if it sits more than 0.01 above the measured rate.

**Evidence:**
- **RED:** at 2badb7ee0, A = 196 and 197.
- **Mutants**, all failing by assertion:
  - every failure counted as a rejection: A = 385;
  - no unbalanced-bracket rule: B = 0.753;
  - EXISTS takes only SELECT: A = 19;
  - the trigger rule ignores EXECUTE: A = 18;
  - text and numeric treated as restricted types: A = 44;
  - `takesSnapshot` ignores the rules: its cross-check fails.

**Docs:**
- `engine/transactions.mdx` and the changelog: "affects only statements that fail in PostgreSQL … about three in five of those PostgreSQL rejects as syntax errors are detected late".
- The divergence `late-syntax-error-snapshot`, and `next/README.md`.

**Review history (Astra):**
- **Round 1, two P2s:** the snapshot must come before analysis, and there should be one re-pin per execution. Fixed in `c59669caa` and `ded7b14bb`.
- **Re-check, two P2s:** 42601 doesn't prove a raw parse failure, and a leading comment hid the keyword. Fixed structurally in `e827b24c5`.
- **Re-check, unmarked grammar checks:** fixed by the audit in `2badb7ee0`.
- **Re-check at `2badb7ee0`, six P2s:**
  - `CREATE VIEW v`;
  - `count(DISTINCT)`;
  - `EXISTS (VALUES …)`;
  - CREATE TRIGGER with EXECUTE FUNCTION, INSTEAD OF or TRUNCATE;
  - generic type modifiers;
  - the guard missed a callback throw.
  
  All are fixed in `d2bf552f0`. Astra hasn't re-checked `d2bf552f0` yet.

**Next steps:**
1. Rerun the full gate on `1fc8c730d`, alone. Tree-shaking must measure this tree: point `node_modules/@minnowdb/*` at the checkout, or use a real install. A symlinked `node_modules` measures another checkout.
   - **If green:** amend the commit message's GREEN line with the result, push, and update the PR body's Gate section.
   - **If red:** fix it. Keep A = 0, and keep the caps untouched.
2. Astra re-checks `d2bf552f0`. The lead marks the PR ready.

**Limits (also in the PR body):**
- B is about three in five of PostgreSQL's rejections in the corpus. These are statements next/ refuses on a form it doesn't read before reaching PostgreSQL's syntax error (`SELECT f(`, `DELETE t`), plus about 4% that are extensions (`?`, `MATCH … AGAINST`, `SELECT 1e`).
- A = 0 is measured on the corpus, not proven for all text.
- `CURRENT_TIMESTAMP(1, 2)` is refused as unsupported precision before it's read further.

## 2. #60: test-build retry-spin guard
Branch `fix/p1-spin-guard`, PR https://github.com/ericwilhite/minnow/pull/60, marked **ready** by the lead, CI was running.

- **Head:** `aad61d0f1`, the lead's own fix for Astra's P3: with the guard off, no progress bookkeeping. My last commit is `23a3a304d`.
- **Gate:** my last gate on `23a3a304d` was green, except a timeout in `sql-write-batch-size.test.ts` under load. That file passes alone in 159 s; it's the known load flake, and C1's engine fix is in progress.

**Review history:**
- **Round 1, two P2s:** progress resets the count, and `RetrySpinError` must escape merge recovery. Fixed in `b843516e4`.
- **Round 2:** rounds that yield through timers were counted. Fixed with a macrotask epoch in `0bb631e8b`.
- **Round 3:** the epoch timer was armed while the guard was off. Fixed in `23a3a304d` (inert when off, `unref`'d).
- **Round 4, P3:** fixed by the lead in `aad61d0f1`.

**Next steps:** watch CI and merge (the lead does this). Nothing pending from P1.

**Limits:** a loop not listed isn't guarded. 64 bounds retries without progress, not how long a wait takes.

## 3. Operator SQLSTATE fix: next in the queue
Branch `fix/p1-operator-sqlstate`, head `524a3a8be` (pushed WIP, **no PR**). **Not gated.**

**What it does:**
- `lower/coerce.ts` `comparisonType` maps `commonType`'s 42804 to 42883, "operator does not exist: <l> <op> <r>", using PostgreSQL's type names. It applies to binary, IN, NOT IN, BETWEEN, IS [NOT] DISTINCT FROM, ANY/ALL, IN subquery, simple CASE and NULLIF.
- An IN list without a common type compares item by item (OR, or AND for NOT IN), as `transformAExprIn` does.
- 42804 is kept for UNION, CASE results and COALESCE.

**Tests:**
- `testing/operator-sqlstate-pglite.test.ts`: 5 tests, 2 seeds × 200 runs. It covers 11 types including VARCHAR and CHAR, and compares rows for the IN forms.
- Reach `OPERATOR_SQLSTATE_REACH`.
- RED and mutants were done when the commit was written. The evidence is in its commit message.

**Docs:** changelog and `next/README`.

**Next steps:**
1. Rebase onto current main. It's 23 commits behind, and #59's and #60's merges may touch the same lowering files.
2. Run its gate alone: build, typecheck, `api:check`, `version:check`, shape/deps, the whole `next/` suite, and tree-shaking if engine files are touched (they aren't today).
3. Push, then open a draft PR against main.

## 4. Follow-up queue (after the operator fix)
1. **Exact accounting of releasable background credit while growth is queued** (from #57's review, P2 accepted as a limit). While writer growth is queued, `retained()` counts all writer credit, so a read in a writable transaction can be refused early. Tell background growth blocked by the requesting statement apart from growth that completes on others' releases. This removes that early-refusal class and #56's two.
2. **string_agg sizing:** size `string_agg` results the way #58 sized the other expressions.
3. **Text-function sizing** (repeat, lpad, rpad, substring, trim, …) once the kernels land. #58's body states they aren't sized yet.
4. **SQL row-wait test after #52:** pin the #57 row-wait fix through SQL once #52's surface exists.
5. **jsonFloatText:** non-termination on float32 inputs. Found earlier, not yet fixed or filed.
6. **Late syntax errors (#59):** lower B by adding token rules, each probed in PGlite and held to A = 0. Lower the ceiling with each.

## 5. Other branches on origin (WIP, no PR, not gated)
- `perf/p1-narrow-insert` (`fd8cd3053`): a cache of the frozen memtable group minimum. It measured as no help (+3.08 ms vs tip, 11/21). It awaits a lead decision and is probably to be dropped.
- `feat/p1-values-row-host` (`04e538c83`): ValuesRowHost and `Governor.servableBeside` WIP (items "A + VALUES" from the 2026-11-22 statement-sizing plan). Its T1/T2 tests passed locally.
- `scratch/p1-e2-batch8192` (`82864e0df`): a BATCH_ROWS 8192 sizing experiment, rejected by a paired claim (83.4 → 88.2). Kept for the record only.
- `wip/p1-puck-*`: uncommitted experiments saved from puck worktrees, none reviewed or gated.
  - `c28-A` and `c28-B`: page/select variants on `60eb64bfb`.
  - `hang` and `hang11`: trace-ring instrumentation for an executor hang, on `e7a3fc7e8` and `e10e0e7c0`.
  - `j2`, `j3`, `main-now`, `p3a`: copies of the head-to-head harness scripts.
  - `srw-mut`: a source-recipe-wait mutant.

## 6. Housekeeping done on puck
- **Stopped:** only my own processes, by recorded PID: the interrupted #59 gate (bash 3785471, npx 3793608, vitest 3793627).
- **Removed:** my scratch worktrees and scratch directories under `/home/eric/.cache/p1`.
- **Logs kept nowhere:** gate logs and corpus explorations were scratch. The numbers above are the record.
