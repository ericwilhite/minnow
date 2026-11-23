# Registry directory: adaptive radix depth — design note

C1, 2026-11-22. Design only. Base: #55 (16 resident leaves, step signals).

## The problem

`owner/registry-directory.ts` maps `(kind, owner, block)` to a spilled 2,080-byte leaf of 16
records, through a radix-16 directory of a fixed **29 levels**:
- 2 digits of kind;
- 14 digits of owner;
- 13 digits of block (record ID / 16).

The pages live in the query's spill file (root-local scratch), so the layout is **not a stored
format** and can change freely.

**The real keys are small.** Kinds are a handful of namespaces, owners are almost always 0 (or a
command ID), and blocks count up from 0. So nearly every one of the 29 levels holds the single
digit 0.

**What each operation costs:**
- **A leaf miss** walks the path. It reuses only the prefix shared with the previous walk, and
  alternating kinds break the prefix at level 0 or 1, so it reads about 28 nodes of 264 bytes.
- **A flush** of a dirty leaf appends the leaf plus all 29 path nodes (copy-on-write).

**Per updated row of the profiled trigger UPDATE, with 16 leaves (#55):**

| Work | Per row |
|---|---|
| leaf misses | 3.9 (compulsory: new blocks) |
| flushes | 2.1 |
| page reads | 115 (about 29 per miss) |
| appends | 64 (about 30 per flush) |

Each read and append also checksums with xxhash32. The page reads and appends are almost
entirely path nodes of a single-child chain.

## Proposal: path compression (skip zero-digit chains)

A node stores, besides its 16 child refs, a **skip**: the number of levels below it that are
single-child and the digits along them. This is the standard compressed radix (PATRICIA/ART
"pessimistic path compression"):

- **Walk:** at a node, compare the key's next `skip` digits with the stored digits.
  - On a match, jump `skip + 1` levels.
  - On a mismatch, the key is absent (get) or the node splits (put).
- **Put that diverges inside a skip:** split. A new branch node holds the shared prefix, and the
  old node keeps the remainder of its skip. That's at most two new nodes per divergence.
- **Flush (copy-on-write):** append only the nodes on the compressed path, not 29.
- **Path cache:** keep the compressed path the same way as today (prefix reuse across consecutive
  walks), indexed by compressed depth.

**Expected depth** for these keys: kind (1–2 branch nodes), then a short tail where blocks
differ. That's about 3–5 nodes per path instead of 29.

**Expected work per row:**
- reads from about 115 to about 15–20;
- appends from about 64 to about 10–15;
- the same number of leaf misses (they're compulsory).

## Alternatives considered

- **Per-component adaptive depth** (digits = those needed for the largest owner or block seen
  so far, grown by adding root levels): growing the owner width moves levels in the middle of the
  path, so the directory would have to be rebuilt. It's more complex than path compression and no
  better.
- **An in-memory index from (kind, owner, block) to a spill ref:** its resident size would grow
  with the record count, which breaks the module's rule that record count changes disk use,
  never resident metadata.
- **More resident leaves (32, 64):** measured in the profile, they don't reduce the compulsory
  misses (3.8 per row at 16, 32 and 64).

## Memory and charge

- The node shape grows by the skip: a count and up to 28 digits, 4 bits each, so 16 bytes. Node
  pages go from 264 to 280 bytes.
- The resident path buffer keeps at most `LEVELS` nodes, unchanged in the worst case. The
  derived `REGISTRY_DIRECTORY_BYTES` follows the new node size: 29 × 16 more, about 0.5 KB.

## Tests and evidence

- **The work test** (`testing/registry-directory-work.test.ts`): add bounds on page reads and
  appends per row near the new measured values. RED is today's code.
- **The existing property** (`owner/registry-directory.test.ts`, records across namespaces, wide
  IDs, the max safe ID, missing records, eviction): it must stay green. Add a dimension for keys
  that diverge deep inside a skip (split coverage), with a reach state.
- **Corruption refusal:** a skip longer than the remaining levels, or digits out of range, is a
  `SpillCorruptionError`, as is a bad magic today.
- **Mutants:**
  - no split (a divergent put overwrites a sibling): the property fails;
  - skip ignored on get (false hits): the missing-record check fails.
- **Throughput:** the trigger UPDATE at 100, 1k, 2k and 20k rows against #55.

## Size

About 1.5–2 days: the walk, split, flush and the corruption checks (1 day), and tests,
measurement and the property dimension (0.5–1 day). No stored-format change.
