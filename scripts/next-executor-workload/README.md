# Executor catalogue workload

The Node executor benchmark uses these committed modules by default. It needs no files in
`~/.local/share`. Build the repository, then run:

```sh
TMPDIR="$HOME/.cache/minnow-bench" node scripts/next-executor-bench.mts \
  --rows 1000000 --repeats 5 --only sort_topn_wide \
  --output "$HOME/.cache/minnow-bench/results"
```

Create the TMPDIR directory before running. `--harness` remains an explicit override for a
compatible directory containing `gen.js` and `queries.js`.

These are the reviewed Gate B analytics workload, copied on 2026-11-07. Original SHA-256:

- `gen.js`: `6f965e234cdaf133991210cadb10e183045a9869aac61c07776b24dfc1675485`
- `queries.js`: `89986f7190f1b661c7dbe70479a79f433cf423945ed1c86f85476288e7bbec18`

Formatting was normalized and one unused string-padding helper was removed. Data and SQL
fingerprints in `workload.test.ts` match the originals. The star data generator already lives in
`apps/site/bench/benchmark.ts`; its query catalogue is included here too. Timing artifacts belong
under the caller's cache directory and are not published benchmark results.
