# SQLLogicTest provenance

The `standard-select*.test` files are mechanically selected subsets of SQLite's database-neutral
SQLLogicTest `test/select1.test` through `test/select5.test`. The exact upstream revision, archive checksum, and source-file checksums are
recorded in `upstream.json`; `standard-exclusions.json` records every source record omitted and
why. Run `npm run test:sql:prepare-standard` to fetch, verify, and reproduce all of them.

The SQLLogicTest author permits redistribution or modification under MIT (among several offered
licenses): <https://sqlite.org/sqllogictest/doc/tip/src/sqllogictest.c>. Minnow uses the MIT option.
The corpus remains the work of its upstream authors. No attribution is required, but this notice
is kept so test provenance never becomes ambiguous.
