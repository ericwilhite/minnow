# Internal native devtools binding

This project is staged for the 1.0 cutover. It is not a package entry point and does not
replace the released panel. `bindNativeDevtools` uses the real native SQL facade,
session-scoped catalog descriptions and logical explain; the existing grid renders the
canonical values through ordinal keys. Duplicate headers stay visible and clipboard
exports keep their positions. BIGINT, NUMERIC and microsecond temporal text are never
converted to Number or Date. Object rows on the canonical Result retain the last label.

The preview is bounded to 1,000 rows and adds lazy display getters rather than retaining
another full payload cache. Results and the panel's presentation are caller-owned memory;
the new catalog/explain replies remain charged until receipt. Explain does not execute SQL or report
measured timings. Catalog FTS and trigger entries are definitions, not runtime health.

The binding owns its pending operations, not the database connection. Destroy cancels and
drains its work. Live queries throw `NativeDevtoolsNotBoundError` until B8 is bound. Row
editing, snapshot controls, the published panel switch and public exports remain separate.
