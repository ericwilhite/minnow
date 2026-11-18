# Internal native React entry

The hooks use the same structural external-store implementation as 0.x. They
preserve whatever exact snapshot B8/B6 supplies; they do not decode or coerce
BIGINT, NUMERIC or temporal values. This separate project is outside the packed
dist and exports. The existing hook suites qualify the unchanged implementation;
the native entry test verifies function identity and exact-value rendering.

`nativeLiveStore()` explicitly throws "not yet bound". It does not start polling,
create an uncharged engine-retained store or replace B8's live protocol. The actual
finite-credit native external-store bridge remains blocked by that seam. Static
caller-owned snapshots in the test do not constitute live-runtime qualification.
