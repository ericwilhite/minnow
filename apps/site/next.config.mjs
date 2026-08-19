import { createMDX } from "fumadocs-mdx/next";

/**
 * The docs site is a static export: Vercel serves `out/` as plain files, with the cross-origin
 * isolation headers the benchmarks page needs coming from the `vercel.json` beside this file.
 *
 * Isolation is scoped to the benchmarks route on purpose: `require-corp` blocks every
 * cross-origin subresource on any page it covers, and applying it site-wide would be a standing
 * constraint on pages that gain nothing from it. The build's own assets under `/_next/` and the
 * vendored engines under `/vendor/` still need CORP and COEP, because a module worker spawned
 * from an isolated document is a new execution context that must opt in, and Chrome blocks its
 * script outright when the response carries neither.
 *
 * `trailingSlash` keeps every published URL shaped the way the previous site published them,
 * so existing links and the browser tests keep resolving.
 */
/**
 * The isolation `vercel.json` gives the published site, applied by the dev server too.
 *
 * A static export cannot carry headers, so this is development-only — but without it the two
 * differ in a way that changes what the benchmarks page measures. An origin that is not
 * cross-origin isolated has `performance.now()` clamped to 100µs instead of 5µs, which is coarser
 * than most of what that page times, and SQLite falls back from its OPFS VFS to the
 * synchronous-access-handle pool. Numbers read in development would be a different browser's.
 */
const developmentHeaders = [
  {
    source: "/benchmarks/:path*",
    headers: [
      { key: "Cross-Origin-Opener-Policy", value: "same-origin" },
      { key: "Cross-Origin-Embedder-Policy", value: "require-corp" },
    ],
  },
  {
    // An isolated document may only load subresources that opt in — its own worker included.
    source: "/:path*",
    headers: [
      { key: "Cross-Origin-Resource-Policy", value: "same-origin" },
      { key: "Cross-Origin-Embedder-Policy", value: "require-corp" },
    ],
  },
];

/**
 * Where this build is published. Empty for the current release, which is always served at the
 * root, and `/v0.1` for an archived one built from that release's tag — every path it emits,
 * including its search index and its llms.txt, is then versioned with it.
 */
const basePath = process.env.SITE_BASE_PATH ?? "";

const config = {
  output: "export",
  trailingSlash: true,
  ...(basePath === "" ? {} : { basePath }),
  // Inlined so client components can build links out of an archived build and back to the
  // current one. `basePath` alone is not readable from the browser.
  env: { NEXT_PUBLIC_SITE_BASE_PATH: basePath },
  reactStrictMode: true,
  ...(process.env.NODE_ENV === "development"
    ? { headers: () => Promise.resolve(developmentHeaders) }
    : {}),
  // The engine and the devtools panel ship as ESM built from source in this repo, and both are
  // loaded only inside client components.
  transpilePackages: ["@minnowdb/client", "@minnowdb/core", "@minnowdb/devtools"],
  typedRoutes: false,
  images: { unoptimized: true },
};

export default createMDX()(config);
