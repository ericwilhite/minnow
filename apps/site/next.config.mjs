import { createMDX } from "fumadocs-mdx/next";

/**
 * The docs site is a static export: Cloudflare Pages serves `out/` as plain files, with the
 * cross-origin isolation headers the benchmarks page needs coming from `public/_headers`.
 *
 * `trailingSlash` keeps every published URL shaped the way the previous site published them,
 * so existing links and the browser tests keep resolving.
 */
const config = {
  output: "export",
  trailingSlash: true,
  reactStrictMode: true,
  // The engine and the devtools panel ship as ESM built from source in this repo, and both are
  // loaded only inside client components.
  transpilePackages: ["@minnowdb/core", "@minnowdb/devtools"],
  typedRoutes: false,
  images: { unoptimized: true },
};

export default createMDX()(config);
