// Minimal static server for the exported site, used by playwright.site.config.ts.
//
// It exists for two reasons. It stays in the foreground, where a framework preview command may
// daemonize itself and make Playwright's webServer watchdog think the process died. And it sets
// the cross-origin isolation headers that Cloudflare Pages applies from public/_headers, so the
// benchmarks page is tested on the same isolated code path it ships on — SQLite reaches its OPFS
// VFS rather than silently falling back to the handle pool.
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../out/", import.meta.url));
const port = Number(process.argv[2] ?? "4185");

const contentTypes = new Map([
  [".html", "text/html; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".mjs", "text/javascript; charset=utf-8"],
  [".css", "text/css; charset=utf-8"],
  [".json", "application/json"],
  [".svg", "image/svg+xml"],
  [".png", "image/png"],
  [".ico", "image/x-icon"],
  [".wasm", "application/wasm"],
  [".woff2", "font/woff2"],
  [".txt", "text/plain; charset=utf-8"],
]);

/**
 * Mirrors public/_headers.
 *
 * Isolation is scoped to the benchmarks document, not the whole site. The build's own assets
 * need CORP and COEP too, though: a module worker spawned from an isolated document is a new
 * execution context that has to opt in, and Chrome blocks its script outright
 * (ERR_BLOCKED_BY_RESPONSE) when the response carries neither. That covers /_next/ rather than
 * everything, so ordinary pages keep serving assets under the default policy.
 */
function isolationHeaders(pathname) {
  if (pathname.startsWith("/_next/") || pathname.startsWith("/vendor/")) {
    return {
      "cross-origin-resource-policy": "same-origin",
      "cross-origin-embedder-policy": "require-corp",
    };
  }
  if (!pathname.startsWith("/benchmarks")) return {};
  return {
    "cross-origin-opener-policy": "same-origin",
    "cross-origin-embedder-policy": "require-corp",
    // WebKit fails a reloaded page's module-worker load under COEP when the script revalidates
    // from cache; bypassing the cache avoids it.
    "cache-control": "no-store",
  };
}

const server = createServer((request, response) => {
  void (async () => {
    const url = new URL(request.url ?? "/", "http://localhost");
    let pathname = decodeURIComponent(url.pathname);
    if (pathname.endsWith("/")) pathname += "index.html";
    const filePath = normalize(join(root, pathname));
    if (!filePath.startsWith(root)) {
      response.writeHead(403).end();
      return;
    }
    try {
      const body = await readFile(filePath);
      response.writeHead(200, {
        "content-type": contentTypes.get(extname(filePath)) ?? "application/octet-stream",
        ...isolationHeaders(pathname),
      });
      response.end(body);
    } catch {
      try {
        // The export's route shape: /docs/sql → /docs/sql/index.html.
        const body = await readFile(join(filePath, "index.html"));
        response.writeHead(200, {
          "content-type": "text/html; charset=utf-8",
          ...isolationHeaders(pathname),
        });
        response.end(body);
      } catch {
        response.writeHead(404, { "content-type": "text/plain" }).end("not found");
      }
    }
  })();
});

server.listen(port, "127.0.0.1", () => {
  console.log(`serving ${root} at http://127.0.0.1:${String(port)}/`);
});
