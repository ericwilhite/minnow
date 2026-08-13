// Minimal static server for the built site, used by playwright.site.config.ts.
// `astro preview` daemonizes itself when stdout is not a TTY, which makes Playwright's
// webServer watchdog think the process died; this stays in the foreground.
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../dist/", import.meta.url));
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
      });
      response.end(body);
    } catch {
      try {
        // Astro's default route shape: /docs/sql → /docs/sql/index.html.
        const body = await readFile(join(filePath, "index.html"));
        response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
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
