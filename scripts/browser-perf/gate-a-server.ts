/** Read-only bench image endpoint; immutable cache paths never come from a request parameter. */
import { createReadStream, statSync } from "node:fs";
import { join } from "node:path";
import type { Plugin } from "vite";
const NAMES = new Set(["manifest.json", "groups.json", "data", "log", "root-0", "root-1"]);
export function gateAFixtureServer(directory: string): Plugin {
  return {
    name: "gate-a-fixture",
    configureServer(server) {
      server.middlewares.use((request, response, next) => {
        const path = new URL(request.url ?? "/", "http://localhost").pathname;
        if (!path.startsWith("/__gate_a_fixture/")) return next();
        const name = path.slice("/__gate_a_fixture/".length);
        if (request.method !== "GET" || !NAMES.has(name)) {
          response.statusCode = 404;
          response.end();
          return;
        }
        const file = join(directory, name);
        try {
          response.setHeader("Content-Length", String(statSync(file).size));
          response.setHeader("Cache-Control", "no-store");
          const stream = createReadStream(file);
          stream.on("error", () => response.destroy());
          stream.pipe(response);
        } catch {
          response.statusCode = 404;
          response.end();
        }
      });
    },
  };
}
