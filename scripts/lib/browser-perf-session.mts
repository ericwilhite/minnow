/**
 * The browser the lane drives: one persistent Chromium profile on a real disk, one fixed origin,
 * and a page per engine so each cell gets a fresh worker and its own renderer process.
 *
 * Validity rules this module enforces:
 *   - `launchPersistentContext` only. An ephemeral context keeps OPFS in memory.
 *   - The profile directory must not be on tmpfs/ramfs (checked before launch), and a probe file
 *     written through OPFS must grow the profile's directory on disk (checked after launch).
 *   - One origin, `http://127.0.0.1:<port>`, on a fixed port. OPFS is per origin, so a load and the
 *     queries that read it later must see the same one; a port already in use is an error, never
 *     a reason to pick another.
 */
import { build } from "esbuild";
import { createReadStream, existsSync, mkdirSync, readFileSync, statSync } from "node:fs";
import { createRequire } from "node:module";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium, type BrowserContext, type Page } from "@playwright/test";
import type { BrowserPerfApi } from "../browser-perf/page/app.ts";
import {
  assertOnRealDisk,
  diskBytes,
  rendererProcesses,
  type DiskCheck,
} from "./browser-perf-host.mts";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "../..");
const pageSources = path.join(repoRoot, "scripts/browser-perf/page");
const require = createRequire(path.join(repoRoot, "package.json"));

export const DEFAULT_PORT = 8791;
export const PORT_ENV = "MINNOW_BROWSER_PERF_PORT";

/**
 * A package's root directory. Not every package exports its own package.json, so the root is
 * found by walking up from any entry Node can resolve.
 */
function packageDirectory(name: string): string {
  let directory = path.dirname(require.resolve(name));
  for (;;) {
    const manifest = path.join(directory, "package.json");
    if (
      existsSync(manifest) &&
      (JSON.parse(readFileSync(manifest, "utf8")) as { name?: string }).name === name
    )
      return directory;
    const parent = path.dirname(directory);
    if (parent === directory) throw new Error(`Could not locate the ${name} package`);
    directory = parent;
  }
}

export function packageVersion(name: string): string {
  return (
    JSON.parse(readFileSync(path.join(packageDirectory(name), "package.json"), "utf8")) as {
      version: string;
    }
  ).version;
}

/** Bundles the page and its workers. Engines' own assets are served as files, never bundled. */
export async function buildPage(outDirectory: string): Promise<void> {
  const coreDist = path.join(repoRoot, "packages/core/dist/engine/client.js");
  if (!existsSync(coreDist))
    throw new Error("packages/core/dist is missing; run `npx tsc -b packages/core` first");
  mkdirSync(outDirectory, { recursive: true });
  await build({
    entryPoints: {
      app: path.join(pageSources, "app.ts"),
      "minnow-worker": path.join(pageSources, "minnow-worker.ts"),
      "sqlite-worker": path.join(pageSources, "sqlite-worker.ts"),
    },
    outdir: outDirectory,
    bundle: true,
    format: "esm",
    platform: "browser",
    target: "es2022",
    minify: false,
    sourcemap: false,
    logLevel: "warning",
  });
}

const INDEX_HTML =
  '<!doctype html><meta charset="utf-8"><title>browser perf</title><script type="module" src="/app.js"></script>';

function contentType(file: string): string {
  if (file.endsWith(".wasm")) return "application/wasm";
  if (/\.m?js$/.test(file)) return "text/javascript";
  if (file.endsWith(".html")) return "text/html";
  return "application/octet-stream";
}

/** Serves the bundle and the engines' shipped browser builds from one fixed origin. */
export async function startServer(buildDirectory: string, port: number): Promise<http.Server> {
  const roots: Record<string, string> = {
    "/vendor/duckdb/": path.join(packageDirectory("@duckdb/duckdb-wasm"), "dist"),
    "/vendor/sqlite/": path.join(packageDirectory("@sqlite.org/sqlite-wasm"), "dist"),
  };
  const server = http.createServer((request, response) => {
    const url = decodeURIComponent((request.url ?? "/").split("?")[0] ?? "/");
    if (url === "/" || url === "/index.html") {
      response.writeHead(200, { "content-type": "text/html", "cache-control": "no-store" });
      response.end(INDEX_HTML);
      return;
    }
    let file: string | undefined;
    for (const [prefix, root] of Object.entries(roots)) {
      if (url.startsWith(prefix)) file = path.join(root, url.slice(prefix.length));
    }
    file ??= path.join(buildDirectory, url);
    const allowed = [buildDirectory, ...Object.values(roots)].some((root) =>
      file.startsWith(root + path.sep),
    );
    if (!allowed || !existsSync(file) || statSync(file).isDirectory()) {
      response.writeHead(404);
      response.end();
      return;
    }
    response.writeHead(200, { "content-type": contentType(file), "cache-control": "no-store" });
    createReadStream(file).pipe(response);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", (error: NodeJS.ErrnoException) => {
      reject(
        error.code === "EADDRINUSE"
          ? new Error(
              `Port ${String(port)} is in use. OPFS is per origin, so the lane keeps one fixed port; free it or set ${PORT_ENV}.`,
            )
          : error,
      );
    });
    server.listen(port, "127.0.0.1", resolve);
  });
  return server;
}

export class PageCrashedError extends Error {
  override readonly name = "PageCrashedError";
}
export class DriverTimeoutError extends Error {
  override readonly name = "DriverTimeoutError";
}

type ApiName = keyof BrowserPerfApi;
type ApiInput<K extends ApiName> = Parameters<BrowserPerfApi[K]>[0];
type ApiOutput<K extends ApiName> = Awaited<ReturnType<BrowserPerfApi[K]>>;

/** A page holding one engine: its own renderer, so its memory and CPU are its own. */
export class LanePage {
  crashed = false;
  constructor(
    readonly page: Page,
    readonly rendererPid: number | undefined,
  ) {
    page.on("crash", () => {
      this.crashed = true;
    });
  }

  /** Calls `window.BrowserPerf[name](input)`. Rejects only on a crash or the driver deadline. */
  async call<K extends ApiName>(
    name: K,
    input: ApiInput<K>,
    timeoutMs: number,
  ): Promise<ApiOutput<K>> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expired = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        reject(new DriverTimeoutError(`${name} did not return within ${String(timeoutMs)} ms`));
      }, timeoutMs);
    });
    try {
      const evaluation = this.page.evaluate(
        async ([method, argument]) => {
          const api = window.BrowserPerf as unknown as Record<
            string,
            (input: unknown) => Promise<unknown>
          >;
          const fn = api[method];
          if (fn === undefined) throw new Error(`BrowserPerf has no ${method}`);
          return fn(argument);
        },
        [name, input] as const,
      );
      return (await Promise.race([evaluation, expired])) as ApiOutput<K>;
    } catch (error) {
      if (this.crashed) throw new PageCrashedError("the tab crashed");
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  async close(): Promise<void> {
    await this.page.close().catch(() => undefined);
  }
}

export interface LaneBrowserOptions {
  readonly laneDirectory: string;
  readonly port: number;
  readonly log: (line: string) => void;
}

export class LaneBrowser {
  readonly profileDirectory: string;
  readonly buildDirectory: string;
  readonly disk: DiskCheck;
  #context: BrowserContext | undefined;
  #server: http.Server | undefined;
  browserVersion = "";

  constructor(readonly options: LaneBrowserOptions) {
    this.profileDirectory = path.join(options.laneDirectory, "profile");
    this.buildDirectory = path.join(options.laneDirectory, "build");
    // Refuse before anything is created inside a memory filesystem.
    this.disk = assertOnRealDisk(options.laneDirectory);
  }

  get origin(): string {
    return `http://127.0.0.1:${String(this.options.port)}`;
  }

  /** OPFS lives here inside the profile; its size on disk is what "stored bytes" measures. */
  get opfsDirectory(): string {
    return path.join(this.profileDirectory, "Default", "File System");
  }

  async start(): Promise<void> {
    mkdirSync(this.profileDirectory, { recursive: true });
    await buildPage(this.buildDirectory);
    this.#server = await startServer(this.buildDirectory, this.options.port);
    this.#context = await chromium.launchPersistentContext(this.profileDirectory, {
      headless: true,
      // A spare renderer would make a fresh page's process ambiguous for /proc sampling.
      args: ["--disable-features=SpareRendererForSitePerProcess"],
    });
    this.browserVersion = this.#context.browser()?.version() ?? "";
    for (const page of this.#context.pages()) await page.close();
    await this.#proveOpfsOnDisk();
  }

  /** Writes 8 MiB through OPFS and requires the profile directory on disk to grow by most of it. */
  async #proveOpfsOnDisk(): Promise<void> {
    const page = await this.openPage();
    try {
      await page.call("opfsProbe", { bytes: 0, remove: true }, 30_000);
      const before = diskBytes(this.profileDirectory);
      await page.call("opfsProbe", { bytes: 8 << 20, remove: false }, 60_000);
      const grown = diskBytes(this.profileDirectory) - before;
      await page.call("opfsProbe", { bytes: 0, remove: true }, 30_000);
      if (grown < 4 << 20) {
        throw new Error(
          `An 8 MiB OPFS write grew the profile on disk by only ${String(grown)} bytes; OPFS is not on this disk, so the lane would measure memory.`,
        );
      }
      this.options.log(
        `OPFS verified on disk (${this.disk.filesystem}): an 8 MiB probe grew the profile by ${String(Math.round(grown / 1048576))} MiB`,
      );
    } finally {
      await page.close();
    }
  }

  /** A fresh page on the fixed origin, with its renderer identified for /proc sampling. */
  async openPage(): Promise<LanePage> {
    const context = this.#context;
    if (context === undefined) throw new Error("Browser is not started");
    const before = new Set(rendererProcesses(this.profileDirectory));
    const page = await context.newPage();
    page.on("console", (message) => {
      const text = message.text();
      if (/^(load |minnow-worker-error)/.test(text))
        this.options.log(`  [page] ${text.slice(0, 300)}`);
    });
    page.on("pageerror", (error) => {
      this.options.log(`  [pageerror] ${String(error).slice(0, 300)}`);
    });
    await page.goto(`${this.origin}/index.html`);
    await page.waitForFunction(() => window.browserPerfReady, null, { timeout: 60_000 });
    const after = rendererProcesses(this.profileDirectory);
    const fresh = after.filter((pid) => !before.has(pid));
    // A new page normally gets its own renderer. If none appeared, the page shares one and its
    // memory and CPU cannot be told apart from the others', so they are reported unavailable.
    const pid =
      fresh.length === 1
        ? fresh[0]
        : after.length === 1 && before.size === 0
          ? after[0]
          : undefined;
    return new LanePage(page, pid);
  }

  async stop(): Promise<void> {
    await this.#context?.close().catch(() => undefined);
    this.#context = undefined;
    await new Promise<void>((resolve) => {
      if (this.#server === undefined) resolve();
      else this.#server.close(() => resolve());
    });
    this.#server = undefined;
  }
}
