import {
  chromium,
  firefox,
  test as base,
  webkit,
  type BrowserContext,
  type Page,
} from "@playwright/test";
import { captureOpfsEvidence } from "./opfs-evidence.js";

function errorText(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

async function captureFailureEvidence(
  context: BrowserContext,
  info: Parameters<typeof captureOpfsEvidence>[1],
): Promise<void> {
  try {
    const capture = await captureOpfsEvidence(context, info, {
      outputName: "opfs-evidence-failure",
      timeoutMs: Math.min(20_000, Math.max(1, Math.floor(info.timeout / 2))),
    });
    await info
      .attach("opfs-evidence-manifest", {
        path: capture.manifestPath,
        contentType: "application/json",
      })
      .catch(() => undefined);
  } catch (error) {
    await info
      .attach("opfs-evidence-manifest", {
        body: Buffer.from(
          `${JSON.stringify(
            {
              version: 1,
              project: info.project.name,
              retry: info.retry,
              limits: {
                timeoutMs: Math.min(20_000, Math.max(1, Math.floor(info.timeout / 2))),
                maxFiles: 20_000,
                maxBytes: 512 * 1024 * 1024,
              },
              complete: false,
              files: [],
              errors: [{ operation: "enumerate", path: "", message: errorText(error) }],
            },
            null,
            2,
          )}\n`,
        ),
        contentType: "application/json",
      })
      .catch(() => undefined);
  }
}

// Storage coverage needs a persistent profile so every engine exercises its disk-backed path.
// Overriding `context` also makes Playwright's built-in `page` fixture use this profile.
export const test = base.extend<{ storageContext: BrowserContext }>({
  context: async ({ browserName }, use, info) => {
    const launcher = { chromium, firefox, webkit }[browserName];
    const profile = info.outputPath("storage-profile");
    const environment =
      browserName === "webkit" ? { ...process.env, CFFIXED_USER_HOME: profile } : undefined;
    const persistent = await launcher.launchPersistentContext(profile, {
      baseURL: info.project.use.baseURL ?? "",
      // Playwright's macOS WebKit embedder redirects IndexedDB and caches into userDataDir but
      // leaves OPFS in Cocoa's application-wide data directory. Isolate that default under this
      // test's retained profile as well.
      ...(environment === undefined ? {} : { env: environment }),
    });
    try {
      await use(persistent);
    } finally {
      // Read through the browser API before closing the failed context. This preserves WebKit's
      // raw OPFS bytes even when the browser's profile layout changes. Capture failures are
      // attached as incomplete metadata and never replace the test's original error.
      if (info.status !== info.expectedStatus) await captureFailureEvidence(persistent, info);
      await persistent.close();
    }
  },
  storageContext: async ({ context }, use) => use(context),
});

/** Probe the worker realm: Linux WebKit may expose OPFS in a page but not in its workers. */
export async function requireWorkerOpfs(page: Page): Promise<void> {
  const supported = await page.evaluate(
    () =>
      new Promise<boolean>((resolve, reject) => {
        const url = URL.createObjectURL(
          new Blob(['self.postMessage(typeof navigator.storage?.getDirectory === "function");'], {
            type: "text/javascript",
          }),
        );
        const worker = new Worker(url);
        const cleanup = () => {
          clearTimeout(timer);
          worker.terminate();
          URL.revokeObjectURL(url);
        };
        const timer = setTimeout(() => {
          cleanup();
          reject(new Error("OPFS capability worker did not answer"));
        }, 5_000);
        worker.addEventListener(
          "message",
          (event: MessageEvent<unknown>) => {
            cleanup();
            resolve(event.data === true);
          },
          { once: true },
        );
        worker.addEventListener(
          "error",
          (event) => {
            cleanup();
            reject(new Error(event.message));
          },
          { once: true },
        );
      }),
  );
  if (!supported && process.env.MINNOW_BROWSER_CONFORMANCE === "1") {
    throw new Error("Browser conformance requires OPFS inside dedicated workers");
  }
  test.skip(!supported, "This browser build exposes no OPFS in dedicated workers");
}
