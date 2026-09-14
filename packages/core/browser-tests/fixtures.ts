import {
  chromium,
  firefox,
  test as base,
  webkit,
  type BrowserContext,
  type Page,
} from "@playwright/test";

// Storage coverage needs a persistent profile so every engine exercises its disk-backed path.
// Overriding `context` also makes Playwright's built-in `page` fixture use this profile.
export const test = base.extend<{ storageContext: BrowserContext }>({
  context: async ({ browserName }, use, info) => {
    const launcher = { chromium, firefox, webkit }[browserName];
    const persistent = await launcher.launchPersistentContext(info.outputPath("storage-profile"), {
      baseURL: info.project.use.baseURL ?? "",
    });
    try {
      await use(persistent);
    } finally {
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
