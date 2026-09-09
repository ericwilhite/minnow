import { test as base, webkit, type BrowserContext } from "@playwright/test";

// Safari needs a persistent (non-private) context for OPFS worker storage.
export const test = base.extend<{ storageContext: BrowserContext }>({
  storageContext: async ({ browserName, context }, use, info) => {
    if (browserName !== "webkit") return use(context);
    const persistent = await webkit.launchPersistentContext(info.outputPath("storage-profile"), {
      baseURL: info.project.use.baseURL ?? "",
    });
    try {
      await use(persistent);
    } finally {
      await persistent.close();
    }
  },
});
