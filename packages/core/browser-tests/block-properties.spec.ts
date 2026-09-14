import { expect, test } from "@playwright/test";
import { blockFormatProperties } from "../src/block-format/property-campaign.js";
import { seedsFor } from "../src/testing/seeds.js";

for (const seed of seedsFor("block-format-properties", [0xb10c])) {
  for (const property of Object.keys(blockFormatProperties)) {
    test(`native block format: ${property} (seed ${String(seed)})`, async ({ page }, info) => {
      test.setTimeout(120_000);
      const errors: string[] = [];
      page.on("pageerror", (error) => errors.push(error.message));
      await page.goto("/packages/core/browser/");
      await info.attach("property-replay.json", {
        contentType: "application/json",
        body: JSON.stringify({
          property,
          seed,
          browser: info.project.name,
          replay: `MINNOW_SEED=${String(seed)} npm run test:browser:library -- block-properties.spec.ts --project=${info.project.name}`,
        }),
      });
      const result = await page.evaluate(
        (input) =>
          new Promise<{ passed?: boolean; error?: string }>((resolve, reject) => {
            const worker = new Worker("/packages/core/browser/block-properties-worker.ts", {
              type: "module",
            });
            worker.onmessage = (event: MessageEvent<{ passed?: boolean; error?: string }>) => {
              worker.terminate();
              resolve(event.data);
            };
            worker.onerror = (event) => {
              worker.terminate();
              reject(new Error(event.message));
            };
            worker.postMessage(input);
          }),
        { property, seed },
      );
      expect(result.error).toBeUndefined();
      expect(result.passed).toBe(true);
      expect(errors).toEqual([]);
    });
  }
}
