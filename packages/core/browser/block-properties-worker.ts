import { blockFormatProperties } from "../src/block-format/property-campaign.js";

self.addEventListener("message", (event: MessageEvent<{ property: string; seed: number }>) => {
  const run = blockFormatProperties[event.data.property];
  if (run === undefined) throw new Error(`Unknown block property: ${event.data.property}`);
  void run(event.data.seed).then(
    () => self.postMessage({ passed: true }),
    (error: unknown) =>
      self.postMessage({ error: error instanceof Error ? error.stack : String(error) }),
  );
});
