let release: (() => void) | undefined;

globalThis.postMessage("ready");
globalThis.addEventListener(
  "message",
  (event: MessageEvent<{ type: "acquire"; lockName: string } | { type: "release" }>) => {
    if (event.data.type === "release") {
      release?.();
      return;
    }
    void navigator.locks
      .request(event.data.lockName, async () => {
        globalThis.postMessage("acquired");
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      })
      .then(
        () => globalThis.postMessage("released"),
        (error: unknown) => {
          globalThis.postMessage({
            error: error instanceof Error ? error.message : String(error),
          });
        },
      );
  },
);
