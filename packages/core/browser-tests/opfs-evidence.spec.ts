// eslint-disable-next-line no-restricted-imports -- Node-only Playwright evidence verifier; never shipped.
import { createHash } from "node:crypto";
// eslint-disable-next-line no-restricted-imports -- Node-only Playwright evidence verifier; never shipped.
import { readFile } from "node:fs/promises";
import { expect } from "@playwright/test";
import { captureOpfsEvidence } from "./opfs-evidence.js";
import { requireWorkerOpfs, test } from "./fixtures.js";

test("failure evidence copies native OPFS bytes while a sync handle remains open", async ({
  page,
  storageContext,
}, info) => {
  await page.goto("/packages/core/browser/opfs-evidence.html");
  await requireWorkerOpfs(page);
  const expected = Uint8Array.from(
    { length: 1024 * 1024 + 17 },
    (_, index) => (index * 37 + 19) & 0xff,
  );

  await page.evaluate(async (length) => {
    const source = `
        let access;
        self.onmessage = async (event) => {
          if (event.data.kind === "write") {
            const root = await navigator.storage.getDirectory();
            const directory = await root.getDirectoryHandle("minnow-evidence", { create: true });
            const file = await directory.getFileHandle("sentinel.bin", { create: true });
            access = await file.createSyncAccessHandle();
            access.truncate(0);
            const bytes = new Uint8Array(event.data.length);
            for (let index = 0; index < bytes.length; index += 1) {
              bytes[index] = (index * 37 + 19) & 0xff;
            }
            access.write(bytes, { at: 0 });
            access.flush();
            self.postMessage({ kind: "written", size: access.getSize() });
          } else if (event.data.kind === "probe") {
            self.postMessage({ kind: "open", size: access.getSize() });
          }
        };
      `;
    const url = URL.createObjectURL(new Blob([source], { type: "text/javascript" }));
    const worker = new Worker(url);
    const state = globalThis as typeof globalThis & {
      evidenceWorker?: Worker;
      evidenceWorkerUrl?: string;
    };
    state.evidenceWorker = worker;
    state.evidenceWorkerUrl = url;
    await new Promise<void>((resolve, reject) => {
      worker.addEventListener(
        "message",
        (event: MessageEvent<{ kind?: unknown; size?: unknown }>) => {
          if (event.data.kind !== "written") return;
          if (event.data.size !== length) {
            reject(new Error(`Worker wrote ${String(event.data.size)} bytes`));
            return;
          }
          resolve();
        },
        { once: true },
      );
      worker.addEventListener("error", (event) => reject(new Error(event.message)), {
        once: true,
      });
      worker.postMessage({ kind: "write", length });
    });
  }, expected.byteLength);

  const limited = await captureOpfsEvidence(storageContext, info, {
    outputName: "opfs-evidence-limited",
    maxBytes: 513,
  });
  expect(limited.manifest.complete).toBe(false);
  expect(limited.manifest.errors).toEqual([
    {
      operation: "limit",
      path: "minnow-evidence/sentinel.bin",
      message: "OPFS evidence byte limit 513 reached",
    },
  ]);
  const limitedSentinel = limited.manifest.files.find(
    (file) => file.path === "minnow-evidence/sentinel.bin",
  );
  expect(limitedSentinel).toMatchObject({
    size: expected.byteLength,
    capturedBytes: 513,
    capturedSha256: createHash("sha256").update(expected.subarray(0, 513)).digest("hex"),
    error: "OPFS evidence byte limit 513 reached",
  });
  if (limitedSentinel === undefined) throw new Error("Missing limited OPFS sentinel evidence");
  await expect(readFile(`${limited.directory}/${limitedSentinel.artifact}`)).resolves.toEqual(
    Buffer.from(expected.subarray(0, 513)),
  );

  const capture = await captureOpfsEvidence(storageContext, info);
  expect(capture.manifest.complete).toBe(true);
  expect(capture.manifest.errors).toEqual([]);
  const sentinel = capture.manifest.files.find(
    (file) => file.path === "minnow-evidence/sentinel.bin",
  );
  expect(sentinel).toBeDefined();
  if (sentinel === undefined) throw new Error("Missing OPFS sentinel evidence");
  expect(sentinel.capturedBytes).toBe(expected.byteLength);
  expect(sentinel.capturedSha256).toBe(createHash("sha256").update(expected).digest("hex"));

  const stillOpen = await page.evaluate(
    () =>
      new Promise<{ kind: string; size: number }>((resolve, reject) => {
        const worker = (globalThis as typeof globalThis & { evidenceWorker?: Worker })
          .evidenceWorker;
        if (worker === undefined) {
          reject(new Error("Missing OPFS evidence worker"));
          return;
        }
        worker.addEventListener(
          "message",
          (event: MessageEvent<{ kind: string; size: number }>) => resolve(event.data),
          { once: true },
        );
        worker.postMessage({ kind: "probe" });
      }),
  );
  expect(stillOpen).toEqual({ kind: "open", size: expected.byteLength });

  await storageContext.close();
  expect(JSON.parse(await readFile(capture.manifestPath, "utf8"))).toEqual(capture.manifest);
  await expect(readFile(`${capture.directory}/${sentinel.artifact}`)).resolves.toEqual(
    Buffer.from(expected),
  );
});
