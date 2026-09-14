// eslint-disable-next-line no-restricted-imports -- Node-only Playwright evidence writer; never shipped.
import { createHash } from "node:crypto";
// eslint-disable-next-line no-restricted-imports -- Node-only Playwright evidence writer; never shipped.
import { mkdir, open, writeFile } from "node:fs/promises";
// eslint-disable-next-line no-restricted-imports -- Node-only Playwright evidence writer; never shipped.
import { dirname } from "node:path";
import type { BrowserContext, JSHandle, Page, TestInfo } from "@playwright/test";

const CAPTURE_CHUNK_BYTES = 1024 * 1024;
const DEFAULT_CAPTURE_TIMEOUT_MS = 20_000;
const DEFAULT_MAX_CAPTURE_FILES = 20_000;
const DEFAULT_MAX_CAPTURE_BYTES = 512 * 1024 * 1024;
const MAX_ENUMERATED_ENTRIES = 40_000;

interface OpfsCaptureError {
  operation: "enumerate" | "limit" | "open" | "read" | "timeout";
  path: string;
  message: string;
}

interface OpfsCapturedFile {
  path: string;
  artifact: string;
  size: number;
  capturedBytes: number;
  capturedSha256?: string;
  error?: string;
}

export interface OpfsEvidenceManifest {
  version: 1;
  origin?: string;
  project: string;
  retry: number;
  limits: {
    timeoutMs: number;
    maxFiles: number;
    maxBytes: number;
  };
  complete: boolean;
  files: OpfsCapturedFile[];
  errors: OpfsCaptureError[];
}

export interface OpfsEvidenceCapture {
  directory: string;
  manifestPath: string;
  manifest: OpfsEvidenceManifest;
}

export interface OpfsEvidenceOptions {
  outputName?: string;
  timeoutMs?: number;
  maxFiles?: number;
  maxBytes?: number;
}

class OpfsEvidenceTimeoutError extends Error {
  override readonly name = "OpfsEvidenceTimeoutError";
}

function errorText(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

async function listOpfsFiles(
  page: Page,
  maxFiles: number,
): Promise<{
  paths: string[][];
  errors: OpfsCaptureError[];
}> {
  return page.evaluate(
    async ({ maxFiles, maxEntries }) => {
      const paths: string[][] = [];
      const errors: OpfsCaptureError[] = [];
      let entriesSeen = 0;
      const describe = (error: unknown): string =>
        error instanceof Error ? `${error.name}: ${error.message}` : String(error);
      const visit = async (
        directory: FileSystemDirectoryHandle,
        parent: string[],
      ): Promise<void> => {
        try {
          const entries = directory as FileSystemDirectoryHandle & {
            entries(): AsyncIterableIterator<[string, FileSystemHandle]>;
          };
          for await (const [name, handle] of entries.entries()) {
            entriesSeen += 1;
            if (entriesSeen > maxEntries) {
              errors.push({
                operation: "limit",
                path: parent.join("/"),
                message: `OPFS evidence entry limit ${String(maxEntries)} reached`,
              });
              return;
            }
            const path = [...parent, name];
            if (handle.kind === "file") {
              if (paths.length >= maxFiles) {
                errors.push({
                  operation: "limit",
                  path: path.join("/"),
                  message: `OPFS evidence file limit ${String(maxFiles)} reached`,
                });
                return;
              }
              paths.push(path);
            } else {
              await visit(handle as FileSystemDirectoryHandle, path);
            }
            if (errors.some((error) => error.operation === "limit")) return;
          }
        } catch (error) {
          errors.push({ operation: "enumerate", path: parent.join("/"), message: describe(error) });
        }
      };

      try {
        await visit(await navigator.storage.getDirectory(), []);
      } catch (error) {
        errors.push({ operation: "enumerate", path: "", message: describe(error) });
      }
      return { paths, errors };
    },
    { maxFiles, maxEntries: MAX_ENUMERATED_ENTRIES },
  );
}

async function snapshotFile(page: Page, path: string[]): Promise<JSHandle<File>> {
  return page.evaluateHandle<File, string[]>(async (segments) => {
    let directory = await navigator.storage.getDirectory();
    for (const segment of segments.slice(0, -1)) {
      directory = await directory.getDirectoryHandle(segment);
    }
    const name = segments.at(-1);
    if (name === undefined) throw new Error("OPFS evidence file path is empty");
    return (await directory.getFileHandle(name)).getFile();
  }, path);
}

async function beforeDeadline<T>(start: () => Promise<T>, deadlineAt: number): Promise<T> {
  const remaining = deadlineAt - performance.now();
  if (remaining <= 0) throw new OpfsEvidenceTimeoutError("OPFS evidence deadline expired");
  const promise = start();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new OpfsEvidenceTimeoutError("OPFS evidence deadline expired")),
          remaining,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function positiveLimit(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
  return value;
}

/** Copies the current origin's OPFS bytes without opening Minnow or changing any OPFS entry. */
export async function captureOpfsEvidence(
  context: BrowserContext,
  info: TestInfo,
  options: OpfsEvidenceOptions = {},
): Promise<OpfsEvidenceCapture> {
  const outputName = options.outputName ?? "opfs-evidence";
  if (!/^opfs-evidence(?:-[a-z0-9-]+)?$/u.test(outputName)) {
    throw new TypeError("OPFS evidence output name is invalid");
  }
  const timeoutMs = positiveLimit(
    options.timeoutMs ?? DEFAULT_CAPTURE_TIMEOUT_MS,
    "OPFS evidence timeout",
  );
  const maxFiles = positiveLimit(
    options.maxFiles ?? DEFAULT_MAX_CAPTURE_FILES,
    "OPFS evidence file limit",
  );
  const maxBytes = positiveLimit(
    options.maxBytes ?? DEFAULT_MAX_CAPTURE_BYTES,
    "OPFS evidence byte limit",
  );
  const deadlineAt = performance.now() + timeoutMs;
  const directory = info.outputPath(outputName);
  const manifestPath = `${directory}/manifest.json`;
  const manifest: OpfsEvidenceManifest = {
    version: 1,
    project: info.project.name,
    retry: info.retry,
    limits: { timeoutMs, maxFiles, maxBytes },
    complete: false,
    files: [],
    errors: [],
  };
  await mkdir(`${directory}/files`, { recursive: true });
  let page: Page | undefined;

  try {
    page = await beforeDeadline(() => context.newPage(), deadlineAt);
    const evidencePage = page;
    await beforeDeadline(async () => {
      await evidencePage.goto("/packages/core/browser/opfs-evidence.html", {
        waitUntil: "domcontentloaded",
        timeout: Math.max(1, deadlineAt - performance.now()),
      });
    }, deadlineAt);
    manifest.origin = await beforeDeadline(
      () => evidencePage.evaluate(() => location.origin),
      deadlineAt,
    );
    const listing = await beforeDeadline(() => listOpfsFiles(evidencePage, maxFiles), deadlineAt);
    listing.paths.sort((left, right) => left.join("/").localeCompare(right.join("/")));
    manifest.errors.push(...listing.errors);
    let totalCapturedBytes = 0;

    for (const [index, path] of listing.paths.entries()) {
      const displayPath = path.join("/");
      const artifact = `files/${String(index).padStart(6, "0")}.bin`;
      const target = `${directory}/${artifact}`;
      const captured: OpfsCapturedFile = {
        path: displayPath,
        artifact,
        size: 0,
        capturedBytes: 0,
      };
      manifest.files.push(captured);
      let snapshot: JSHandle<File> | undefined;
      let hash: ReturnType<typeof createHash> | undefined;
      let timedOut = false;
      try {
        snapshot = await beforeDeadline(() => snapshotFile(evidencePage, path), deadlineAt);
        const fileSnapshot = snapshot;
        captured.size = await beforeDeadline(
          () => fileSnapshot.evaluate((file: File) => file.size),
          deadlineAt,
        );
        const captureSize = Math.min(captured.size, maxBytes - totalCapturedBytes);
        await mkdir(dirname(target), { recursive: true });
        const output = await open(target, "wx");
        hash = createHash("sha256");
        try {
          while (captured.capturedBytes < captureSize) {
            const offset = captured.capturedBytes;
            const length = Math.min(CAPTURE_CHUNK_BYTES, captureSize - offset);
            const encoded = await beforeDeadline(
              () =>
                fileSnapshot.evaluate(
                  async (file: File, range: { offset: number; length: number }) => {
                    const bytes = new Uint8Array(
                      await file.slice(range.offset, range.offset + range.length).arrayBuffer(),
                    );
                    let binary = "";
                    for (let start = 0; start < bytes.length; start += 0x8000) {
                      binary += String.fromCharCode(...bytes.subarray(start, start + 0x8000));
                    }
                    return btoa(binary);
                  },
                  { offset, length },
                ),
              deadlineAt,
            );
            const bytes = Buffer.from(encoded, "base64");
            if (bytes.byteLength !== length) {
              throw new Error(
                `Expected ${String(length)} bytes at offset ${String(offset)}, got ${String(bytes.byteLength)}`,
              );
            }
            let written = 0;
            while (written < bytes.byteLength) {
              const result = await output.write(
                bytes,
                written,
                bytes.byteLength - written,
                offset + written,
              );
              if (result.bytesWritten === 0)
                throw new Error("OPFS evidence output stopped writing");
              hash.update(bytes.subarray(written, written + result.bytesWritten));
              written += result.bytesWritten;
              captured.capturedBytes += result.bytesWritten;
              totalCapturedBytes += result.bytesWritten;
            }
          }
          captured.capturedSha256 = hash.digest("hex");
          hash = undefined;
        } finally {
          await output.close();
        }
        if (captured.capturedBytes !== captured.size) {
          const message = `OPFS evidence byte limit ${String(maxBytes)} reached`;
          captured.error = message;
          manifest.errors.push({ operation: "limit", path: displayPath, message });
        }
      } catch (error) {
        const message = errorText(error);
        timedOut = error instanceof OpfsEvidenceTimeoutError;
        captured.error = message;
        manifest.errors.push({
          operation: timedOut ? "timeout" : captured.size === 0 ? "open" : "read",
          path: displayPath,
          message,
        });
      } finally {
        if (hash !== undefined) captured.capturedSha256 = hash.digest("hex");
        if (snapshot !== undefined) {
          const disposable = snapshot;
          await beforeDeadline(() => disposable.dispose(), deadlineAt).catch(() => undefined);
        }
      }
      if (timedOut) break;
    }
  } catch (error) {
    manifest.errors.push({
      operation: error instanceof OpfsEvidenceTimeoutError ? "timeout" : "enumerate",
      path: "",
      message: errorText(error),
    });
  } finally {
    if (page !== undefined) {
      const closingPage = page;
      await beforeDeadline(() => closingPage.close(), performance.now() + 2_000).catch(
        () => undefined,
      );
    }
    manifest.complete =
      manifest.errors.length === 0 &&
      manifest.files.every((file) => file.capturedBytes === file.size && file.error === undefined);
    await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  }

  return { directory, manifestPath, manifest };
}
