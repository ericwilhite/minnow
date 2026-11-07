/** Test-only loader for the rejected source snapshot; never imports experimental production code. */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { build } from "esbuild";
import type { FramePayload, WireFlat } from "../../packages/core/src/next/exec/result.js";
import type { FlatVector } from "../../packages/core/src/next/vector/vector.js";
import type { RESULT_OWNED_LAYOUT_REACH } from "../../packages/core/src/testing/reach.js";
import { packedExperiment } from "./packed-experiment.js";

type Native =
  Uint8Array | Uint16Array | Uint32Array | Int32Array | Float32Array | Float64Array | BigInt64Array;
export interface PackedApi {
  FrameBufferError: typeof import("../../packages/core/src/next/exec/result.js").FrameBufferError;
  frameTransferList(payload: FramePayload): ArrayBuffer[];
  payloadArrays(payload: FramePayload): Native[];
  packedFrameBytes(payload: FramePayload): number;
  packFrame<T extends FramePayload>(
    payload: T,
    buffer: ArrayBuffer,
  ): T & { readonly buffer: ArrayBuffer };
  packedInput(
    kind: (typeof RESULT_OWNED_LAYOUT_REACH)[number],
    n: number,
    salt: number,
  ): FlatVector;
  StreamDictionaries: new () => {
    apply(payload: FramePayload): void;
    parts(id: number): readonly WireFlat[];
    size(id: number): number;
    locate(id: number, code: number): { readonly entries: WireFlat; readonly index: number };
  };
  transferFor(buffers: readonly ArrayBuffer[], packed: boolean): ArrayBuffer[];
  Governor: typeof import("../../packages/core/src/next/memory/governor.js").Governor;
  ResultChannel: typeof import("../../packages/core/src/next/exec/result.js").ResultChannel;
  ResultSink: typeof import("../../packages/core/src/next/exec/result-sink.js").ResultSink;
  close(): void;
}
export async function packedTestRuntime(): Promise<PackedApi> {
  const snapshot = packedExperiment(),
    scratch = mkdtempSync(join(tmpdir(), "minnow-packed-proof-"));
  const file = join(scratch, "candidate.mjs");
  try {
    await build({
      stdin: {
        contents: `
export {FrameBufferError,frameTransferList,payloadArrays} from "./packages/core/src/next/vector/frame.ts";
export {packFrame,packedFrameBytes} from "./packages/core/src/next/vector/pack-frame.ts";
export {StreamDictionaries,transferFor} from "./packages/core/src/next/owner/frame.ts";
export {Governor} from "./packages/core/src/next/memory/governor.ts";
export {ResultChannel} from "./packages/core/src/next/exec/result.ts";
export {ResultSink} from "./packages/core/src/next/exec/result-sink.ts";
export {packedInput} from "./packages/core/src/next/testing/packed-input.ts";
`,
        resolveDir: process.cwd(),
        loader: "ts",
      },
      bundle: true,
      platform: "node",
      format: "esm",
      outfile: file,
      plugins: [snapshot.esbuild],
    });
    const loaded = (await import(/* @vite-ignore */ pathToFileURL(file).href)) as Omit<
      PackedApi,
      "close"
    >;
    return {
      ...loaded,
      close() {
        snapshot.close();
        rmSync(scratch, { recursive: true, force: true });
      },
    };
  } catch (error) {
    snapshot.close();
    rmSync(scratch, { recursive: true, force: true });
    throw error;
  }
}
