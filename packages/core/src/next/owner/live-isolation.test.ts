/** Page live helpers share native value contracts without importing native engine state. */
import { build } from "esbuild";
import { expect, it } from "vitest";

it("bundles page live decoding, reconciliation and diagnostics without native writers or governor", async () => {
  const result = await build({
    stdin: {
      sourcefile: "page-live-entry.ts",
      resolveDir: import.meta.dirname,
      contents: `
        export { decodeLiveColumn } from "./live-column.js";
        export { LiveRows } from "../live/rows.js";
        export { NativeLiveState } from "../live/state.js";
        export { prepareLiveError } from "../live/errors.js";
        export { MemoryAdmissionError } from "../memory-values.js";
      `,
    },
    bundle: true,
    platform: "browser",
    format: "esm",
    write: false,
    metafile: true,
  });
  const inputs = Object.keys(result.metafile.inputs);
  expect(inputs.some((name) => name.endsWith("/vector/column-codec.ts"))).toBe(true);
  expect(inputs.some((name) => name.endsWith("/memory-values.ts"))).toBe(true);
  expect(
    inputs.filter((name) => /\/next\/(?:exec|write|page|lower|storage|memory|spill)\//u.test(name)),
  ).toEqual([]);
  expect(
    inputs.filter((name) => /\/engine\/|\/owner\/(?:server|compose|host|native)\.ts$/u.test(name)),
  ).toEqual([]);
});
