/** A client result must not make the reader download the executor or a storage implementation. */
import { build } from "esbuild";
import { expect, it } from "vitest";

it("keeps canonical Result decoding independent of owner execution and storage implementations", async () => {
  const result = await build({
    entryPoints: [new URL("./result.ts", import.meta.url).pathname],
    bundle: true,
    platform: "browser",
    format: "esm",
    write: false,
    metafile: true,
  });
  const inputs = Object.keys(result.metafile.inputs);
  expect(inputs.some((path) => path.endsWith("owner/result.ts"))).toBe(true);
  expect(
    inputs.filter((path) => /\/next\/(?:exec|write|page|lower|storage|memory|spill)\//u.test(path)),
  ).toEqual([]);
  expect(inputs.filter((path) => path.includes("/engine/"))).toEqual([]);
  expect(
    inputs.filter((path) => /owner\/(?:compose|server|host|connector|client)\.ts$/u.test(path)),
  ).toEqual([]);
});
