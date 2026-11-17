/** The whole thin-client entry includes RPC and decoding, and no native execution/store. */
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { expect, it } from "vitest";
it("bundles facade, protocol and canonical Result without an owner implementation", async () => {
  const bundled = await build({
    stdin: {
      sourcefile: "native-client-entry.ts",
      resolveDir: fileURLToPath(new URL(".", import.meta.url)),
      contents:
        'export { MinnowDatabaseClient } from "./database-client.js"; export { ProtocolClient } from "./client.js";',
    },
    bundle: true,
    platform: "browser",
    format: "esm",
    write: false,
    metafile: true,
  });
  const outputs = Object.values(bundled.metafile.outputs);
  expect(outputs).toHaveLength(1);
  const output = outputs[0];
  if (output === undefined) throw new Error("No client bundle");
  expect(output.exports).toEqual(["MinnowDatabaseClient", "ProtocolClient"]);
  const used = Object.entries(output.inputs)
    .filter(([, info]) => info.bytesInOutput > 0)
    .map(([path]) => path);
  for (const name of ["client", "database-client", "result"])
    expect(used.some((path) => path.endsWith(`/owner/${name}.ts`))).toBe(true);
  expect(
    used.filter(
      (path) =>
        /\/next\/(?:exec|write|page|lower|storage|memory|spill)\//u.test(path) &&
        !path.endsWith("/exec/frame-buffers.ts"),
    ),
  ).toEqual([]);
  expect(
    used.filter((path) =>
      /\/engine\/|\/owner\/(?:server|compose|host|native-boot)\.ts$/u.test(path),
    ),
  ).toEqual([]);
});
