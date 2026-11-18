/** Internal cutover entries must stay store-specific and side-effect-free. */
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { expect, it } from "vitest";

for (const store of ["memory", "indexeddb", "opfs", "auto"])
  for (const worker of [false, true])
    it(`bundles ${store} worker=${String(worker)} without unrelated stores`, async () => {
      const bundled = await build({
        entryPoints: [
          fileURLToPath(
            new URL(`./entries/${worker ? "worker" : "storage"}/${store}.ts`, import.meta.url),
          ),
        ],
        bundle: true,
        platform: "browser",
        format: "esm",
        write: false,
        metafile: true,
      });
      const used = Object.entries(Object.values(bundled.metafile.outputs)[0]?.inputs ?? {})
        .filter(([, info]) => info.bytesInOutput > 0)
        .map(([path]) => path);
      for (const other of ["memory", "indexeddb", "opfs"])
        if (store !== "auto" && other !== store)
          expect(
            used.filter((path) =>
              path.endsWith(`/storage/${other === "memory" ? "memory-store" : other}.ts`),
            ),
          ).toEqual([]);
      expect(
        used.filter((path) =>
          /\/engine\/(?:database|client|worker|worker-server)\.ts$/u.test(path),
        ),
      ).toEqual([]);
      if (!worker)
        expect(
          used.filter(
            (path) =>
              /\/next\/(?:exec|write|lower|owner)\//u.test(path) && !path.includes("/entries/"),
          ),
        ).toEqual([]);
      else expect(used.some((path) => path.endsWith("/owner/native-boot.ts"))).toBe(true);
      // Importing any ordinary entry may not install a worker handler.
      expect(bundled.outputFiles[0]?.text).not.toMatch(/self\.onmessage\s*=/u);
    });

it("bundles the worker connector without execution or a storage backend", async () => {
  const bundled = await build({
    entryPoints: [fileURLToPath(new URL("./entries/client.ts", import.meta.url))],
    bundle: true,
    platform: "browser",
    format: "esm",
    write: false,
    metafile: true,
  });
  const output = Object.values(bundled.metafile.outputs)[0];
  expect(output?.exports).toContain("connectNativeWorkerClient");
  const used = Object.entries(output?.inputs ?? {})
    .filter(([, info]) => info.bytesInOutput > 0)
    .map(([path]) => path);
  expect(
    used.filter((path) =>
      /\/engine\/|\/next\/(?:write|lower|storage|memory|spill|page)\//u.test(path),
    ),
  ).toEqual([]);
});
