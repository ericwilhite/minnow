/**
 * Pins the Kysely adapter's browser cost. In particular, the live wrapper must inspect Kysely's
 * final operation tree rather than retaining Minnow's full SQL parser merely to recognize a
 * top-level ORDER BY.
 *
 * These probes bundle `dist/`, just as an application does. The repository build runs before the
 * unit suite in every checked CI job, so declarations, exports, and emitted JavaScript are tested
 * together.
 */
import { join } from "node:path";
import { constants, gzipSync } from "node:zlib";
import { build, type Metafile } from "esbuild";
import { describe, expect, it } from "vitest";

const LIVE_RAW_BUDGET = 16 * 1024;
const LIVE_GZIP_BUDGET = 6 * 1024;
const COMPLETE_RAW_BUDGET = 184 * 1024;
const COMPLETE_GZIP_BUDGET = 40 * 1024;
const repoRoot = join(import.meta.dirname, "..", "..", "..");

interface BundleMeasurement {
  readonly rawBytes: number;
  readonly gzipBytes: number;
  readonly metafile: Metafile;
}

async function measure(contents: string): Promise<BundleMeasurement> {
  const result = await build({
    stdin: { contents, resolveDir: repoRoot },
    bundle: true,
    write: false,
    minify: true,
    format: "esm",
    platform: "browser",
    target: "es2022",
    define: { "process.env.NODE_ENV": '"production"' },
    metafile: true,
    logLevel: "silent",
  });
  const output = result.outputFiles[0];
  if (output === undefined) throw new Error("esbuild produced no Kysely adapter output");
  return {
    rawBytes: output.contents.byteLength,
    gzipBytes: gzipSync(output.contents, { level: constants.Z_BEST_COMPRESSION }).byteLength,
    metafile: result.metafile,
  };
}

function emittedInputBytes(metafile: Metafile, suffix: string): number {
  return Object.values(metafile.outputs).reduce(
    (total, output) =>
      total +
      Object.entries(output.inputs).reduce(
        (subtotal, [path, input]) => subtotal + (path.endsWith(suffix) ? input.bytesInOutput : 0),
        0,
      ),
    0,
  );
}

describe("Kysely adapter packaging", () => {
  it("keeps the named live wrapper small and leaves the core SQL parser out", async () => {
    const measurement = await measure(
      'export { createKyselyLiveQueries } from "@minnowdb/kysely";',
    );

    expect(
      emittedInputBytes(measurement.metafile, "/core/dist/engine/query.js"),
      "core SQL parser bytes in named live wrapper",
    ).toBe(0);
    expect(measurement.rawBytes, "named live wrapper raw bytes").toBeLessThanOrEqual(
      LIVE_RAW_BUDGET,
    );
    expect(measurement.gzipBytes, "named live wrapper gzip bytes").toBeLessThanOrEqual(
      LIVE_GZIP_BUDGET,
    );
  });

  it("keeps the complete adapter below its raw and download budgets", async () => {
    const measurement = await measure('export * from "@minnowdb/kysely";');

    expect(measurement.rawBytes, "complete Kysely adapter raw bytes").toBeLessThanOrEqual(
      COMPLETE_RAW_BUDGET,
    );
    expect(measurement.gzipBytes, "complete Kysely adapter gzip bytes").toBeLessThanOrEqual(
      COMPLETE_GZIP_BUDGET,
    );
  });
});
