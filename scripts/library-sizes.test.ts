import { describe, expect, it } from "vitest";
import { ENGINES, measure, type EngineSpec } from "./library-sizes.mjs";

function engine(id: string): EngineSpec {
  const spec = ENGINES.find((candidate) => candidate.id === id);
  if (spec === undefined) throw new Error(`Missing size comparison engine: ${id}`);
  return spec;
}

describe("compressed download budgets", () => {
  it("keeps the engine with its larger durable adapter below SQLite's loader and Wasm", async () => {
    const [minnow, sqlite] = await Promise.all([
      measure(engine("minnow")),
      measure(engine("sqlite")),
    ]);

    expect(sqlite.assets.map((asset) => asset.name)).toContain("sqlite3.wasm");
    expect(minnow.totalGzipBytes, "engine plus OPFS gzip bytes").toBeLessThan(
      sqlite.totalGzipBytes,
    );
  });

  it("bounds the generic worker with every store and its separate client", async () => {
    const base = engine("minnow");
    const [worker, client] = await Promise.all([
      measure({ ...base, entrySource: 'import "@minnowdb/core/worker";' }),
      measure({ ...base, entrySource: 'export * from "@minnowdb/core/client";' }),
    ]);

    // The client and worker run in separate realms. Count both downloads without sharing code;
    // disabling splitting also counts all dynamically imported adapters in the worker bundle.
    // A tight absolute budget, not a claim that this two-download setup is below SQLite: the
    // current measurement plus a little headroom (534.1 KiB measured; the history is in git).
    expect(
      worker.totalGzipBytes + client.totalGzipBytes,
      "worker plus client gzip bytes",
    ).toBeLessThanOrEqual(535 * 1024);
  });
});
