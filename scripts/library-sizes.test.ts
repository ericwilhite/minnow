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
    // frame-parser measurement plus a little headroom (570,953 bytes). Against main f452eb82's
    // 569,970 bytes, the worker grows 983 gzip bytes and the client is byte-identical.
    // library-size-audit.mts attributes all 3,353 added raw bytes to shared engine/query.js
    // (+3,149), frame-offsets.js (+158) and query-identity.js (+46); no next/ or test input.
    // These implement signed/rounded frame parsing, bound validation and exact literal
    // identity, alongside typed compilation in the same R19 parser. Explicitly rebaseline
    // main's 556.75 KiB cap to 558 KiB: the old cap fails by 841 bytes; 439 bytes remain.
    // Native interval/series provenance: worker plus client is 571,449 gzip bytes.
    // Raise this combined cap once by 256 bytes; standalone client isolation is unchanged.
    expect(
      worker.totalGzipBytes + client.totalGzipBytes,
      "worker plus client gzip bytes",
    ).toBeLessThanOrEqual(558.25 * 1024);
  });
});
