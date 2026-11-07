/** Execution-owned parameter storage is charged before materialization, and dies with execution. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import { bindParameter } from "./parameters.js";
import { Governor } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";

it.each(seedsFor("next-executor-parameter-charge", [0xc801, 0xc802]))(
  "charges text, bytes and numeric binding storage (seed %i)",
  async (seed) => {
    const reach = createReach("parameter charge", ["text", "bytes", "decimal", "utf8"]);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("text", "bytes", "decimal"),
        fc.integer({ min: 1, max: 2048 }),
        fc.boolean(),
        async (kind, size, unicode) => {
          reach.beginHistory();
          const type: PhysicalType =
            kind === "decimal"
              ? { kind: "decimal", precision: null, scale: null }
              : kind === "bytes"
                ? { kind: "bytes" }
                : { kind: "text" };
          const value =
            kind === "bytes"
              ? new Uint8Array(size).fill(17)
              : kind === "decimal"
                ? "12345678901234567890.125"
                : (unicode ? "a🦦é" : "x").repeat(size);
          const governor = new Governor(8 << 20);
          const grant = await governor.admit({
            label: "parameter",
            minimumBytes: 128 * 1024,
            spillable: false,
          });
          try {
            const vector = bindParameter(type, value, 0, grant);
            const minimum =
              vector.data.layout === "varlen"
                ? vector.data.bytes.byteLength + vector.data.offsets.byteLength
                : 32;
            expect(grant.allocated).toBeGreaterThanOrEqual(minimum);
            reach.reach(kind);
            if (kind === "text" && unicode) reach.reach("utf8");
          } finally {
            grant.close();
            expect(governor.usedBytes).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 30,
        examples: [
          ["text", 100, true],
          ["bytes", 100, false],
          ["decimal", 1, false],
        ],
      },
    );
    reach.assertReached();
  },
);
