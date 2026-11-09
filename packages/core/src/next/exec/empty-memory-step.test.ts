/** An empty memory tail has no read step and must not allocate full-width chunk staging. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach } from "../../testing/reach.js";
import type { ColumnDef } from "../access/catalog.js";
import type { MemorySegment, TableSnapshot } from "../access/snapshot.js";
import { Governor } from "../memory/governor.js";
import { OutputArena } from "./kernels/vectors.js";
import { TableScan } from "./table-scan.js";

it.each(seedsFor("next-empty-memory-step", [0xc421, 0xc422]))(
  "skips empty decoder/staging for generated widths (seed %i)",
  async (seed) => {
    const reach = createReach("empty memory step", [
      "narrow",
      "wide",
      "no-read",
      "bounded",
      "released",
    ]);
    await fc.assert(
      fc.asyncProperty(fc.constantFrom(1, 64, 256, 512), async (width) => {
        reach.beginHistory();
        const gov = new Governor(8 << 20),
          grant = await gov.admit({ label: "empty scan", minimumBytes: 65536, spillable: false });
        const columns: ColumnDef[] = Array.from({ length: width }, (_, id) => ({
          id,
          name: `c${String(id)}`,
          type: { kind: "int32", bits: 32 },
          declared: { type: "number", declaredTypeName: "INTEGER" },
          nullable: true,
          defaultSql: null,
          generatedSql: null,
          identitySequence: null,
          missing: null,
        }));
        let touched = false;
        const unexpected = () => {
          touched = true;
          throw new Error("empty memory decoder was opened");
        };
        const segment: MemorySegment = {
          kind: "memory",
          index: 0,
          rowCount: 0,
          liveRowCount: 0,
          deletions: unexpected,
          statistics: () => null,
          pages: () => [],
          chunk: unexpected,
          readHint: unexpected,
        };
        const table: TableSnapshot = {
          tableId: 1,
          schemaVersion: 1,
          columns,
          liveRowCount: 0,
          segments: [segment],
        };
        const arena = new OutputArena(grant, 2048),
          scan = new TableScan({
            table,
            pages: { peek: () => null, read: unexpected },
            reads: { open: unexpected },
            budget: grant,
            columns: columns.map((c) => c.id),
            filter: null,
            emitLocator: false,
            targetBytes: 4096,
            context: { arena, params: { slots: [] }, input: [] },
          });
        try {
          expect(await scan.next()).toBeNull();
          expect(touched).toBe(false);
          reach.reach("no-read");
          expect(grant.allocated).toBe(0);
          expect(gov.peakBytes).toBe(65536);
          reach.reach("bounded");
          reach.reach(width === 1 ? "narrow" : "wide");
        } finally {
          scan.close();
          arena.close();
          grant.close();
          expect(gov.usedBytes).toBe(0);
          reach.reach("released");
        }
      }),
      { seed, numRuns: 16, examples: [[1], [512]] },
    );
    reach.assertReached();
  },
);
