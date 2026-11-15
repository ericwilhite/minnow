/** Portable catalog scope is complete native MCAT plus one exact restart horizon per sequence. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_CATALOG_SCOPE_REACH } from "../../testing/reach.js";
import type { ByteBudget } from "../storage/extents.js";
import { SnapshotKind, SnapshotStreamReader } from "../format/snapshot.js";
import { Catalog, encodeLogged, decodeLogged } from "./catalog.js";
import { exportSnapshot } from "./snapshot-capture.js";
import {
  decodeSnapshotCatalog,
  snapshotCatalogParts,
  type SnapshotHorizon,
} from "./snapshot-catalog.js";
import { column, tableInput, Harness, autocommit } from "./test-support.js";
import { vectorOf } from "../page/test-support.js";
class Budget implements ByteBudget {
  used = 0;
  allocate(n: number) {
    this.used += n;
    return true;
  }
  free(n: number) {
    this.used -= n;
    if (this.used < 0) throw new Error("budget underflow");
  }
}
function encoded(
  catalog: Catalog,
  horizons: readonly SnapshotHorizon[],
  budget: Budget,
): Uint8Array {
  const parts: Uint8Array[] = [];
  for (const bytes of snapshotCatalogParts(catalog, horizons, budget)) parts.push(bytes.slice());
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let at = 0;
  for (const bytes of parts) {
    out.set(bytes, at);
    at += bytes.length;
  }
  return out;
}
for (const seed of seedsFor("next-snapshot-catalog-scope", [0xb287, 0xb288]))
  it(`roundtrips native catalog lanes and rejects noncanonical scope seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot catalog scope", SNAPSHOT_CATALOG_SCOPE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 65, max: 512 }),
        fc.integer({ min: 1, max: 100 }),
        async (bits, salt) => {
          reach.beginHistory();
          const budget = new Budget();
          let catalog = Catalog.empty();
          const bound = 1n << BigInt(bits),
            horizon = bound - BigInt(salt);
          for (const increment of [1n, -1n])
            catalog = catalog.change(
              {
                kind: "createSequence",
                sequence: {
                  name: increment > 0n ? "up" : "down",
                  start: 0n,
                  increment,
                  min: -bound,
                  max: bound,
                  cycle: false,
                  ownedBy: null,
                },
              },
              catalog.version + 1,
            ).catalog;
          const missing = vectorOf({ kind: "float64" }, [0]);
          if (missing.data.layout !== "f64") throw new Error("fixture");
          new DataView(missing.data.values.buffer).setBigUint64(0, 0x7ff8000000000042n, true);
          const changed = catalog.change(
            {
              kind: "createTable",
              table: tableInput(
                "scope",
                [
                  { ...column("n", { kind: "float64" }), missing },
                  {
                    ...column("b", { kind: "bytes" }),
                    missing: vectorOf({ kind: "bytes" }, [new Uint8Array([0, 255, salt])]),
                  },
                ],
                { primaryKey: null },
              ),
            },
            catalog.version + 1,
          );
          catalog = changed.catalog;
          for (const logged of changed.logged)
            expect(encodeLogged(decodeLogged(encodeLogged(logged)))).toEqual(encodeLogged(logged));
          const horizons = [
              { sequenceId: 1, reservedThrough: horizon },
              { sequenceId: 2, reservedThrough: -horizon },
            ],
            bytes = encoded(catalog, horizons, budget);
          expect(budget.used).toBe(0);
          const decoded = decodeSnapshotCatalog(bytes, budget);
          try {
            expect(decoded.value.catalog.encode()).toEqual(catalog.encode());
            expect(decoded.value.horizons).toEqual(horizons);
          } finally {
            budget.free(decoded.charge);
          }
          for (const state of ["bigint horizons", "typed missing", "logged typed lanes"] as const)
            reach.reach(state);
          const length = new DataView(bytes.buffer).getUint32(8, true),
            record = 24 + length;
          for (const mutation of [
            "strict horizon count",
            "strict horizon id",
            "canonical decimal",
            "sequence reserved",
            "catalog reserved",
            "unknown version",
            "trailing bytes",
          ] as const) {
            let bad = bytes.slice();
            const view = new DataView(bad.buffer);
            switch (mutation) {
              case "strict horizon count":
                view.setUint32(16, 1, true);
                break;
              case "strict horizon id":
                view.setUint32(record, 2, true);
                break;
              case "canonical decimal":
                bad[record + 16] = 48;
                break;
              case "sequence reserved":
                bad[record + 12] = 1;
                break;
              case "catalog reserved":
                bad[6] = 1;
                break;
              case "unknown version":
                bad[4] = 2;
                break;
              case "trailing bytes": {
                const extra = new Uint8Array(bad.length + 1);
                extra.set(bad);
                bad = extra;
                break;
              }
            }
            expect(() => decodeSnapshotCatalog(bad, budget)).toThrow(
              expect.objectContaining({ code: "MINNOW_SNAPSHOT_FORMAT" }),
            );
            expect(budget.used).toBe(0);
            reach.reach(mutation);
          }
          for (const direction of [1n, -1n]) {
            const h = new Harness("strict", "memory"),
              path = await h.open();
            try {
              await autocommit(path, (st) =>
                st.changeCatalog({
                  kind: "createSequence",
                  sequence: {
                    name: "s",
                    start: 0n,
                    increment: direction,
                    min: -1000n,
                    max: 1000n,
                    cycle: false,
                    ownedBy: null,
                  },
                }),
              );
              await path.sequences.set(1, 37n * direction, false);
              await path.flush();
              const archive: Uint8Array[] = [];
              let writes = 0;
              const assignments: bigint[] = [];
              const exporting = exportSnapshot(
                path,
                new Uint8Array(16),
                {
                  async write(part) {
                    archive.push(part.slice());
                    if (++writes === 1) {
                      assignments.push(91n * direction);
                      await path.sequences.set(1, 91n * direction, false);
                    }
                  },
                },
                budget,
              );
              // A checkpoint after yielding may discard the older reservation history.
              assignments.push(99n * direction);
              const later = path.sequences.set(1, 99n * direction, false);
              if (typeof later !== "number") throw new Error("fitting setval must be synchronous");
              path.sequences.checkpoint(later);
              await exporting;
              expect(budget.used).toBe(0);
              const reader = await SnapshotStreamReader.open(
                  (async function* () {
                    yield* archive;
                  })(),
                  budget,
                ),
                parts: Uint8Array[] = [];
              for await (const frame of reader.frames())
                if (frame.kind === SnapshotKind.catalogPart) parts.push(frame.payload.slice());
              const whole = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
              let at = 0;
              for (const part of parts) {
                whole.set(part, at);
                at += part.length;
              }
              const captured = decodeSnapshotCatalog(whole, budget);
              try {
                expect(captured.value.horizons).toEqual([
                  { sequenceId: 1, reservedThrough: 37n * direction },
                ]);
              } finally {
                budget.free(captured.charge);
              }
              const out = { repr: "int64" as const, values: new BigInt64Array(1) };
              await path.sequences.next(1, 1, out, 0);
              expect(assignments).toHaveLength(2);
              expect(out.values[0]).toBe(assignments.at(-1));
              reach.reach("continued sequence");
              reach.reach("before first await");
              const controller = new AbortController();
              controller.abort(new Error("capture cancelled"));
              await expect(
                exportSnapshot(path, new Uint8Array(16), { write: () => undefined }, budget, {
                  signal: controller.signal,
                }),
              ).rejects.toThrow("capture cancelled");
              expect(budget.used).toBe(0);
              reach.reach("cancel release");
            } finally {
              await h.close();
            }
            expect(h.governor.usedBytes).toBe(0);
          }
        },
      ),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  }, 60000);
