/** The 1 MiB key work target cannot truncate or refuse a complete borrowed mutation batch.
 * Two indexes plus a statement's retained pre-flush deltas need >32 MiB for the 48/80-row
 * maximum-width full lifecycle (documented peak). Keep 32 MiB for 32 rows; 64/128 for larger
 * complete minima. The separate one-index ingress campaign keeps its 160-row 32 MiB law. */
import fc from "fast-check";
import { afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WIDE_KEY_BATCH_STATES } from "../../testing/reach.js";
import type { SnapshotImpl } from "./view.js";
import { WritePathClosedError } from "../access/write-path.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import { UNCHARGED } from "./bytes.js";
import { ConflictKind } from "../access/modify.js";
import { Governor } from "../memory/governor.js";
import { encodeEqualityKey } from "../keys/key-encoding.js";
import { need } from "./bytes.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  TEXT,
  INT,
  texts,
  ints,
  tableInput,
  rules,
  rowsOf,
  locators,
} from "./test-support.js";
const reach = createReach("complete wide key batches", WIDE_KEY_BATCH_STATES);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-wide-key-batches", [0xb51c]))
  it(`consumes every byte-sized key batch through writes and replay (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(13106, 16384),
        fc.constantFrom(32, 48, 80),
        fc.constantFrom(64, 128),
        fc.boolean(),
        async (units, count, chosenCap, selected) => {
          const cap = count === 32 ? 32 : chosenCap;
          reach.beginHistory();
          let holdBuild = false,
            releaseBuild: () => void = () => undefined;
          const buildGate = new Promise<void>((resolve) => {
            releaseBuild = resolve;
          });
          const h = new Harness("strict", "memory", 16, undefined, {
            merges: { mode: "off" },
            beforeFlushPublish: async () => {
              if (holdBuild) {
                await buildGate;
                throw new WritePathClosedError();
              }
            },
          });
          Object.assign(h, { governor: new Governor(cap << 20) });
          await h.open();
          const values = Array.from(
            { length: count },
            (_, i) => "\u0800".repeat(units - 1) + String.fromCharCode(0x0810 + i),
          );
          const inactive = "\u0800".repeat(16385);
          const physical = selected ? values.flatMap((value) => [inactive, value]) : values;
          const slots = values.map((_, i) => (selected ? 2 * i + 1 : i));
          const proposed = {
            ...batch(
              texts(physical),
              ints(physical.map((_, i) => (selected ? Math.floor(i / 2) : i))),
            ),
            selection: selected ? { rows: Uint16Array.from(slots), count } : null,
          };
          let table = 0,
            secondary = 0,
            phase = "insert";
          try {
            await autocommit(h.path, async (s) => {
              table = need(
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("k", TEXT, false), column("n", INT, false)]),
                  })
                ).id,
              );
            });
            await autocommit(h.path, (s) => s.table(table).insert(proposed, null));
            await h.path.quiesce();
            reach.reach("wide insert slices");
            phase = "backfill";
            holdBuild = true;
            await autocommit(h.path, async (s) => {
              secondary = need(
                (
                  await s.changeCatalog({
                    kind: "createIndex",
                    index: {
                      tableId: table,
                      name: "wide",
                      columnIds: [need(s.catalog.table(table)?.columns[0]).id],
                      unique: true,
                      role: "secondary",
                      ...(units === 16384 ? { keyLimit: "scalar" as const } : {}),
                    },
                  })
                ).id,
              );
            });
            async function positionsOfAll() {
              const view = h.path.pin();
              const positions: number[] = [];
              try {
                for (const value of values) {
                  const found: number[] = [];
                  await view
                    .index(table, secondary)
                    .resolve(
                      encodeEqualityKey([{ type: TEXT, value: new TextEncoder().encode(value) }]),
                      found,
                    );
                  expect(found).toHaveLength(1);
                  positions.push(need(found[0]));
                }
              } finally {
                view.release();
              }
              return positions;
            }
            await positionsOfAll();
            reach.reach("wide backfill visible before replay");
            reach.reach("wide backfill slices");
            phase = "replay";
            expect(
              decodeManifestRoot(need(h.space.root), UNCHARGED).value.catalogVersion,
            ).toBeLessThan(h.path.catalog.version);
            const closing = h.close();
            releaseBuild();
            await closing;
            holdBuild = false;
            await h.open();
            const positions = await positionsOfAll();
            reach.reach("wide replay tail");
            phase = "conflicts";
            const tx = await h.path.begin(),
              s = tx.statement({ rules });
            try {
              const out = {
                kinds: new Uint8Array(proposed.length),
                locators: new Float64Array(proposed.length),
                earlier: new Int32Array(proposed.length),
                indexIds: new Int32Array(proposed.length),
              };
              await s.table(table).conflicts(proposed, [secondary], out);
              expect(slots.map((slot) => out.kinds[slot])).toEqual(
                values.map(() => ConflictKind.existing),
              );
              if (selected)
                expect(slots.map((slot) => out.kinds[slot - 1])).toEqual(
                  values.map(() => ConflictKind.none),
                );
              await s.end();
              tx.rollback();
            } catch (e) {
              s.rollback();
              tx.rollback();
              throw e;
            }
            reach.reach("wide conflict tail");
            reach.reach(selected ? "wide sparse selected offsets" : "wide dense offsets");
            phase = "update";
            await h.path.quiesce();
            await autocommit(h.path, (s) => {
              const tableView = (s.view as SnapshotImpl).internalTable(table);
              const at = positions.map((id) => tableView.locator(id));
              return s
                .table(table, { assigned: [need(s.catalog.table(table)?.columns[1]).id] })
                .update(batch(locators(at, table), ints(values.map((_, i) => i + 1000))), null);
            });
            await h.reopen();
            const updated = h.path.pin();
            try {
              expect(
                (await rowsOf(updated, table)).map((r) => Number(r[1])).sort((a, b) => a - b),
              ).toEqual(values.map((_, i) => i + 1000));
            } finally {
              updated.release();
            }
            reach.reach("wide update tail");
            phase = "delete";
            await autocommit(h.path, async (s) => {
              const v = (s.view as SnapshotImpl).internalTable(table),
                ids: number[] = [];
              for (const seg of v.segments) {
                const deleted = await seg.deletions();
                for (let n = 0; n < seg.rowCount; n++)
                  if (!deleted?.isDeleted(n)) ids.push(v.locator(v.rowId(seg.index * 65536 + n)));
              }
              await s.table(table).delete(batch(locators(ids, table)), null);
            });
            await h.reopen();
            const empty = h.path.pin();
            try {
              expect(await rowsOf(empty, table)).toEqual([]);
            } finally {
              empty.release();
            }
            reach.reach("wide delete tail");
            reach.reach(
              cap === 32
                ? "wide thirty-two MiB"
                : cap === 64
                  ? "wide sixty-four MiB"
                  : "wide one-twenty-eight MiB",
            );
            reach.reach(units === 16384 ? "wide scalar provenance" : "wide tuple provenance");
          } catch (error) {
            throw new Error(`wide ${phase}: ${String(error)}`, { cause: error });
          } finally {
            releaseBuild();
            await h.close();
          }
          expect(h.governor.usedBytes).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [13106, 32, 64, true],
          [16384, 80, 64, false],
          [16384, 80, 128, true],
        ],
      },
    );
  }, 300_000);
