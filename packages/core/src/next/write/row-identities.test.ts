/** Stable WAL identities preserve physical operands across a newer checkpoint catalog. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, ROW_IDENTITIES_REACH } from "../../testing/reach.js";
import { Replayer } from "../format/replay.js";
import type { BodyEffect } from "../format/log-frame.js";
import { encodeBody, decodeBody } from "./body.js";
import { encodeColumn } from "./columns.js";
import { ints } from "./test-support.js";

for (const seed of seedsFor("row-identities", [47399, 47400]))
  it(`checks row identities and strict positional controls seed ${String(seed)}`, () => {
    const reach = createReach("WAL stable identities", ROW_IDENTITIES_REACH);
    fc.assert(
      fc.property(
        fc.uniqueArray(fc.integer({ min: 1, max: 1000 }), { minLength: 1, maxLength: 5 }),
        fc.integer({ min: 2, max: 8 }),
        (ids, schema) => {
          reach.beginHistory();
          const effect: Extract<BodyEffect, { kind: "rows" }> = {
            kind: "rows",
            tableId: 1,
            schemaVersion: schema - 1,
            inserts: {
              firstRowId: 1,
              rowCount: 1,
              columnIds: ids,
              columns: ids.map((id) => encodeColumn(ints([id]), 0, 1)),
            },
            deletes: {
              indexIds: ids,
              rowIds: new Float64Array([2]),
              keys: [ids.map((id) => Uint8Array.of(id & 255))],
            },
          };
          const roundtrip = (value: typeof effect) => {
            const body = { request: null, effects: [value] };
            return decodeBody(encodeBody(body));
          };
          expect(roundtrip(effect).effects).toEqual([effect]);
          reach.reach("stable columns");
          reach.reach("stable tombstone indexes");
          const replayer = () =>
            new Replayer({
              globalLsn: 10,
              nextTableId: 2,
              tables: new Map([[1, { schemaVersion: schema, flushedThroughLsn: 0 }]]),
            });
          expect(
            replayer().next(1, [
              { kind: "rows", tableId: 1, schemaVersion: schema - 1, stableIds: true },
            ]),
          ).toEqual([true]);
          reach.reach("older identified rows");
          expect(() =>
            replayer().next(1, [{ kind: "rows", tableId: 1, schemaVersion: schema - 1 }]),
          ).toThrow(expect.objectContaining({ code: "MINNOW_LOG_CORRUPT" }));
          reach.reach("positional mismatch refused");
          for (const version of [0, schema + 1])
            expect(() =>
              replayer().next(1, [
                { kind: "rows", tableId: 1, schemaVersion: version, stableIds: true },
              ]),
            ).toThrow(expect.objectContaining({ code: "MINNOW_LOG_CORRUPT" }));
          reach.reach("invalid version refused");
          const inserts = effect.inserts;
          const deletes = effect.deletes;
          if (!inserts || !deletes) throw new Error("missing codec fixture");
          for (const columnIds of [[0, ...ids.slice(1)], [...ids, ids[0] ?? 1], []])
            expect(() => roundtrip({ ...effect, inserts: { ...inserts, columnIds } })).toThrow(
              expect.objectContaining({ code: "MINNOW_LOG_CORRUPT" }),
            );
          for (const indexIds of [[0, ...ids.slice(1)], [...ids, ids[0] ?? 1], []])
            expect(() => roundtrip({ ...effect, deletes: { ...deletes, indexIds } })).toThrow(
              expect.objectContaining({ code: "MINNOW_LOG_CORRUPT" }),
            );
          reach.reach("malformed IDs refused");
          const bytes = encodeBody({
            request: null,
            effects: [
              {
                ...effect,
                inserts: {
                  ...inserts,
                  columns: [inserts.columns[0] ?? new Uint8Array()],
                  columnIds: [1],
                },
              },
            ],
          });
          bytes[10] = 2; // body9: first ROW's column-ID presence flag, after one-byte counts.
          expect(() => decodeBody(bytes)).toThrow(
            expect.objectContaining({ code: "MINNOW_LOG_CORRUPT" }),
          );
          reach.reach("unknown flag refused");
        },
      ),
      { seed, numRuns: 40 },
    );
    reach.assertReached();
  });
