/** A durable multipart prefix never becomes a partial transaction after power loss. */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { createReach, WRITE_STREAM_CRASH_STATES } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { SimBackend, SimulatedCrash } from "../storage/sim-disk.js";
import { MAX_FRAME_BODY_BYTES } from "../format/log-frame.js";
import { encodeBody } from "./body.js";
import { ByteWriter } from "./bytes.js";
import {
  Harness,
  INT,
  TEXT,
  autocommit,
  batch,
  column,
  ints,
  texts,
  tableInput,
  rowsOf,
  rules,
} from "./test-support.js";

const reach = createReach("streamed commit crash cuts", WRITE_STREAM_CRASH_STATES);

for (const seed of seedsFor("next-write-streamed-crash", [0xc2a5]))
  describe(`streamed commit crash cuts (seed ${String(seed)})`, () => {
    it("discards every incomplete prefix, keeps a complete body, and continues writing", async () => {
      const cuts = new Set<string>();
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            width: fc.constantFrom(768, 1536),
            cut: fc.integer({ min: 1, max: 8 }),
            strict: fc.boolean(),
          }),
          async (s) => {
            reach.beginHistory();
            reach.reach(s.strict ? "strict" : "relaxed");
            const h = new Harness(s.strict ? "strict" : "relaxed", "sim");
            let armed = false;
            let part = 0;
            let stop = 0;
            const open = h.backend.open.bind(h.backend);
            h.backend.open = async (lease) => {
              const session = await open(lease);
              const append = session.appendLog.bind(session);
              session.appendLog = async (bytes) => {
                const result = await append(bytes);
                if (armed) {
                  part++;
                  // Make this prefix physically durable; the appender must not acknowledge it.
                  await session.flushLog();
                  if (part === stop) {
                    armed = false;
                    h.disk.powerLoss();
                    throw new SimulatedCrash();
                  }
                }
                return result;
              };
              return session;
            };
            let path = await h.open();
            let table = 0;
            await autocommit(path, async (st) => {
              table =
                (
                  await st.changeCatalog({
                    kind: "createTable",
                    table: tableInput("t", [column("id", INT, false), column("v", TEXT)]),
                  })
                ).id ?? 0;
            });
            const tx = await path.begin();
            const statement = tx.statement({ rules });
            const ids = Array.from({ length: 2048 }, (_, k) => k);
            const values = ids.map((k) => `${String(k)}-${"x".repeat(s.width)}`);
            await statement.table(table).insert(batch(ints(ids), texts(values)), null);
            await statement.end();
            const bytes = encodeBody({
              request: null,
              effects: tx.effects(new ByteWriter()),
            });
            const parts = Math.ceil(bytes.length / MAX_FRAME_BODY_BYTES);
            expect(parts).toBeGreaterThan(1);
            stop = 1 + ((s.cut - 1) % parts);
            cuts.add(stop === parts ? "complete" : "prefix");
            reach.reach(stop === parts ? "complete" : "prefix");
            armed = true;
            await expect(tx.commit(null)).rejects.toThrow();
            expect(part).toBe(stop);
            await h.close();
            if (!(h.backend instanceof SimBackend)) throw new Error("Not a crash backend");
            h.backend.restart();
            path = await h.open();
            const view = path.pin();
            try {
              const rows = await rowsOf(view, table);
              expect(rows.length, "all rows or none after a multipart crash").toBe(
                stop === parts ? ids.length : 0,
              );
              for (let k = 0; k < rows.length; k++) expect(rows[k]).toEqual([k, values[k]]);
            } finally {
              view.release();
            }
            await autocommit(path, (st) =>
              st.table(table).insert(batch(ints([9999]), texts(["after crash"])), null),
            );
            path = await h.reopen();
            const reopened = path.pin();
            try {
              expect((await rowsOf(reopened, table)).length).toBe(
                (stop === parts ? ids.length : 0) + 1,
              );
            } finally {
              reopened.release();
            }
            await h.close();
            reach.reach("retry and reopen");
            expect(h.governor.usedBytes).toBe(0);
          },
        ),
        { seed, numRuns: 12 },
      );
      expect([...cuts].sort()).toEqual(["complete", "prefix"]);
    }, 180_000);
  });

it("reaches streamed crash states", () => reach.assertReached());
