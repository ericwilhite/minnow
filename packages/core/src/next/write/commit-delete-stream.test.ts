/** Large unordered deletion bodies keep canonical row order with a charged, bounded index. */
import fc from "fast-check";
import { watchGrants } from "./test-support.js";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_DELETE_STREAM_STATES } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { encodeBody } from "./body.js";
import { ByteWriter } from "./bytes.js";
import { ByteWindow } from "./window.js";
import { streamBody } from "./stream-body.js";
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
  locators,
  rowsOf,
  rules,
} from "./test-support.js";

const reach = createReach("charged streamed deletion order", WRITE_DELETE_STREAM_STATES);
for (const seed of seedsFor("next-write-delete-stream", [0xde1]))
  it(`preserves canonical deletion bytes and repays the ordering loan (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({ rows: fc.constantFrom(6000, 50000), reverse: fc.boolean() }),
        async (s) => {
          reach.beginHistory();
          reach.reach(s.reverse ? "reverse" : "zigzag");
          const h = new Harness("strict", "memory");
          const governor = new Governor(32 * 1024 * 1024);
          Object.assign(h, { governor });
          let loans = 0;
          watchGrants(governor, (grant) => {
            if (grant.label === "write commit buffers") {
              const borrow = grant.borrow.bind(grant);
              grant.borrow = (bytes, signal) => {
                loans++;
                return borrow(bytes, signal);
              };
            }
          });
          let capture: Uint8Array | null = null;
          let at = 0;
          const open = h.backend.open.bind(h.backend);
          h.backend.open = async (lease) => {
            const session = await open(lease);
            const append = session.appendLog.bind(session);
            session.appendLog = async (bytes) => {
              if (capture !== null) {
                const length = new DataView(
                  bytes.buffer,
                  bytes.byteOffset,
                  bytes.byteLength,
                ).getUint32(16, true);
                capture.set(bytes.subarray(28, 28 + length), at);
                at += length;
              }
              return append(bytes);
            };
            return session;
          };
          let path = await h.open();
          try {
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
            const ids = Array.from({ length: s.rows }, (_, k) => k);
            await autocommit(path, (st) =>
              st.table(table).insert(batch(ints(ids), texts(ids.map(String))), null),
            );
            const first = path.state().tables.get(table)?.memtables[0]?.firstRowId;
            if (first === undefined) throw new Error("Missing resident fixture");
            const tx = await path.begin();
            const st = tx.statement({ rules });
            const snapshot = st.view.internalTable(table);
            const order = ids.map((k) =>
              snapshot.locator(
                first +
                  (s.reverse
                    ? s.rows - 1 - k
                    : k % 2 === 0
                      ? k / 2
                      : s.rows - 1 - Math.floor(k / 2)),
              ),
            );
            for (let from = 0; from < order.length; from += 2048)
              await st
                .table(table)
                .delete(batch(locators(order.slice(from, from + 2048), table)), null);
            await st.end();
            // Force the allocation-free rank walk on a bounded prefix from each generated
            // unordered fixture. No order index is passed. Preserve the real transaction's
            // complete undo log for the subsequent engine commit and reopen below.
            const saved = tx.undo.slice();
            try {
              const prefix = saved
                .filter((entry) => entry.kind === "memDelete" || entry.kind === "groupDelete")
                .slice(0, 32);
              expect(prefix).toHaveLength(32);
              expect(
                prefix.some(
                  (entry, k) => k > 0 && entry.rowId < (prefix[k - 1]?.rowId ?? -Infinity),
                ),
              ).toBe(true);
              tx.undo.length = 0;
              tx.undo.push(...prefix);
              const legacy = encodeBody({ request: null, effects: tx.effects(new ByteWriter()) });
              const actual = new Uint8Array(legacy.length);
              const scratch = new Uint8Array(4096);
              for (let from = 0; from < actual.length; from += 31) {
                await streamBody(
                  tx,
                  null,
                  () => null,
                  new ByteWindow(actual.subarray(from, from + 31), from, scratch),
                  () => undefined,
                );
              }
              expect(Buffer.compare(actual, legacy), "bounded rank-walk deletion bytes").toBe(0);
              reach.reach("bounded rank walk");
            } finally {
              tx.undo.length = 0;
              for (const entry of saved) tx.undo.push(entry);
            }
            const expected = encodeBody({
              request: null,
              effects: tx.effects(new ByteWriter()),
            });
            capture = new Uint8Array(expected.length);
            const receipt = await tx.commit(null);
            await receipt.durable;
            expect(at).toBe(expected.length);
            expect(Buffer.compare(capture, expected), "canonical sorted deletion body").toBe(0);
            expect(loans).toBeGreaterThan(0);
            reach.reach("charged order loan");
            capture = null;
            path = await h.reopen();
            const view = path.pin();
            try {
              expect(await rowsOf(view, table)).toEqual([]);
            } finally {
              view.release();
            }
            reach.reach("empty after reopen");
          } finally {
            capture = null;
            await h.close();
          }
          expect(governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 4 },
    );
  }, 180_000);
it("reaches streamed deletion ordering states", () => reach.assertReached());
