/** Transfer changes the account, never the number of live owned bytes (governor rules 1/4). */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { Governor, OBJECT_OVERHEAD_BYTES } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, SORT_TRANSFER_ACCOUNTING_REACH } from "../../testing/reach.js";
import { INT, batchesOf, rowsOf } from "./join-fixtures.js";
import { drive, type Operator } from "./pipeline.js";
import { ArraySource } from "./join-fixtures.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";
import { ResultSink } from "./result-sink.js";
import { ResultChannel, type ResultFrame } from "./result.js";
import type { Batch } from "../vector/vector.js";

const reach = createReach("next-sort-transfer-accounting", SORT_TRANSFER_ACCOUNTING_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});

for (const seed of seedsFor("next-sort-transfer-accounting", [0x6a11, 0x7b23]))
  for (const mode of ["adopt", "refuse", "cancel", "error", "loan"] as const)
    it(`moves each native charge exactly once (${String(seed)}, ${mode})`, async () => {
      const random = mulberry32(seed),
        width = 1 + Math.floor(random() * 3),
        chunks = 2 + Math.floor(random() * 2);
      const n = chunks * 2048,
        types = Array.from({ length: width }, () => INT);
      const rows = Array.from({ length: n }, (_, i) =>
        Array.from({ length: width }, (_, c) => (c === 0 ? i : (i * 17 + c) % 103)),
      );
      const input = batchesOf(types, rows),
        governor = new Governor(32 << 20),
        spill = new MemorySpillService();
      const source = await governor.admit({
        label: "sort",
        minimumBytes: sortMinimumBytes(types),
        spillable: true,
      });
      const result = await governor.admit({
        label: "frames",
        minimumBytes: 2 << 20,
        spillable: true,
      });
      const loan = mode === "loan" ? await source.borrow(8192) : null;
      const channel = new ResultChannel(
        types.map((type, c) => ({ name: String(c), type, nullable: false })),
        { windowBytes: 2 << 20 },
      );
      const refusing = new Proxy(result, {
        get(target, key) {
          if (key === "allocate")
            return (bytes: number) =>
              mode === "refuse" && bytes === width * 8192 ? false : target.allocate(bytes);
          const value: unknown = Reflect.get(target, key);
          return typeof value === "function" ? (value.bind(target) as unknown) : value;
        },
      });
      let liveAtPublication = 0,
        published = false;
      const offer = channel.offer.bind(channel);
      channel.offer = (frame) => {
        expect(
          source.allocated + result.allocated,
          "single owner when a frame becomes visible",
        ).toBe(liveAtPublication);
        reach.reach("single owner before publication");
        const status = offer(frame);
        published = true;
        return status;
      };
      const sink = new ResultSink(types, refusing, channel, 0),
        frames: ResultFrame[] = [],
        actualIds: number[] = [];
      let offered = 0,
        frameBytes = 0;
      const reason = new Error("generated transfer stop");
      const observer: Operator = {
        reclassifiesTransfers: true,
        push(batch, token) {
          if (mode === "cancel" && offered > 0) throw reason;
          published = false;
          const before = source.allocated + result.allocated;
          const heldBefore = source.held;
          const metadata = OBJECT_OVERHEAD_BYTES * (3 + 2 * width);
          const copyBitmaps = mode === "refuse" ? width * 256 : 0;
          if (!token) throw Error("missing transfer");
          liveAtPublication =
            before + metadata + copyBitmaps + (mode === "refuse" ? width * 8192 : 0);
          const check = () =>
            expect(
              source.allocated + result.allocated,
              "live owned bytes after transfer step",
            ).toBe(before + metadata + copyBitmaps);
          const status = sink.push(batch, {
            ...token,
            reclassify(buffers) {
              token.reclassify?.(buffers);
              check();
            },
            release() {
              token.release();
              if (published) check();
            },
          });
          expect(status).toBe("ready");
          // Existing scratch/reservations are unchanged by the handoff. The native values
          // already existed. Only frame objects, plus copying fallback bitmaps, are new.
          expect(source.allocated + result.allocated, "live owned bytes after readiness").toBe(
            before + metadata + copyBitmaps,
          );
          if (loan === null)
            expect(source.held, "retired native holding leaves the source too").toBe(
              heldBefore - width * 8192,
            );
          else {
            expect(source.loaned).toBe(8192);
            expect(source.held).toBeGreaterThanOrEqual(source.allocated + source.loaned);
            reach.reach("loan credit stays at source");
          }
          reach.reach("single owner after readiness");
          const frame = channel.poll();
          if (!frame) throw Error("missing offered frame");
          frames.push(frame);
          const first = frame.payload.columns[0],
            original = batch.columns[0];
          if (first?.form !== "flat" || original?.form !== "flat")
            throw Error("wrong fixture layout");
          if (mode === "refuse") expect(first.data).not.toBe(original.data);
          else expect(first.data).toBe(original.data);
          const received = structuredClone(frame.payload, { transfer: [...frame.transfer] });
          const decoded = {
            length: received.rowCount,
            selection: null,
            columns: received.columns.map((v, c) => ({ ...v, type: types[c] })),
          } as Batch;
          expect(rowsOf(decoded)).toEqual(rows.slice(offered * 2048, (offered + 1) * 2048));
          actualIds.push(...rowsOf(decoded).map((row) => Number(row[0])));
          offered++;
          frameBytes += metadata + width * 8192 + copyBitmaps;
          expect(
            result.allocated,
            "frame account equals retained native capacities and metadata",
          ).toBe(frameBytes);
          reach.reach("irreversible detach");
          if (offered === 1 && mode === "error") throw reason;
          if (offered === 1 && mode === "cancel") sort.abort(reason);
          return status;
        },
        ready: () => sink.ready(),
        finish: () => sink.finish(),
        abort: (e) => sink.abort(e),
      };
      const sort = new SortOperator(
        {
          types,
          keys: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
          limit: null,
          offset: 0,
          withTies: false,
        },
        { grant: source, spill, queryId: 1, columnar: true },
        observer,
      );
      reach.beginHistory();
      reach.reach(mode);
      try {
        const feeding = drive(new ArraySource(input), sort);
        if (mode === "cancel" || mode === "error")
          await expect(feeding).rejects.toThrow(reason.message);
        else await feeding;
        await sort.closed;
        expect(source.allocated).toBe(0);
        expect(result.allocated).toBe(frameBytes);
        reach.reach("operator close with live frames");
        for (const frame of frames) {
          const before = result.allocated;
          frame.release();
          frame.release();
          expect(result.allocated).toBeLessThan(before);
          frameBytes = result.allocated;
          expect(source.allocated + result.allocated).toBe(frameBytes);
        }
        if (offered > 1) reach.reach("multiple source chunks");
        expect(input.flatMap(rowsOf)).toEqual(rows);
        const expected = (
          await pg.query<{ id: number }>(
            `SELECT i AS id FROM generate_series(0,${String(n - 1)}) g(i) ORDER BY i`,
          )
        ).rows.map((r) => r.id);
        expect(actualIds).toEqual(expected.slice(0, actualIds.length));
      } finally {
        sort.abort(reason);
        await sort.closed;
        sink.discard(reason);
        channel.cancel(reason);
        for (const frame of frames) frame.release();
        loan?.repay();
        result.close();
        expect(governor.usedBytes).toBe(0);
        expect(spill.bytesInUse).toBe(0);
      }
    }, 120000);
