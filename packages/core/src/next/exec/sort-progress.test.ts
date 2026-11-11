/** Legacy isolated sorts share the reserve but must never wait while retaining a step loan. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, SORT_PROGRESS_LOANS_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { Governor, type Grant } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import {
  ArraySource,
  CollectSink,
  batchesOf,
  rowsOf,
  INT,
  TEXT,
  BIG,
  DBL,
  type Row,
} from "./join-fixtures.js";
import { drive, type Operator } from "./pipeline.js";
import { SortOperator, GroupTopKOperator, sortMinimumBytes } from "./sort.js";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const reach = createReach("sort legacy progress", SORT_PROGRESS_LOANS_REACH);
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
  reach.assertReached();
});
const types = [INT, TEXT, TEXT, TEXT, TEXT, TEXT, BIG, DBL];
for (const seed of seedsFor("next-sort-progress-loans", [0xb753, 0xb764]))
  for (const cancel of [false, true])
    it(`two standalone sorts progress at the shared reserve (${String(seed)}, cancel=${String(cancel)})`, async () => {
      await fc.assert(
        fc.asyncProperty(
          fc.record({
            rows: fc.integer({ min: 1, max: 5 }),
            chars: fc.integer({ min: 350000, max: 370000 }),
            numeric: fc.boolean(),
            nullTail: fc.boolean(),
            misaligned: fc.boolean(),
            group: fc.boolean(),
            shifting: fc.boolean(),
          }),
          async ({ rows, chars, numeric, nullTail, misaligned, group, shifting }) => {
            reach.beginHistory();
            const governor = new Governor((group ? 176 : 128) * 1024 * 1024),
              spill = new MemorySpillService();
            if (misaligned) {
              const create = spill.create.bind(spill);
              spill.create = async (id) => {
                const file = await create(id);
                const read = file.read.bind(file);
                file.read = async (ref) => {
                  const bytes = await read(ref);
                  const shifted = new Uint8Array(bytes.length + 1);
                  shifted.set(bytes, 1);
                  return shifted.subarray(1);
                };
                return file;
              };
              reach.reach("misaligned spill reads");
            }
            const a = await governor.admit({
              label: "legacy sort A",
              minimumBytes: sortMinimumBytes(types),
              spillable: true,
            });
            const b = await governor.admit({
              label: "legacy sort B",
              minimumBytes: sortMinimumBytes(types),
              spillable: true,
            });
            const resident = await governor.admit({
              label: "busy execution",
              minimumBytes: 0,
              spillable: true,
            });
            const outA = deferred(),
              releaseA = deferred(),
              waitingB = deferred();
            const abortA = new AbortController(),
              abortB = new AbortController();
            const data: Row[] = Array.from({ length: rows }, (_, i) => [
              i,
              ...Array.from({ length: 5 }, (_, c) =>
                nullTail && c === 4 && i === 1
                  ? null
                  : `${String.fromCharCode(97 + rows - i)}${"汉".repeat(shifting && (i === 1 || i === 2) ? (c === i ? (i === 1 ? 1000000 : 1333333) : 1) : chars + c)}${String(c)}`,
              ),
              i % 2 ? -(1n << 62n) : (1n << 62n) + BigInt(i),
              i % 2 ? NaN : Infinity,
            ]);
            const expectedA: Row[] = [],
              actualA: Row[] = [],
              actualB: Row[] = [];
            const track = (grant: Grant, second: boolean): Grant =>
              new Proxy(grant, {
                get(target, key) {
                  if (key === "borrow")
                    return (bytes: number, signal?: AbortSignal) => {
                      expect(target.loaned, "no loan may survive another credit request").toBe(0);
                      reach.reach("no overlapping loan");
                      if (second) waitingB.resolve();
                      return target.borrow(bytes, signal);
                    };
                  const value: unknown = Reflect.get(target, key);
                  return typeof value === "function" ? (value.bind(target) as unknown) : value;
                },
              });
            const sink = (into: Row[], gated: boolean): Operator => ({
              push(batch) {
                into.push(...rowsOf(batch));
                if (gated) {
                  outA.resolve();
                  return "blocked";
                }
                return "ready";
              },
              ready: () => (gated ? releaseA.promise : Promise.resolve()),
              finish: () => undefined,
              abort: () => undefined,
            });
            const keys = numeric
              ? [{ column: 0, direction: "desc" as const, nulls: "last" as const, collation: null }]
              : [
                  { column: 1, direction: "asc" as const, nulls: "last" as const, collation: null },
                  { column: 0, direction: "asc" as const, nulls: "last" as const, collation: null },
                ];
            const spec = { types, keys, limit: null, offset: 0, withTies: false };
            const make = (grant: Grant, id: number, signal: AbortSignal, downstream: Operator) =>
              group
                ? new GroupTopKOperator(
                    { types, partitionBy: numeric ? [0] : [], orderBy: keys, k: 1 },
                    { grant, spill, queryId: id, signal },
                    downstream,
                  )
                : new SortOperator(spec, { grant, spill, queryId: id, signal }, downstream);
            const opA = make(track(a, false), 11, abortA.signal, sink(actualA, true));
            const opB = make(track(b, true), 12, abortB.signal, sink(actualB, false));
            let workA: Promise<void> | undefined, workB: Promise<void> | undefined;
            try {
              await pg.exec(
                "DROP TABLE IF EXISTS p; CREATE TABLE p(id int, v0 text, v1 text, v2 text, v3 text, v4 text, b bigint, f float8)",
              );
              for (const row of data)
                await pg.query(
                  "INSERT INTO p VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                  row.map((v) => (typeof v === "bigint" ? String(v) : v)),
                );
              const order = numeric ? "id DESC" : 'v0 COLLATE "C",id';
              const sql = group
                ? `SELECT * FROM (SELECT *,ROW_NUMBER() OVER (${numeric ? "PARTITION BY id" : ""} ORDER BY ${order}) rn FROM p) q WHERE rn<=1 ORDER BY ${numeric ? "id" : 'v0 COLLATE "C",id'}`
                : `SELECT * FROM p ORDER BY ${order}`;
              const expected = (await pg.query<Record<string, unknown>>(sql)).rows;
              for (const row of expected)
                expectedA.push([
                  Number(row.id),
                  ...[0, 1, 2, 3, 4].map((c) => row[`v${String(c)}`] as string | null),
                  BigInt(String(row.b)),
                  Number(row.f),
                  ...(group ? [BigInt(String(row.rn))] : []),
                ]);
              if (group)
                reach.reach(numeric ? "grouped boundary row" : "global top-k boundary row");
              if (shifting) reach.reach("moving wide fields");
              expect(
                resident.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
              reach.reach("legacy shared reserve");
              reach.reach(numeric ? "numeric first key" : "text first key");
              if (rows > 2) reach.reach("multi-run wide merge");
              if (nullTail && rows > 1) reach.reach("null payload");
              workA = Promise.resolve(
                drive(
                  new ArraySource(batchesOf(types, data, { batchRows: 1, int64: "int64" })),
                  opA,
                ),
              );
              void workA.catch(() => undefined);
              await Promise.race([
                outA.promise,
                workA.then(() => {
                  throw new Error("first sort never reached its output");
                }),
              ]);
              expect(a.loaned).toBeGreaterThan(0);
              reach.reach("first sort holds complete merge");
              workB = Promise.resolve(
                drive(
                  new ArraySource(batchesOf(types, data, { batchRows: 1, int64: "int64" })),
                  opB,
                ),
              );
              void workB.catch(() => undefined);
              let settled = false;
              void workB.then(
                () => {
                  settled = true;
                },
                () => {
                  settled = true;
                },
              );
              await Promise.race([
                waitingB.promise,
                workB.then(() => {
                  throw new Error("second sort did not request progress");
                }),
              ]);
              await Promise.resolve();
              expect(settled).toBe(false);
              expect(b.loaned).toBe(0);
              reach.reach("second sort waits unloaned");
              if (cancel) {
                const reason = new Error("cancel waiting sort loan");
                abortB.abort(reason);
                opB.abort(reason);
                await expect(workB).rejects.toBe(reason);
                await opB.closed;
                reach.reach("waiting sort cancelled");
              }
              releaseA.resolve();
              await workA;
              await opA.closed;
              if (!cancel) {
                await workB;
                await opB.closed;
                reach.reach("both sorts complete");
              }
              const exact = (actual: Row[]) =>
                actual.length === expectedA.length &&
                actual.every(
                  (r, i) =>
                    r.length === expectedA[i]?.length &&
                    r.every((v, c) => Object.is(v, expectedA[i]?.[c])),
                );
              expect(exact(actualA), "PG-exact wide text/int8/nonfinite output A").toBe(true);
              if (!cancel)
                expect(exact(actualB), "PG-exact wide text/int8/nonfinite output B").toBe(true);
              reach.reach("PG exact mixed payload");
            } finally {
              releaseA.resolve();
              abortA.abort(new Error("cleanup"));
              abortB.abort(new Error("cleanup"));
              opA.abort(abortA.signal.reason);
              opB.abort(abortB.signal.reason);
              await Promise.allSettled([workA, workB, opA.closed, opB.closed]);
              resident.close();
              a.close();
              b.close();
              expect(governor.usedBytes).toBe(0);
              expect(spill.bytesInUse).toBe(0);
              reach.reach("released all resources");
            }
          },
        ),
        {
          seed,
          numRuns: 5,
          examples: [
            [
              {
                rows: 1,
                chars: 360000,
                numeric: false,
                nullTail: false,
                misaligned: false,
                group: false,
                shifting: false,
              },
            ],
            [
              {
                rows: 3,
                chars: 360000,
                numeric: true,
                nullTail: true,
                misaligned: true,
                group: false,
                shifting: false,
              },
            ],
            [
              {
                rows: 5,
                chars: 360000,
                numeric: true,
                nullTail: true,
                misaligned: true,
                group: true,
                shifting: true,
              },
            ],
            [
              {
                rows: 3,
                chars: 360000,
                numeric: false,
                nullTail: false,
                misaligned: false,
                group: true,
                shifting: false,
              },
            ],
          ],
        },
      );
    }, 120000);

// A many-column staging chunk is not an oversized value. Keep the normal bounded-page path.
for (const seed of seedsFor("next-sort-progress-short-schema", [0xb775, 0xb786]))
  it(`short values across many columns retain batched spill pages (${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          columns: fc.integer({ min: 24, max: 36 }),
          rows: fc.integer({ min: 32, max: 96 }),
          afterWide: fc.boolean(),
          forceEmpty: fc.boolean(),
          buffered: fc.boolean(),
        }),
        async ({ columns, rows, afterWide, forceEmpty, buffered }) => {
          reach.beginHistory();
          const shortTypes = Array.from({ length: columns }, () => TEXT);
          const governor = new Governor(48 * 1024 * 1024);
          const grant = await governor.admit({
            label: "short wide schema",
            minimumBytes: sortMinimumBytes(shortTypes),
            spillable: true,
          });
          const resident = await governor.admit({
            label: "busy execution",
            minimumBytes: 0,
            spillable: true,
          });
          const spill = new MemorySpillService(),
            sink = new CollectSink();
          const create = spill.create.bind(spill);
          let pages = 0;
          spill.create = async (
            id,
            budget?: { allocate(bytes: number): boolean; free(bytes: number): void },
          ) => {
            const file = await create(id),
              append = file.append.bind(file),
              close = file.close.bind(file);
            let buffer: Uint8Array | undefined;
            if (buffered && budget !== undefined) {
              const normalRoom = grant.held - grant.loaned - grant.allocated;
              const active = grant.loaned > 0;
              const accepted = budget.allocate(64 * 1024);
              if (accepted && active && normalRoom < 64 * 1024) {
                budget.free(64 * 1024);
                await close();
                expect(accepted, "spill buffering never consumes progress credit").toBe(false);
              }
              if (accepted) buffer = new Uint8Array(64 * 1024);
              if (active && normalRoom < 64 * 1024 && !accepted)
                reach.reach("backend buffering refused loan credit");
            }
            file.append = (page) => {
              pages++;
              return append(page);
            };
            file.close = async () => {
              await close();
              if (buffer !== undefined) {
                const bytes = buffer.byteLength;
                buffer = undefined;
                budget?.free(bytes);
              }
            };
            return file;
          };
          let pinned = 0,
            loans = 0;
          const pressureGrant = new Proxy(grant, {
            get(target, key) {
              if (key === "borrow")
                return (bytes: number, signal?: AbortSignal) => {
                  expect(target.loaned, "small-row step requests no overlapping loan").toBe(0);
                  loans++;
                  return target.borrow(bytes, signal);
                };
              if (key === "close")
                return () => {
                  // Model a nested operator's retained fixed buffers. They disappear with this
                  // fixture's owner, before closing its grant; they are not source payloads.
                  if (pinned > 0) target.free(pinned);
                  pinned = 0;
                  target.close();
                };
              const value: unknown = Reflect.get(target, key);
              return typeof value === "function" ? (value.bind(target) as unknown) : value;
            },
          });
          const op = new SortOperator(
            {
              types: shortTypes,
              keys: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
              limit: null,
              offset: 0,
              withTies: false,
            },
            { grant: pressureGrant, spill, queryId: 301 },
            sink,
          );
          const data: Row[] = Array.from({ length: rows }, (_, i) =>
            Array.from({ length: columns }, (_, c) =>
              afterWide && i === 0 && c === 0
                ? "汉".repeat(360000)
                : `${String(rows - i).padStart(3, "0")}:${String(c)}`,
            ),
          );
          try {
            await pg.exec(
              `DROP TABLE IF EXISTS short_rows; CREATE TABLE short_rows (${shortTypes.map((_, c) => `c${String(c)} TEXT`).join(",")})`,
            );
            for (const row of data)
              await pg.query(
                `INSERT INTO short_rows VALUES (${row.map((_, c) => `$${String(c + 1)}`).join(",")})`,
                [...row],
              );
            const expected = (
              await pg.query<Record<string, unknown>>(
                `SELECT * FROM short_rows ORDER BY c0 COLLATE "C"`,
              )
            ).rows.map((row) => shortTypes.map((_, c) => row[`c${String(c)}`]));
            const pressure = () =>
              expect(
                resident.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes),
              ).toBe(true);
            if (forceEmpty) {
              pinned = grant.held - grant.allocated;
              expect(grant.allocate(pinned)).toBe(true);
              pressure();
              reach.reach("small row step before input");
            }
            for (const batch of batchesOf(shortTypes, data)) {
              if (op.push(batch) === "blocked") await op.ready();
              expect(grant.loaned, "repaid before source readiness").toBe(0);
            }
            if (!forceEmpty) pressure();
            await op.reclaim();
            expect(pages, "normal short-value spill batches rows").toBe(afterWide ? 2 : 1);
            await op.finish();
            await op.closed;
            expect(
              JSON.stringify(sink.rows) === JSON.stringify(expected),
              "PG-exact short-value output",
            ).toBe(true);
            reach.reach("many short output columns");
            if (afterWide) reach.reach("short rows after a wide row");
            if (forceEmpty) {
              expect(loans).toBeGreaterThan(0);
              reach.reach("repaid before source readiness");
            }
          } finally {
            op.abort(new Error("cleanup"));
            await op.closed;
            resident.close();
            grant.close();
            expect(governor.usedBytes).toBe(0);
            expect(spill.bytesInUse).toBe(0);
          }
        },
      ),
      {
        seed,
        numRuns: 7,
        examples: [
          [{ columns: 32, rows: 64, afterWide: false, forceEmpty: false, buffered: false }],
          [{ columns: 32, rows: 64, afterWide: true, forceEmpty: false, buffered: false }],
          [{ columns: 32, rows: 64, afterWide: false, forceEmpty: true, buffered: false }],
          [{ columns: 32, rows: 64, afterWide: true, forceEmpty: true, buffered: false }],
          [{ columns: 32, rows: 64, afterWide: false, forceEmpty: true, buffered: true }],
          [{ columns: 32, rows: 64, afterWide: true, forceEmpty: true, buffered: true }],
        ],
      },
    );
  }, 120000);
