/** Gate D / governor rules 1, 2, 4: actual commit-buffer capacities stay charged and bounded. */
import fc from "fast-check";
import { describe, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_COMMIT_BUFFER_STATES } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { createIndexedDbBackend } from "../storage/indexeddb.js";
import { SimLocks } from "../storage/sim-disk.js";
import { simIndexedDb } from "../storage/sim-idb.js";
import { encodeBody } from "./body.js";
import { ByteWriter } from "./bytes.js";
import { CommitBuffers, COMMIT_MINIMUM_BYTES, SMALL_COMMIT_BYTES } from "./commit-buffers.js";
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
  rowsOf,
  rules,
} from "./test-support.js";

const MiB = 1024 * 1024;
const reach = createReach("charged commit buffer ownership", WRITE_COMMIT_BUFFER_STATES);
const scenario = fc.record({
  rows: fc.constantFrom(32, 768, 2048),
  width: fc.constantFrom(16, 768, 1536),
  columns: fc.constantFrom(1, 2),
  sameTransaction: fc.boolean(),
  capMiB: fc.constantFrom(32, 48),
  strict: fc.boolean(),
  request: fc.boolean(),
  largeValue: fc.boolean(),
  largeCatalog: fc.boolean(),
});

for (const backend of ["memory", "indexeddb"] as const)
  describe(`bounded commit encoding over ${backend}`, () => {
    for (const seed of seedsFor("next-write-commit-buffers", [0xb3c]))
      it(`preserves bytes, rows and independent live-buffer accounting (seed ${String(seed)})`, async () => {
        await fc.assert(
          fc.asyncProperty(scenario, async (s) => {
            const count = s.largeValue ? 8 : s.rows;
            const sameTransaction = s.sameTransaction || s.largeCatalog;
            const defaultSql = s.largeCatalog ? `'${"x".repeat(600000)}'` : null;
            const width = s.largeValue ? 80 * 1024 : s.width;
            const request = s.request
              ? {
                  clientId: new Uint8Array(16).fill(0x39),
                  sequence: 7,
                  sentAtMs: 1234.5,
                  counts: { inserted: count, "emoji😀": 1 },
                }
              : null;
            reach.beginHistory();
            if (s.request) reach.reach("request metadata");
            if (s.largeValue) reach.reach("value over 64 KiB");
            reach.reach(backend);
            const governor = new Governor(s.capMiB * MiB);
            const grants: Grant[] = [];
            const admit = governor.admit.bind(governor);
            governor.admit = async (options) => {
              const grant = await admit(options);
              grants.push(grant);
              return grant;
            };
            const live = new Set<Uint8Array>();
            let independentPeak = 0;
            let capacityPeak = 0;
            const h = new Harness(s.strict ? "strict" : "relaxed", "memory", undefined, undefined, {
              onCommitBuffer: (buffer, owned) => {
                const grant = grants.find((g) => g.label === "write commit buffers" && g.held > 0);
                if (!grant) throw new Error("No commit-buffer grant");
                if (owned) {
                  if (live.has(buffer)) throw new Error("Commit buffer owned twice");
                  live.add(buffer);
                } else if (!live.delete(buffer)) throw new Error("Commit buffer released twice");
                const bytes = [...live].reduce((sum, value) => sum + value.buffer.byteLength, 0);
                if (owned && grant.allocated < bytes)
                  throw new Error("Live commit-buffer bytes exceed governor allocation");
                if (owned) {
                  capacityPeak = Math.max(capacityPeak, bytes);
                  const other = grants
                    .filter((g) => g !== grant)
                    .reduce((sum, g) => sum + g.allocated, 0);
                  independentPeak = Math.max(independentPeak, other + bytes);
                  expect(governor.peakBytes).toBeGreaterThanOrEqual(independentPeak);
                }
              },
            });
            Object.assign(h, { governor });
            if (backend === "indexeddb")
              Object.assign(h, {
                backend: createIndexedDbBackend({
                  database: () => simIndexedDb(h.disk),
                  name: "commit-buffers",
                  locks: new SimLocks(),
                  memory: governor.registerCache({ evictableBytes: 0, evict: () => 0 }),
                }),
              });
            const open = h.backend.open.bind(h.backend);
            h.backend.open = async (lease) => {
              const session = await open(lease);
              const append = session.appendLog.bind(session);
              session.appendLog = async (bytes) => {
                if (![...live].some((buffer) => buffer.buffer === bytes.buffer))
                  throw new Error("Log append buffer has no governor charge");
                return append(bytes);
              };
              return session;
            };
            let path = await h.open();
            let table = 0;
            const definition = tableInput("t", [
              {
                ...column("id", INT, false),
                ...(s.largeCatalog ? { identity: { start: 1n, increment: 1n } } : {}),
              },
              ...Array.from({ length: s.columns }, (_, k) => ({
                ...column(`v${String(k)}`, TEXT),
                defaultSql: k === 0 ? defaultSql : null,
              })),
            ]);
            if (!sameTransaction)
              await autocommit(path, async (statement) => {
                table =
                  (await statement.changeCatalog({ kind: "createTable", table: definition })).id ??
                  0;
              });
            const tx = await path.begin();
            const statement = tx.statement({ rules });
            if (sameTransaction) {
              reach.reach("catalog and rows together");
              table =
                (await statement.changeCatalog({ kind: "createTable", table: definition })).id ?? 0;
            }
            const ids = Array.from({ length: count }, (_, k) => k);
            const values = ids.map((id) => `${String(id)}-"\\\u0000😀${"x".repeat(width)}`);
            try {
              await statement
                .table(table)
                .insert(
                  batch(ints(ids), ...Array.from({ length: s.columns }, () => texts(values))),
                  null,
                );
              await statement.end();
              const impl = tx;
              const expected = encodeBody({
                request,
                effects: impl.effects(new ByteWriter()),
              });
              const scratch = new Uint8Array(4096);
              // Test-only full output: compare both whole and deliberately unaligned windows.
              const actual = new Uint8Array(expected.length);
              for (let at = 0; at < actual.length; at += 65531)
                await streamBody(
                  impl,
                  request,
                  () => null,
                  new ByteWindow(
                    actual.subarray(at, Math.min(actual.length, at + 65531)),
                    at,
                    scratch,
                  ),
                );
              expect(Buffer.compare(actual, expected), "byte-identical legacy body encoding").toBe(
                0,
              );
              reach.reach(
                expected.length > SMALL_COMMIT_BYTES ? "streamed body" : "single-buffer body",
              );
              const stringify = JSON.stringify;
              const jsonSpy = s.largeCatalog
                ? vi
                    .spyOn(JSON, "stringify")
                    .mockImplementation((...args: Parameters<typeof JSON.stringify>) => {
                      const value: unknown = args[0];
                      if (
                        typeof value === "object" &&
                        value !== null &&
                        "op" in value &&
                        value.op === "createTable"
                      )
                        throw new Error("Large catalog stringified before streaming");
                      return Reflect.apply(stringify, JSON, args);
                    })
                : null;
              try {
                const receipt = await tx.commit(request);
                await receipt.durable;
              } finally {
                jsonSpy?.mockRestore();
              }
              expect(capacityPeak).toBeLessThanOrEqual(COMMIT_MINIMUM_BYTES);
              expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
              expect(governor.peakBytes).toBeGreaterThanOrEqual(independentPeak);
              reach.reach(s.strict ? "strict" : "relaxed");
              const view = path.pin();
              try {
                const rows = await rowsOf(view, table);
                expect(rows.length).toBe(count);
                for (let k = 0; k < rows.length; k++)
                  expect(rows[k]).toEqual([
                    k,
                    ...Array.from({ length: s.columns }, () => values[k]),
                  ]);
              } finally {
                view.release();
              }
              path = await h.reopen();
              expect(path.catalog.table(table)?.columns[1]?.defaultSql).toBe(defaultSql);
              if (s.largeCatalog) {
                const sequence = path.catalog.table(table)?.columns[0]?.identitySequence;
                expect(sequence).not.toBeNull();
                expect(path.catalog.sequence(sequence ?? 0)?.start).toBe(1n);
                reach.reach("streamed catalog and bigint tags");
              }
              const reopened = path.pin();
              try {
                expect((await rowsOf(reopened, table)).length).toBe(count);
              } finally {
                reopened.release();
              }
              reach.reach("durable reopen");
              if (process.env.MINNOW_COMMIT_BUFFER_REPORT)
                console.info(
                  JSON.stringify({
                    backend,
                    rows: count,
                    width,
                    columns: s.columns,
                    cap: governor.capBytes,
                    governorPeak: governor.peakBytes,
                    independentPeak,
                    bufferCapacityPeak: capacityPeak,
                  }),
                );
            } finally {
              await h.close();
            }
            expect(live.size).toBe(0);
            expect(governor.usedBytes).toBe(0);
            reach.reach("ownership released once");
          }),
          { seed, numRuns: 24 },
        );
      }, 180_000);
  });
for (const seed of seedsFor("next-write-commit-construction", [0xa110c]))
  it(`charges actual constructors with and without observation (seed ${String(seed)})`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          observed: fc.boolean(),
          steps: fc.array(
            fc.record({
              release: fc.boolean(),
              slot: fc.nat(),
              bytes: fc.constantFrom(1, 31, 4096, 65536),
            }),
            { minLength: 1, maxLength: 24 },
          ),
        }),
        async (s) => {
          reach.beginHistory();
          const governor = new Governor(32 * MiB);
          const grant = await governor.admit({
            label: "commit construction witness",
            minimumBytes: COMMIT_MINIMUM_BYTES,
            spillable: false,
          });
          const pool = new CommitBuffers(grant, s.observed ? () => undefined : undefined);
          const live = new Set<Uint8Array>();
          const failures: string[] = [];
          let creating = false;
          const original = globalThis.Uint8Array;
          vi.stubGlobal(
            "Uint8Array",
            new Proxy(original, {
              construct(target, args) {
                if (creating) {
                  const argument: unknown = args[0];
                  const bytes = typeof argument === "number" ? argument : 0;
                  const existing = [...live].reduce(
                    (sum, buffer) => sum + buffer.buffer.byteLength,
                    0,
                  );
                  if (grant.allocated < existing + bytes)
                    failures.push("Commit bytes were not charged before the actual constructor");
                }
                return Reflect.construct(target, args) as Uint8Array;
              },
            }),
          );
          try {
            for (const step of s.steps) {
              if (step.release && live.size > 0) {
                const buffer = [...live][step.slot % live.size];
                if (buffer === undefined) throw new Error("No live test buffer");
                pool.release(buffer);
                live.delete(buffer);
              } else {
                creating = true;
                const buffer = pool.create(step.bytes);
                creating = false;
                expect(live.has(buffer)).toBe(false);
                live.add(buffer);
                if (failures.length > 0) throw new Error(failures.join("; "));
              }
              expect(grant.allocated).toBeGreaterThanOrEqual(
                [...live].reduce((sum, buffer) => sum + buffer.buffer.byteLength, 0),
              );
            }
            for (const buffer of live) pool.release(buffer);
            live.clear();
            expect(grant.allocated).toBe(0);
            reach.reach(
              s.observed ? "observed constructor precharge" : "production constructor precharge",
            );
          } finally {
            creating = false;
            vi.unstubAllGlobals();
            pool.close();
          }
          expect(governor.usedBytes).toBe(0);
        },
      ),
      { seed, numRuns: 100 },
    );
  });
it("reaches charged commit buffer states", () => reach.assertReached());
