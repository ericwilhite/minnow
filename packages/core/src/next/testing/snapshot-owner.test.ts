/** next/ public snapshot API -> owner -> real native exporter/spool/loader/recovery. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_OWNER_API_REACH } from "../../testing/reach.js";
import { snapshotOwner, ownerCatalog, nativeNext } from "./snapshot-owner-support.js";

for (const seed of seedsFor("next-snapshot-owner-api", [0xb2e3, 0xb2e4]))
  it(`public snapshot replacement and continued writes on both adapters ${String(seed)}`, async () => {
    const reach = createReach("snapshot owner API", SNAPSHOT_OWNER_API_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 5 }),
        fc.boolean(),
        fc.boolean(),
        async (count, stored, streamed) => {
          reach.beginHistory();
          for (const kind of ["OPFS", "IndexedDB"] as const) {
            const source = await snapshotOwner(kind, "source"),
              target = await snapshotOwner(kind, "target");
            try {
              const from = await source.current.connect(),
                into = await target.current.connect();
              await from.query("CREATE TABLE t (id INT PRIMARY KEY, payload TEXT)");
              await from.query("CREATE INDEX lookup_id ON t (id)");
              await from.query("CREATE SEQUENCE s");
              const rows: unknown[][] = [];
              for (let n = 1; n <= count; n++) {
                const text = n % 3 === 0 ? null : `value ${String(n)} é`;
                await from.query("INSERT INTO t VALUES ($1, $2)", [n, text]);
                rows.push([n, text]);
              }
              expect(await nativeNext(source)).toBe(1n);
              const sourcePath = source.current.path.writePath;
              if (stored) await sourcePath.recycleLog();
              const proof = await source.current.governor.admit({
                label: "fixture representation",
                minimumBytes: 1 << 20,
                spillable: false,
              });
              const pin = sourcePath.pin();
              try {
                const table = pin.table(1, { budget: proof });
                const segments = table.segments.filter((segment) => segment.rowCount > 0);
                expect(segments.length).toBeGreaterThan(0);
                if (stored) {
                  expect(segments.every((segment) => segment.kind === "group")).toBe(true);
                  reach.reach("stored source");
                } else {
                  expect(segments.some((segment) => segment.kind === "memory")).toBe(true);
                  reach.reach("resident source");
                }
              } finally {
                pin.release();
                proof.close();
              }
              await into.query("CREATE TABLE t (id INT PRIMARY KEY, payload INT)");
              await into.query("CREATE INDEX lookup_id ON t (id)");
              await into.query("CREATE SEQUENCE s");
              await into.query("INSERT INTO t VALUES (99, -1)");
              expect((await into.query("SELECT payload FROM t ORDER BY id")).rows()).toEqual([
                [-1],
              ]);
              const catalog = await ownerCatalog(from),
                oldId = target.current.engine.databaseId;
              const progress: string[] = [];
              let bytes: Uint8Array;
              if (streamed) {
                const chunks: Uint8Array[] = [];
                for await (const chunk of from.exportSnapshotStream({
                  onProgress: (p) => progress.push(p.phase),
                })) {
                  expect(chunk.byteOffset).toBe(0);
                  expect(chunk.length).toBe(chunk.buffer.byteLength);
                  expect(chunk.length).toBeLessThanOrEqual(1 << 20);
                  chunks.push(chunk);
                }
                bytes = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0));
                let at = 0;
                for (const chunk of chunks) {
                  bytes.set(chunk, at);
                  at += chunk.length;
                }
                await into.importSnapshotStream(
                  (async function* () {
                    for (let at = 0; at < bytes.length; at += 137)
                      yield bytes.subarray(at, at + 137);
                  })(),
                  { onProgress: (p) => progress.push(p.phase) },
                );
                reach.reach("stream API");
              } else {
                bytes = await from.exportSnapshot({ onProgress: (p) => progress.push(p.phase) });
                await into.importSnapshot(bytes, { onProgress: (p) => progress.push(p.phase) });
                reach.reach("array API");
              }
              expect(bytes.length).toBeGreaterThan(64);
              expect(progress.filter((p) => p === "done")).toHaveLength(2);
              expect(target.current.engine.databaseId).not.toBe(oldId);
              await expect(into.query("SELECT * FROM t")).rejects.toMatchObject({
                code: "MINNOW_DATABASE_REPLACED",
              });
              const fresh = await target.current.connect(false, into.clientId);
              reach.reach("fresh incarnation same client ID");
              expect((await fresh.query("SELECT id, payload FROM t ORDER BY id")).rows()).toEqual(
                rows,
              );
              expect((await fresh.query("SELECT payload FROM t ORDER BY id")).rows()).toEqual(
                rows.map((row) => [row[1]]),
              );
              expect(await ownerCatalog(fresh)).toEqual(catalog);
              expect(await nativeNext(target)).toBe(2n);
              await fresh.query("INSERT INTO t VALUES (100, 'after restore')");
              const reopened = await target.restart(),
                again = await reopened.connect();
              expect((await again.query("SELECT id, payload FROM t ORDER BY id")).rows()).toEqual([
                ...rows,
                [100, "after restore"],
              ]);
              reach.reach("rows and catalog");
              reach.reach("sequence high water");
              reach.reach("old client refused");
              reach.reach("write and reopen");
              reach.reach(kind);
            } finally {
              await target.close();
              await source.close();
            }
          }
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [1, false, false],
          [2, true, true],
        ],
      },
    );
    reach.assertReached();
  }, 180000);
