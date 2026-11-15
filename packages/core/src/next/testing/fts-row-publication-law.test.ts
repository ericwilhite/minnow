import { need } from "../write/bytes.js";
import { decodeManifestRoot } from "../format/manifest-codec.js";
import { decodeBody } from "../write/body.js";
import { scanLog } from "../format/log-frame.js";
import type { StoreSession } from "../storage/contract.js";
/** Law: accepted rows and their accelerator claim have one WAL commit point; preparation
 * is durably protected first. Authority: B7-fts-1.0-design and the approved write seam.
 * Uses only parent APIs so the identical generated law runs RED before integration.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_ROW_PUBLICATION_REACH } from "../../testing/reach.js";
import {
  Harness,
  autocommit,
  column,
  tableInput,
  TEXT,
  texts,
  batch,
} from "../write/test-support.js";
import { ftsDef } from "../access/fts-def.js";
import { ftsSemantics } from "../fts-config.js";
import { ftsValue, FTS_WORKSPACE_BYTES } from "../exec/fts-value.js";
import type { FtsWriteRenderer } from "../access/fts-write.js";

for (const seed of seedsFor("next-fts-row-publication", [0xb841, 0xb842]))
  it(`publishes accepted row/FTS histories atomically ${String(seed)}`, async () => {
    const reach = createReach("FTS row publication", FTS_ROW_PUBLICATION_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("strict" as const, "relaxed" as const),
        fc.array(fc.constantFrom(null, "", "a a", "日本語", "ＦＯＯ 42"), {
          minLength: 1,
          maxLength: 5,
        }),
        fc.boolean(),
        async (durability, values, savepoint) => {
          reach.beginHistory();
          const h = new Harness(durability);
          let session: StoreSession | undefined,
            watch = false,
            protectedAppends = 0;
          const funding: import("../storage/extents.js").ByteBudget = {
            allocate: () => true,
            free: () => undefined,
          };
          const open = h.backend.open.bind(h.backend);
          h.backend.open = async (...args) => {
            const native = await open(...args);
            session = native;
            const append = native.appendLog.bind(native);
            native.appendLog = async (bytes) => {
              if (watch) {
                const raw = await native.readRoot();
                expect(raw).not.toBeNull();
                const prepared = decodeManifestRoot(need(raw), funding);
                expect(
                  (prepared.value as unknown as { fts?: { staged: unknown } }).fts?.staged,
                  "FTS pages must be protected BEFORE the row frame append",
                ).toBeTruthy();
                funding.free(prepared.charge);
                protectedAppends++;
              }
              return append(bytes);
            };
            return native;
          };
          try {
            const path = await h.open();
            (path as { installFtsRenderer?: (r: FtsWriteRenderer) => void }).installFtsRenderer?.({
              ...ftsSemantics(),
              workspaceBytes: FTS_WORKSPACE_BYTES,
              render: ftsValue,
            });
            await autocommit(path, async (s) => {
              await s.changeCatalog({
                kind: "createTable",
                table: tableInput("docs", [column("body", TEXT)], { primaryKey: null }),
              });
              const { id: _id, ...fts } = ftsDef(1, 1, 1);
              void _id;
              await s.changeCatalog({ kind: "createFts", fts });
            });
            const tx = await path.begin();
            const s = tx.statement({
              rules: () => ({ check: () => null, generate: () => undefined }),
            });
            await s.table(1).insert(batch(texts(values)), null);
            await s.end();
            reach.reach("multiple statements");
            if (savepoint) {
              const point = tx.savepoint();
              const rolled = tx.statement({
                rules: () => ({ check: () => null, generate: () => undefined }),
              });
              await rolled.table(1).insert(batch(texts(["rolled back"])), null);
              await rolled.end();
              tx.rollbackTo(point);
              reach.reach("savepoint");
            }
            const rolled = tx.statement({
              rules: () => ({ check: () => null, generate: () => undefined }),
            });
            await rolled.table(1).insert(batch(texts(["statement rollback"])), null);
            rolled.rollback();
            reach.reach("rollback");
            watch = true;
            const receipt = await tx.commit(null);
            await receipt.durable;
            watch = false;
            expect(protectedAppends).toBeGreaterThan(0);
            if (!session) throw new Error("Missing native session");
            const scanned = scanLog(
              await session.readLog(0, session.logLength()),
              await session.readLogHeaders(),
              path.state().root.log,
            );
            const bodies = scanned.frames
              .filter((frame) => frame.partCount === 1)
              .map((frame) => decodeBody(frame.body));
            const rowBodies = bodies.filter((body) => body.effects.some((e) => e.kind === "rows"));
            expect(rowBodies).toHaveLength(1);
            expect(rowBodies[0]?.effects.filter((e) => e.kind === "fts")).toHaveLength(1);
            reach.reach("protected before append");
            reach.reach("row and FTS body");
            reach.reach(durability);
            if (values.includes(null)) reach.reach("NULL");
            if (values.includes("")) reach.reach("empty");
          } finally {
            await h.close();
          }
        },
      ),
      { seed, numRuns: 24 },
    );
    reach.assertReached();
  }, 60_000);
