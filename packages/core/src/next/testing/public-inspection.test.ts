/** Canonical metadata is detached, funded through delivery, and scoped to native SQL state. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_INSPECTION_REACH } from "../../testing/reach.js";
import { ProtocolClient } from "../owner/client.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";

const types = ["INTEGER", "BIGINT", "NUMERIC(12,4)", "TIMESTAMP", "VARCHAR(23)", "mood"] as const;
for (const seed of seedsFor("next-public-inspection", [46964, 46965]))
  it(`describes native catalogs/plans without retaining metadata charge (${String(seed)})`, async () => {
    const pg = new PGlite(),
      reach = createReach("public inspection", PUBLIC_INSPECTION_REACH);
    try {
      await pg.exec("CREATE TYPE mood AS ENUM('sad','ok')");
      await fc.assert(
        fc.asyncProperty(
          fc.array(fc.constantFrom(...types), { minLength: 1, maxLength: 6 }),
          fc.integer({ min: 1, max: 16384 }),
          async (columns, width) => {
            reach.beginHistory();
            const owner = await publicOwner({ planCacheBytes: 0 });
            const db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
            const governor = owner.core.harness.governor;
            const channel = new MessageChannel(),
              protocol = new ProtocolClient();
            owner.server.accept(channel.port2);
            await protocol.connect(channel.port1);
            const other = new MinnowDatabaseClient(protocol, { ready: Promise.resolve() });
            const postedCharges: number[] = [];
            let baseline: number;
            let metadataPosts = 0;
            const post = owner.channel.port2.postMessage.bind(owner.channel.port2);
            const delivery = vi
              .spyOn(owner.channel.port2, "postMessage")
              .mockImplementation(
                (message: unknown, options?: Transferable[] | StructuredSerializeOptions) => {
                  const value = message as {
                    t?: string;
                    value?: { version?: number; tables?: Array<{ name: string }> };
                  };
                  if (value.t === "value" && value.value?.version === 1) {
                    metadataPosts++;
                    postedCharges.push(governor.usedBytes);
                    if (Array.isArray(options)) post(message, options);
                    else post(message, options);
                    // Mutation after the synchronous clone must not affect the receiver.
                    const table = value.value.tables?.[0];
                    if (table) table.name = "corrupt-after-post";
                  } else if (Array.isArray(options)) post(message, options);
                  else post(message, options);
                },
              );
            try {
              const ddl = `CREATE TABLE t(id INT PRIMARY KEY,${columns.map((type, i) => `"a.b${String(i)}" ${type}`).join(",")},v TEXT DEFAULT 'default')`;
              await pg.exec("DROP TABLE IF EXISTS t");
              await pg.exec(ddl);
              await db.execute("CREATE TYPE mood AS ENUM('sad','ok')");
              await db.execute(ddl);
              await db.execute("INSERT INTO t(id,v) VALUES(1,$1)", ["Ж".repeat(width)]);
              await db.catalog(); // Fence releases from preceding query frames.
              baseline = governor.usedBytes;
              const description = await db.describeCatalog();
              await db.catalog(); // Receipt ACK precedes this fence on the same port.
              expect(governor.usedBytes).toBe(baseline);
              expect(postedCharges[0]).toBeGreaterThan(baseline);
              expect(description.version).toBe(1);
              const table = description.tables.find((table) => table.name === "t");
              expect(table).toBeDefined();
              const expected = await pg.query<{ name: string; sqltype: string; nullable: boolean }>(
                "SELECT a.attname name,format_type(a.atttypid,a.atttypmod) sqltype,NOT a.attnotnull nullable FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relname='t' AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",
              );
              expect(
                table?.columns.map((column) => ({
                  name: column.name,
                  sqltype: column.sqlType,
                  nullable: column.nullable,
                })),
              ).toEqual(expected.rows);
              expect(table?.indexes).toMatchObject([
                { role: "primary", unique: true, directions: ["asc"] },
              ]);
              expect(table?.columns.at(-1)?.defaultSql).toContain("default");
              expect(
                description.types.map((type) => ({ name: type.name, labels: type.labels })),
              ).toEqual([{ name: "mood", labels: ["sad", "ok"] }]);
              expect(JSON.stringify(description)).not.toContain("Ж".repeat(Math.min(width, 100)));
              reach.reach("PG catalog types and names");
              reach.reach("funded metadata delivery");
              reach.reach("detached caller metadata");
              const handler = owner.channel.port1.onmessage;
              const receipt: { held: MessageEvent | null } = { held: null };
              let delivered: () => void = () => undefined;
              const captured = new Promise<void>((resolve) => {
                delivered = resolve;
              });
              owner.channel.port1.onmessage = (event) => {
                if ((event.data as { t?: string }).t === "value") {
                  receipt.held = event;
                  delivered();
                } else handler?.call(owner.channel.port1, event);
              };
              try {
                const receiving = db.describeCatalog();
                await captured;
                expect(governor.usedBytes).toBeGreaterThan(baseline);
                if (receipt.held === null) throw new Error("Missing held metadata");
                owner.channel.port1.onmessage = handler;
                handler?.call(owner.channel.port1, receipt.held);
                const received = await receiving;
                await db.catalog();
                expect(governor.usedBytes).toBe(baseline);
                expect(received.tables[0]?.name).toBe("t");
              } finally {
                owner.channel.port1.onmessage = handler;
              }
              reach.reach("queued reply owns credit");

              const admit = governor.admitGroup.bind(governor);
              let changed = false;
              const admission = vi
                .spyOn(governor, "admitGroup")
                .mockImplementation(async (options) => {
                  if (options.label === "owner inspection" && !changed) {
                    changed = true;
                    await other.execute("ALTER TABLE t ADD COLUMN switched TEXT");
                  }
                  return admit(options);
                });
              try {
                const current = await db.describeCatalog();
                expect(
                  current.tables.find((table) => table.name === "t")?.columns.at(-1)?.name,
                ).toBe("switched");
                expect(current.catalogVersion).toBeGreaterThan(description.catalogVersion);
              } finally {
                admission.mockRestore();
              }
              reach.reach("catalog drift during admission");

              await db.catalog();
              const beforePlan = governor.usedBytes;
              const explained = await db.explain("SELECT id FROM t WHERE id=314159265");
              expect(explained).toMatchObject({
                version: 1,
                command: "SELECT",
                kind: "query",
                columns: [{ name: "id", sqlType: "integer", nullable: false }],
              });
              expect(explained.parameters).toEqual([]);
              expect(explained.text).toContain("keyLookup");
              expect(explained.text).not.toContain("314159265");
              expect(explained.text).toContain("not executed");
              await db.catalog();
              expect(governor.usedBytes).toBe(beforePlan);
              expect((await db.explain("SELECT $1::bigint n")).parameters).toEqual(["bigint"]);
              await db.explain("INSERT INTO t(id,v) VALUES(2,'should-not-write')");
              expect((await db.query("SELECT count(*)::int n FROM t")).tuples()).toEqual([[1]]);
              await db.execute("CREATE SEQUENCE audit_seq");
              await db.explain("SELECT nextval('audit_seq')");
              // Lowering knows the sequence; this base intentionally refuses execution.
              await expect(db.query("SELECT nextval('audit_seq')")).rejects.toMatchObject({
                sqlState: "0A000",
                construct: "function",
              });
              reach.reach("explain never executes");
              reach.reach("explain hides literal payload");
              await db.execute("BEGIN");
              await db.execute("CREATE TABLE private_ddl(k INT)");
              const privateCatalog = await db.describeCatalog();
              expect(privateCatalog.tables.some((table) => table.name === "private_ddl")).toBe(
                true,
              );
              expect(
                (await other.describeCatalog()).tables.some(
                  (table) => table.name === "private_ddl",
                ),
              ).toBe(false);
              expect(
                (await db.explain("SELECT k FROM private_ddl")).columns.map(
                  (column) => column.name,
                ),
              ).toEqual(["k"]);
              await db.execute("ROLLBACK");
              expect(
                (await db.describeCatalog()).tables.some((table) => table.name === "private_ddl"),
              ).toBe(false);
              expect(privateCatalog.tables.some((table) => table.name === "private_ddl")).toBe(
                true,
              );
              expect(
                (await other.describeCatalog()).tables.some(
                  (table) => table.name === "private_ddl",
                ),
              ).toBe(false);
              expect(description.tables.map((table) => table.name)).toEqual(["t"]);
              reach.reach("private DDL and rollback");
              expect(metadataPosts).toBeGreaterThan(5);
            } finally {
              delivery.mockRestore();
              await other.close();
              channel.port1.close();
              channel.port2.close();
              await db.close();
              await owner.close();
              expect(governor.usedBytes).toBe(0);
            }
          },
        ),
        {
          seed,
          numRuns: 8,
          examples: [
            [types.slice(), 16384],
            [["mood"], 1],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 60000);
