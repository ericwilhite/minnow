/**
 * A PGlite started from the run's initdb snapshot (scripts/pglite-snapshot.client.ts) must be
 * indistinguishable from one that ran initdb itself, or a test could pass or fail because of how
 * its database was started. Compared on what a test could observe: the whole system catalog, every
 * setting, the transaction and OID counters, and the next objects a test creates.
 */
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";

/** Everything observable about a just-started cluster, in a stable order. */
async function fingerprint(db: PGlite): Promise<unknown> {
  const rows = async (sql: string) => (await db.query(sql)).rows;
  return {
    classes: await rows(
      "select oid::int, relname, relnamespace::int, relkind, relpages, reltuples from pg_class order by oid",
    ),
    types: await rows("select oid::int, typname from pg_type order by oid"),
    procs: await rows("select count(*)::int as n, max(oid)::int as top from pg_proc"),
    attributes: await rows("select count(*)::int as n from pg_attribute"),
    namespaces: await rows("select oid::int, nspname from pg_namespace order by oid"),
    databases: await rows(
      "select datname, datfrozenxid::text, datminmxid::text, encoding, datcollate from pg_database order by datname",
    ),
    settings: await rows("select name, setting from pg_settings order by name"),
    roles: await rows("select rolname, rolsuper from pg_roles order by rolname"),
    xid: await rows("select txid_current()::text as xid"),
    created: await (async () => {
      await db.exec("create table probe (id int primary key, v text); create sequence probe_seq");
      return rows(
        "select 'probe'::regclass::oid::int as table_oid, 'probe_seq'::regclass::oid::int as seq_oid, nextval('probe_seq')::int as next",
      );
    })(),
    stats: await rows(
      "select count(*)::int as n from pg_stat_user_tables where n_tup_ins > 0 or seq_scan > 0",
    ),
  };
}

describe("PGlite initdb snapshot", () => {
  it("is in use for default-option instances in this run", () => {
    const path = process.env.MINNOW_PGLITE_SNAPSHOT ?? "";
    expect(path === "off" || path.endsWith("initdb.tar"), path).toBe(true);
  });

  it("starts a cluster indistinguishable from a fresh initdb", { timeout: 120_000 }, async () => {
    // No arguments: the snapshot, when the run has one. An options argument: a real initdb.
    const fromSnapshot = new PGlite();
    const fresh = new PGlite({});
    try {
      expect(await fingerprint(fromSnapshot)).toEqual(await fingerprint(fresh));
    } finally {
      await Promise.all([fromSnapshot.close(), fresh.close()]);
    }
  });

  it("starts PGlite.create() from the snapshot too, and passes any argument through", async () => {
    const created = await PGlite.create();
    try {
      expect(created).toBeInstanceOf(PGlite);
      expect((await created.query<{ one: number }>("select 1 as one")).rows).toEqual([{ one: 1 }]);
    } finally {
      await created.close();
    }
  });
});
