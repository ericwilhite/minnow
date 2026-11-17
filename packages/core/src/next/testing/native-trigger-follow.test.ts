/** Original-target follow is a Minnow extension. PG reference schemas/queries below
 * model that documented identity mapping, not PostgreSQL's omitted ON UPDATE default. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_FOLLOW_REACH } from "../../testing/reach.js";
import { nativeTriggerSession } from "./native-trigger-session.test-support.js";
type Mode = "shift" | "swap" | "rotate";
const text = (v: string | null) => (v === null ? "NULL" : `'${v.replaceAll("'", "''")}'`);
function followScript(
  size: number,
  mode: Mode,
  self: boolean,
  info: boolean,
  payload: string | null,
  nested: boolean,
  nullRef: boolean,
) {
  const direct = mode === "shift" && !info,
    relational = !direct;
  const rootPk = relational
    ? "id INT UNIQUE DEFERRABLE INITIALLY DEFERRED,n INT PRIMARY KEY"
    : "id INT PRIMARY KEY,n INT";
  const pid = self ? "pid INT UNIQUE" : "pid INT";
  const pgPid = self && relational ? "pid INT UNIQUE DEFERRABLE INITIALLY DEFERRED" : pid;
  const control = [
    "CREATE TABLE p_log(tag TEXT,n INT,old_id INT,new_id INT,old_pid INT,new_pid INT)",
    "CREATE TABLE c_log(tag TEXT,cid INT,old_pid INT,new_pid INT)",
    "CREATE TABLE audit(n INT)",
    "CREATE TABLE tally(id INT PRIMARY KEY,v INT)",
    "INSERT INTO tally VALUES(1,0)",
  ];
  const nativeSchema = [
    `CREATE TABLE p(id INT PRIMARY KEY,n INT,${pid}${self ? ` REFERENCES p(id)${info ? " NOT ENFORCED" : ""}` : ""},payload TEXT)`,
    ...(!self
      ? [
          `CREATE TABLE c(id INT PRIMARY KEY,pid INT UNIQUE REFERENCES p(id)${info ? " NOT ENFORCED" : ""},payload TEXT)`,
        ]
      : []),
    ...control,
  ];
  const pgSchema = [
    `CREATE TABLE p(${rootPk},${pgPid},payload TEXT${self && relational ? ",ref_n INT" : ""})`,
    ...(!self
      ? [
          `CREATE TABLE c(id INT PRIMARY KEY,pid INT UNIQUE${relational ? " DEFERRABLE INITIALLY DEFERRED" : ""},payload TEXT${relational ? ",ref_n INT" : ""})`,
        ]
      : []),
    ...control,
  ];
  const ref = (i: number) => (self ? (nullRef && i === 1 ? null : (i % size) + 1) : null);
  const pRows = Array.from({ length: size }, (_, at) => {
    const i = at + 1,
      r = ref(i);
    return `(${String(i)},${String(i)},${r === null ? "NULL" : String(r)},${text(payload)}${self && relational ? `,${r === null ? "NULL" : String(r)}` : ""})`;
  });
  const nativePRows = Array.from({ length: size }, (_, at) => {
    const i = at + 1,
      r = ref(i);
    return `(${String(i)},${String(i)},${r === null ? "NULL" : String(r)},${text(payload)})`;
  });
  if (self && info) {
    nativePRows.push(`(9999,9999,999999,${text(payload)})`);
    pRows.push(`(9999,9999,999999,${text(payload)},NULL)`);
  }
  const childRows = Array.from(
    { length: size },
    (_, at) =>
      `(${String(at + 101)},${String(at + 1)},${text(payload)}${relational ? `,${String(at + 1)}` : ""})`,
  );
  const nativeChildRows = Array.from(
    { length: size },
    (_, at) => `(${String(at + 101)},${String(at + 1)},${text(payload)})`,
  );
  if (!self && info) {
    nativeChildRows.push(`(9999,999999,${text(payload)})`);
    childRows.push(`(9999,999999,${text(payload)},NULL)`);
  }
  if (!self && nullRef) {
    nativeChildRows.push(`(8888,NULL,${text(payload)})`);
    childRows.push(`(8888,NULL,${text(payload)}${relational ? ",NULL" : ""})`);
  }
  const owner = self ? "p" : "c";
  const nativeSeed = [
    `INSERT INTO p VALUES${nativePRows.join(",")}`,
    ...(!self ? [`INSERT INTO c VALUES${nativeChildRows.join(",")}`] : []),
  ];
  const pgSeed = [
    `INSERT INTO p VALUES${pRows.join(",")}`,
    ...(!self ? [`INSERT INTO c VALUES${childRows.join(",")}`] : []),
    `ALTER TABLE ${owner} ADD CONSTRAINT follow_fk FOREIGN KEY(${relational ? "ref_n" : "pid"}) REFERENCES p(${relational ? "n" : "id"})${direct ? " ON UPDATE CASCADE" : ""}`,
  ];
  const specs = [
    {
      name: "p_before",
      table: "p",
      timing: "BEFORE",
      commands: ["INSERT INTO p_log VALUES('B',OLD.n,OLD.id,NEW.id,OLD.pid,NEW.pid)"],
    },
    {
      name: "zz_p_after",
      table: "p",
      timing: "AFTER",
      commands: [
        "INSERT INTO p_log VALUES('A',OLD.n,OLD.id,NEW.id,OLD.pid,NEW.pid)",
        `INSERT INTO audit VALUES(NEW.${self ? "pid" : "id"})`,
      ],
    },
    ...(!self
      ? [
          {
            name: "c_before",
            table: "c",
            timing: "BEFORE",
            commands: ["INSERT INTO c_log VALUES('B',OLD.id,OLD.pid,NEW.pid)"],
          },
          {
            name: "zz_c_after",
            table: "c",
            timing: "AFTER",
            commands: [
              "INSERT INTO c_log VALUES('A',NEW.id,OLD.pid,NEW.pid)",
              "INSERT INTO audit VALUES(NEW.pid)",
            ],
          },
        ]
      : []),
    ...(nested
      ? [
          {
            name: "audit_echo",
            table: "audit",
            timing: "AFTER",
            event: "INSERT",
            commands: ["UPDATE tally SET v=v+COALESCE(NEW.n,0) WHERE id=1"],
          },
        ]
      : []),
  ];
  const sql = `UPDATE p SET id=${mode === "shift" ? "id+7" : mode === "swap" ? "CASE WHEN id=1 THEN 2 WHEN id=2 THEN 1 ELSE id END" : `(id%${String(size)})+1`} WHERE id<=${String(size)}`;
  // Deferred unique business keys permit a swap in PG. ref_n is the original logical
  // target identity (seeded n=id and never changed), not a PG FK to a deferrable key.
  // After the real parent UPDATE, a relational join performs Minnow's documented follow.
  // Real PG child/self triggers run on that UPDATE. Compare final data/log multisets;
  // do not call this raw-PG timing parity for implicit follow. Native phase/barrier/FIFO
  // laws are separately covered by B5b's provided concrete follow tests.
  const reference = relational
    ? `UPDATE ${owner} AS child SET pid=parent.id FROM p AS parent WHERE child.ref_n=parent.n AND child.pid IS DISTINCT FROM parent.id`
    : null;
  return { direct, nativeSchema, pgSchema, nativeSeed, pgSeed, specs, sql, reference };
}
const queries = [
  "SELECT id,n,pid,payload FROM p ORDER BY n",
  "SELECT tag,n,old_id,new_id,old_pid,new_pid FROM p_log ORDER BY tag,n,old_id,new_id,old_pid,new_pid",
  "SELECT * FROM c_log ORDER BY tag,cid,old_pid,new_pid",
  "SELECT * FROM audit ORDER BY n",
  "SELECT * FROM tally ORDER BY id",
];
for (const seed of seedsFor("next-native-trigger-follow", [0xb7ea, 0xb7eb])) {
  const reach = createReach("native trigger follow", NATIVE_TRIGGER_FOLLOW_REACH),
    ledger = new Map<number, { signature: string; completed: number }>(),
    attempts = [0, 0];
  // Replay the same inputs in each group, executing each history exactly once. This
  // bounds coverage cost without dropping any seed, run, example or generated case.
  for (const selfGroup of [false, true])
    it(`matches captured-identity PG references for native follow seed ${String(seed)}, self ${String(selfGroup)}`, async () => {
      const pg = new PGlite();
      let ordinal = 0;
      try {
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 2, max: 5 }),
            fc.constantFrom<Mode>("shift", "swap", "rotate"),
            fc.boolean(),
            fc.boolean(),
            fc.constantFrom<string | null>(null, "", "é漢"),
            fc.boolean(),
            fc.boolean(),
            fc.boolean(),
            async (size, mode, self, info, payload, nested, nullRef, published) => {
              const at = ordinal++,
                signature = JSON.stringify([
                  size,
                  mode,
                  self,
                  info,
                  payload,
                  nested,
                  nullRef,
                  published,
                ]);
              let record = ledger.get(at);
              if (record)
                expect(record.signature, "same follow history in each group").toBe(signature);
              else {
                record = { signature, completed: 0 };
                ledger.set(at, record);
              }
              if (self !== selfGroup) return;
              reach.beginHistory();
              const h = followScript(size, mode, self, info, payload, nested, nullRef);
              const native = await nativeTriggerSession(
                [
                  ...h.nativeSchema,
                  ...h.nativeSeed,
                  ...h.specs.map(
                    (s) =>
                      `CREATE TRIGGER "${s.name}" ${s.timing} ${"event" in s ? s.event : "UPDATE"} ON ${s.table} BEGIN ${s.commands.join(";")}; END`,
                  ),
                ],
                h.sql,
                published,
              );
              try {
                await pg.exec("ROLLBACK;DROP TABLE IF EXISTS c,p,p_log,c_log,audit,tally CASCADE");
                for (const sql of [...h.pgSchema, ...h.pgSeed]) await pg.exec(sql);
                for (const [i, s] of h.specs.entries())
                  await pg.exec(
                    `CREATE OR REPLACE FUNCTION follow_f${String(i)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${s.commands.join(";")}; RETURN NEW; END $$; CREATE TRIGGER "${s.name}" ${s.timing} ${"event" in s ? s.event : "UPDATE"} ON ${s.table} FOR EACH ROW EXECUTE FUNCTION follow_f${String(i)}()`,
                  );
                await pg.exec("BEGIN");
                await pg.exec(h.sql);
                if (h.reference) await pg.exec(h.reference);
                await pg.exec("SET CONSTRAINTS ALL IMMEDIATE;COMMIT");
                await expect(native.execute({ slots: [] })).resolves.toBe(size);
                await native.commit();
                for (const sql of [
                  ...queries,
                  ...(!self ? ["SELECT id,pid,payload FROM c ORDER BY id"] : []),
                ])
                  expect(
                    await native.committedQuery(sql),
                    `follow PG data ${mode}/${self ? "self" : "child"}: ${sql}`,
                  ).toEqual((await pg.query(sql)).rows);
                reach.reach(mode);
                reach.reach(self ? "self reference" : "external child");
                reach.reach(info ? "informational orphan unchanged" : "enforced reference");
                reach.reach(published ? "published" : "resident");
                reach.reach(
                  h.direct ? "raw PG cascade counterpart" : "PG captured-identity reference",
                );
                reach.reach(nested ? "nested tally body" : "no further body");
                reach.reach(
                  payload === null
                    ? "NULL payload"
                    : payload === ""
                      ? "empty payload"
                      : "UTF8 payload",
                );
                if (nullRef) reach.reach("NULL reference unchanged");
                if (mode === "swap" && size > 2) reach.reach("unchanged roots still fire");
                reach.reach("exact original-target rows");
              } finally {
                await native.close();
              }
              reach.reach("zero native ownership");
              record.completed++;
            },
          ),
          {
            seed,
            numRuns: 12,
            examples: [
              [2, "swap", false, false, "é漢", true, false, false],
              [4, "rotate", true, false, null, true, true, true],
              [3, "shift", false, false, "", false, true, false],
              [4, "swap", true, true, "é漢", true, false, true],
              [3, "rotate", false, true, null, false, true, false],
              [3, "shift", true, false, "", true, false, true],
            ],
          },
        );
        attempts[selfGroup ? 1 : 0] = ordinal;
      } finally {
        await pg.close();
      }
    }, 120000);

  it(`executes every follow history once and meets Reach seed ${String(seed)}`, () => {
    expect(attempts[0]).toBeGreaterThan(0);
    expect(attempts[1]).toBe(attempts[0]);
    expect(ledger.size).toBe(attempts[0]);
    for (const record of ledger.values())
      expect(record.completed, "every follow history exactly once").toBe(1);
    reach.assertReached();
  });
}
