/** Shared SQL specifications only. PostgreSQL models the documented captured-identity
 * follow extension explicitly, never by changing PG expectations to native output. */
export type Mode = "shift" | "swap" | "rotate";
const text = (v: string | null) => (v === null ? "NULL" : `'${v.replaceAll("'", "''")}'`);
export function followScript(
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
export const followQueries = [
  "SELECT id,n,pid,payload FROM p ORDER BY n",
  "SELECT tag,n,old_id,new_id,old_pid,new_pid FROM p_log ORDER BY tag,n,old_id,new_id,old_pid,new_pid",
  "SELECT * FROM c_log ORDER BY tag,cid,old_pid,new_pid",
  "SELECT * FROM audit ORDER BY n",
  "SELECT * FROM tally ORDER BY id",
];
