/**
 * Landing-page showcase: each entry pairs a displayed snippet with a `run()` that
 * executes the same operations against a throwaway in-memory store. The printed output
 * beside the code is real — if the snippet and the implementation drift, the output
 * shows it.
 */
import {
  MinnowDatabase,
  createMinnow,
  column,
  schema,
  table,
  type Minnow,
  type InferDatabase,
} from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";

export interface ShowcaseExample {
  id: string;
  title: string;
  code: string;
  run: () => Promise<string>;
}

function freshDatabase(): MinnowDatabase {
  return new MinnowDatabase(new MemoryBlockStore());
}

function show(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

const people = table("people", {
  name: column.string().unique(),
  score: column.number(),
  joined: column.datetime().nullable(),
});
const appSchema = schema([people]);
interface DB extends InferDatabase<typeof appSchema> {}

async function typedDb(): Promise<{ database: MinnowDatabase; db: Minnow<DB> }> {
  const database = freshDatabase();
  await database.migrate(appSchema);
  return { database, db: createMinnow<DB>(database, { schema: appSchema }) };
}

const entries: ShowcaseExample[] = [
  {
    id: "sql",
    title: "Tables, batch writes, SQL",
    code: `const database = new MinnowDatabase(store);

await database.createTable({
  name: "people",
  uniqueKey: "name",
  columns: [
    { name: "name", type: "string" },
    { name: "score", type: "number" },
  ],
});

await database.insertBatch("people", [
  { name: "Ada", score: 10 },
  { name: "Grace", score: 25 },
  { name: "Katherine", score: 30 },
  { name: "Linus", score: 25 },
]);

const result = await database.query(\`
  SELECT score, COUNT(*) AS people
  FROM people
  WHERE score >= 20
  GROUP BY score
  ORDER BY score DESC
\`);`,
    run: async () => {
      const database = freshDatabase();
      await database.createTable({
        name: "people",
        uniqueKey: "name",
        columns: [
          { name: "name", type: "string" },
          { name: "score", type: "number" },
        ],
      });
      await database.insertBatch("people", [
        { name: "Ada", score: 10 },
        { name: "Grace", score: 25 },
        { name: "Katherine", score: 30 },
        { name: "Linus", score: 25 },
      ]);
      const result = await database.query(`
        SELECT score, COUNT(*) AS people
        FROM people
        WHERE score >= 20
        GROUP BY score
        ORDER BY score DESC
      `);
      return show(result.rows);
    },
  },
  {
    id: "typed",
    title: "Typed schema, migrations, and writes",
    code: `const people = table("people", {
  name: column.string().unique(),
  score: column.number(),
  joined: column.datetime().nullable(),
});
const appSchema = schema([people]);
await database.migrate(appSchema);

interface DB extends InferDatabase<typeof appSchema> {}
const db = createMinnow<DB>(database, { schema: appSchema });
const written = await db
  .insertInto("people")
  .values({ name: "Ada", score: 10 }) // joined may be omitted; it pads to null
  .returningAll()
  .executeTakeFirstOrThrow();
// { name: "Ada", score: 10, joined: null }`,
    run: async () => {
      const { db } = await typedDb();
      const written = await db
        .insertInto("people")
        .values({ name: "Ada", score: 10 })
        .returningAll()
        .executeTakeFirstOrThrow();
      return show(written);
    },
  },
  {
    id: "builder",
    title: "Query builder compiles to the same plans as SQL",
    code: `const rows = await db
  .selectFrom("people as p")
  .where("p.score", ">=", 20)
  .groupBy("p.score")
  .select((eb) => [eb.ref("p.score").as("score"), eb.fn.countAll().as("people")])
  .orderBy("score", "desc")
  .execute(); // Array<{ score: number; people: number }>`,
    run: async () => {
      const { db } = await typedDb();
      await db
        .insertInto("people")
        .values([
          { name: "Ada", score: 10 },
          { name: "Grace", score: 25 },
          { name: "Katherine", score: 30 },
          { name: "Linus", score: 25 },
        ])
        .execute();
      const rows = await db
        .selectFrom("people as p")
        .where("p.score", ">=", 20)
        .groupBy("p.score")
        .select((eb) => [eb.ref("p.score").as("score"), eb.fn.countAll().as("people")])
        .orderBy("score", "desc")
        .execute();
      return show(rows);
    },
  },
  {
    id: "search",
    title: "Generated keys, defaults, and zero-ceremony search",
    code: `const notes = table("notes", {
  id: column.number().unique().autoIncrement(),
  slug: column.string().default(() => crypto.randomUUID().slice(0, 8)),
  body: column.string(),
});
await database.migrate(schema([notes]));

await db.insertInto("notes").values([
  { body: "The quick brown fox" },       // id and slug generate
  { body: "Lazy dogs sleep quickly" },
  { body: "Columnar storage layout" },
]).execute();

const hits = await db
  .selectFrom("notes")
  .select(["id", "body"])
  .search("quick fo*") // every column is searchable; ranked by BM25, no index DDL
  .execute(); // Array<{ id: number; body: string }>`,
    run: async () => {
      const database = freshDatabase();
      const notes = table("notes", {
        id: column.number().unique().autoIncrement(),
        slug: column.string().default(() => crypto.randomUUID().slice(0, 8)),
        body: column.string(),
      });
      const notesSchema = schema([notes]);
      type NotesDB = InferDatabase<typeof notesSchema>;
      await database.migrate(notesSchema);
      const db = createMinnow<NotesDB>(database, { schema: notesSchema });
      await db
        .insertInto("notes")
        .values([
          { body: "The quick brown fox" },
          { body: "Lazy dogs sleep quickly" },
          { body: "Columnar storage layout" },
        ])
        .execute();
      const hits = await db
        .selectFrom("notes")
        .select(["id", "body"])
        .search("quick fo*")
        .execute();
      return show(hits);
    },
  },
  {
    id: "snapshot",
    title: "Snapshot scopes pin one version; queries are always fresh",
    code: `const before = await database.query(
  "SELECT COUNT(*) AS people FROM people",
); // [{ people: 2 }]

const observed = await database.snapshot(async (session) => {
  const pinned = await session.query("SELECT COUNT(*) AS people FROM people");
  await database.insertBatch("people", [{ name: "Margaret", score: 40 }]);
  const stillPinned = await session.query("SELECT COUNT(*) AS people FROM people");
  const fresh = await database.query("SELECT COUNT(*) AS people FROM people");
  return { pinned: pinned.rows, stillPinned: stillPinned.rows, fresh: fresh.rows };
});
// observed.pinned      → [{ people: 2 }]
// observed.stillPinned → [{ people: 2 }] — the scope holds one version
// observed.fresh       → [{ people: 3 }] — outside the scope, always current`,
    run: async () => {
      const database = freshDatabase();
      await database.createTable({
        name: "people",
        uniqueKey: "name",
        columns: [
          { name: "name", type: "string" },
          { name: "score", type: "number" },
        ],
      });
      await database.insertBatch("people", [
        { name: "Ada", score: 10 },
        { name: "Grace", score: 25 },
      ]);
      const observed = await database.snapshot(async (session) => {
        const pinned = await session.query("SELECT COUNT(*) AS people FROM people");
        await database.insertBatch("people", [{ name: "Margaret", score: 40 }]);
        const stillPinned = await session.query("SELECT COUNT(*) AS people FROM people");
        const fresh = await database.query("SELECT COUNT(*) AS people FROM people");
        return { pinned: pinned.rows, stillPinned: stillPinned.rows, fresh: fresh.rows };
      });
      return show(observed);
    },
  },
];

/** Typed-first ordering: the showcase leads with the best-practice API. */
export const showcaseExamples: ShowcaseExample[] = [
  "typed",
  "search",
  "builder",
  "sql",
  "snapshot",
].map((id) => {
  const entry = entries.find((candidate) => candidate.id === id);
  if (entry === undefined) throw new Error(`Missing showcase example: ${id}`);
  return entry;
});
