/**
 * Landing-page showcase: each entry pairs a displayed snippet with a `run()` that
 * executes the same operations against a throwaway in-memory store. The printed output
 * beside the code is real — if the snippet and the implementation drift, the output
 * shows it.
 */
import {
  BrowserDatabase,
  createBrowserDb,
  column,
  schema,
  table,
  type BrowserDb,
  type InferDatabase,
} from "@browserdatabase/engine";
import { MemoryBlockStore } from "@browserdatabase/storage-idb";

export interface ShowcaseExample {
  id: string;
  title: string;
  code: string;
  run: () => Promise<string>;
}

function freshDatabase(): BrowserDatabase {
  return new BrowserDatabase(new MemoryBlockStore());
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
type DB = InferDatabase<typeof appSchema>;

async function typedDb(): Promise<{ database: BrowserDatabase; db: BrowserDb<DB> }> {
  const database = freshDatabase();
  await database.migrate(appSchema);
  return { database, db: createBrowserDb(database, { schema: appSchema }) };
}

const entries: ShowcaseExample[] = [
  {
    id: "sql",
    title: "Tables, column batches, SQL",
    code: `const database = new BrowserDatabase(store);

await database.createTable({
  name: "people",
  uniqueKey: "name",
  columns: [
    { name: "name", type: "string" },
    { name: "score", type: "number" },
  ],
});

await database.insertBatch("people", {
  columns: {
    name: ["Ada", "Grace", "Katherine", "Linus"],
    score: [10, 25, 30, 25],
  },
});

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
      await database.insertBatch("people", {
        columns: {
          name: ["Ada", "Grace", "Katherine", "Linus"],
          score: [10, 25, 30, 25],
        },
      });
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

// DB is inferred from the schema — no generic passing.
const db = createBrowserDb(database, { schema: appSchema });
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
    id: "snapshot",
    title: "Prepared queries hold one immutable snapshot",
    code: `const prepared = await database.prepareQuery(
  "SELECT COUNT(*) AS people FROM people",
);
const before = prepared.execute().rows; // [{ people: 2 }]

await database.insertBatch("people", {
  columns: { name: ["Margaret"], score: [40] },
});

const stillBefore = prepared.execute().rows; // [{ people: 2 }] — same snapshot
const after = (await database.query(
  "SELECT COUNT(*) AS people FROM people",
)).rows; // [{ people: 3 }]
prepared.close();`,
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
      await database.insertBatch("people", {
        columns: { name: ["Ada", "Grace"], score: [10, 25] },
      });
      const prepared = await database.prepareQuery("SELECT COUNT(*) AS people FROM people");
      const before = prepared.execute().rows;
      await database.insertBatch("people", {
        columns: { name: ["Margaret"], score: [40] },
      });
      const stillBefore = prepared.execute().rows;
      const after = (await database.query("SELECT COUNT(*) AS people FROM people")).rows;
      prepared.close();
      return show({ before, stillBefore, after });
    },
  },
];

/** Typed-first ordering: the showcase leads with the best-practice API. */
export const showcaseExamples: ShowcaseExample[] = ["typed", "builder", "sql", "snapshot"].map(
  (id) => {
    const entry = entries.find((candidate) => candidate.id === id);
    if (entry === undefined) throw new Error(`Missing showcase example: ${id}`);
    return entry;
  },
);
