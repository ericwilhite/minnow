import { MinnowDatabase, column, schema, table } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import type { Kysely } from "kysely";
import { describe, expect, expectTypeOf, it } from "vitest";
import { createKysely } from "./create-kysely.js";
import { search } from "./search.js";
import type { InferKyselyDatabase } from "./schema.js";

const catalogueSchema = schema([
  table("products", {
    id: column.integer().unique(),
    name: column.string(),
    brand: column.string().nullable(),
    price: column.number(),
  }),
]);

type Catalogue = InferKyselyDatabase<typeof catalogueSchema>;

function invalidSearchesAreRejected(db: Kysely<Catalogue>): void {
  db.selectFrom("products").where((builder) => {
    // @ts-expect-error missing is not a visible column
    return search.match(builder, ["missing"], "coffee");
  });
  db.selectFrom("products").where((builder) => {
    // @ts-expect-error search requires at least one column
    return search.match(builder, [], "coffee");
  });
  db.selectFrom("products as p").where((builder) => {
    // @ts-expect-error the original table name is out of scope after aliasing
    return search.match(builder, ["products.name"], "coffee");
  });
}

describe("Kysely full-text search", () => {
  it("checks columns, binds query text, and returns a numeric rank", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    await database.migrate(catalogueSchema);
    const db = createKysely({ driver: database, schema: catalogueSchema });
    expect(invalidSearchesAreRejected).toBeTypeOf("function");
    const byPrice = db
      .selectFrom("products")
      .select("id")
      .where((builder) => search.match(builder, ["price"], "120"));
    expect(byPrice.compile()).toMatchObject({
      sql: 'select "id" from "products" where MATCH("price") AGAINST $1',
      parameters: ["120"],
    });
    expect(
      db
        .selectFrom("products as p")
        .select("p.id")
        .where((builder) => search.match(builder, ["p.name"], "espresso"))
        .compile(),
    ).toMatchObject({
      sql: 'select "p"."id" from "products" as "p" where MATCH("p"."name") AGAINST $1',
      parameters: ["espresso"],
    });

    const query = "espresso grinder";
    const hits = db
      .selectFrom("products")
      .select((builder) => [
        "id",
        "name",
        search.rank(builder, ["name", "brand"], query).as("rank"),
      ])
      .where((builder) => search.match(builder, ["name", "brand"], query))
      .orderBy("rank", "desc");

    expect(hits.compile()).toMatchObject({
      sql: 'select "id", "name", BM25("name", "brand") AGAINST $1 as "rank" from "products" where MATCH("name", "brand") AGAINST $2 order by "rank" desc',
      parameters: [query, query],
    });
    expectTypeOf<Awaited<ReturnType<typeof hits.execute>>>().toEqualTypeOf<
      Array<{ id: number; name: string; rank: number }>
    >();

    await db
      .insertInto("products")
      .values([
        { id: 1, name: "Espresso grinder", brand: "Minnow", price: 120 },
        { id: 2, name: "Espresso cups", brand: "Grinder Works", price: 24 },
        { id: 3, name: "Tea infuser", brand: null, price: 12 },
      ])
      .execute();

    const rows = await hits.execute();
    expect(rows.map(({ id }) => id)).toEqual([1, 2]);
    expect(rows[0]?.rank).toBeGreaterThan(rows[1]?.rank ?? 0);
    expect(await byPrice.execute()).toEqual([{ id: 1 }]);

    await db.destroy();
    store.close();
  });
});
