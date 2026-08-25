import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { MinnowDatabase, type QueryResult } from "@minnowdb/core";
import { streamCsv, streamNdjson, type QueryCursorSource } from "./index.js";

async function text(stream: ReadableStream<Uint8Array>): Promise<string> {
  return new TextDecoder().decode(await new Response(stream).arrayBuffer());
}

describe("streaming exports", () => {
  it("escapes CSV fields once across query pages", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 2 });
    await database.createTable({
      name: "items",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "note", type: "string", nullable: true },
        { name: "created", type: "datetime", nullable: true },
      ],
    });
    await database.insertBatch("items", {
      columns: {
        id: [-0, 2, 3],
        note: ["plain", 'comma, quote " and\nline', null],
        created: [new Date("2024-01-02T03:04:05.000Z"), null, new Date(0)],
      },
    });

    expect(
      await text(
        streamCsv(database, "SELECT id, note, created FROM items", {
          batchRows: 1,
          nullValue: "NULL",
        }),
      ),
    ).toBe(
      'id,note,created\r\n-0,plain,2024-01-02T03:04:05.000Z\r\n2,"comma, quote "" and\nline",NULL\r\n3,NULL,1970-01-01T00:00:00.000Z\r\n',
    );
    await database.close();
  });

  it("emits exact NDJSON scalars and rejects values JSON cannot represent", async () => {
    let returned = false;
    const batches: QueryResult[] = [
      {
        columns: ["id", "at", "name"],
        rows: [{ id: -0, at: new Date(0), name: "Minnow" }],
      },
    ];
    const source: QueryCursorSource = {
      async *queryCursor() {
        try {
          yield* batches;
          return undefined;
        } finally {
          returned = true;
        }
      },
    };
    expect(await text(streamNdjson(source, "SELECT"))).toBe(
      '{"id":-0,"at":"1970-01-01T00:00:00.000Z","name":"Minnow"}\n',
    );
    expect(returned).toBe(true);

    batches[0] = { columns: ["value"], rows: [{ value: Number.NaN }] };
    returned = false;
    await expect(text(streamNdjson(source, "SELECT"))).rejects.toThrow(
      "NDJSON cannot represent NaN",
    );
    expect(returned).toBe(true);
  });

  it("cancels the underlying cursor when the readable stream is cancelled", async () => {
    let returned = false;
    const source: QueryCursorSource = {
      queryCursor() {
        let page = 0;
        return {
          next: async () => ({
            done: false as const,
            value: { columns: ["id"], rows: [{ id: page++ }] },
          }),
          return: async () => {
            returned = true;
            return { done: true as const, value: undefined };
          },
          [Symbol.asyncIterator]() {
            return this;
          },
        };
      },
    };
    const reader = streamCsv(source, "SELECT", { batchRows: 1 }).getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("id\r\n0\r\n");
    await reader.cancel();
    expect(returned).toBe(true);
  });
});
