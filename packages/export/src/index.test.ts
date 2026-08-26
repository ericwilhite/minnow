import { describe, expect, it, vi } from "vitest";
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
        columnDomains: [null, null, null],
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

    batches[0] = { columns: ["value"], columnDomains: [null], rows: [{ value: Number.NaN }] };
    returned = false;
    await expect(text(streamNdjson(source, "SELECT"))).rejects.toThrow(
      "NDJSON cannot represent NaN",
    );
    expect(returned).toBe(true);

    batches[0] = {
      columns: ["value"],
      columnDomains: [null],
      rows: [{ value: new Date(Number.NaN) }],
    };
    returned = false;
    await expect(text(streamNdjson(source, "SELECT"))).rejects.toThrow(
      "Cannot export an invalid Date",
    );
    expect(returned).toBe(true);
  });

  it("serializes a Date without invoking cursor-controlled methods", async () => {
    const value = new Date("2026-08-25T12:34:56.789Z");
    Object.defineProperties(value, {
      getTime: {
        value: () => {
          throw new Error("cursor getTime must not run");
        },
      },
      toISOString: {
        value: () => {
          throw new Error("cursor toISOString must not run");
        },
      },
    });
    const source: QueryCursorSource = {
      async *queryCursor() {
        yield { columns: ["at"], columnDomains: [null], rows: [{ at: value }] };
        return undefined;
      },
    };

    await expect(text(streamCsv(source, "SELECT"))).resolves.toBe(
      "at\r\n2026-08-25T12:34:56.789Z\r\n",
    );
  });

  it("keeps using captured Date intrinsics after the prototype changes", async () => {
    const value = new Date("2026-08-25T12:34:56.789Z");
    const getTime = vi.spyOn(Date.prototype, "getTime").mockImplementation(() => {
      throw new Error("mutated Date.prototype.getTime must not run");
    });
    const toISOString = vi.spyOn(Date.prototype, "toISOString").mockImplementation(() => {
      throw new Error("mutated Date.prototype.toISOString must not run");
    });
    try {
      const source: QueryCursorSource = {
        async *queryCursor() {
          yield { columns: ["at"], columnDomains: [null], rows: [{ at: value }] };
          return undefined;
        },
      };
      await expect(text(streamNdjson(source, "SELECT"))).resolves.toBe(
        '{"at":"2026-08-25T12:34:56.789Z"}\n',
      );
    } finally {
      getTime.mockRestore();
      toISOString.mockRestore();
    }
  });

  it("cancels the underlying cursor when the readable stream is cancelled", async () => {
    let returned = false;
    const source: QueryCursorSource = {
      queryCursor() {
        let page = 0;
        return {
          next: async () => ({
            done: false as const,
            value: { columns: ["id"], columnDomains: [null], rows: [{ id: page++ }] },
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

  it("validates CSV options at the JavaScript boundary before opening a cursor", () => {
    let opened = false;
    const source: QueryCursorSource = {
      async *queryCursor() {
        opened = true;
        yield { columns: [], columnDomains: [], rows: [] };
        return undefined;
      },
    };
    expect(() => streamCsv(source, "SELECT", { delimiter: "ab" })).toThrow(
      "CSV delimiter must be one character",
    );
    expect(() => streamCsv(source, "SELECT", { delimiter: null as unknown as string })).toThrow(
      "CSV delimiter must be one character",
    );
    expect(() => streamCsv(source, "SELECT", { newline: "\r" as "\n" })).toThrow(
      "CSV newline must be LF or CRLF",
    );
    expect(() => streamCsv(source, "SELECT", { newline: null as unknown as "\n" })).toThrow(
      "CSV newline must be LF or CRLF",
    );
    expect(() => streamCsv(source, "SELECT", { nullValue: null as unknown as string })).toThrow(
      "CSV nullValue must be a string",
    );
    expect(() => streamCsv(source, "SELECT", { header: "yes" as unknown as boolean })).toThrow(
      "CSV header must be a boolean",
    );
    expect(opened).toBe(false);
  });
});
