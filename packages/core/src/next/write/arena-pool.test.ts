/** vector/vector.ts: a borrow keeps old backing storage immutable until it ends. */
import { expect, it } from "vitest";
import { ArenaPool, Memtable } from "./memtable.js";
import { Harness, autocommit, tableInput, column, TEXT, texts } from "./test-support.js";
it("holds borrowed generations and returns cached capacity on clear", async () => {
  const h = new Harness();
  const path = await h.open();
  let id = 0;
  await autocommit(path, async (s) => {
    id =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("v", TEXT)], { primaryKey: null }),
        })
      ).id ?? 0;
  });
  const def = path.catalog.table(id);
  if (!def) throw new Error("missing table");
  let charged = 0;
  const charge = {
    allocate: (n: number) => {
      charged += n;
      return true;
    },
    free: (n: number) => {
      charged -= n;
    },
  };
  const pool = new ArenaPool(charge, 65536);
  const m = new Memtable(id, 0, def.schemaVersion, def.columns, [], charge, 2048, pool);
  try {
    m.append([texts(["7".repeat(4096)])], null, 0, 1);
    m.mark(1);
    m.borrow(0, -1);
    const old = m.chunks[0]?.[0]?.view();
    m.truncate(0, 1);
    expect(pool.bytes).toBe(0);
    m.append([texts(["9".repeat(4096)])], null, 0, 1);
    m.mark(2);
    expect(
      old?.data.layout === "varlen"
        ? new TextDecoder().decode(old.data.bytes.subarray(0, 4096))
        : null,
    ).toBe("7".repeat(4096));
    m.unborrow(0);
    expect(pool.bytes).toBeGreaterThan(0);
    expect(charged).toBe(pool.bytes + m.bytes + ArenaPool.overhead);
    const cached = pool.bytes;
    pool.clear();
    expect(pool.bytes).toBe(0);
    expect(charged).toBe(m.bytes + ArenaPool.overhead);
    expect(cached).toBeGreaterThan(0);
  } finally {
    m.releaseAll();
    pool.close();
    await h.close();
  }
  expect(charged).toBe(0);
});

it.each([
  { columns: 13, limit: 16384, expected: 16384 },
  { columns: 513, limit: 16 * 1024 * 1024, expected: 512 * 4096 },
])(
  "bounds both bytes and entry bookkeeping ($columns columns)",
  async ({ columns, limit, expected }) => {
    const h = new Harness();
    const path = await h.open();
    let id = 0;
    await autocommit(path, async (s) => {
      id =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("v", TEXT)], { primaryKey: null }),
          })
        ).id ?? 0;
    });
    const base = path.catalog.table(id)?.columns[0];
    if (!base) throw new Error("missing column");
    let charged = 0;
    const charge = {
      allocate: (n: number) => {
        charged += n;
        return true;
      },
      free: (n: number) => {
        charged -= n;
      },
    };
    const pool = new ArenaPool(charge, limit);
    const defs = Array.from({ length: columns }, (_, k) => ({ ...base, id: k }));
    const first = new Memtable(id, 0, 1, defs, [], charge, 2048, pool);
    const next = new Memtable(id, 1, 1, defs, [], charge, 2048, pool);
    try {
      const input = defs.map(() => texts(["3".repeat(4096)]));
      first.append(input, null, 0, 1);
      first.release();
      expect(first.bytes).toBe(0);
      expect(pool.bytes).toBe(expected);
      expect(charged).toBe(pool.bytes + ArenaPool.overhead);
      next.append(input, null, 0, 1);
      expect(pool.bytes).toBe(0);
      expect(charged).toBe(next.bytes + ArenaPool.overhead);
    } finally {
      first.releaseAll();
      next.releaseAll();
      pool.close();
      await h.close();
    }
    expect(charged).toBe(0);
  },
);

// Exact capacity keeps the ordinary memtable sizing/allocation plan unchanged.
it("only adopts exact capacities and keeps the existing native charge", () => {
  let charged = 0;
  const charge = {
    allocate: (n: number) => {
      charged += n;
      return true;
    },
    free: (n: number) => {
      charged -= n;
    },
  };
  const pool = new ArenaPool(charge, 65536);
  charge.allocate(5000);
  const odd = new Uint8Array(5000);
  expect(pool.put(odd)).toBe(false);
  charge.free(5000);
  charge.allocate(8192);
  const buffer = new Uint8Array(8192);
  expect(pool.put(buffer.subarray(0, 4096))).toBe(false);
  expect(pool.put(buffer)).toBe(true);
  expect(pool.take(4096)).toBeUndefined();
  expect(pool.bytes).toBe(8192);
  const owned = pool.take(8192);
  expect(owned).toBe(buffer);
  expect(charged).toBe(ArenaPool.overhead + 8192);
  expect(pool.bytes).toBe(0);
  charge.free(8192);
  pool.close();
  expect(charged).toBe(0);
});
