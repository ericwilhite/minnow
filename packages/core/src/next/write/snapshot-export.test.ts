/** Pinned export law: source values once in row order across resident/durable/re-pointed sources. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_EXPORT_REACH } from "../../testing/reach.js";
import { SnapshotKind, SnapshotStreamReader } from "../format/snapshot.js";
import type { ByteBudget } from "../storage/extents.js";
import { exportPinnedSnapshot } from "./snapshot-export.js";
import { decodeSnapshotColumnHeader } from "./snapshot-column.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  locators,
  tableInput,
  texts,
  rules,
} from "./test-support.js";

class Budget implements ByteBudget {
  used = 0;
  peak = 0;
  constructor(readonly cap = 8 << 20) {}
  allocate(n: number): boolean {
    if (this.used + n > this.cap) return false;
    this.used += n;
    this.peak = Math.max(this.peak, this.used);
    return true;
  }
  free(n: number): void {
    this.used -= n;
    if (this.used < 0) throw new Error("Double free");
  }
}
async function* input(bytes: readonly Uint8Array[]): AsyncGenerator<Uint8Array> {
  yield* bytes;
}
async function* catalog(): AsyncGenerator<Uint8Array> {
  yield new Uint8Array([1]);
}
async function values(bytes: readonly Uint8Array[]): Promise<unknown[][]> {
  const budget = new Budget(16 << 20),
    reader = await SnapshotStreamReader.open(input(bytes), budget);
  let columnId = 0,
    parts: Uint8Array[] = [];
  const columns: unknown[][] = [[], []];
  for await (const frame of reader.frames()) {
    if (frame.kind === SnapshotKind.columnBegin) {
      columnId = new DataView(frame.payload.buffer, frame.payload.byteOffset).getUint32(8, true);
      parts = [];
    }
    if (frame.kind === SnapshotKind.columnPart) parts.push(frame.payload.slice());
    if (frame.kind !== SnapshotKind.columnEnd) continue;
    const data = new Uint8Array(parts.reduce((n, part) => n + part.length, 0));
    let start = 0;
    for (const part of parts) {
      data.set(part, start);
      start += part.length;
    }
    const count = decodeSnapshotColumnHeader(
        columnId === 1 ? INT : { kind: "text" },
        data.subarray(0, 16),
      ),
      v = new DataView(data.buffer);
    let at = 16;
    for (let row = 0; row < count; row++) {
      const valid = data[at++] ?? 0;
      if (valid === 0) {
        columns[columnId - 1]?.push(null);
        continue;
      }
      expect(valid).toBe(1);
      if (columnId === 1) {
        columns[0]?.push(v.getInt32(at, true));
        at += 4;
      } else {
        const length = v.getUint32(at, true);
        at += 4;
        columns[1]?.push(new TextDecoder().decode(data.subarray(at, at + length)));
        at += length;
        expect(data[at++]).toBe(2);
        expect(v.getUint32(at, true)).toBe(0);
        at += 4;
      }
    }
    expect(at).toBe(data.length);
  }
  expect(budget.used).toBe(0);
  return columns;
}
for (const seed of seedsFor("next-snapshot-export", [0xb253, 0xb254]))
  it(`preserves pinned values and releases ownership seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot export", SNAPSHOT_EXPORT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(fc.option(fc.string({ minLength: 0, maxLength: 24 }), { nil: null }), {
          minLength: 2,
          maxLength: 40,
        }),
        fc.boolean(),
        async (strings, deleted) => {
          reach.beginHistory();
          for (const mode of ["resident", "persisted", "repoint", "during export"] as const) {
            const h = new Harness("strict", "memory"),
              path = await h.open();
            try {
              let table = 0;
              await autocommit(path, async (st) => {
                table =
                  (
                    await st.changeCatalog({
                      kind: "createTable",
                      table: tableInput("t", [
                        column("id", INT, false),
                        column("s", { kind: "text" }),
                      ]),
                    })
                  ).id ?? 0;
              });
              const ids = strings.map((_, i) => i + 1);
              await autocommit(path, (st) =>
                st.table(table).insert(batch(ints(ids), texts(strings)), null),
              );
              if (deleted)
                await autocommit(path, (st) =>
                  st.table(table).delete(batch(locators([0], table)), null),
                );
              reach.reach(deleted ? "deleted rows" : "all live");
              if (mode === "persisted") await path.flush();
              const pin = path.pin();
              if (mode === "repoint") await path.flush();
              const chunks: Uint8Array[] = [],
                budget = new Budget();
              let calls = 0;
              await exportPinnedSnapshot(
                pin,
                new Uint8Array(16),
                catalog(),
                {
                  write: async (bytes) => {
                    chunks.push(bytes.slice());
                    if (++calls === 1) {
                      await autocommit(path, (st) =>
                        st.table(table).insert(batch(ints([999]), texts(["later"])), null),
                      );
                      if (mode === "during export") await path.flush();
                    }
                  },
                },
                budget,
              );
              expect(pin.released).toBe(true);
              expect(budget.used).toBe(0);
              expect(await values(chunks)).toEqual([
                deleted ? ids.slice(1) : ids,
                deleted ? strings.slice(1) : strings,
              ]);
              reach.reach(mode);
              const failedPin = path.pin(),
                denied = new Budget(1);
              await expect(
                exportPinnedSnapshot(
                  failedPin,
                  new Uint8Array(16),
                  catalog(),
                  { write: () => undefined },
                  denied,
                ),
              ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_BUDGET" });
              expect(failedPin.released).toBe(true);
              expect(denied.used).toBe(0);
              reach.reach("quota");
              const cancelledPin = path.pin(),
                controller = new AbortController(),
                cancelled = new Budget();
              await expect(
                exportPinnedSnapshot(
                  cancelledPin,
                  new Uint8Array(16),
                  catalog(),
                  {
                    write: () => {
                      controller.abort(new Error("export cancelled"));
                    },
                  },
                  cancelled,
                  { signal: controller.signal },
                ),
              ).rejects.toThrow("export cancelled");
              expect(cancelledPin.released).toBe(true);
              expect(cancelled.used).toBe(0);
              reach.reach("cancel");
            } finally {
              await h.close();
            }
            expect(h.governor.usedBytes).toBe(0);
          }
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 60_000);

it("streams a large durable value with bounded charged frame memory", async () => {
  const h = new Harness("strict", "memory"),
    path = await h.open();
  try {
    let table = 0;
    await autocommit(path, async (st) => {
      table =
        (
          await st.changeCatalog({
            kind: "createTable",
            table: tableInput("large", [column("id", INT, false), column("s", { kind: "text" })]),
          })
        ).id ?? 0;
    });
    const text = "z".repeat(3 * 1024 * 1024 + 17);
    await autocommit(path, (st) => st.table(table).insert(batch(ints([1]), texts([text])), null));
    await path.flush();
    const chunks: Uint8Array[] = [],
      budget = new Budget(5 << 20);
    await exportPinnedSnapshot(
      path.pin(),
      new Uint8Array(16),
      catalog(),
      {
        write: (bytes) => {
          chunks.push(bytes.slice());
        },
      },
      budget,
    );
    expect(budget.used).toBe(0);
    expect(budget.peak).toBeLessThan(5 << 20);
    expect(await values(chunks)).toEqual([[1], [text]]);
  } finally {
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});

for (const seed of seedsFor("next-snapshot-export-chunks", [0xb255, 0xb256]))
  it(`preserves dictionary/constant pages and multiple memory chunks seed ${String(seed)}`, async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 2051, max: 2100 }), fc.boolean(), async (n, constant) => {
        const h = new Harness("strict", "memory", 8192),
          path = await h.open();
        try {
          await autocommit(path, (st) =>
            st.changeCatalog({
              kind: "createTable",
              table: tableInput("many", [column("id", INT, false), column("s", { kind: "text" })]),
            }),
          );
          const ids = Array.from({ length: n }, (_, i) => i + 1),
            strings = ids.map((i) => (constant ? "constant" : i % 2 === 0 ? "one" : "two"));
          await autocommit(path, (st) =>
            st.table(1).insert(batch(ints(ids), texts(strings)), null),
          );
          const resident = path.pin(),
            repointed = path.pin();
          for (const pin of [resident, repointed]) {
            const bytes: Uint8Array[] = [],
              budget = new Budget();
            await exportPinnedSnapshot(
              pin,
              new Uint8Array(16),
              catalog(),
              {
                write: (chunk) => {
                  bytes.push(chunk.slice());
                },
              },
              budget,
            );
            expect(await values(bytes)).toEqual([ids, strings]);
            expect(budget.used).toBe(0);
            await path.flush();
          }
        } finally {
          await h.close();
        }
        expect(h.governor.usedBytes).toBe(0);
      }),
      { seed, numRuns: 4 },
    );
  }, 60_000);

it("refuses a statement view without ending its caller-owned lifetime", async () => {
  const h = new Harness("strict", "memory"),
    path = await h.open(),
    b = new Budget();
  const tx = await path.begin(),
    statement = tx.statement({ rules });
  let writes = 0;
  try {
    await expect(
      exportPinnedSnapshot(
        statement.view,
        new Uint8Array(16),
        catalog(),
        {
          write: () => {
            writes++;
          },
        },
        b,
      ),
    ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_SCOPE" });
    expect(statement.view.released).toBe(false);
    expect(writes).toBe(0);
    expect(b.used).toBe(0);
  } finally {
    statement.rollback();
    tx.rollback();
    await h.close();
  }
  expect(h.governor.usedBytes).toBe(0);
});
