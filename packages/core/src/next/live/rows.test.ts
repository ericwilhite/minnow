import { createReach, LIVE_ROW_VALUE_REACH } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { afterAll, expect, it } from "vitest";
import { LiveRows } from "./rows.js";
import { stringBytes } from "../memory/governor.js";
it("keeps stable rows and reconstructs patches, with exact keys and explicit resets", () => {
  let bytes = 0;
  const rows = new LiveRows<{ id: bigint; v: string }>(
    {
      allocate: (n) => {
        bytes += n;
        return true;
      },
      free: (n) => {
        bytes -= n;
      },
    },
    "id",
    { table: "t", maxRows: 3 },
  );
  const a = rows.apply(
    [
      { id: 1n, v: "a" },
      { id: 2n, v: "b" },
    ],
    1,
  );
  const b = rows.apply(
    [
      { id: 2n, v: "b" },
      { id: 1n, v: "changed" },
      { id: 3n, v: "c" },
    ],
    2,
  );
  expect(b.rows[0]).toBe(a.rows[1]);
  expect(b.patch.type).toBe("patch");
  if (b.patch.type === "patch") {
    const patch = b.patch;
    expect(Array.from(patch.retained)).toEqual([1, -1, -1]);
    const reconstructed = Array.from(patch.retained, (old, index) =>
      old >= 0 ? a.rows[old] : patch.changedRows.find((r) => r.index === index)?.row,
    );
    expect(reconstructed).toEqual(b.rows);
  }
  expect(rows.apply(b.rows, 3)).toBe(b);
  expect(() =>
    rows.apply(
      [
        { id: 1n, v: "a" },
        { id: 1n, v: "b" },
      ],
      4,
    ),
  ).toThrow("Duplicate");
  expect(rows.getSnapshot()).toBe(b);
  expect(() => rows.apply([...b.rows, { id: 4n, v: "d" }], 4)).toThrow("window");
  const c = rows.apply(
    [
      { id: 2n, v: "b" },
      { id: 8n, v: "changed" },
      { id: 3n, v: "c" },
    ],
    4,
    {
      keyChanges: [
        { table: "t", from: 1n, to: 5n },
        { table: "t", from: 5n, to: 8n },
      ],
    },
  );
  expect(c.changes.map((c) => c.type)).toEqual(["rekey"]);
  const d = rows.apply([{ id: 9n, v: "changed" }], 5, {
    reset: true,
    keyChanges: [{ table: "t", from: 8n, to: 9n }],
  });
  expect(d.reset).toBe(true);
  expect(d.patch.type).toBe("reset");
  expect(d.keyChanges).toEqual([]);
  expect(d.changes).toEqual([]);
  rows.close();
  expect(bytes).toBe(0);
});
it("never infers a rekey from an unrelated delete/insert", () => {
  const rows = new LiveRows<{ id: number }>({ allocate: () => true, free: () => undefined }, "id", {
    table: "t",
  });
  rows.apply([{ id: 1 }], 1);
  expect(rows.apply([{ id: 2 }], 2).changes.map((c) => c.type)).toEqual(["delete", "insert"]);
  rows.close();
});
it("retains the last good baseline when comparison memory is refused", () => {
  let accept = true,
    bytes = 0;
  const rows = new LiveRows<{ id: number }>(
    {
      allocate: (n) => {
        if (!accept) return false;
        bytes += n;
        return true;
      },
      free: (n) => {
        bytes -= n;
      },
    },
    "id",
  );
  const first = rows.apply([{ id: 1 }], 1);
  accept = false;
  expect(() => rows.apply([{ id: 2 }], 2)).toThrow("live row reconciliation");
  expect(rows.getSnapshot()).toBe(first);
  rows.close();
  expect(bytes).toBe(0);
});
it("uses accepted row identity across swaps, non-key updates and deleted/reused keys", () => {
  const rows = new LiveRows<{ id: number }>({ allocate: () => true, free: () => undefined }, "id", {
    table: "t",
  });
  rows.apply([{ id: 1 }, { id: 2 }], 1);
  const swapped = rows.apply([{ id: 1 }, { id: 2 }], 2, {
    keyChanges: [
      { table: "t", from: 1, to: 2, fromRowId: 10, toRowId: 11 },
      { table: "t", from: 2, to: 1, fromRowId: 20, toRowId: 21 },
    ],
  });
  expect(swapped.changes.filter((change) => change.type === "rekey")).toHaveLength(2);
  const reused = rows.apply([{ id: 3 }], 3, {
    keyChanges: [
      { table: "t", kind: "delete", from: 1, to: 1, fromRowId: 21, toRowId: 21 },
      { table: "t", from: 1, to: 3, fromRowId: 30, toRowId: 31 },
      { table: "t", kind: "delete", from: 2, to: 2, fromRowId: 11, toRowId: 11 },
    ],
  });
  expect(reused.changes.some((change) => change.type === "rekey")).toBe(false);
  rows.close();
});

const valueReach = createReach("next-live-row-values", LIVE_ROW_VALUE_REACH);
afterAll(() => valueReach.assertReached());

// Native temporal outputs are strings; adapters may explicitly return Date values.
for (const seed of seedsFor("next-live-row-values", [8840, 8841])) {
  it(`reconciles explicit adapter Dates by instant seed=${String(seed)}`, () => {
    let bytes = 0;
    const rows = new LiveRows<{ id: Date; value: Date; payload: string }>(
      {
        allocate: (n) => {
          bytes += n;
          return true;
        },
        free: (n) => {
          bytes -= n;
        },
      },
      "id",
      { table: "composite" },
    );
    const random = mulberry32(seed);
    valueReach.beginHistory();
    try {
      for (let step = 0; step < 30; step++) {
        const input = Array.from({ length: 1 + Math.floor(random() * 5) }, (_, id) => ({
          id: new Date(id * 1000),
          value: new Date(Math.floor(random() * 3) * 1000),
          payload: "x".repeat(step % 2 === 0 ? 16384 : 0),
        }));
        const previous = rows.getSnapshot();
        const keyChanges = [
          {
            table: "composite",
            from: [step, "old"],
            to: [step + 1, "new"],
            fromRowId: step,
            toRowId: step + 1,
          },
        ];
        const snapshot = rows.apply(input, step, { keyChanges });
        expect(snapshot.rows).toEqual(input);
        expect(bytes, "previous rows retained by diffs remain charged").toBeGreaterThanOrEqual(
          previous?.rows.reduce((sum, row) => sum + stringBytes(row.payload), 0) ?? 0,
        );
        valueReach.reach("retained previous rows");
        expect(snapshot.keyChanges).toEqual(step === 0 ? [] : keyChanges);
        expect(snapshot.changes.some((change) => change.type === "rekey")).toBe(false);
        valueReach.reach("composite notices");
        valueReach.reach("Date keys");
        if (previous && previous.rows[0]?.value.getTime() !== input[0]?.value.getTime())
          valueReach.reach("Date value changes");
        input[0]?.value.setTime(99999);
        expect(snapshot.rows[0]?.value.getTime()).not.toBe(99999);
        valueReach.reach("captured input");
      }
      expect(() =>
        rows.apply([{ id: new Date(NaN), value: new Date(0), payload: "" }], 31),
      ).toThrow("valid Dates");
      valueReach.reach("invalid key refused");
    } finally {
      rows.close();
    }
    expect(bytes).toBe(0);
    valueReach.reach("zero charges");
  });
}
