import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { CollectSink, INT, TEXT, batchesOf, type Row } from "./join-fixtures.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";

it("spills resident sort rows for a read, serializes the next batch, and preserves the result", async () => {
  const types = [INT, TEXT],
    governor = new Governor(8 * 1024 * 1024);
  const grant = await governor.admit({
    label: "sort",
    minimumBytes: sortMinimumBytes(types),
    spillable: true,
  });
  const reader = await governor.admit({ label: "reader", minimumBytes: 0, spillable: true });
  const resident = await governor.admit({
    label: "non-reclaimable",
    minimumBytes: 0,
    spillable: true,
  });
  const spill = new MemorySpillService(),
    sink = new CollectSink();
  let entered!: () => void, release!: () => void;
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const create = spill.create.bind(spill);
  spill.create = async (query) => {
    entered();
    await gate;
    return create(query);
  };
  const sort = new SortOperator(
    {
      types,
      keys: [{ column: 0, direction: "asc", nulls: "last", collation: null }],
      limit: null,
      offset: 0,
      withTies: false,
    },
    { grant, spill, queryId: 1 },
    sink,
  );
  const rows: Row[] = Array.from({ length: 1024 }, (_, i) => [
    1024 - i,
    `${String(i)}:${"x".repeat(300)}`,
  ]);
  for (const batch of batchesOf(types, rows, { batchRows: 256 }))
    if (sort.push(batch) === "blocked") await sort.ready();
  expect(resident.allocate(governor.capBytes - governor.reserveBytes - governor.usedBytes)).toBe(
    true,
  );
  try {
    const room = reader.makeRoom?.(256 * 1024);
    await started;
    let released = false;
    const tail: Row[] = [[-1, "tail"]];
    const batch = batchesOf(types, tail, { batchRows: 1 })[0];
    if (batch === undefined) throw new Error("missing input batch");
    expect(
      sort.push(batch, {
        release() {
          released = true;
        },
      }),
    ).toBe("blocked");
    expect(released).toBe(false);
    release();
    expect(await room).toBe(true);
    await sort.ready();
    expect(released).toBe(true);
    resident.close();
    reader.close();
    await sort.finish();
    await sort.closed;
    expect(sort.profile.spilledBytes).toBeGreaterThan(0);
    expect(sink.rows).toEqual(
      [...rows, ...tail].sort((a, b) => (a[0] as number) - (b[0] as number)),
    );
  } finally {
    release();
    sort.abort(new Error("cleanup"));
    await sort.closed.catch(() => undefined);
    grant.close();
    reader.close();
    resident.close();
    expect(governor.usedBytes).toBe(0);
    expect(spill.bytesInUse).toBe(0);
  }
});
