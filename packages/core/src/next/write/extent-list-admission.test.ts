/** R25 review 1: keeper-owned working state is admitted and bounded, including error paths. */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import { ExtentListKeeper, LIST_PAGE_EXTENTS, listedExtents } from "../storage/extent-list.js";
import { ReadDeniedError, type ExtentRef } from "../storage/extents.js";

const refs = (count: number): ExtentRef[] =>
  Array.from({ length: count }, (_, index) => ({ offset: 2 ** 30 + index * 8192, length: 1000 }));
const unlimited = { allocate: () => true, free: () => undefined };

it("refuses before copying or writing a large delta when its finite grant refuses", async () => {
  const governor = new Governor(4 << 20);
  const grant = await governor.admit({
    label: "refused keeper",
    minimumBytes: 0,
    spillable: false,
  });
  const space = new MemoryExtentSpace();
  const writer = space.beginWrite("refused");
  const keeper = new ExtentListKeeper([], { reader: space, budget: grant });
  try {
    await expect(
      keeper.prepare(1, { added: refs(100_000), released: [], writer }),
    ).rejects.toBeInstanceOf(ReadDeniedError);
    expect(writer.extents).toHaveLength(0);
    expect(grant.allocated).toBe(0);
  } finally {
    grant.close();
  }
});

it("keeps touched-page decode and encoded output bounded under a finite governor", async () => {
  const space = new MemoryExtentSpace();
  const seed = new ExtentListKeeper([], { reader: space, budget: unlimited });
  const existing = refs(100_000);
  const initial = await seed.prepare(1, {
    added: existing,
    released: [],
    writer: space.beginWrite("seed"),
  });
  seed.commit(1, 1);
  const governor = new Governor(4 << 20);
  const grant = await governor.admit({ label: "bounded keeper", minimumBytes: 0, spillable: true });
  const keeper = new ExtentListKeeper(initial.list, { reader: space, budget: grant });
  const writer = space.beginWrite("rewrite");
  const write = writer.write.bind(writer);
  let maxChunks = 0;
  let minChargeAtWrite = Infinity;
  writer.write = async (chunks) => {
    maxChunks = Math.max(maxChunks, chunks.length);
    minChargeAtWrite = Math.min(minChargeAtWrite, grant.allocated);
    return write(chunks);
  };
  const released = existing.filter((_, index) => index % LIST_PAGE_EXTENTS === 0);
  const added = released.map(({ offset, length }) => ({ offset: offset + 4096, length }));
  try {
    const prepared = await keeper.prepare(1, { added, released, writer });
    expect(minChargeAtWrite).toBeGreaterThan(0);
    expect(maxChunks).toBe(1);
    expect(governor.peakBytes).toBeLessThan(2 << 20);
    expect(prepared.list.reduce((sum, page) => sum + page.count, 0)).toBe(existing.length);
    expect(grant.allocated).toBeGreaterThan(0); // result metadata survives until settle
    keeper.commit(1, 2);
    expect(grant.allocated).toBeGreaterThan(0); // the keeper retains the committed list
    keeper.close();
    expect(grant.allocated).toBe(0);
  } finally {
    grant.close();
  }
});

it("returns reservations after a failed write and abort, then permits a retry", async () => {
  const governor = new Governor(8 << 20);
  const grant = await governor.admit({ label: "failed keeper", minimumBytes: 0, spillable: true });
  const space = new MemoryExtentSpace();
  const keeper = new ExtentListKeeper([], { reader: space, budget: grant });
  const writer = space.beginWrite("failed pages");
  const write = writer.write.bind(writer);
  let writes = 0;
  const failure = new Error("page write failed");
  writer.write = async (chunks) => {
    if (++writes === 3) throw failure;
    return write(chunks);
  };
  const added = refs(100_000);
  try {
    await expect(keeper.prepare(1, { added, released: [], writer })).rejects.toBe(failure);
    expect(grant.allocated).toBe(0);
    expect(keeper.list).toHaveLength(0);
    const prepared = await keeper.prepare(2, {
      added,
      released: [],
      writer: space.beginWrite("retry"),
    });
    expect(prepared.list.length).toBeGreaterThan(1);
    expect(grant.allocated).toBeGreaterThan(0);
    keeper.abort(2);
    expect(grant.allocated).toBe(0);
    expect(keeper.list).toHaveLength(0);
    keeper.close();
  } finally {
    grant.close();
  }
});

it("keeps decoded list objects charged across generator suspension and releases on return", async () => {
  const space = new MemoryExtentSpace();
  const seed = new ExtentListKeeper([], { reader: space, budget: unlimited });
  const prepared = await seed.prepare(1, {
    added: refs(4096),
    released: [],
    writer: space.beginWrite("seed"),
  });
  const governor = new Governor(4 << 20);
  const grant = await governor.admit({ label: "listed decode", minimumBytes: 0, spillable: true });
  const iterator = listedExtents(prepared.list, space, grant);
  try {
    await iterator.next();
    expect(grant.allocated).toBeGreaterThanOrEqual(4096 * 80);
    await iterator.return(undefined);
    expect(grant.allocated).toBe(0);
  } finally {
    await iterator.return(undefined);
    grant.close();
  }
});

it("releases failed-read scratch and preserves a previously committed reservation on abort", async () => {
  const governor = new Governor(8 << 20);
  const grant = await governor.admit({
    label: "retained keeper",
    minimumBytes: 0,
    spillable: true,
  });
  const space = new MemoryExtentSpace();
  const keeper = new ExtentListKeeper([], { reader: space, budget: grant });
  const existing = refs(4097);
  try {
    await keeper.prepare(1, { added: existing, released: [], writer: space.beginWrite("first") });
    keeper.commit(1, 1);
    const baseline = grant.allocated;
    expect(baseline).toBeGreaterThan(0);
    const addition = { offset: 2 ** 30 + 4096, length: 1000 };
    await keeper.prepare(2, { added: [addition], released: [], writer: space.beginWrite("abort") });
    keeper.abort(2);
    expect(grant.allocated).toBe(baseline);
    const failure = new Error("page read failed");
    const failed = new ExtentListKeeper(keeper.list, {
      budget: grant,
      reader: {
        read: async () => {
          throw failure;
        },
        peek: (ref, options) => space.peek(ref, options),
      },
    });
    await expect(
      failed.prepare(1, { added: [addition], released: [], writer: space.beginWrite("bad read") }),
    ).rejects.toBe(failure);
    expect(grant.allocated).toBe(baseline);
    failed.close();
    keeper.close();
    expect(grant.allocated).toBe(0);
  } finally {
    grant.close();
  }
});
