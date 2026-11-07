import { afterAll, expect, it, vi } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, METADATA_JOURNAL_REACH } from "../../testing/reach.js";
import { TaskClock } from "../task.js";
import { Governor } from "../memory/governor.js";
import {
  METADATA_PAGE_BYTES,
  StoreCorruptionError,
  type MetadataStore,
} from "../storage/contract.js";
import { ReadDeniedError } from "../storage/extents.js";
import { bytesOf, countingBudget, expectBytes } from "../storage/space-test-support.js";
import {
  MetadataJournal,
  MetadataWorkspace,
  METADATA_PAYLOAD_BYTES,
  METADATA_WORKSPACE_BYTES,
} from "../storage/metadata-journal.js";

/** Physical storage is independent of managed frames. Simulate one native cloned response. */
class Pages implements MetadataStore {
  readonly physical = new Map<number, Uint8Array>();
  native = 0;
  maxNative = 0;
  writes = 0;
  failAt = -1;
  afterRead: (() => void) | null = null;
  afterWrite: (() => void) | null = null;
  async write(page: number, bytes: Uint8Array): Promise<void> {
    if (++this.writes === this.failAt) throw new Error("scripted metadata write failure");
    this.physical.set(page, bytes.slice());
    await Promise.resolve();
    this.afterWrite?.();
  }
  async read(page: number, target: Uint8Array): Promise<void> {
    const bytes = this.physical.get(page);
    if (bytes === undefined) throw new StoreCorruptionError("missing page");
    this.native += bytes.byteLength;
    this.maxNative = Math.max(this.maxNative, this.native);
    try {
      const response = bytes.slice();
      await Promise.resolve();
      target.set(response);
      this.afterRead?.();
    } finally {
      this.native -= bytes.byteLength;
    }
  }
  async reset(): Promise<void> {
    this.physical.clear();
  }
}

const reach = createReach("metadata journal", METADATA_JOURNAL_REACH);
for (const seed of seedsFor("metadata-journal", [1531, 1532, 1533, 1534]))
  it(`keeps paged chains and queue links off heap, with constant prepaid frames (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      pages = new Pages(),
      budget = countingBudget(METADATA_WORKSPACE_BYTES);
    const original = Uint8Array;
    let frames = 0,
      limit = 0,
      requests = 0,
      peak = 0;
    const allocate = budget.allocate.bind(budget);
    budget.allocate = (bytes) => {
      requests++;
      const accepted = allocate(bytes);
      peak = Math.max(peak, budget.used);
      return accepted;
    };
    vi.stubGlobal(
      "Uint8Array",
      new Proxy(original, {
        construct(target, args, constructor) {
          if (args[0] === METADATA_PAGE_BYTES) {
            expect(budget.used).toBe(METADATA_WORKSPACE_BYTES);
            if (++frames > limit) throw new Error("Uninventoried metadata frame");
          }
          return Reflect.construct(target, args, constructor) as Uint8Array;
        },
      }),
    );
    const workspace = MetadataWorkspace.create(budget),
      journal = new MetadataJournal(pages, workspace);
    try {
      limit = 3;
      await workspace.run("source", async () => {
        const source = bytesOf(3 * METADATA_PAYLOAD_BYTES + 1 + Math.floor(random() * 1000), seed);
        const writer = await journal.writer(1, seed);
        let at = 0;
        while (at < source.length) {
          const next = Math.min(source.length, at + 1 + Math.floor(random() * 65537));
          await writer.append(source.subarray(at, next));
          at = next;
        }
        const handle = await writer.finish();
        expect((await journal.describe(handle)).length).toBe(source.length);
        const cursor = await journal.cursor(handle, 1);
        await journal.release(handle); // cursor owns the last reference, independently of its caller
        const reserved = await journal.allocate();
        expect(reserved).not.toBe(handle); // protected metadata cannot reenter the free stack
        await journal.discard(reserved, 0, 0);
        at = 0;
        try {
          for (let chunk = await cursor.next(); chunk !== null; chunk = await cursor.next()) {
            expectBytes(chunk, source.subarray(at, at + chunk.length));
            at += chunk.length;
          }
        } finally {
          await cursor.close();
        }
        expect(at).toBe(source.length);
        reach.reach("multi page input");
        reach.reach("cursor protects released owner");
        const high = journal.pages;
        const empty = await journal.writer(1);
        await journal.release(await empty.finish());
        const retry = await journal.writer(1);
        await retry.append(source);
        await journal.release(await retry.finish());
        expect(journal.pages).toBe(high);
        reach.reach("constant time chain reuse");
      });
      expect(frames).toBe(3);
      frames = 0;
      limit = 4;
      await workspace.run("merge", async () => {
        let head = 0,
          tail = 0;
        for (let index = 0; index < 24; index++) {
          const writer = await journal.writer(3, seed + index);
          await writer.append(bytesOf(512 + index, seed + index));
          const handle = await writer.finish();
          if (tail !== 0) await journal.link(tail, handle);
          else head = handle;
          tail = handle;
        }
        let index = 0;
        while (head !== 0) {
          const descriptor = await journal.describe(head);
          expect(descriptor.tag).toBe(seed + index);
          const cursor = await journal.cursor(head, 1);
          await journal.release(head);
          try {
            const bytes = await cursor.next();
            expectBytes(bytes, bytesOf(512 + index, seed + index));
            expect(await cursor.next()).toBe(null);
          } finally {
            await cursor.close();
          }
          head = descriptor.next;
          index++;
        }
        expect(index).toBe(24);
        reach.reach("disk queue backlog");
      });
      expect(frames).toBe(4);
      expect(pages.maxNative).toBe(METADATA_PAGE_BYTES);
      expect(pages.native).toBe(0);
      expect(requests).toBe(1);
      expect(peak).toBe(METADATA_WORKSPACE_BYTES);
      reach.reach("admitted before frames");
    } finally {
      workspace.close();
      vi.unstubAllGlobals();
    }
    expect(budget.used).toBe(0);
    reach.reach("close returns charge");
  });

it("refuses before any frame exists, and transfers the existing reservation at a full governor", async () => {
  expect(() => MetadataWorkspace.create(countingBudget(METADATA_WORKSPACE_BYTES - 1))).toThrow(
    ReadDeniedError,
  );
  const governor = new Governor(4 << 20);
  const grant = await governor.admit({
    label: "startup",
    minimumBytes: governor.capBytes - governor.reserveBytes,
    spillable: false,
  });
  const workspace = MetadataWorkspace.create(grant);
  expect(grant.allocated).toBe(METADATA_WORKSPACE_BYTES);
  workspace.transfer((cache) => governor.registerCache(cache));
  expect(grant.allocated).toBe(0);
  expect(grant.held).toBe(governor.capBytes - governor.reserveBytes - METADATA_WORKSPACE_BYTES);
  grant.close();
  expect(governor.usedBytes).toBe(METADATA_WORKSPACE_BYTES);
  workspace.close();
  workspace.close();
  expect(workspace.bytes).toBe(0);
  expect(governor.usedBytes).toBe(0);
});

it("keeps the startup reservation on a refused transfer", () => {
  const budget = countingBudget(METADATA_WORKSPACE_BYTES),
    workspace = MetadataWorkspace.create(budget);
  let unregistered = false;
  expect(() =>
    workspace.transfer(() => ({
      charge: () => false,
      release: () => {
        throw new Error("nothing transferred");
      },
      unregister: () => {
        unregistered = true;
      },
    })),
  ).toThrow(ReadDeniedError);
  expect(unregistered).toBe(true);
  expect(budget.used).toBe(METADATA_WORKSPACE_BYTES);
  workspace.close();
  expect(budget.used).toBe(0);
});

it("rejects simultaneous slot use, escaped cursors and overlapping phases", async () => {
  const budget = countingBudget(METADATA_WORKSPACE_BYTES),
    workspace = MetadataWorkspace.create(budget),
    pages = new Pages(),
    journal = new MetadataJournal(pages, workspace);
  try {
    let escaped: Awaited<ReturnType<MetadataJournal["writer"]>> | undefined;
    await workspace.run("source", async () => {
      const writer = await journal.writer(1);
      await expect(journal.writer(1)).rejects.toThrow("already borrowed");
      await expect(workspace.run("merge", async () => undefined)).rejects.toThrow("busy");
      expect(() => workspace.close()).toThrow("active");
      await writer.abort();
      escaped = await journal.writer(1);
    });
    await workspace.run("source", async () => {
      if (escaped === undefined) throw new Error("No escaped writer");
      await expect(escaped.append(new Uint8Array([1]))).rejects.toThrow("escaped");
    });
  } finally {
    workspace.close();
  }
  expect(budget.used).toBe(0);
});

it("checks cancellation after native resume and returns partial chains for retry", async () => {
  const budget = countingBudget(METADATA_WORKSPACE_BYTES),
    workspace = MetadataWorkspace.create(budget),
    pages = new Pages(),
    journal = new MetadataJournal(pages, workspace);
  const controller = new AbortController(),
    reason = new Error("cancel during input spool");
  let tick = 0;
  const clock = new TaskClock(() => tick++);
  try {
    await workspace.run("source", async () => {
      const writer = await journal.writer(1);
      pages.afterWrite = () => {
        controller.abort(reason);
      };
      await expect(
        writer.append(bytesOf(5 * METADATA_PAYLOAD_BYTES, 1), { clock, signal: controller.signal }),
      ).rejects.toBe(reason);
      pages.afterWrite = null;
      await writer.abort();
      const high = journal.pages;
      const retry = await journal.writer(1);
      await retry.append(bytesOf(METADATA_PAYLOAD_BYTES, 2));
      await journal.release(await retry.finish());
      expect(journal.pages).toBe(high);
    });
  } finally {
    workspace.close();
  }
  expect(budget.used).toBe(0);
});

it("refuses corrupt pages and fails closed after a native write error", async () => {
  for (const mode of ["corrupt", "failed write"] as const) {
    const budget = countingBudget(METADATA_WORKSPACE_BYTES),
      workspace = MetadataWorkspace.create(budget),
      pages = new Pages(),
      journal = new MetadataJournal(pages, workspace);
    try {
      await workspace.run("source", async () => {
        const writer = await journal.writer(1);
        await writer.append(new Uint8Array([1, 2]));
        if (mode === "failed write") {
          pages.failAt = pages.writes + 1;
          await expect(writer.finish()).rejects.toThrow("scripted metadata write failure");
        } else {
          const handle = await writer.finish();
          const descriptor = await journal.describe(handle);
          const raw = pages.physical.get(descriptor.first - 1);
          if (raw === undefined) throw new Error("Missing data page");
          raw[24] = 127;
          const cursor = await journal.cursor(handle, 1);
          await expect(cursor.next()).rejects.toThrow(StoreCorruptionError);
          await expect(cursor.close()).rejects.toThrow("journal failed");
        }
        await expect(journal.allocate()).rejects.toThrow("journal failed");
      });
    } finally {
      workspace.close();
    }
    expect(budget.used).toBe(0);
  }
});

afterAll(() => reach.assertReached());
