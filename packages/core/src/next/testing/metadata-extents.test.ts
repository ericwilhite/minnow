/** External sorting and R8 pair operations must use bounded frames even above 2^40. */
import { afterAll, expect, it } from "vitest";
import {
  createReach,
  METADATA_EXTENTS_REACH,
  METADATA_WRITE_FAULT_REACH,
} from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { TaskClock } from "../task.js";
import {
  MetadataJournal,
  MetadataWorkspace,
  METADATA_WORKSPACE_BYTES,
} from "../storage/metadata-journal.js";
import {
  ExtentPairs,
  mergeExtents,
  sortExtents,
  spoolExtents,
} from "../storage/metadata-extents.js";
import { countingBudget } from "../storage/space-test-support.js";
import { ScriptedMetadataPages } from "./metadata-test-support.js";

const reach = createReach("private extent streams", METADATA_EXTENTS_REACH);
const faultReach = createReach("metadata native write faults", METADATA_WRITE_FAULT_REACH);
const BASE = 2 ** 40;
const lengthOf = (index: number) => 4097 + (index % 255);
for (const seed of seedsFor("metadata-extents", [1541, 1542, 1543, 1544]))
  it(`sorts external runs, merges and differences with exact coordinates (${String(seed)})`, async () => {
    reach.beginHistory();
    const random = mulberry32(seed),
      count = 12_031 + (seed % 2) * 4_000;
    const input = Uint32Array.from({ length: count }, (_, i) => i);
    for (let i = input.length - 1; i > 0; i--) {
      const at = Math.floor(random() * (i + 1)),
        old = input[i] ?? 0;
      input[i] = input[at] ?? 0;
      input[at] = old;
    }
    const budget = countingBudget(METADATA_WORKSPACE_BYTES),
      pages = new ScriptedMetadataPages();
    let requests = 0;
    const allocate = budget.allocate.bind(budget);
    budget.allocate = (bytes) => {
      requests++;
      return allocate(bytes);
    };
    const workspace = MetadataWorkspace.create(budget),
      journal = new MetadataJournal(pages, workspace);
    try {
      let raw = 0,
        even = 0;
      await workspace.run("source", async () => {
        raw = await spoolExtents(
          journal,
          (function* () {
            for (const index of input) {
              yield { offset: BASE + index * 8192, length: lengthOf(index) };
              if (index % 197 === 0) yield { offset: BASE + index * 8192, length: lengthOf(index) };
            }
          })(),
          1,
        );
        even = await spoolExtents(
          journal,
          (function* () {
            for (let index = 0; index < count; index += 2)
              yield { offset: BASE + index * 8192, length: lengthOf(index) };
          })(),
          1,
        );
      });
      await workspace.run("merge", async () => {
        const sorted = await sortExtents(journal, raw, "exact");
        reach.reach("external passes");
        if (count > 12_031) reach.reach("odd external tail");
        reach.reach("exact duplicates coalesce");
        const difference = await mergeExtents(journal, sorted, even, "difference");
        const union = await mergeExtents(journal, sorted, even, "union");
        for (const [handle, start, step] of [
          [sorted, 0, 1],
          [difference, 1, 2],
          [union, 0, 1],
        ] as const) {
          const pairs = new ExtentPairs(await journal.cursor(handle, 1));
          let index = start;
          try {
            while (await pairs.next()) {
              if (pairs.offset !== BASE + index * 8192 || pairs.length !== lengthOf(index))
                throw new Error(`Bad pair at ${String(index)}`);
              index += step;
            }
          } finally {
            await pairs.close();
          }
          expect(index).toBe(start + Math.ceil((count - start) / step) * step);
        }
        reach.reach("difference");
        reach.reach("union");
        reach.reach("wide offsets");
        for (const handle of [raw, even, sorted, difference, union]) await journal.release(handle);
      });
      expect(requests).toBe(1);
      expect(budget.used).toBe(METADATA_WORKSPACE_BYTES);
      reach.reach("fixed admission");
    } finally {
      workspace.close();
    }
    expect(budget.used).toBe(0);
  });

it("services and cancels during input copying, before reading the whole producer", async () => {
  const budget = countingBudget(METADATA_WORKSPACE_BYTES),
    workspace = MetadataWorkspace.create(budget),
    journal = new MetadataJournal(new ScriptedMetadataPages(), workspace);
  const controller = new AbortController(),
    reason = new Error("cancel pair input");
  let seen = 0,
    tick = 0,
    during = false;
  try {
    await workspace.run("source", async () => {
      await expect(
        spoolExtents(
          journal,
          (function* () {
            for (let index = 0; index < 100_000; index++) {
              seen++;
              yield { offset: BASE + index * 4096, length: 1 };
            }
          })(),
          1,
          {
            signal: controller.signal,
            clock: new TaskClock(() => (tick += 1)),
            yieldTask: () =>
              new Promise<void>((resolve) =>
                setImmediate(() => {
                  if (seen >= 256) {
                    during = true;
                    controller.abort(reason);
                  }
                  resolve();
                }),
              ),
          },
        ),
      ).rejects.toBe(reason);
      expect(during).toBe(true);
      expect(seen).toBeGreaterThanOrEqual(256);
      expect(seen).toBeLessThan(512);
      const high = journal.pages;
      const retry = await spoolExtents(journal, [{ offset: BASE, length: 1 }], 1);
      await journal.release(retry);
      expect(journal.pages).toBe(high);
    });
  } finally {
    workspace.close();
  }
  expect(budget.used).toBe(0);
});

it("rejects duplicate retirement inputs and disagreeing or overlapping extents", async () => {
  const budget = countingBudget(METADATA_WORKSPACE_BYTES),
    workspace = MetadataWorkspace.create(budget),
    journal = new MetadataJournal(new ScriptedMetadataPages(), workspace);
  try {
    for (const input of [
      [
        { offset: BASE, length: 1 },
        { offset: BASE, length: 1 },
      ],
      [
        { offset: BASE, length: 1 },
        { offset: BASE, length: 2 },
      ],
      [
        { offset: BASE, length: 8192 },
        { offset: BASE + 4096, length: 1 },
      ],
    ]) {
      let raw = 0;
      await workspace.run("source", async () => {
        raw = await spoolExtents(journal, input, 1);
      });
      await workspace.run("merge", async () => {
        await expect(sortExtents(journal, raw, "refuse")).rejects.toThrow("overlapping");
        expect((await journal.describe(raw)).refs).toBe(1);
        await journal.release(raw);
      });
    }
  } finally {
    workspace.close();
  }
  expect(budget.used).toBe(0);
});

afterAll(() => {
  reach.assertReached();
  faultReach.assertReached();
});

it("rejects unordered merge inputs and releases a non-pair cursor on refusal", async () => {
  const budget = countingBudget(METADATA_WORKSPACE_BYTES),
    workspace = MetadataWorkspace.create(budget),
    journal = new MetadataJournal(new ScriptedMetadataPages(), workspace);
  try {
    await workspace.run("merge", async () => {
      const raw = await spoolExtents(
        journal,
        [
          { offset: BASE + 4096, length: 1 },
          { offset: BASE, length: 1 },
        ],
        1,
      );
      const empty = await spoolExtents(journal, [], 1);
      await expect(mergeExtents(journal, raw, empty, "difference")).rejects.toThrow("Unordered");
      const bytes = await journal.writer(1);
      await bytes.append(new Uint8Array([1]));
      const nonPair = await bytes.finish();
      await expect(sortExtents(journal, nonPair, "refuse")).rejects.toThrow("Truncated");
      for (const handle of [raw, empty, nonPair]) {
        expect((await journal.describe(handle)).refs).toBe(1);
        await journal.release(handle);
      }
    });
  } finally {
    workspace.close();
  }
  expect(budget.used).toBe(0);
});

it("preserves the native error and returns memory at every external-sort write boundary", async () => {
  const run = async (fault: number) => {
    const budget = countingBudget(METADATA_WORKSPACE_BYTES),
      workspace = MetadataWorkspace.create(budget),
      pages = new ScriptedMetadataPages(),
      journal = new MetadataJournal(pages, workspace);
    try {
      let input = 0;
      await workspace.run("source", async () => {
        input = await spoolExtents(
          journal,
          (function* () {
            for (let index = 3_840; index >= 0; index--)
              yield { offset: BASE + index * 4096, length: 1 };
          })(),
          1,
        );
      });
      const before = pages.writes;
      if (fault > 0) pages.failAt = before + fault;
      await workspace.run("merge", async () => {
        const result = sortExtents(journal, input, "refuse");
        if (fault > 0) {
          await expect(result).rejects.toThrow("scripted metadata write failure");
          faultReach.reach("native error preserved");
        } else {
          const sorted = await result;
          expect((await journal.describe(sorted)).length).toBe(3_841 * 16);
        }
      });
      return pages.writes - before;
    } finally {
      workspace.close();
      expect(budget.used).toBe(0);
      if (fault > 0) faultReach.reach("zero final charge");
    }
  };
  const writes = await run(0);
  expect(writes).toBeGreaterThan(10);
  for (let fault = 1; fault <= writes; fault++) {
    faultReach.beginHistory();
    await run(fault);
  }
});

for (const mode of ["spool", "sort", "merge"] as const)
  it(`${mode}: cancellation during the final descriptor write returns its output`, async () => {
    const budget = countingBudget(METADATA_WORKSPACE_BYTES),
      workspace = MetadataWorkspace.create(budget),
      pages = new ScriptedMetadataPages(),
      journal = new MetadataJournal(pages, workspace);
    let input = 0,
      empty = 0;
    const controller = new AbortController(),
      reason = new Error("cancel final descriptor");
    try {
      await workspace.run("source", async () => {
        input = await spoolExtents(journal, [{ offset: BASE, length: 1 }], 1);
        empty = await spoolExtents(journal, [], 1);
      });
      await workspace.run("merge", async () => {
        pages.afterWrite = (page, bytes) => {
          if (page === input - 1 || page === empty - 1) return;
          if (
            new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(4, true) === 1
          )
            controller.abort(reason);
        };
        const operation = (signal?: AbortSignal) =>
          mode === "spool"
            ? spoolExtents(
                journal,
                [{ offset: BASE, length: 1 }],
                1,
                signal === undefined ? undefined : { signal },
              )
            : mode === "sort"
              ? sortExtents(journal, input, "refuse", signal === undefined ? undefined : { signal })
              : mergeExtents(
                  journal,
                  input,
                  empty,
                  "union",
                  signal === undefined ? undefined : { signal },
                );
        await expect(operation(controller.signal)).rejects.toBe(reason);
        expect(controller.signal.aborted).toBe(true);
        pages.afterWrite = null;
        expect((await journal.describe(input)).refs).toBe(1);
        expect((await journal.describe(empty)).refs).toBe(1);
        const high = journal.pages;
        const result = await operation();
        await journal.release(result);
        expect(journal.pages).toBe(high);
        await journal.release(input);
        await journal.release(empty);
      });
    } finally {
      workspace.close();
    }
    expect(budget.used).toBe(0);
  });

for (const stop of [1, 200, 8_000, 16_000, 22_000])
  it(`cleans sort/merge cancellation at yielded task ${String(stop)} and retries`, async () => {
    const budget = countingBudget(METADATA_WORKSPACE_BYTES),
      workspace = MetadataWorkspace.create(budget),
      journal = new MetadataJournal(new ScriptedMetadataPages(), workspace);
    // Keep every existing cancellation point reachable after scalar clock probes are batched.
    // More input extends the history; no yielded-task stop, retry or assertion is removed.
    const count = stop <= 200 ? 8_000 : stop <= 16_000 ? 160_000 : 320_000;
    let input = 0;
    try {
      await workspace.run("source", async () => {
        input = await spoolExtents(
          journal,
          (function* () {
            for (let index = count - 1; index >= 0; index--)
              yield { offset: BASE + index * 4096, length: 1 };
          })(),
          1,
        );
      });
      await workspace.run("merge", async () => {
        let yields = 0,
          tick = 0;
        const controller = new AbortController(),
          reason = new Error("cancel external sort");
        await expect(
          sortExtents(journal, input, "refuse", {
            clock: new TaskClock(() => (tick += 5)),
            signal: controller.signal,
            yieldTask: () =>
              new Promise<void>((resolve) =>
                setImmediate(() => {
                  if (++yields === stop) controller.abort(reason);
                  resolve();
                }),
              ),
          }),
        ).rejects.toBe(reason);
        expect(yields).toBe(stop);
        if (count > 8_000) reach.reach("extended cancellation history");
        expect((await journal.describe(input)).refs).toBe(1);
        const sorted = await sortExtents(journal, input, "refuse");
        const cursor = new ExtentPairs(await journal.cursor(sorted, 1));
        let index = 0;
        try {
          while (await cursor.next()) {
            if (cursor.offset !== BASE + index * 4096 || cursor.length !== 1)
              throw new Error("Retry lost a pair");
            index++;
          }
        } finally {
          await cursor.close();
        }
        expect(index).toBe(count);
        await journal.release(sorted);
        await journal.release(input);
      });
    } finally {
      workspace.close();
    }
    expect(budget.used).toBe(0);
  });
