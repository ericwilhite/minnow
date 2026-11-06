/**
 * Review B3-p2a-3 (befa216): the reviewer's probes, as tests. Findings 1, 2 and 6 (resident group
 * metadata) are answered by the chunked directory (groups.ts, groups.test.ts); 3 by the pending
 * publication owning its charges; 5 by `LogAppender.append`'s boundary check. Finding 4 is in
 * testing/divergences.test.ts (write/ may not import testing/).
 */
import { describe, expect, it } from "vitest";
import { MAX_LSN } from "../access/snapshot.js";
import { MAX_FRAME_BODY_BYTES } from "../format/log-frame.js";
import type { Grant } from "../memory/governor.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { LogAppender, LogExhaustedError } from "./log.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rowsOf,
  tableInput,
} from "./test-support.js";
import type { WritePathImpl } from "./engine.js";

/** A harness whose "write path metadata" grant the test can read. */
function metered(): { harness: Harness; grant: () => Grant } {
  const harness = new Harness("strict", "memory", 2);
  let grant: Grant | null = null;
  const admit = harness.governor.admit.bind(harness.governor);
  harness.governor.admit = async (options) => {
    const admitted = await admit(options);
    if (options.label === "write path metadata") grant = admitted;
    return admitted;
  };
  return {
    harness,
    grant: () => {
      if (grant === null) throw new Error("no metadata grant yet");
      return grant;
    },
  };
}

async function table(path: WritePathImpl): Promise<number> {
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false)]),
        })
      ).id ?? 0;
  });
  return t;
}

async function insert(path: WritePathImpl, t: number, ...rows: number[][]): Promise<void> {
  for (const ids of rows) await autocommit(path, (s) => s.table(t).insert(batch(ints(ids)), null));
}

describe("review B3-p2a-3", () => {
  it("#2: an older snapshot keeps its generation's root and directory charged until it closes", async () => {
    const { harness, grant } = metered();
    const path = await harness.open();
    const t = await table(path);
    await insert(path, t, [1, 2], [3, 4], [5, 6]);
    await path.quiesce();
    const old = path.pin();
    await insert(path, t, [7, 8]);
    await path.quiesce();
    const now = path.pin();
    expect(old.state.root).not.toBe(now.state.root);
    expect(old.state.tables.get(t)?.groups).not.toBe(now.state.tables.get(t)?.groups);
    expect((await rowsOf(old, t)).length).toBe(6);
    const before = grant().allocated;
    // The old generation's root and its own directory chunk are charged beside the current ones.
    expect(before).toBeGreaterThan(path.residentMetadataBytes);
    old.release();
    const after = grant().allocated;
    expect(before - after).toBeGreaterThan(0);
    // With only the current generation pinned, the grant holds exactly what it reports.
    expect(after).toBe(path.residentMetadataBytes);
    now.release();
    await harness.close();
  });

  // Raced against a later flush (the reviewer's probe) and against the table's first flush, whose
  // publication grows the page index from nothing.
  for (const earlier of [3, 0])
    it(`#3: a drop during an in-flight publish leaves no orphan charge (${String(earlier)} earlier flushes)`, async () => {
      const { harness, grant } = metered();
      const path = await harness.open();
      const t = await table(path);
      await insert(
        path,
        t,
        ...[
          [1, 2],
          [3, 4],
          [5, 6],
        ].slice(0, earlier),
      );
      await path.quiesce();
      const publish = harness.space.publish.bind(harness.space);
      let resume!: () => void;
      let entered!: () => void;
      const gate = new Promise<void>((resolve) => {
        resume = resolve;
      });
      const hit = new Promise<void>((resolve) => {
        entered = resolve;
      });
      harness.space.publish = async (...args) => {
        entered();
        await gate;
        return publish(...args);
      };
      await insert(path, t, [7, 8], [9, 10]);
      await hit;
      await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
      resume();
      await path.quiesce();
      expect(grant().allocated).toBe(path.residentMetadataBytes);
      await harness.close();
    });

  it("a drop returns its directory charge once, after the last older reader", async () => {
    const { harness, grant } = metered();
    const path = await harness.open();
    const t = await table(path);
    await insert(path, t, [1, 2], [3, 4]);
    await path.quiesce();
    const first = path.pin();
    const last = path.pin();
    const before = grant().allocated;
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
    first.release();
    expect((await rowsOf(last, t)).length).toBe(4);
    last.release();
    const after = grant().allocated;
    expect(after).toBeLessThan(before);
    expect(after).toBe(path.residentMetadataBytes);
    last.release();
    expect(grant().allocated).toBe(after);
    await harness.close();
  });

  it("a reader between fold and prune sees the folded bitmap after an earlier reader closes", async () => {
    const harness = new Harness("strict", "memory", 2);
    const path = await harness.open();
    const t = await table(path);
    await insert(path, t, [1, 2], [3, 4], [5, 6]);
    await path.quiesce();
    await autocommit(path, (s) => s.table(t).delete(batch(locators([0], t)), null));
    const old = path.pin();
    await insert(path, t, [7, 8], [9, 10]);
    await path.quiesce();
    const middle = path.pin();
    expect((await rowsOf(old, t)).some((row) => row[0] === 1)).toBe(false);
    old.release();
    expect((await rowsOf(middle, t)).some((row) => row[0] === 1)).toBe(false);
    middle.release();
    await harness.close();
  });

  it("a refused publish returns the proposed metadata charges after extent work", async () => {
    const { harness, grant } = metered();
    const path = await harness.open();
    const t = await table(path);
    const before = grant().allocated;
    const failure = new Error("injected publish refusal");
    harness.space.publish = () => Promise.reject(failure);
    await insert(path, t, [1, 2], [3, 4]);
    await path.quiesce().catch(() => undefined);
    expect(path.failure).toBe(failure);
    expect(grant().allocated).toBe(before);
    await harness.close();
  });
});

describe("review B3-p2a-3 #5: the log refuses LSN exhaustion before changing anything", () => {
  async function appender(lastLsn: number) {
    const backend = new MemoryBackend();
    const lease = await backend.acquire();
    const session = await backend.open(lease);
    const log = new LogAppender(
      session,
      {
        salt: 1,
        chain: 1,
        end: 0,
        lastLsn,
        generation: 0,
        nextSlot: 0,
        acked: { end: 0, chain: 1, lsn: lastLsn },
        rootSequence: 1,
      },
      false,
      () => undefined,
    );
    return {
      log,
      close: async () => {
        await log.flush();
        await session.close();
        await lease.release();
      },
    };
  }

  it("takes the last single-part commit at MAX_LSN, then refuses the next", async () => {
    const { log, close } = await appender(MAX_LSN - 1);
    expect(log.append(new Uint8Array([1]))).toBe(MAX_LSN);
    const bytes = log.bytes;
    const end = log.end;
    expect(() => log.append(new Uint8Array([2]))).toThrow(LogExhaustedError);
    expect([log.bytes, log.end, log.lastLsn]).toEqual([bytes, end, MAX_LSN]);
    await close();
  });

  it("refuses at MAX_LSN without queueing a frame (the reviewer's probe)", async () => {
    const { log, close } = await appender(MAX_LSN);
    expect(() => log.append(new Uint8Array([0]))).toThrow(LogExhaustedError);
    expect(log.bytes).toBe(0);
    await close();
  });

  it("refuses a multipart commit that would cross MAX_LSN, leaving room for a smaller one", async () => {
    const { log, close } = await appender(MAX_LSN - 1);
    const twoParts = new Uint8Array(MAX_FRAME_BODY_BYTES + 1);
    expect(() => log.append(twoParts)).toThrow(LogExhaustedError);
    expect([log.bytes, log.end, log.lastLsn]).toEqual([0, 0, MAX_LSN - 1]);
    // Nothing changed: the chain and position still take the one commit that fits.
    expect(log.append(new Uint8Array([3]))).toBe(MAX_LSN);
    await close();
  });
});
