/**
 * Relaxed durability after a power loss, at the two places where the strict reader's rules
 * used to refuse a database that relaxed semantics accept:
 *
 *  - A checkpoint interrupted between its first slot's flush and its mirror's. A relaxed log is
 *    flushed before the first slot is written, so the log bridges the two generations; a
 *    database checkpointed by a relaxed writer from before that flush shows an empty log beside
 *    two complete slots, and a relaxed opener takes the newer one (a strict opener still
 *    refuses: under strict durability an empty log beside two sequences means the log was lost).
 *  - A zero-filled frame inside the unflushed tail, with a later frame behind it: the prefix
 *    before the hole is the recovered state, where a strict log fails closed.
 */
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { PowerLossModel } from "./power-loss-model.js";
import {
  errorText,
  leaderHandles,
  leaderTable,
  leaderTableNames,
  walFrameRanges,
} from "./leader-test-helpers.js";

/** Two tables, a mirrored checkpoint, two more tables, then a checkpoint whose mirror flush dies. */
async function checkpointInterruptedAtMirror(strict: boolean, name: string) {
  const shim = new MemoryOpfs();
  const h = await leaderHandles(shim, name);
  let armed = false;
  let slotFlushes = 0;
  const model = new PowerLossModel(shim, (path, phase) => {
    if (!armed || phase !== "flush" || !/checkpoint-[ab]$/.test(path)) return;
    slotFlushes += 1;
    if (slotFlushes === 2) throw new DOMException("power lost", "InvalidStateError");
  });
  const leader = await h.recover({ strict });
  await leader.addTable(leaderTable("a"));
  await leader.addTable(leaderTable("b"));
  leader.checkpointNow();
  await leader.addTable(leaderTable("c"));
  await leader.addTable(leaderTable("d"));
  armed = true;
  expect(() => leader.checkpointNow()).toThrow();
  armed = false;
  leader.crash();
  model.powerLoss();
  return { shim, h };
}

describe("relaxed: checkpoint mirror write interrupted by power loss", () => {
  for (const strict of [true, false]) {
    it(`${strict ? "strict" : "relaxed"}: the log was flushed before the first slot, so recovery bridges the generations`, async () => {
      const { shim, h } = await checkpointInterruptedAtMirror(strict, `mirror-${String(strict)}`);
      // The frames the new checkpoint covers survived the power loss in both modes.
      expect(walFrameRanges(shim.readFileBytes(h.walPath) ?? new Uint8Array()).length).toBe(2);
      const leader = await h.recover({ strict });
      expect(await leaderTableNames(leader)).toEqual(["a", "b", "c", "d"]);
      leader.crash();
    });
  }

  it("a database checkpointed before the log flush: relaxed opens on the newer slot, strict refuses", async () => {
    const { shim, h } = await checkpointInterruptedAtMirror(false, "mirror-legacy");
    // What such a database looks like: the log lost with the power, the slots as they were.
    shim.writeFileBytes(h.walPath, new Uint8Array());
    const relaxed = await h.recover({ strict: false });
    expect(await leaderTableNames(relaxed)).toEqual(["a", "b", "c", "d"]);
    relaxed.crash();
    await expect(h.recover({ strict: true })).rejects.toThrow(
      /disagree without a WAL bridge; refusing a silent rollback/,
    );
  });

  it("strict: two slots at adjacent generations, differing sequences, and no log is still refused", async () => {
    const { shim, h } = await checkpointInterruptedAtMirror(true, "mirror-strict-lost-log");
    shim.writeFileBytes(h.walPath, new Uint8Array());
    let error: unknown;
    try {
      await h.recover({ strict: true });
    } catch (caught) {
      error = caught;
    }
    expect(errorText(error)).toMatch(/disagree without a WAL bridge/);
  });
});

describe("relaxed: a zero-filled frame inside the unflushed tail", () => {
  async function logWithHole(name: string) {
    const shim = new MemoryOpfs();
    const h = await leaderHandles(shim, name);
    const leader = await h.recover({ strict: false });
    await leader.addTable(leaderTable("a"));
    await leader.addTable(leaderTable("b"));
    await leader.addTable(leaderTable("c"));
    leader.crash();
    const wal = shim.readFileBytes(h.walPath);
    if (wal === undefined) throw new Error("wal");
    const frames = walFrameRanges(wal);
    expect(frames.length).toBe(3);
    const second = frames[1];
    if (second === undefined) throw new Error("frame");
    wal.fill(0, second.start, second.end);
    shim.writeFileBytes(h.walPath, wal);
    return h;
  }

  it("relaxed recovers the consecutive prefix before the hole", async () => {
    const h = await logWithHole("hole-relaxed");
    const leader = await h.recover({ strict: false });
    expect(await leaderTableNames(leader)).toEqual(["a"]);
    // The frames after the hole are gone for good: the next append starts where "a" ended.
    await leader.addTable(leaderTable("d"));
    leader.crash();
    const reopened = await h.recover({ strict: false });
    expect(await leaderTableNames(reopened)).toEqual(["a", "d"]);
    reopened.crash();
  });

  it("strict fails closed on the same log", async () => {
    const h = await logWithHole("hole-strict");
    await expect(h.recover({ strict: true })).rejects.toThrow(/WAL frame marker mismatch/);
  });

  it("relaxed still refuses a zero-filled frame inside the acknowledged prefix", async () => {
    const shim = new MemoryOpfs();
    const h = await leaderHandles(shim, "hole-acknowledged");
    // A strict writer acknowledged every frame; a relaxed opener must not roll them back.
    const strict = await h.recover({ strict: true });
    await strict.addTable(leaderTable("a"));
    await strict.addTable(leaderTable("b"));
    await strict.addTable(leaderTable("c"));
    strict.crash();
    const wal = shim.readFileBytes(h.walPath);
    if (wal === undefined) throw new Error("wal");
    const second = walFrameRanges(wal)[1];
    if (second === undefined) throw new Error("frame");
    wal.fill(0, second.start, second.end);
    shim.writeFileBytes(h.walPath, wal);
    await expect(h.recover({ strict: false })).rejects.toThrow(/WAL frame marker mismatch/);
  });
});
