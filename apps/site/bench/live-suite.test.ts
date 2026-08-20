/**
 * The live-query cases, run at unit speed over an in-memory store exactly as the browser harness
 * runs them over IndexedDB or OPFS: subscriptions registered through `MinnowDatabaseClient` on a
 * message channel, one commit per sample, the clock stopped by the last affected notification.
 * This proves the measurement itself — that it counts the right notifications, verifies the row
 * counts, and never credits a subscription the commit could not have affected.
 */
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { describe, expect, it } from "vitest";
import { createMinnowDriver } from "./engines/minnow";
import type { DatasetRecord } from "./protocol";
import { liveCaseDefinitions, measureLiveCase } from "./worker/live-suite";

const record: DatasetRecord = {
  id: "in-memory",
  createdAt: new Date(0).toISOString(),
  scale: 0.1,
  totalRows: 0,
  tableRows: {},
  compression: "raw",
  targetBlockBytes: 1_048_576,
  durability: "relaxed",
  engines: {},
};

const driver = createMinnowDriver({
  id: "minnow",
  persistence: "memory",
  openStore: () => Promise.resolve(new MemoryBlockStore()),
  deleteDataset: () => Promise.resolve(),
});

describe("live-query suite", () => {
  it("times every case through the worker client and verifies the notifications", async () => {
    if (driver.openLiveSession === undefined) throw new Error("minnow has no live session");
    const session = await driver.openLiveSession(record);
    try {
      for (const definition of liveCaseDefinitions()) {
        const measured = await measureLiveCase(session, definition, `t_${definition.id}`);
        expect({ id: definition.id, ...measured }).toEqual({
          id: definition.id,
          engine: "minnow",
          supported: true,
          subscribeMs: expect.any(Number) as number,
          medianMs: expect.any(Number) as number,
          p95Ms: expect.any(Number) as number,
          notifications: definition.affected,
          verified: true,
        });
        expect(measured.medianMs).toBeGreaterThan(0);
        expect(measured.p95Ms).toBeGreaterThanOrEqual(measured.medianMs);
      }
    } finally {
      await session.close();
    }
  }, 60_000);

  it("reports a session that cannot deliver notifications as unsupported, with the reason", async () => {
    if (driver.openLiveSession === undefined) throw new Error("minnow has no live session");
    const session = await driver.openLiveSession(record);
    try {
      // A subscription whose callbacks never fire: the measurement must fail closed, not hang
      // or report a number.
      const silent = {
        ...session,
        subscribe: () => Promise.resolve({ close: () => Promise.resolve() }),
      };
      const measured = await measureLiveCase(
        silent,
        { id: "never", name: "never", subscriptions: 1, affected: 1 },
        "t_silent",
        { notificationTimeoutMs: 200 },
      );
      expect(measured.supported).toBe(false);
      expect(measured.error).toMatch(/not notified within/);
    } finally {
      await session.close();
    }
  }, 30_000);
});
