/** The latency lane must observe the real clock and retire its watchdog on every outcome. */
import { expect, it, vi } from "vitest";
import { EventLoopWork } from "./event-loop-work.test-support.js";
import { yieldToEventLoop } from "../work-slicer.js";

it("keeps real-clock measurements separate from counted work", async () => {
  const originalNow: unknown = Reflect.get(performance, "now");
  const wall = new EventLoopWork("wall");
  const measured = await wall.measure(async () => {
    expect(Reflect.get(performance, "now")).toBe(originalNow);
    expect(vi.isMockFunction(Reflect.get(performance, "now") as unknown)).toBe(false);
    const rows = [{ id: 1 }];
    expect(wall.rows(rows)).toBe(rows);
    wall.item();
    await yieldToEventLoop();
  });
  expect(measured.wallLargest).toBeGreaterThanOrEqual(0);
  expect(measured.wallElapsed).toBeGreaterThanOrEqual(0);
  expect(measured.items).toBe(0);
  expect(Reflect.get(performance, "now")).toBe(originalNow);
});

it("counts the same work units independently of the host clock", async () => {
  const work = new EventLoopWork();
  const measured = await work.measure(async () => {
    expect(vi.isMockFunction(Reflect.get(performance, "now") as unknown)).toBe(true);
    for (let i = 0; i < 20; i++) work.item();
    await yieldToEventLoop();
    for (let i = 0; i < 10; i++) work.item();
  });
  expect(measured.wallLargest).toBeNull();
  expect(measured.wallElapsed).toBeNull();
  expect(measured.items).toBe(30);
  expect(measured.largest).toBe(20 / 16);
  expect(measured.turns).toBeGreaterThan(1);
});

it("retires a real watchdog and restores task observation after a failure", async () => {
  const originalNow: unknown = Reflect.get(performance, "now");
  const channel = MessageChannel;
  const intervals = new Set<ReturnType<typeof setInterval>>();
  const start = setInterval;
  const stop = clearInterval;
  const install = vi.spyOn(globalThis, "setInterval").mockImplementation((handler, timeout) => {
    const timer = start(handler, timeout);
    intervals.add(timer);
    return timer;
  });
  const retire = vi.spyOn(globalThis, "clearInterval").mockImplementation((timer) => {
    intervals.delete(timer as ReturnType<typeof setInterval>);
    stop(timer);
  });
  try {
    const error = new Error("latency measurement failed");
    await expect(
      new EventLoopWork("wall").measure(async () => {
        expect(intervals.size).toBe(1);
        throw error;
      }),
    ).rejects.toBe(error);
    expect(intervals.size).toBe(0);
    expect(MessageChannel).toBe(channel);
    expect(Reflect.get(performance, "now")).toBe(originalNow);
  } finally {
    for (const timer of intervals) stop(timer);
    install.mockRestore();
    retire.mockRestore();
  }
});
