import { afterAll, describe, expect, it, vi } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { createReach, LIVE_CLOSE_REACH } from "../../testing/reach.js";
import type { WireValue } from "../owner/protocol.js";
import type { ClientLiveHandle } from "../owner/client.js";
import { setup, until } from "./live-runtime.test-support.js";

interface CapturedState {
  readonly params: readonly WireValue[];
  readonly tables?: readonly number[];
  readonly closed: boolean;
  readonly queued?: unknown;
  readonly delivering?: boolean;
}
/** Observe actual retained object references without depending on GC scheduling. */
function captureRegistration(sql: string, marker: "metadata" | "delivering") {
  let captured: CapturedState | undefined;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- called with the intercepted native Set below.
  const originalAdd = Set.prototype.add;
  const spy = vi.spyOn(Set.prototype, "add").mockImplementation(function (
    this: Set<unknown>,
    value: unknown,
  ) {
    if (
      typeof value === "object" &&
      value !== null &&
      Object.getOwnPropertyDescriptor(value, "sql")?.value === sql &&
      Object.hasOwn(value, marker)
    ) {
      captured = value as CapturedState;
    }
    return originalAdd.call(this, value);
  });
  return { current: () => captured, stop: () => spy.mockRestore() };
}
const reach = createReach("next-live-close", LIVE_CLOSE_REACH);
afterAll(() => reach.assertReached());

describe("closed live handle payload ownership", () => {
  for (const seed of seedsFor("next-live-close", [8832, 8833])) {
    it(`drops native parameter and dependency captures on disposal seed=${String(seed)}`, async () => {
      reach.beginHistory();
      const active = await setup(128 * 1024 * 1024);
      const sql = "SELECT $1::TEXT AS n FROM closed_capture";
      const width = 16 * 1024 + Math.floor(mulberry32(seed)() * 64);
      const source = "x".repeat(width);
      let handle: ClientLiveHandle | undefined;
      const capture = captureRegistration(sql, "metadata");
      const errors: Error[] = [];
      let rows: ReadonlyArray<readonly unknown[]> = [];
      try {
        await active.client.query("CREATE TABLE closed_capture (id INTEGER)");
        await active.client.query("INSERT INTO closed_capture VALUES (1)");
        expect((await active.client.query(sql, [source])).rows()).toEqual([[source]]);
        handle = active.client.subscribeLive(sql, [source], {
          onResult(result) {
            rows = result.rows();
          },
          onError(error) {
            errors.push(error);
          },
        });
        await until(() => rows.length === 1 && active.engine.live?.stats.waiting === 0);
        expect(rows).toEqual([[source]]);
        expect(errors).toEqual([]);
        const entry = capture.current();
        if (entry === undefined) throw new Error("Native live registration not observed");
        expect(entry.params[0]).toBe(source);
        expect(entry.tables?.length).toBeGreaterThan(0);
        handle.close();
        await active.client.query("SELECT 9");
        await until(() => active.engine.live?.subscriptionCount === 0);
        expect(entry.closed).toBe(true);
        expect(
          entry.params,
          "a closed native handle must release its private parameter copy",
        ).toHaveLength(0);
        expect(
          entry.tables,
          "a closed native handle must release its dependency vector",
        ).toHaveLength(0);
        reach.reach("native captures released");
      } finally {
        capture.stop();
        handle?.close();
        await active.close();
      }
      expect(active.h.governor.usedBytes).toBe(0);
      reach.reach("zero charges");
    });

    for (const disposal of ["handle", "client"] as const) {
      it(`drops queued and captured client payloads on ${disposal} disposal seed=${String(seed)}`, async () => {
        reach.beginHistory();
        const active = await setup(128 * 1024 * 1024);
        const sql = "SELECT $1::TEXT AS n";
        const width = 16 * 1024 + Math.floor(mulberry32(seed)() * 64);
        const source = "x".repeat(width);
        const capture = captureRegistration(sql, "delivering");
        let release: (() => void) | undefined;
        const barrier = new Promise<void>((resolve) => {
          release = resolve;
        });
        let handle: ClientLiveHandle | undefined;
        let deliveries = 0;
        let rows: ReadonlyArray<readonly unknown[]> = [];
        const errors: Error[] = [];
        try {
          expect((await active.client.query(sql, [source])).rows()).toEqual([[source]]);
          handle = active.client.subscribeLive(sql, [source], {
            onResult(result) {
              deliveries++;
              rows = result.rows();
              return deliveries === 1 ? barrier : undefined;
            },
            onError(error) {
              errors.push(error);
            },
          });
          await until(() => deliveries === 1);
          expect(rows).toEqual([[source]]);
          const state = capture.current();
          if (state === undefined) throw new Error("Client live registration not observed");
          expect(state.params[0]).toBe(source);
          await active.reconnect();
          await until(() => state.queued !== null && state.queued !== undefined);
          expect(state.delivering).toBe(true);
          expect(state.params[0], "reconnect preserves the captured parameters").toBe(source);
          expect(state.queued).toMatchObject({ kind: "result" });
          reach.reach("active decoder and replacement");
          if (disposal === "handle") handle.close();
          else active.client.close();
          expect(state.closed).toBe(true);
          expect(
            state.queued,
            "dispose must drop the replacement before the active decoder drains",
          ).toBeNull();
          expect(
            state.params,
            "a closed client handle must release its captured parameters",
          ).toHaveLength(0);
          reach.reach(
            disposal === "handle" ? "handle captures released" : "client replacement released",
          );
          release?.();
          await until(() => state.delivering === false);
          expect(deliveries, "a disposed replacement cannot call the consumer").toBe(1);
          expect(errors).toEqual([]);
          reach.reach("no delivery after disposal");
        } finally {
          capture.stop();
          release?.();
          handle?.close();
          await active.close();
        }
        expect(active.h.governor.usedBytes).toBe(0);
        reach.reach("zero charges");
      });
    }
  }
});
