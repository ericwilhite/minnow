/** Native B8 binding reproduction, sharing the unchanged64MiB owner governor. */
import { expect, it, vi } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { openNativeOwner } from "../owner/native-boot.js";
import { ProtocolClient, type ClientLiveHandle } from "../owner/client.js";
import { QueryRuntime } from "../exec/run.js";
import { decodeResult } from "../owner/result.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { until } from "./live-runtime.test-support.js";
it.each(seedsFor("next-demand-live", [0xb9e11, 0xb9e12]))(
  "native ordered live binding publishes updates at64MiB seed %i",
  async (seed) => {
    const random = mulberry32(seed),
      owner = await openNativeOwner({ backend: new MemoryBackend(), memoryLimitBytes: 64 << 20 }),
      client = new ProtocolClient(),
      channel = new MessageChannel();
    let handle: ClientLiveHandle | undefined;
    owner.accept(channel.port2);
    await client.connect(channel.port1);
    try {
      await client.query(
        "CREATE TABLE person_record(id INT PRIMARY KEY,first_name TEXT,amount NUMERIC)",
      );
      await client.query(
        "INSERT INTO person_record VALUES(1,'before',12.3400),(2,'later',22.0000)",
      );
      const errors: Error[] = [],
        results: unknown[][][] = [];
      const admit = owner.governor.tryAdmit.bind(owner.governor),
        minimums: number[] = [],
        readQueries = new Set<string>();
      // eslint-disable-next-line @typescript-eslint/unbound-method -- Rebound to the intercepted runtime via apply below.
      const reserve = QueryRuntime.prototype.reserve;
      const readSpy = vi.spyOn(QueryRuntime.prototype, "reserve").mockImplementation(function (
        this: QueryRuntime,
        ...args
      ) {
        if (this.request.governor === owner.governor && this.plan.root.op !== "modify")
          readQueries.add(`query#${String(this.request.queryId)}/fixed-buffers`);
        return reserve.apply(this, args);
      });
      const spy = vi.spyOn(owner.governor, "tryAdmit").mockImplementation((options) => {
        if (readQueries.has(options.label)) {
          minimums.push(options.minimumBytes);
          expect(options.minimumBytes, "native small-query minimum").toBeLessThan(8 << 20);
        }
        return admit(options);
      });
      try {
        handle = client.subscribeLive("SELECT * FROM person_record ORDER BY id LIMIT $1", [1], {
          onResult: (result) => {
            results.push(decodeResult(result).tuples());
          },
          onError: (error) => {
            errors.push(error);
          },
        });
        await until(() => results.length >= 1 || errors.length > 0);
        expect(errors).toEqual([]);
        expect(results.at(-1)).toEqual([[1, "before", "12.3400"]]);
        for (let round = 0; round < 4; round++) {
          const text = (round % 2 ? "λ" : "after").repeat(1 + Math.floor(random() * 1024)),
            before = results.length;
          // Updating one field retains the native write owner while the live rerun admits.
          await client.query("UPDATE person_record SET first_name=$1 WHERE id=1", [text]);
          await until(() => results.length > before || errors.length > 0);
          expect(errors).toEqual([]);
          expect(results.at(-1)).toEqual([[1, text, "12.3400"]]);
        }
        expect(minimums.length).toBeGreaterThanOrEqual(5);
        expect(owner.governor.peakBytes).toBeLessThanOrEqual(64 << 20);
      } finally {
        spy.mockRestore();
        readSpy.mockRestore();
        handle?.close();
        handle = undefined;
      }
    } finally {
      handle?.close();
      client.close();
      await owner.close();
      channel.port1.close();
      channel.port2.close();
    }
    expect(owner.governor.usedBytes).toBe(0);
    expect(owner.spill.bytesInUse).toBe(0);
  },
  60000,
);
