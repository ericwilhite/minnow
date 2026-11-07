/** R23.2/.4/.5: resolved spill writes do not restart or yield the worker task. */
import { afterAll, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { Governor } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import { MemorySpillService } from "../spill/memory-spill.js";
import { INT, TEXT, batchesOf } from "./join-fixtures.js";
import { HashJoin, hashJoinMinimumBytes } from "./join.js";
import { beginTask, taskClock } from "./pipeline.js";

const reach = createReach("next-join-staging-clock", [
  "600000-byte rows",
  "generated widths",
  "immediate append",
  "shared clock slices",
  "timer serviced",
  "abort on resume",
  "released",
]);
afterAll(() => reach.assertReached());
for (const seed of seedsFor("next-join-staging-clock", [0xc101, 0xc102]))
  for (const mode of ["clock", "timer", "abort"] as const)
    it(`oversized staging ${mode}, seed ${String(seed)}`, async () => {
      reach.beginHistory();
      const random = mulberry32(seed);
      const width = seed === 0xc101 ? 600000 : 500000 + Math.floor(random() * 150000);
      const spec = {
        kind: "inner",
        probeTypes: [INT],
        buildTypes: [INT, TEXT],
        probeKeys: [0],
        buildKeys: [0],
        nullSafe: [false],
        residual: null,
        publishes: [],
      } as const;
      const minimum = hashJoinMinimumBytes(spec);
      const governor = new Governor(
        Math.max(minimum + (1 << 20), Math.ceil((minimum * 8) / 7) + 1),
      );
      const base = new MemorySpillService();
      const perTask = new Map<number, number>();
      let measuring = false,
        logical = 0;
      const originalNow = taskClock.now;
      const spill: SpillService = {
        get bytesInUse() {
          return base.bytesInUse;
        },
        sweep: () => base.sweep(),
        async create(queryId) {
          const file = await base.create(queryId);
          return {
            get queryId() {
              return file.queryId;
            },
            get length() {
              return file.length;
            },
            read: (ref) => file.read(ref),
            readInto: (ref, target) => file.readInto(ref, target),
            close: () => file.close(),
            append(page) {
              if (measuring) {
                perTask.set(taskClock.epoch, (perTask.get(taskClock.epoch) ?? 0) + 1);
                logical += 1; // Exactly one millisecond of copy work per page, not per clock check.
              }
              return file.append(page); // Already resolved: only a microtask, not a task yield.
            },
          };
        },
      };
      const grant = await governor.admit({
        label: "staging clock",
        minimumBytes: minimum,
        spillable: true,
      });
      const controller = new AbortController();
      const join = new HashJoin(spec, { grant, spill, queryId: seed, signal: controller.signal });
      let tick: Promise<void> | undefined;
      try {
        for (let i = 0; i < 10; i++)
          for (const batch of batchesOf(spec.buildTypes, [[i, "x".repeat(width)]]))
            if (join.build.push(batch) === "blocked") await join.build.ready();
        // All construction precedes the measured task, with one partition and many pages.
        const batch = [
          ...batchesOf(
            spec.buildTypes,
            Array.from({ length: 64 }, () => [1, "x".repeat(width)]),
            { batchRows: 64 },
          ),
        ][0];
        if (batch === undefined) throw new Error("missing input batch");
        let timer = false;
        const reason = new Error("cancel during staging yield");
        if (mode !== "clock")
          tick = new Promise<void>((resolve) =>
            setTimeout(() => {
              timer = true;
              if (mode === "abort") controller.abort(reason);
              resolve();
            }, 0),
          );
        if (mode === "clock") taskClock.now = () => logical;
        beginTask();
        measuring = true;
        const work = async () => {
          if (join.build.push(batch) === "blocked") await join.build.ready();
        };
        if (mode === "abort") {
          await expect(work()).rejects.toBe(reason);
          expect(timer).toBe(true);
          reach.reach("abort on resume");
        } else {
          await work();
          if (mode === "clock") {
            expect(perTask.size).toBeGreaterThan(1);
            expect(Math.max(...perTask.values())).toBeLessThanOrEqual(4);
            expect([...perTask.values()].reduce((a, b) => a + b, 0)).toBe(64);
            reach.reach("shared clock slices");
          } else {
            expect(timer).toBe(true);
            reach.reach("timer serviced");
          }
        }
        reach.reach(seed === 0xc101 ? "600000-byte rows" : "generated widths");
        reach.reach("immediate append");
      } finally {
        measuring = false;
        taskClock.now = originalNow;
        beginTask();
        join.abort(new Error("test complete"));
        await join.closed;
        await tick;
        expect(governor.usedBytes).toBe(0);
        expect(base.bytesInUse).toBe(0);
        reach.reach("released");
      }
    }, 30000);
