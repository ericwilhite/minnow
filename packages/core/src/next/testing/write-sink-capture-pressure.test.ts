/** R25: internal preparation is not an external observer attempt or failure. */
import fc from "fast-check";
import { expect, it, onTestFinished, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_SINK_CAPTURE_REACH } from "../../testing/reach.js";
import type { Grant } from "../memory/governor.js";
import * as capture from "../storage/writer-extents.js";
import { MetadataPressure } from "../write/budget.js";
import { ExtentSinkError } from "../write/engine.js";
import {
  Harness,
  autocommit,
  batch,
  column,
  INT,
  ints,
  rowsOf,
  tableInput,
  watchGrants,
} from "../write/test-support.js";

for (const seed of seedsFor("next-write-sink-capture-pressure", [47197, 47198]))
  it(`separates internal capture pressure from actual sink failures (seed ${String(seed)})`, async () => {
    const reach = createReach("write sink capture", WRITE_SINK_CAPTURE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("internal", "sink error", "sink pressure"),
        fc.integer({ min: 1, max: 6 }),
        async (mode, count) => {
          reach.beginHistory();
          const h = new Harness("strict", "memory", 64, undefined, {
            merges: { mode: "off" },
          });
          let metadata: Grant | undefined;
          watchGrants(h.governor, (grant) => {
            if (grant.label === "write path metadata") metadata = grant;
          });
          let active = false,
            injected = false,
            faulted = false,
            unaskedAborts = 0,
            restored = false;
          const attempts = new Set<number>();
          const fault =
            mode === "sink pressure"
              ? new MetadataPressure(256, 256)
              : new Error("external sink failure");
          h.extentDeltas = {
            prepare: (attempt) => {
              attempts.add(attempt);
              if (active && mode !== "internal" && !faulted) {
                faulted = true;
                throw fault;
              }
              return null;
            },
            commit: (attempt) => {
              expect(attempts.delete(attempt)).toBe(true);
            },
            abort: (attempt) => {
              if (!attempts.delete(attempt)) unaskedAborts++;
            },
          };
          const seen = new WeakMap<object, number>();
          const original = capture.captureWriterExtents;
          const observer = vi
            .spyOn(capture, "captureWriterExtents")
            .mockImplementation((...args) => {
              const writer = args[0];
              if (active) {
                const call = (seen.get(writer) ?? 0) + 1;
                seen.set(writer, call);
                // Keeper capture is first. The second capture still belongs to the engine,
                // before the external sink has been entered. A transient admission race
                // must unwind/retry exactly like any other internal MetadataPressure.
                if (mode === "internal" && !injected && call === 2) {
                  if (metadata === undefined) throw new Error("Missing native metadata grant");
                  injected = true;
                  throw new MetadataPressure(256, metadata.held + 256);
                }
              }
              return original(...args);
            });
          const restore = () => {
            active = false;
            if (!restored) {
              restored = true;
              observer.mockRestore();
            }
          };
          let opened = false;
          onTestFinished(async () => {
            restore();
            if (opened) await h.close();
          });
          try {
            const path = await h.open();
            opened = true;
            let table = 0;
            await autocommit(path, async (s) => {
              table =
                (
                  await s.changeCatalog({
                    kind: "createTable",
                    table: tableInput("sink_capture", [column("id", INT, false)]),
                  })
                ).id ?? 0;
            });
            await path.quiesce();
            const ids = Array.from({ length: count }, (_, i) => i + 1);
            await autocommit(path, (s) => s.table(table).insert(batch(ints(ids)), null));
            active = true;
            const put = await path.recycleLog().then(
              () => null,
              (error: unknown) => error,
            );
            const done = await path.quiesce().then(
              () => null,
              (error: unknown) => error,
            );
            if (mode === "internal") {
              expect(injected).toBe(true);
              expect(put).toBeNull();
              expect(done).toBeNull();
              expect(path.failure).toBeNull();
              reach.reach("internal capture retries");
            } else {
              expect(faulted).toBe(true);
              expect(path.failure).toBeInstanceOf(ExtentSinkError);
              expect(path.failure?.cause).toBe(fault);
              expect(done).toBe(path.failure);
              if (put !== null) expect(put).toBe(path.failure);
              reach.reach(mode);
            }
            expect(unaskedAborts).toBe(0);
            expect(attempts.size).toBe(0);
            reach.reach("no unasked sink abort");
            restore();
            await h.reopen();
            const snapshot = h.path.pin();
            try {
              expect(await rowsOf(snapshot, table)).toEqual(ids.map((id) => [id]));
            } finally {
              snapshot.release();
            }
            reach.reach("accepted rows survive reopen");
          } finally {
            restore();
            if (opened) {
              await h.close();
              opened = false;
            }
          }
          expect(h.governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          ["internal", 1],
          ["sink error", 3],
          ["sink pressure", 6],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
