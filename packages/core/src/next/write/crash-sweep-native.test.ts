/**
 * The exhaustive crash sweep with native steps (writer floors steps 2-3). Every write runs inside
 * a native step (`prepareNativeStep`), which keeps no merge minimum: merges the flushes schedule are
 * deferred to their own admission. Merges at fan-in 2 over small memtables interleave with the
 * native statements, and power is lost at every physical step; recovery keeps every acknowledged
 * row and transaction, indexes agree, and writing continues (crash-sweep.test-support.ts verify).
 *
 * `MINNOW_CRASH_STEP=n` starts the sweep at step n (to replay a failure first).
 */
import { describe, expect, it } from "vitest";
import {
  NativeWriteStepChangedError,
  type NativeWriteStepBoundary,
} from "../access/native-write-step.js";
import type { SimDisk } from "../storage/sim-disk.js";
import { MEMTABLE_ROWS, probe, sweep, type Outcome } from "./crash-sweep.test-support.js";
import type { WritePathImpl } from "./engine.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rules,
  tableInput,
} from "./test-support.js";

interface NativeStatement {
  prepareNativeStep(bytes: number, signal: AbortSignal): Promise<NativeWriteStepBoundary>;
}

/** One transaction whose write runs as a native step: funded through its boundary, then written. */
async function native(
  path: WritePathImpl,
  work: (
    statement: ReturnType<Awaited<ReturnType<WritePathImpl["begin"]>>["statement"]>,
  ) => unknown,
): Promise<void> {
  const tx = await path.begin();
  const statement = tx.statement({ rules });
  try {
    // As a native root does (owner/native-root-credits.ts): the boundary funds the step and is
    // released before the step's writes run inside the funded native floor; a footprint change
    // during funding (a background flush or merge) retries the funding, never the writes.
    for (;;) {
      let boundary: NativeWriteStepBoundary | null = null;
      try {
        boundary = await (statement as unknown as NativeStatement).prepareNativeStep(
          4096,
          new AbortController().signal,
        );
        boundary.check();
        break;
      } catch (error) {
        if (!(error instanceof NativeWriteStepChangedError)) throw error;
      } finally {
        boundary?.release();
      }
    }
    await work(statement);
    await statement.end();
  } catch (error) {
    statement.rollback();
    tx.rollback();
    throw error;
  }
  await (
    await tx.commit(null)
  ).durable;
}

async function nativeScenario(harness: Harness, outcome: Outcome): Promise<void> {
  let path = await harness.open();
  let t = 0;
  await autocommit(path, async (s) => {
    t =
      (
        await s.changeCatalog({
          kind: "createTable",
          table: tableInput("t", [column("id", INT, false), column("v", INT)]),
        })
      ).id ?? 0;
  });
  const v = path.catalog.table(t)?.columns[1]?.id ?? 0;
  await autocommit(path, (s) =>
    s.changeCatalog({
      kind: "createIndex",
      index: { tableId: t, name: "v_idx", columnIds: [v], unique: false, role: "secondary" },
    }),
  );
  const step = async (
    ids: number[],
    states: Array<number | "deleted">,
    work: () => Promise<unknown>,
  ) => {
    ids.forEach((id, k) => {
      outcome.uncertain.set(id, [outcome.acked.get(id) ?? "absent", states[k] ?? "absent"]);
    });
    await work();
    ids.forEach((id, k) => {
      outcome.uncertain.delete(id);
      outcome.acked.set(id, states[k] ?? "deleted");
    });
  };
  const insert = (ids: number[]) =>
    step(
      ids,
      ids.map((id) => id * 10),
      () =>
        native(path, (s) =>
          s.table(t).insert(batch(ints(ids), ints(ids.map((id) => id * 10))), null),
        ),
    );
  // Enough native transactions over 16-row memtables that flushes schedule merges at fan-in 2
  // while native statements keep arriving.
  for (let k = 0; k < 8; k++) await insert(Array.from({ length: 12 }, (_, j) => k * 12 + j));
  const primary = path.catalog.table(t)?.primaryKey ?? 0;
  const doomed = [3, 20, 40];
  const located = await probe(path, t, primary, doomed);
  await step(doomed, ["deleted", "deleted", "deleted"], () =>
    native(path, (s) => s.table(t).delete(batch(locators(located, t)), null)),
  );
  const [five] = await probe(path, t, primary, [5]);
  await step([5], [555], () =>
    native(path, (s) =>
      s.table(t, { assigned: [v] }).update(batch(locators([five ?? 0], t), ints([555])), null),
    ),
  );
  for (let k = 8; k < 12; k++) await insert(Array.from({ length: 12 }, (_, j) => k * 12 + j));
  await path.quiesce();
  path = await harness.reopen();
  await insert([1000, 1001]);
  await path.quiesce();
  await harness.close();
}

describe("crash sweep", () => {
  it("survives a crash at every physical step of native steps with deferred merges", async () => {
    const make = (disk?: SimDisk) =>
      new Harness("strict", "sim", MEMTABLE_ROWS / 2, disk, {
        merges: { fanIn: 2, sliceEntries: 4 },
      });
    // The clean run merges while native statements run, at more than one level.
    const clean = make();
    await nativeScenario(clean, { acked: new Map(), uncertain: new Map() });
    const path = await clean.open();
    const t = path.catalog.tableByName("t")?.id ?? 0;
    const levels = [...(path.state().tables.get(t)?.runs.values() ?? [])].flatMap((runs) =>
      runs.map((run) => run.level),
    );
    expect(Math.max(...levels)).toBeGreaterThanOrEqual(2);
    await clean.close();
    await sweep(make, nativeScenario);
  }, 3_600_000);
});
