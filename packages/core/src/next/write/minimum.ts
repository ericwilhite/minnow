/** Bounds one publication from its actual schema, directories and pending inputs (governor rule 3). */
import type { MaybePromise } from "../access/key-index.js";
import type { RunRef } from "../format/manifest.js";
import { parseCharge } from "../format/manifest-codec.js";
import { taskClock, yieldTask } from "../exec/pipeline.js";
import { indexMaximumKeyBytes } from "./index-limits.js";
import type { Catalog } from "./catalog.js";
import { GroupDirectory } from "./groups.js";
import { mergeInputBytes, MERGE_FIXED_BYTES, MERGE_INPUT_BYTES } from "./merge.js";
import { runKeyBytes } from "./overflow-keys.js";
import { RUN_ENCODER_MINIMUM, RunReader } from "./runs.js";
import type { WriteState } from "./state.js";

export interface WriteStepMinimum {
  readonly metadata: number;
  readonly merge: number;
}

/** The caller retains `state` through completion. The shared worker clock spans every table. */
export function writeStepMinimum(
  state: WriteState,
  catalog: Catalog,
  raw: number,
  fanIn: number,
  merging: boolean,
  known: WeakMap<RunRef, RunReader>,
  check: () => void,
): MaybePromise<WriteStepMinimum> {
  function* calculate(): Generator<void, WriteStepMinimum> {
    let largest = raw,
      runs = 0,
      directory = 0,
      runMetadata = 0,
      mergeBytes = MERGE_INPUT_BYTES,
      keyBytes = 2048;
    for (const def of catalog.tables) {
      largest = Math.max(largest, 512 + def.columns.length * 512);
      yield;
    }
    for (const table of state.root.tables) {
      largest = Math.max(largest, table.groups.length, table.statistics?.length ?? 0);
      yield;
    }
    for (const view of state.tables.values()) {
      // A fold may add several groups; deletion updates may copy every directory chunk.
      directory = Math.max(
        directory,
        GroupDirectory.buildCharge(view.groups.count + view.memtables.length + 1),
      );
      for (let group = 0; group < view.groups.count; group++) {
        largest = Math.max(largest, view.groups.root(group).length);
        yield;
      }
      const definition = catalog.table(view.tableId);
      for (const index of definition?.indexes ?? []) {
        if (definition !== undefined)
          keyBytes = Math.max(keyBytes, indexMaximumKeyBytes(index, definition));
        const refs = view.runs.get(index.id) ?? [];
        let missing = 0,
          pending = 0;
        for (const ref of refs) {
          if (!known.has(ref)) missing += RunReader.residentBound(ref);
          mergeBytes = Math.max(mergeBytes, mergeInputBytes(ref));
          keyBytes = Math.max(keyBytes, runKeyBytes(ref));
          yield;
        }
        for (const memtable of view.memtables) {
          pending++; // A delta run, in addition to any already published staged runs.
          for (const ref of memtable.staged?.runs.get(index.id) ?? []) {
            pending++;
            if (!known.has(ref)) missing += RunReader.residentBound(ref);
            mergeBytes = Math.max(mergeBytes, mergeInputBytes(ref));
            keyBytes = Math.max(keyBytes, runKeyBytes(ref));
            yield;
          }
          yield;
        }
        runs = Math.max(runs, refs.length + pending);
        runMetadata = Math.max(runMetadata, missing);
        yield;
      }
      yield;
    }
    return {
      metadata:
        RUN_ENCODER_MINIMUM +
        (keyBytes > 2048 ? 8 * keyBytes : 0) +
        4096 +
        directory +
        runMetadata +
        2 * parseCharge(largest) +
        5 * (raw + 512),
      merge: merging
        ? MERGE_FIXED_BYTES +
          Math.max(runs, fanIn) * mergeBytes +
          (keyBytes > 2048 ? 8 * keyBytes : 0)
        : 0,
    };
  }
  const work = calculate();
  const advance = (): MaybePromise<WriteStepMinimum> => {
    for (;;) {
      check();
      const step = work.next();
      if (step.done) return step.value;
      if (taskClock.due()) return yieldTask().then(advance);
    }
  };
  return advance();
}
