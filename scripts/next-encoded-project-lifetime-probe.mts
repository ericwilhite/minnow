/** Isolated GC witness for expired ordinary batches across encoded projection steps.
 * Forced GC belongs only to this lifetime test, never to a performance clock.
 */
import { strict as assert } from "node:assert";
import { setImmediate as nextTask } from "node:timers/promises";
import { PageProjectOperator } from "../packages/core/dist/next/exec/filter.js";
import type { Operator } from "../packages/core/dist/next/exec/pipeline.js";
import type {
  PageColumnMap,
  ScanPageConsumer,
} from "../packages/core/dist/next/exec/page-consumer.js";
import { OutputArena } from "../packages/core/dist/next/exec/kernels/vectors.js";
import { F64, UNCHARGED, col } from "../packages/core/dist/next/exec/kernels/test-support.js";

export interface EncodedBorrowCase {
  depth: number;
  blocked: boolean;
  throwing: boolean;
}
const gc = (globalThis as { gc?: () => void }).gc;
if (!gc) throw new Error("Run the lifetime witness with --expose-gc");
const keepers: PageProjectOperator[] = [];
function ordinary(project: PageProjectOperator): WeakRef<ArrayBuffer> {
  // One logical value borrowing a larger backing store. Keep capacity, not logical length.
  const values = new Float64Array(1 << 20);
  values[0] = 1.25;
  const weak = new WeakRef(values.buffer);
  project.push({
    length: 1,
    selection: null,
    columns: [
      { form: "flat", type: F64, length: 1, validity: null, data: { layout: "f64", values } },
    ],
  });
  return weak;
}
function encoded(
  project: PageProjectOperator,
  throwing: boolean,
  failure: Error,
): WeakRef<PageColumnMap> {
  const map: PageColumnMap = { pageColumn: (index) => index },
    weak = new WeakRef(map),
    rows = Uint16Array.of(0);
  assert.equal(project.canConsumePages([], rows, 1, map), true);
  const run = () => project.consumePages([], rows, 1, map);
  if (throwing) assert.throws(run, (error) => error === failure);
  else assert.equal(run(), undefined);
  return weak;
}
const cases = JSON.parse(process.argv[2] ?? "[]") as EncodedBorrowCase[];
assert(cases.length > 0);
let retainedInputBytes = 0,
  retainedBindings = 0;
for (const scenario of cases) {
  assert(Number.isInteger(scenario.depth) && scenario.depth >= 1 && scenario.depth <= 4);
  const failure = new Error("encoded receiver failed"),
    arena = new OutputArena(UNCHARGED, 2048),
    context = { arena, input: [F64], params: { slots: [] } },
    capability: ScanPageConsumer = {
      canConsumePages: () => true,
      consumePages(_columns, _rows, _count, map) {
        assert.equal(map?.pageColumn(0) ?? 0, 0);
        if (scenario.throwing) throw failure;
        return undefined;
      },
    },
    sink: Operator = {
      pageConsumer: capability,
      push: () => (scenario.blocked ? "blocked" : "ready"),
      ready: () => Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
  let downstream = sink;
  for (let level = 0; level < scenario.depth; level++)
    downstream = new PageProjectOperator([col(0, F64)], context, downstream);
  const project = downstream as PageProjectOperator;
  keepers.push(project);
  try {
    const source = ordinary(project);
    if (scenario.blocked) await project.ready();
    await nextTask();
    gc();
    assert(source.deref(), "positive control: the ordinary projection must still be strongly live");
    const binding = encoded(project, scenario.throwing, failure);
    for (let turn = 0; turn < 4; turn++) {
      await nextTask();
      gc();
    }
    retainedInputBytes += source.deref()?.byteLength ?? 0;
    retainedBindings += binding.deref() === undefined ? 0 : 1;
    assert.equal(keepers[keepers.length - 1], project, "projection stays live through the check");
    assert.equal(retainedInputBytes, 0, "encoded projection retained an expired source buffer");
    assert.equal(retainedBindings, 0, "encoded projection retained borrowed binding metadata");
  } finally {
    project.abort(undefined);
    arena.close();
    assert.equal(keepers.pop(), project);
  }
}
console.log(JSON.stringify({ cases: cases.length, retainedInputBytes, retainedBindings }));
