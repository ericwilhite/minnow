/**
 * The memtable charge bounds what an insert step allocates (statement-sizing review, PR 2).
 *
 * Law (memory governor contract, next/memory/README.md): every byte a write step allocates is
 * charged before it is allocated, within what the write path admitted for that step. An insert
 * step admits `estimate + max(0, growth − 2 × input)` (write/transaction.ts `#beyondEstimate`):
 * per-row metadata plus the larger of `2 × input` and the live arenas' growth with the resize
 * overlap. Before, it admitted their sum, 2.0-3.0× what it allocates.
 *
 * The census, for every step (one `admit` call to the next, or to the statement's end):
 * - the row account's allocation never rises above its level at admission by more than the step
 *   admitted (the peak, with each arena resize's old and new arena both live);
 * - every typed-array backing the step constructs is charged: the bytes constructed never exceed
 *   the bytes the row account charged during the step.
 * Generated: widths from a few bytes to multi-MiB, batches that cross arena doublings and the
 * 2,048-row chunk, one to three text columns growing in one statement, a secondary index,
 * UPDATE rewriting whole rows, the copy of one column into another, and integers beyond 2^53
 * widening a live chunk.
 */
import fc from "fast-check";
import { afterEach, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { createReach, MEMTABLE_CHARGE_CENSUS_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  tableInput,
  TEXT,
  texts,
} from "../write/test-support.js";

const MiB = 1 << 20;

interface Account {
  allocated: number;
  allocate(bytes: number): boolean;
  free(bytes: number): void;
}
interface Path {
  admit(bytes: number, growth?: () => number): unknown;
  writerHoldings(): { readonly grants: readonly Account[] } | null;
}

interface Step {
  readonly admitted: number;
  /** Net bytes the step's own work (`#insertRows`) charged to the row account, and its peak. */
  net: number;
  peak: number;
  charged: number;
  constructed: number;
  readonly label: string;
}

/** Whether the caller is the insert step's own work (not commit or executor work). */
const inStep = () => {
  const limit = Error.stackTraceLimit;
  Error.stackTraceLimit = 80;
  const stack = new Error().stack ?? "";
  Error.stackTraceLimit = limit;
  return stack.includes("#insertRows");
};

const TYPED = [
  "ArrayBuffer",
  "Uint8Array",
  "Int8Array",
  "Uint16Array",
  "Int16Array",
  "Uint32Array",
  "Int32Array",
  "Float32Array",
  "Float64Array",
  "BigInt64Array",
  "BigUint64Array",
] as const;

/** Observes the write path's insert steps; `check()` asserts the law on each. */
function census(writePath: unknown) {
  const path = writePath as Path;
  const rows = path.writerHoldings()?.grants[0];
  if (rows === undefined) throw new Error("write path row account");
  const steps: Step[] = [];
  let current: Step | null = null;
  let label = "";
  const allocate = rows.allocate.bind(rows),
    free = rows.free.bind(rows),
    admit = path.admit.bind(path);
  rows.allocate = (bytes: number) => {
    const ok = allocate(bytes);
    if (ok && current !== null && inStep()) {
      current.charged += bytes;
      current.net += bytes;
      current.peak = Math.max(current.peak, current.net);
    }
    return ok;
  };
  rows.free = (bytes: number) => {
    free(bytes);
    if (current !== null && inStep()) current.net -= bytes;
  };
  path.admit = (bytes: number, growth?: () => number) => {
    current = {
      admitted: bytes + (growth?.() ?? 0),
      net: 0,
      peak: 0,
      charged: 0,
      constructed: 0,
      label,
    };
    steps.push(current);
    return admit(bytes, growth);
  };
  // Backings constructed while a step is open (not views of an existing buffer).
  const restore: Array<() => void> = [];
  for (const name of TYPED) {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, name);
    if (descriptor === undefined || typeof descriptor.value !== "function") continue;
    const original = descriptor.value as new (...args: unknown[]) => object;
    const proxy = new Proxy(original, {
      construct(target, args, newTarget) {
        const made = Reflect.construct(target, args, newTarget) as object;
        if (current !== null && inStep()) {
          if (name === "ArrayBuffer") current.constructed += (made as ArrayBuffer).byteLength;
          else if (!(args[0] instanceof ArrayBuffer) && !(args[0] instanceof SharedArrayBuffer))
            current.constructed += (made as ArrayBufferView).byteLength;
        }
        return made;
      },
    });
    Object.defineProperty(globalThis, name, { ...descriptor, value: proxy });
    restore.push(() => Object.defineProperty(globalThis, name, descriptor));
  }
  return {
    /** Names the statement whose steps follow; closes the previous statement's last step. */
    statement(name: string) {
      current = null;
      label = name;
    },
    end() {
      current = null;
    },
    steps,
    check() {
      for (const step of steps) {
        expect(step.peak, `${step.label}: peak above admission`).toBeLessThanOrEqual(step.admitted);
        expect(step.constructed, `${step.label}: uncharged backing`).toBeLessThanOrEqual(
          step.charged,
        );
      }
    },
    restore() {
      for (const undo of restore) undo();
      rows.allocate = allocate;
      rows.free = free;
      path.admit = admit;
    },
  };
}

interface Statement {
  readonly kind: "insert" | "update" | "copy";
  readonly rows: number;
  readonly width: number;
  readonly columns: 1 | 2 | 3;
  /** An insert's `big` beyond 2^53: an existing chunk widens its integers in the step. */
  readonly wide?: boolean;
}

afterEach(() => {
  Governor.stallAfterMs = null;
});

async function run(
  statements: readonly Statement[],
  indexed: boolean,
  reach: ReturnType<typeof createReach>,
  keyed = true,
) {
  const core = new StorageCore(1 << 16);
  Object.defineProperty(core.harness, "governor", { value: new Governor(512 * MiB) });
  await core.open();
  await core.execute(
    `CREATE TABLE t(id INT${keyed ? " PRIMARY KEY" : ""}, n INT, big BIGINT, d NUMERIC, a TEXT, b TEXT, c TEXT)`,
  );
  if (indexed) await core.execute("CREATE INDEX t_n ON t(n)");
  const watch = census(core.harness.path);
  try {
    let next = 1;
    for (const [at, statement] of statements.entries()) {
      const value = "v".repeat(statement.width);
      const names = ["a", "b", "c"].slice(0, statement.columns);
      watch.statement(
        `#${String(at)} ${statement.kind} ${String(statement.rows)}×${String(statement.width)}×${String(statement.columns)}`,
      );
      if (statement.kind === "insert") {
        const first = next;
        next += statement.rows;
        const expression = names.map(() => "$1").join(", ");
        await core.execute(
          `INSERT INTO t(id, n, big, d, ${names.join(", ")}) SELECT g, g % 7, ${statement.wide === true ? "9007199254740993, 123456789012345678901234567890" : "g, g"}, ${expression} FROM generate_series(${String(first)}, ${String(first + statement.rows - 1)}) g`,
          [value],
        );
      } else if (statement.kind === "update") {
        await core.execute(
          `UPDATE t SET ${names.map((name) => `${name} = $1`).join(", ")} WHERE id <= ${String(statement.rows)}`,
          [value],
        );
      } else {
        await core.execute(`UPDATE t SET b = a WHERE id <= ${String(statement.rows)}`);
      }
      watch.end();
      if (statement.rows > 2048) reach.reach("batch across a chunk");
      if (statement.width * statement.rows > MiB) reach.reach("multi-MiB step");
      if (statement.columns > 1) reach.reach("several columns growing");
      reach.reach(statement.kind);
      if (statement.kind === "insert" && statement.wide === true)
        reach.reach(
          at > 0 ? "integers widened in a live chunk" : "integers widened in a fresh chunk",
        );
    }
    for (const step of watch.steps) if (step.peak > 0) reach.reach("allocating step");
    watch.check();
    if (indexed) reach.reach("secondary index");
    if (!keyed) reach.reach("no primary key");
  } finally {
    watch.restore();
    await core.close();
  }
}

it("bounds batches that double one arena several times in a step (the resize overlap)", async () => {
  const reach = createReach("memtable census pinned", [
    "insert",
    "allocating step",
    "multi-MiB step",
  ]);
  reach.beginHistory();
  // Rows of 1 KiB into a fresh 256-byte arena, and into a live arena a small row opened: each
  // doubles several times within a step, the last resize holding the arena it replaces
  // (charged in that step) beside the new one.
  await run([{ kind: "insert", rows: 300, width: 1024, columns: 1 }], false, reach);
  // Just over 1 MiB in one step: the fresh arena ends at 2 MiB with the 1 MiB one beside it,
  // half again more than twice the input.
  await run([{ kind: "insert", rows: 300, width: 4100, columns: 1 }], false, reach);
  await run(
    [
      { kind: "insert", rows: 3, width: 8, columns: 1 },
      { kind: "insert", rows: 300, width: 1024, columns: 1 },
    ],
    false,
    reach,
  );
  reach.assertReached();
}, 120_000);

it("bounds one write-path batch that doubles a fresh arena several times", async () => {
  // The executor hands the write path byte-bounded batches; its `insert` takes any batch. 300
  // rows of 4,100 bytes in one: the fresh arena ends at 2 MiB with the 1 MiB one beside it, half
  // again more than twice the input.
  const h = new Harness("strict", "memory", 1 << 16);
  Object.defineProperty(h, "governor", { value: new Governor(512 * MiB) });
  const path = await h.open();
  const watch = census(path);
  try {
    let t = 0;
    await autocommit(path, async (s) => {
      t =
        (
          await s.changeCatalog({
            kind: "createTable",
            table: tableInput("t", [column("id", INT, false), column("v", TEXT)]),
          })
        ).id ?? 0;
    });
    const ids = Array.from({ length: 300 }, (_, i) => i);
    watch.statement("write-path batch 300×4100");
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints(ids), texts(ids.map(() => "w".repeat(4100)))), null),
    );
    watch.end();
    expect(watch.steps.length).toBe(1);
    expect(watch.steps[0]?.peak).toBeGreaterThan(3 * MiB - 4096);
    watch.check();
  } finally {
    watch.restore();
    await h.close();
  }
}, 60_000);

it("charges a fresh chunk's integer and decimal widening (Astra, review of #56)", async () => {
  // BIGINT beyond 2^53 and a 30-digit NUMERIC into a table with no key: the fresh chunk widens in
  // the step (was admitted 16,692 / 20,788 bytes, peaked at 32,800 / 53,280).
  const reach = createReach("memtable census fresh widening", [
    "insert",
    "allocating step",
    "integers widened in a fresh chunk",
    "no primary key",
  ]);
  reach.beginHistory();
  await run([{ kind: "insert", rows: 1, width: 8, columns: 1, wide: true }], false, reach, false);
  reach.assertReached();
}, 60_000);

const statement: fc.Arbitrary<Statement> = fc.oneof(
  fc.record({
    kind: fc.constantFrom("insert" as const, "update" as const, "copy" as const),
    rows: fc.constantFrom(1, 3, 300, 2100),
    width: fc.constantFrom(8, 100, 1024, 5000),
    columns: fc.constantFrom(1 as const, 2 as const, 3 as const),
    wide: fc.boolean(),
  }),
  fc.record({
    kind: fc.constantFrom("insert" as const, "update" as const, "copy" as const),
    rows: fc.constantFrom(1, 2),
    width: fc.constantFrom(300_000, MiB, 3 * MiB),
    columns: fc.constantFrom(1 as const, 2 as const),
  }),
);

for (const seed of seedsFor("next-memtable-charge-census", [0x5d01, 0x5d02]))
  it(`charges every insert step at least what it allocates (seed ${String(seed)})`, async () => {
    const reach = createReach("memtable charge census", MEMTABLE_CHARGE_CENSUS_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.array(statement, { minLength: 1, maxLength: 4 }),
        fc.boolean(),
        fc.boolean(),
        async (statements, indexed, keyed) => {
          reach.beginHistory();
          await run(statements, indexed, reach, keyed);
        },
      ),
      {
        seed,
        numRuns: 24,
        examples: [
          // Astra's review of #56: a fresh chunk widens (no key masks it).
          [[{ kind: "insert", rows: 1, width: 8, columns: 1, wide: true }], false, false],
          [[{ kind: "insert", rows: 300, width: 1024, columns: 1 }], false, true],
          // A later row's integer beyond 2^53 widens the live chunk's integers (16 KiB).
          [
            [
              { kind: "insert", rows: 3, width: 8, columns: 1 },
              { kind: "insert", rows: 1, width: 8, columns: 1, wide: true },
            ],
            false,
            true,
          ],
          // A small row first, so the next step doubles the live arena several times.
          [
            [
              { kind: "insert", rows: 3, width: 8, columns: 1 },
              { kind: "insert", rows: 300, width: 1024, columns: 1 },
            ],
            false,
            true,
          ],
          [
            [
              { kind: "insert", rows: 2, width: 3 * MiB, columns: 1 },
              { kind: "copy", rows: 2, width: 3 * MiB, columns: 1 },
            ],
            true,
            true,
          ],
          [
            [
              { kind: "insert", rows: 2100, width: 100, columns: 3 },
              { kind: "update", rows: 2100, width: 1024, columns: 3 },
            ],
            true,
            true,
          ],
        ],
      },
    );
    reach.assertReached();
  }, 900_000);
