/** A body sees exact typed scalar copies after the native helper has retired its borrow. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, TRIGGER_BINDING_REACH } from "../../testing/reach.js";
import { ROW_IMAGE_UNREAD, type RowBindingLease } from "../access/statement-command.js";
import type { TriggerBinding } from "../access/trigger-def.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import { TaskClock, yieldTask } from "../task.js";
import { TriggerBindingProgram, type OwnedTriggerBindings } from "./trigger-bindings.js";
import { sortRowView } from "./sort-row.js";
import {
  BOOL,
  DATE,
  F32,
  F64,
  I32,
  I64,
  TEXT,
  TS,
  bigints,
  constant,
  decimal,
  decimals,
  dictionary,
  evaluate,
  numbers,
  sqlLiteral,
  texts,
  valueAt,
} from "./kernels/test-support.js";

function need<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Missing generated fixture");
  return value;
}
const column = (name: string, type: TriggerBinding["type"]) => ({ name, type });

function sample(kind: number, n: number, width: number): { vector: FlatVector; sql: string } {
  const NUMERIC = decimal(null, null);
  switch (kind) {
    case 0:
      return { vector: numbers(I32, [n]), sql: sqlLiteral("int4", n) };
    case 1:
      return { vector: numbers(I64, [n]), sql: sqlLiteral("int8", n) };
    case 2: {
      const value = 2n ** 60n + BigInt(n);
      return { vector: bigints(I64, [value]), sql: sqlLiteral("int8", value) };
    }
    case 3:
    case 4:
    case 5:
    case 6: {
      const tier = ["safe", "int64", "int128", "wide"] as const,
        repr = tier[kind - 3] ?? "safe",
        coefficient = BigInt(n) * 10n ** BigInt([3, 15, 25, 50][kind - 3] ?? 3) + 123n,
        value = [coefficient, 7] as const;
      return { vector: decimals(NUMERIC, [value], repr), sql: sqlLiteral("numeric", value) };
    }
    case 7: {
      const micros = BigInt(n) * 1000000n + 123456n;
      return {
        vector: bigints(TS, [micros]),
        sql: `TIMESTAMP '2000-01-01' + ${String(n)} * INTERVAL '1 second' + INTERVAL '0.123456 seconds'`,
      };
    }
    case 8:
      return { vector: numbers(F32, [n / 8]), sql: sqlLiteral("float4", n / 8) };
    case 9:
      return { vector: numbers(F64, [n / 8]), sql: sqlLiteral("float8", n / 8) };
    case 10: {
      const text = `雪'\\\n${String(n)}${"x".repeat(width)}`;
      return { vector: texts([text]), sql: sqlLiteral("text", text) };
    }
    case 11:
      return { vector: numbers(BOOL, [n & 1]), sql: sqlLiteral("bool", (n & 1) === 1) };
    case 12:
      return { vector: numbers(DATE, [n]), sql: `DATE '2000-01-01' + ${String(n)}` };
    default: {
      const json = `{"n": ${String(n)}, "x": null}`;
      return {
        vector: texts([json], { kind: "json", binary: true }),
        sql: sqlLiteral("jsonb", json),
      };
    }
  }
}
function wrap(vector: FlatVector, form: number): Vector {
  if (form === 1) return constant(vector, 1);
  if (form === 2) return dictionary(vector, [0]);
  return vector;
}
function zero(vector: FlatVector): void {
  const d = vector.data;
  switch (d.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64":
      d.values.fill(0);
      break;
    case "varlen":
    case "fixed16":
      d.bytes.fill(0);
      break;
    case "int64":
      if (d.values.repr === "safe") d.values.values.fill(0);
      else d.values.values.fill(0n);
      break;
    case "interval":
      d.months.fill(0);
      d.days.fill(0);
      if (d.micros.repr === "safe") d.micros.values.fill(0);
      else d.micros.values.fill(0n);
      break;
    case "decimal":
      if (d.coefficients.repr === "safe") d.coefficients.values.fill(0);
      else if (d.coefficients.repr === "int128") d.coefficients.limbs.fill(0);
      else d.coefficients.values.fill(0n);
      d.scales?.fill(0);
      d.specials?.fill(0);
      break;
  }
}
function account(limit = Infinity) {
  let held = 0,
    peak = 0;
  return {
    allocate: (n: number) => {
      if (held + n > limit) return false;
      held += n;
      peak = Math.max(peak, held);
      return true;
    },
    free: (n: number) => {
      held -= n;
      expect(held).toBeGreaterThanOrEqual(0);
    },
    get held() {
      return held;
    },
    get peak() {
      return peak;
    },
  };
}
for (const seed of seedsFor("next-trigger-bindings", [0xb7d2, 0xb7d3]))
  it(`copies unique typed OLD/NEW slots and releases before body seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("trigger bindings", TRIGGER_BINDING_REACH);
    try {
      await pg.exec("SET standard_conforming_strings=on");
      await fc.assert(
        fc.asyncProperty(
          fc.array(fc.integer({ min: 0, max: 13 }), { minLength: 1, maxLength: 8 }),
          fc.integer({ min: -999, max: 999 }),
          fc.integer({ min: 0, max: 131073 }),
          fc.integer({ min: 0, max: 2 }),
          fc.boolean(),
          async (kinds, n, width, form, missing) => {
            reach.beginHistory();
            const old = kinds.map((k) => sample(k, n, width)),
              next = kinds.map((k) => sample(k, n + 17, width)),
              columns = kinds.map((_, i) => ({
                ...column(`c${String(i)}`, need(old[i]).vector.type),
                id: 10 + i,
              })),
              bindings: TriggerBinding[] = [];
            for (const source of ["old", "new"] as const)
              for (const col of columns)
                bindings.push({ source, columnId: col.id, name: col.name, type: col.type });
            bindings.push(need(bindings[0]), need(bindings[0]));
            if (missing)
              for (const value of [need(old[0]), need(next[0])])
                value.vector.validity = Uint32Array.of(0);
            const program = new TriggerBindingProgram(
                { id: 7, columns: [...columns, { ...column("unrelated", TEXT), id: 999 }] },
                bindings,
              ),
              charge = account(),
              expected = [...old, ...next, need(old[0]), need(old[0])].map((s) =>
                structuredClone(sortRowView(s.vector, 0)),
              ),
              sqls = [...old, ...next, need(old[0]), need(old[0])].map((s) =>
                s.vector.validity
                  ? `NULL::${s.vector.type.kind === "json" ? "jsonb" : "text"}`
                  : s.sql,
              );
            let released = 0,
              owned: OwnedTriggerBindings | undefined;
            try {
              owned = await program.bind(
                { spoolId: 1, row: 0 },
                {
                  readRowBindings: async (_ref, projection) => {
                    expect(projection.old).toEqual(columns.map((c) => c.id));
                    expect(projection.new).toEqual(columns.map((c) => c.id));
                    return {
                      tableId: 7,
                      old: [...old.map((s) => wrap(s.vector, form)), ROW_IMAGE_UNREAD],
                      new: [...next.map((s) => wrap(s.vector, form)), ROW_IMAGE_UNREAD],
                      release: () => {
                        if (released++) return;
                        for (const s of [...old, ...next]) zero(s.vector);
                      },
                    };
                  },
                },
                charge,
                { signal: new AbortController().signal },
              );
              expect(released, "image borrow retires before the body").toBe(1);
              expect(owned.slots).toEqual(expected);
              expect(owned.slots.at(-1)).toBe(owned.slots[0]);
              expect(owned.slots.at(-2)).toBe(owned.slots[0]);
              // The body uses typed IR params directly, never raw caller ParameterValues.
              for (const [i, slot] of owned.slots.entries()) {
                const got = evaluate(
                  {
                    op: "call",
                    fn: "to_json",
                    type: { kind: "json", binary: false },
                    args: [{ op: "param", slot: i, type: slot.type }],
                  },
                  { length: 1, columns: [], selection: null },
                  owned,
                )[0];
                const want = (
                  await pg.query<{ v: string | null }>(`SELECT to_json((${need(sqls[i])}))::text v`)
                ).rows[0]?.v;
                expect(got).toBe(want);
              }
              const uniqueBytes = expected.slice(0, kinds.length * 2).reduce((s, v) => {
                const data = v.data;
                return s + (data.layout === "varlen" ? data.bytes.length : 0);
              }, 0);
              expect(charge.peak).toBeLessThan(
                program.minimumBytes + uniqueBytes + 1024 * kinds.length,
              );
              reach.reach("unique repeated bindings");
              reach.reach("release before body");
              reach.reach("unrequested payload unread");
              reach.reach("exact native buffers");
              reach.reach("PG typed body value");
              reach.reach("zero ownership");
              reach.reach(form === 0 ? "flat" : form === 1 ? "constant" : "dictionary");
              if (missing) reach.reach("NULL");
              if (width > 65536 && kinds.includes(10)) reach.reach("fragmented UTF8");
              for (const kind of kinds) {
                if (kind >= 3 && kind <= 6)
                  reach.reach(
                    need(
                      (
                        ["decimal safe", "decimal int64", "decimal int128", "decimal wide"] as const
                      )[kind - 3],
                    ),
                  );
                if (kind === 2) reach.reach("wide int64");
                if (kind === 7) reach.reach("timestamp microseconds");
              }
            } finally {
              owned?.close();
              owned?.close();
              expect(charge.held).toBe(0);
              const closed = owned;
              if (closed) expect(() => closed.slots).toThrow("closed");
            }
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [Array.from({ length: 14 }, (_, i) => i), -67, 65537, 0, false],
            [Array.from({ length: 14 }, (_, i) => i), 123, 131073, 1, true],
            [[10, 2, 6, 7], 1, 131073, 2, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);

it("ignores NULL payloads and checks projected identity before copying", async () => {
  const owner = { id: 7, columns: [{ ...column("c", TEXT), id: 11 }] },
    program = new TriggerBindingProgram(owner, [
      { source: "old", columnId: 11, name: "c", type: TEXT },
    ]),
    charge = account();
  const vector: FlatVector = {
    form: "flat",
    type: TEXT,
    length: 1,
    validity: Uint32Array.of(0),
    get data(): never {
      throw new Error("NULL payload was read");
    },
  };
  let released = 0;
  const bind = (lease: RowBindingLease) =>
    program.bind({ spoolId: 1, row: 0 }, { readRowBindings: async () => lease }, charge, {
      signal: new AbortController().signal,
    });
  const good = await bind({
    tableId: 7,
    old: [vector],
    new: null,
    release: () => {
      released++;
    },
  });
  expect(valueAt(need(good.slots[0]), 0)).toBeNull();
  good.close();
  for (const lease of [
    { tableId: 8, old: [vector], new: null },
    { tableId: 7, old: [ROW_IMAGE_UNREAD], new: null },
    { tableId: 7, old: null, new: null },
    { tableId: 7, old: [numbers(I32, [1])], new: null },
  ] as const)
    await expect(
      bind({
        ...lease,
        release: () => {
          released++;
        },
      }),
    ).rejects.toMatchObject({ code: "MINNOW_TRIGGER_BINDING_STATE" });
  expect(released).toBe(5);
  expect(charge.held).toBe(0);
  expect(
    () =>
      new TriggerBindingProgram(owner, [{ source: "new", columnId: 12, name: "c", type: TEXT }]),
  ).toThrow("identity");
});
it("retains image/owned charges through fragment cancellation and unwinds release errors", async () => {
  const vector = texts(["雪".repeat(65537)]),
    program = new TriggerBindingProgram({ id: 7, columns: [{ ...column("c", TEXT), id: 1 }] }, [
      { source: "new", columnId: 1, name: "c", type: TEXT },
    ]);
  const cancel = new AbortController(),
    reason = new Error("cancel binding fragment"),
    charge = account();
  let released = 0,
    yields = 0,
    ticks = 0;
  await expect(
    program.bind(
      { spoolId: 1, row: 0 },
      {
        readRowBindings: async () => ({
          tableId: 7,
          old: null,
          new: [vector],
          release: () => {
            released++;
          },
        }),
      },
      charge,
      {
        signal: cancel.signal,
        clock: new TaskClock(() => (ticks += 5)),
        yieldTask: async () => {
          expect(released).toBe(0);
          expect(charge.held).toBeGreaterThan(
            vector.data.layout === "varlen" ? vector.data.bytes.length : 0,
          );
          await yieldTask();
          if (++yields === 3) cancel.abort(reason);
        },
      },
    ),
  ).rejects.toBe(reason);
  expect(yields).toBe(3);
  expect(released).toBe(1);
  expect(charge.held).toBe(0);
  await expect(
    program.bind(
      { spoolId: 1, row: 0 },
      {
        readRowBindings: async () => ({
          tableId: 8,
          old: null,
          new: [vector],
          release: () => {
            throw new Error("image release");
          },
        }),
      },
      charge,
      { signal: new AbortController().signal },
    ),
  ).rejects.toThrow(AggregateError);
  expect(charge.held).toBe(0);
});
it("opens no image for a binding-free body and returns charges when admission/read fails", async () => {
  const charge = account(),
    signal = new AbortController().signal;
  const empty = new TriggerBindingProgram({ id: 7, columns: [] }, []);
  const owned = await empty.bind(
    { spoolId: 1, row: 0 },
    {
      readRowBindings: async () => {
        throw new Error("unexpected read");
      },
    },
    charge,
    { signal },
  );
  expect(owned.slots).toEqual([]);
  owned.close();
  expect(charge.held).toBe(0);
  await expect(
    empty.bind(
      { spoolId: 1, row: 0 },
      {
        readRowBindings: async () => {
          throw new Error("unexpected read");
        },
      },
      account(0),
      { signal },
    ),
  ).rejects.toThrow("not admitted");
  const program = new TriggerBindingProgram({ id: 7, columns: [{ ...column("c", TEXT), id: 1 }] }, [
    { source: "new", columnId: 1, name: "c", type: TEXT },
  ]);
  await expect(
    program.bind(
      { spoolId: 1, row: 0 },
      {
        readRowBindings: async () => {
          throw new Error("read fault");
        },
      },
      charge,
      { signal },
    ),
  ).rejects.toThrow("read fault");
  expect(charge.held).toBe(0);
});

it("keeps prepaid ownership until a cancelled pending image read settles", async () => {
  const program = new TriggerBindingProgram(
      { id: 7, columns: [{ id: 1, name: "c", type: TEXT }] },
      [{ source: "new", columnId: 1, name: "c", type: TEXT }],
    ),
    cancel = new AbortController(),
    reason = new Error("cancel pending image"),
    charge = account();
  let releaseRead: (lease: RowBindingLease) => void = () => {
      throw new Error("read not started");
    },
    releases = 0,
    settled = false;
  const pending = new Promise<RowBindingLease>((resolve) => {
    releaseRead = resolve;
  });
  const binding = program.bind({ spoolId: 1, row: 0 }, { readRowBindings: () => pending }, charge, {
    signal: cancel.signal,
  });
  const observed = binding.then(
    (value) => {
      value.close();
      settled = true;
    },
    (error: unknown) => {
      settled = true;
      throw error;
    },
  );
  cancel.abort(reason);
  await Promise.resolve();
  expect(settled).toBe(false);
  expect(charge.held).toBe(program.minimumBytes);
  releaseRead({
    tableId: 7,
    old: null,
    new: [texts(["late"])],
    release: () => {
      releases++;
    },
  });
  await expect(observed).rejects.toBe(reason);
  expect(releases).toBe(1);
  expect(charge.held).toBe(0);
});
