/** Decoded grouping keys belong to the grouping consumers, not every reserved lane. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  createReach,
  JSON_GROUP_SCOPED_REACH,
  JSON_GROUP_CONSUMER_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { Execution } from "../exec/executor.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
const closers = new Set<() => Promise<void>>();
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterEach(async () => {
  await Promise.all([...closers].map((close) => close()));
});
afterAll(async () => {
  await pg.close();
});
const quote = (text: string) => "'" + text.replaceAll("'", "''") + "'";
function repeated(type: PhysicalType, value: Uint8Array, other = value): FlatVector {
  const bytes = new Uint8Array(value.length + other.length);
  bytes.set(value);
  bytes.set(other, value.length);
  return {
    form: "flat",
    type,
    length: 2,
    validity: null,
    data: { layout: "varlen", bytes, offsets: Int32Array.of(0, value.length, bytes.length) },
  };
}
describe.each(seedsFor("next-json-group-scoped-workspace", [0xb73c, 0xb73d]))(
  "scoped group workspace seed %i",
  (seed) => {
    for (const family of [
      "literal",
      "jsonb",
      "array",
      "jsonb-order",
      "array-order",
      "jsonb-outer",
      "array-outer",
      "jsonb-window",
      "array-window",
      "opaque",
    ] as const) {
      const opaque = family === "opaque";
      const orderedKey = family.endsWith("-order");
      const parentKind =
        opaque || family.endsWith("-outer") ? "sort" : family.endsWith("-window") ? "window" : null;
      const composed = orderedKey || parentKind !== null;
      const kind = family.startsWith("jsonb")
        ? "jsonb"
        : family.startsWith("array")
          ? "array"
          : "literal";
      it(`funds ${family} keys at small caps with all other credit occupied`, async () => {
        // afterEach also retires the property: an abandoned fast-check body must not
        // start another shrink history after its previous history was cancelled.
        let retired = false;
        const retire = (): Promise<void> => {
          retired = true;
          closers.delete(retire);
          return Promise.resolve();
        };
        closers.add(retire);
        const reach = createReach<string>("scoped JSON grouping workspace", [
          ...(opaque
            ? JSON_GROUP_CONSUMER_REACH.opaque
            : JSON_GROUP_SCOPED_REACH.filter((s) => !composed || s !== "sixteen MiB cap")),
          ...(!opaque && composed ? JSON_GROUP_CONSUMER_REACH.composed : []),
          ...(orderedKey ? JSON_GROUP_CONSUMER_REACH.ordered : []),
          ...(!opaque && parentKind
            ? [...JSON_GROUP_CONSUMER_REACH.parent, ...JSON_GROUP_CONSUMER_REACH[parentKind]]
            : []),
          ...(!opaque && kind === "literal" ? JSON_GROUP_CONSUMER_REACH.single : []),
        ]);
        await fc.assert(
          fc.asyncProperty(
            fc.constantFrom(1, 4),
            fc.integer({ min: 60000, max: 65536 }),
            fc.boolean(),
            fc.boolean(),
            fc.boolean(),
            async (keys, total, one, utf8, small) => {
              if (retired) throw new Error("group workspace property closed");
              reach.beginHistory();
              const single = !opaque && kind === "literal" && one;
              const cap =
                (opaque
                  ? small
                    ? 12
                    : 16
                  : composed
                    ? 32
                    : single && keys === 1 && small
                      ? 10
                      : 16) << 20;
              const governor = new Governor(cap),
                core = new StorageCore(16);
              let blocker: Grant | undefined,
                execution: Execution | undefined,
                opening: Promise<StorageCore> | undefined,
                occupied = false,
                parentOccupied = false,
                closed = false,
                closing: Promise<void> | undefined;
              const close = (): Promise<void> =>
                (closing ??= (async () => {
                  closed = true;
                  blocker?.close();
                  blocker = undefined;
                  execution?.cancel(new Error("group workspace history closed"));
                  if (execution)
                    await Promise.allSettled([execution.executed, execution.delivered]);
                  if (opening)
                    await opening.then(
                      () => core.close(),
                      () => undefined,
                    );
                  closers.delete(close);
                })());
              closers.add(close);
              const guard = () => {
                if (closed) throw new Error("group workspace history closed");
              };
              const native = governor.admitGroup.bind(governor);
              const controlled = new Proxy(governor, {
                get(target, property) {
                  if (property === "tryAdmit")
                    return (options: Parameters<Governor["tryAdmit"]>[0]) =>
                      options.label.endsWith("/fixed-buffers") ? null : target.tryAdmit(options);
                  if (property === "admitGroup")
                    return async (options: Parameters<Governor["admitGroup"]>[0]) => {
                      guard();
                      if (options.label.endsWith("/fixed-buffers")) {
                        blocker?.close();
                        blocker = undefined;
                      }
                      const grants = await native(options);
                      if (closed) {
                        for (const grant of grants) grant.close();
                        guard();
                      }
                      if (
                        options.label.endsWith("/fixed-buffers") &&
                        ((!occupied &&
                          options.grants.some(
                            (g) => /\/aggregate#\d+\/minimum$/u.test(g.label) && g.minimumBytes > 0,
                          )) ||
                          (parentKind !== null &&
                            occupied &&
                            !parentOccupied &&
                            options.grants.some(
                              (g) =>
                                g.label.includes("/" + parentKind + "#") &&
                                g.label.endsWith("/minimum") &&
                                g.minimumBytes > 0,
                            )))
                      ) {
                        const bytes = cap - governor.usedBytes;
                        expect(bytes).toBeGreaterThan(0);
                        const parts = await native({
                          label: "competing grouping credit",
                          includesProgress: true,
                          ...(options.signal === undefined ? {} : { signal: options.signal }),
                          grants: [
                            {
                              label: "competing grouping credit",
                              minimumBytes: bytes,
                              spillable: true,
                            },
                          ],
                        });
                        blocker = parts[0];
                        if (!blocker) throw new Error("Missing competing credit");
                        expect(blocker.allocate(bytes)).toBe(true);
                        if (occupied) parentOccupied = true;
                        else occupied = true;
                        expect(governor.usedBytes).toBe(cap);
                      }
                      return grants;
                    };
                  const value: unknown = Reflect.get(target, property, target);
                  return typeof value === "function"
                    ? (value as (...args: unknown[]) => unknown).bind(target)
                    : value;
                },
              });
              try {
                opening = core.open();
                await opening;
                guard();
                // Wide window peer-flag allocation has a separate native failure on55684d20
                // too (B6-window-peer-flags-pressure.md). Keep these added window controls
                // in its working range; original16MiB grouping controls stay60–64KiB.
                const width =
                  parentKind === "window"
                    ? kind === "array"
                      ? Math.floor(total / 10)
                      : Math.min(total, 64000)
                    : total;
                const text = (utf8 ? "汉" : "k").repeat(Math.floor(width / keys / (utf8 ? 3 : 1)));
                const labels = Array.from({ length: keys }, (_, i) => String(i) + text);
                let sql: string;
                if (opaque) {
                  const pick = one ? 1 : 2;
                  const argument = `CASE WHEN n=1 THEN '1'::jsonb ELSE '${JSON.stringify(text)}'::jsonb END`;
                  const call = `jsonb_agg(${argument}) FILTER(WHERE n=${String(pick)})`;
                  sql = `SELECT ${call}::text j FROM generate_series(1,${keys === 1 ? "2" : "4"})t(n) GROUP BY n ORDER BY ${call}`;
                } else if (kind === "literal") {
                  const group = labels
                    .map((label) => quote(JSON.stringify(label)) + "::jsonb")
                    .join(",");
                  sql = single
                    ? `SELECT json_agg(1)::text j GROUP BY ${group}`
                    : `SELECT json_agg(n ORDER BY n)::text j FROM (VALUES(1),(2))t(n) GROUP BY ${group}`;
                } else {
                  await core.createTable({
                    name: "scoped_keys",
                    columns: [
                      { name: "n", type: "number" },
                      ...labels.map((_, i) => ({
                        name: "k" + String(i),
                        type: "string" as const,
                        sqlDomain:
                          kind === "jsonb"
                            ? { kind: "jsonb" as const }
                            : { kind: "array" as const, element: "text" },
                      })),
                    ],
                  });
                  guard();
                  const element: PhysicalType = { kind: "text" };
                  const columns: FlatVector[] = [
                    {
                      form: "flat",
                      type: { kind: "float64" },
                      length: 2,
                      validity: null,
                      data: { layout: "f64", values: Float64Array.of(1, 2) },
                    },
                  ];
                  const encode = (label: string) =>
                    kind === "jsonb"
                      ? new TextEncoder().encode(JSON.stringify(label))
                      : encodeArrayValue(element, {
                          dimensions: [2],
                          lowerBounds: [1],
                          elements: [new TextEncoder().encode(label), null],
                        });
                  for (const [i, label] of labels.entries())
                    columns.push(
                      repeated(
                        kind === "jsonb"
                          ? { kind: "json", binary: true }
                          : { kind: "array", element },
                        encode(label),
                        encode(parentKind && i === 0 ? "A" + label : label),
                      ),
                    );
                  const table = core.harness.path.catalog.tableByName("scoped_keys");
                  if (!table) throw new Error("Missing key table");
                  await core.insertVectors(table, { length: 2, columns, selection: null });
                  guard();
                  await core.harness.path.recycleLog();
                  guard();
                  await core.harness.path.quiesce();
                  guard();
                  await pg.exec(
                    "DROP TABLE IF EXISTS scoped_keys;CREATE TABLE scoped_keys(n DOUBLE PRECISION," +
                      labels
                        .map((_, i) => "k" + String(i) + (kind === "jsonb" ? " JSONB" : " TEXT[]"))
                        .join(",") +
                      ")",
                  );
                  guard();
                  for (const n of [1, 2]) {
                    const values = labels.map((label, i) => {
                      const actual = parentKind && n === 2 && i === 0 ? "A" + label : label;
                      return kind === "jsonb" ? JSON.stringify(actual) : '{"' + actual + '",NULL}';
                    });
                    guard();
                    await pg.query(
                      "INSERT INTO scoped_keys VALUES($1," +
                        labels
                          .map(
                            (_, i) =>
                              "$" + String(i + 2) + (kind === "jsonb" ? "::jsonb" : "::text[]"),
                          )
                          .join(",") +
                        ")",
                      [n, ...values],
                    );
                    guard();
                  }
                  guard();
                  const keys = labels.map((_, i) => "k" + String(i)).join(",");
                  sql = parentKind
                    ? `SELECT j${parentKind === "window" ? ",row_number() OVER(ORDER BY k0)::int4 rn" : ""} FROM(SELECT ${keys},json_agg(n ORDER BY n)::text j FROM scoped_keys GROUP BY ${keys})q ORDER BY ${parentKind === "window" ? "rn" : "k0"}`
                    : `SELECT json_agg(n ORDER BY ${orderedKey ? "k0,n" : "n"})::text j FROM scoped_keys GROUP BY ${keys}`;
                }
                const expected = (await pg.query(sql)).rows;
                guard();
                const prepare = core.executor.prepare.bind(core.executor);
                core.executor.prepare = (plan) => {
                  const p = prepare(plan);
                  return {
                    ...p,
                    execute: (request) => {
                      guard();
                      execution = p.execute({ ...request, governor: controlled });
                      return execution;
                    },
                  };
                };
                const actual = await core.execute(sql);
                guard();
                if (!("batches" in actual)) throw new Error("Expected grouping rows");
                expect(oracleRows(actual)).toEqual(expected);
                expect(occupied).toBe(true);
                reach.reach("competing credit includes reserve");
                expect(
                  core.lastProfile?.operators.reduce((n, op) => n + op.spilledBytes, 0) ?? 0,
                ).toBeGreaterThan(0);
                reach.reach("observed spill");
                if (opaque) {
                  expect(parentOccupied).toBe(true);
                  expect(expected).toHaveLength(keys === 1 ? 2 : 4);
                  reach.reach("opaque aggregate result");
                  reach.reach(one ? "small result" : "large result");
                  reach.reach(utf8 ? "UTF8 literal" : "ASCII literal");
                  reach.reach(keys === 1 ? "two groups" : "four groups");
                  reach.reach(small ? "twelve MiB cap" : "sixteen MiB cap");
                  reach.reach("parent consumer pressure");
                } else {
                  reach.reach(keys === 1 ? "one key" : "four distinct keys");
                  reach.reach(utf8 ? "UTF8 keys" : "ASCII keys");
                  if (single) reach.reach("single input");
                  else reach.reach("multiple inputs");
                  if (composed) {
                    reach.reach("thirty-two MiB cap");
                    if (orderedKey) reach.reach("structural aggregate ORDER BY");
                    if (parentKind) {
                      expect(parentOccupied).toBe(true);
                      expect(expected).toHaveLength(2);
                      reach.reach("parent consumer pressure");
                      reach.reach("two groups");
                      reach.reach(parentKind);
                      if (parentKind === "window") reach.reach("bounded window key range");
                    }
                  } else if (cap === 10 << 20) reach.reach("ten MiB cap");
                  else reach.reach("sixteen MiB cap");
                }
                reach.reach("exact PG output");
              } finally {
                await close();
              }
              expect(governor.usedBytes).toBe(0);
              expect(core.spill.bytesInUse).toBe(0);
              reach.reach("zero resources");
            },
          ),
          {
            seed,
            numRuns: 8,
            examples: [
              [1, 65536, true, false, true],
              [1, 65536, true, false, false],
              [4, 65536, false, false, false],
              [1, 65536, false, true, false],
              [4, 64000, true, true, false],
            ],
          },
        );
        reach.assertReached();
        await retire();
      }, 120000);
    }
  },
);
