/** Independent keys and payloads under a competing grant holding all spare credit. */
import { expect, vi } from "vitest";
import type { PGlite } from "@electric-sql/pglite";
import { SortOperator } from "../exec/sort.js";
import { Governor, type Grant } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { oracleRows } from "./differential-adapter.js";
import {
  admitGroup,
  retireJsonHistories,
  type JsonHistory,
} from "./json-construct-history.test.fixture.js";

// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied.
const tryAdmit = Governor.prototype.tryAdmit;
// eslint-disable-next-line @typescript-eslint/unbound-method -- original receiver supplied.
const finishSort = SortOperator.prototype.finish;
const pendingPg = new Set<Promise<unknown>>();
const utf8 = new TextEncoder();
const quote = (text: string) => "'" + text.replaceAll("'", "''") + "'";
export interface StepCase {
  readonly family: "literal" | "computed" | "jsonb" | "array";
  readonly widths: readonly number[];
  readonly rows: number;
  readonly payload: number;
  readonly unicode: boolean;
  readonly filter: boolean;
  readonly nullKey: boolean;
  /** Exact known-blocker reproduction overrides only its diagnostic key-text total. */
  readonly storedKeyBudget?: number;
}
export async function retireStepHistories(): Promise<void> {
  await retireJsonHistories();
  await Promise.allSettled([...pendingPg]);
}
function variable(type: PhysicalType, values: ReadonlyArray<Uint8Array | null>): FlatVector {
  const offsets = new Int32Array(values.length + 1);
  let size = 0;
  const validity = new Uint32Array(Math.ceil(values.length / 32));
  for (const [i, value] of values.entries()) {
    if (value) {
      size += value.length;
      validity[i >>> 5] = (validity[i >>> 5] ?? 0) | (1 << (i & 31));
    }
    offsets[i + 1] = size;
  }
  const bytes = new Uint8Array(size);
  for (const [i, value] of values.entries()) if (value) bytes.set(value, offsets[i] ?? 0);
  return {
    form: "flat",
    type,
    length: values.length,
    validity,
    data: { layout: "varlen", bytes, offsets },
  };
}
export async function stepHistory(history: JsonHistory, pg: PGlite, data: StepCase) {
  // Keep wide-payload stored prefixes inside the supported source-admission domain.
  // Two equal-key rows still exercise aggregate ORDER BY; tiny payloads keep2..17.
  const stored = data.family === "jsonb" || data.family === "array";
  const rows = stored && data.payload ? Math.min(data.rows, 2) : data.rows;
  const { core, governor } = history;
  const cap = governor.capBytes;
  let occupied = false,
    release: (() => void) | undefined;
  let querySignal: AbortSignal | undefined;
  const guardGranted = (grants: readonly Grant[]) => {
    try {
      history.guard();
    } catch (error) {
      for (const grant of grants) grant.close();
      throw error;
    }
  };
  history.observe(
    vi.spyOn(governor, "tryAdmit").mockImplementation((options) => {
      history.guard();
      return options.label.endsWith("/fixed-buffers") ? null : tryAdmit.call(governor, options);
    }),
  );
  history.observe(
    vi.spyOn(governor, "admitGroup").mockImplementation(async (options) => {
      history.guard();
      if (options.label.endsWith("/fixed-buffers")) {
        querySignal = options.signal;
        release?.();
        release = undefined;
      }
      const grants = await admitGroup.call(governor, options);
      try {
        history.guard();
        if (
          data.family === "literal" &&
          rows === 1 &&
          !occupied &&
          options.label.endsWith("/fixed-buffers")
        ) {
          const bytes = cap - governor.usedBytes;
          const parts = await admitGroup.call(governor, {
            label: "competing scalar",
            includesProgress: true,
            ...(querySignal ? { signal: querySignal } : {}),
            grants: [{ label: "competing scalar", minimumBytes: bytes, spillable: true }],
          });
          guardGranted(parts);
          const blocker = parts[0];
          if (!blocker) throw new Error("Missing competing scalar grant");
          release = history.ownRelease(() => blocker.close());
          expect(blocker.allocate(bytes)).toBe(true);
          occupied = true;
        }
        return grants;
      } catch (error) {
        for (const grant of grants) grant.close();
        throw error;
      }
    }),
  );
  // Pressure begins only at the child-ready input-sort boundary: FILTER's CASE
  // projection has finished; the actual semantic comparisons have not begun.
  history.observe(
    vi.spyOn(SortOperator.prototype, "finish").mockImplementation(async function (
      this: SortOperator,
    ) {
      history.guard();
      if (!occupied) {
        const bytes = cap - governor.usedBytes;
        const parts = await admitGroup.call(governor, {
          label: "competing semantic step",
          includesProgress: true,
          ...(querySignal ? { signal: querySignal } : {}),
          grants: [{ label: "competing semantic step", minimumBytes: bytes, spillable: true }],
        });
        guardGranted(parts);
        const blocker = parts[0];
        if (!blocker) throw new Error("Missing competing step grant");
        release = history.ownRelease(() => blocker.close());
        history.guard();
        expect(blocker.allocate(bytes)).toBe(true);
        occupied = true;
        expect(governor.usedBytes).toBe(cap);
      }
      await this.reclaim();
      return finishSort.call(this);
    }),
  );
  const query = async (sql: string, values?: unknown[]) => {
    history.guard();
    const work = pg.query(sql, values);
    pendingPg.add(work);
    try {
      const result = await work;
      history.guard();
      return result;
    } finally {
      pendingPg.delete(work);
    }
  };
  const keys = data.widths.length;
  const sum = data.widths.reduce((n, width) => n + width, 0);
  // Keep this NEW stored dimension inside the known supported source-prefix
  // domain: <=256KiB across rows (including64KiB payloads and metadata margin).
  // Literal/computed histories retain the full four64KiB-key regression. The
  // pinned OPEN-ISSUES reproduction explicitly overrides this diagnostic limit.
  const storedKeyBudget = Math.max(
    4096,
    Math.floor(262144 / rows) - data.payload - 2048 * keys - 1024,
  );
  const keyBudget = data.storedKeyBudget ?? (stored ? Math.min(131072, storedKeyBudget) : 262144);
  const scale = Math.min(1, keyBudget / sum);
  const labels = data.widths.map(
    (width, i) =>
      String(i) +
      (data.unicode ? "汉" : "k").repeat(
        Math.max(1, Math.floor((width * scale) / (data.unicode ? 3 : 1))),
      ),
  );
  const payload = (data.unicode ? "汉" : "p").repeat(
    Math.floor(data.payload / (data.unicode ? 3 : 1)),
  );
  const keyNames = labels.map((_, i) => "k" + String(i));
  let sql: string;
  if (data.family === "literal" || data.family === "computed") {
    const from =
      rows === 1
        ? ""
        : " FROM(VALUES" +
          Array.from({ length: rows }, (_, i) => "(" + String(i + 1) + ")").join(",") +
          ")t(n)";
    const argument = data.payload ? quote(payload) + "::text" : rows === 1 ? "1" : "n";
    const order = rows === 1 ? "" : " ORDER BY n DESC";
    const filter = data.filter
      ? " FILTER(WHERE " + (rows === 1 ? "TRUE" : "n>" + String(Math.floor(rows / 2))) + ")"
      : "";
    const group = labels
      .map((label, i) =>
        data.nullKey && i === 0
          ? "NULL::jsonb"
          : data.family === "computed"
            ? "CASE WHEN " +
              (i === keys - 1 ? "n%2=0" : "n>0") +
              " THEN " +
              quote(JSON.stringify(label)) +
              "::jsonb ELSE " +
              quote(JSON.stringify(label + "z")) +
              "::jsonb END"
            : quote(JSON.stringify(label)) + "::jsonb",
      )
      .join(",");
    sql = `SELECT json_agg(${argument}${order})${filter}::text j${from} GROUP BY ${group}`;
  } else {
    const array = data.family === "array";
    await core.createTable({
      name: "step_keys",
      columns: [
        { name: "n", type: "number" },
        { name: "p", type: "string" },
        ...keyNames.map((name) => ({
          name,
          nullable: true,
          type: "string" as const,
          sqlDomain: array
            ? { kind: "array" as const, element: "text" as const }
            : { kind: "jsonb" as const },
        })),
      ],
    });
    history.guard();
    const element: PhysicalType = { kind: "text" };
    const keyType: PhysicalType = array
      ? { kind: "array", element }
      : { kind: "json", binary: true };
    const cells = labels.map((label, k) =>
      Array.from({ length: rows }, (_, row) => {
        const suffix = k === keys - 1 && !data.payload ? String(row % 3) : "same";
        const doc =
          row % 2
            ? `{"s":${JSON.stringify(label + suffix)},"n":1.00}`
            : `{"n":1,"s":${JSON.stringify(label + suffix)}}`;
        return {
          bytes:
            data.nullKey && k === 0
              ? null
              : array
                ? encodeArrayValue(element, {
                    dimensions: [3],
                    lowerBounds: [1],
                    elements: [utf8.encode(label), utf8.encode(suffix), null],
                  })
                : utf8.encode(doc),
          pg:
            data.nullKey && k === 0
              ? null
              : array
                ? '{"' + label + '","' + suffix + '",NULL}'
                : doc,
        };
      }),
    );
    const columns: FlatVector[] = [
      {
        form: "flat",
        type: { kind: "float64" },
        length: rows,
        validity: null,
        data: { layout: "f64", values: Float64Array.from({ length: rows }, (_, i) => i + 1) },
      },
      variable(
        { kind: "text" },
        Array.from({ length: rows }, (_, i) => utf8.encode(payload + String(i + 1))),
      ),
      ...cells.map((column) =>
        variable(
          keyType,
          column.map((value) => value.bytes),
        ),
      ),
    ];
    const table = core.harness.path.catalog.tableByName("step_keys");
    if (!table) throw new Error("Missing step key table");
    await core.insertVectors(table, { length: rows, columns, selection: null });
    history.guard();
    await core.harness.path.recycleLog();
    history.guard();
    await core.harness.path.quiesce();
    history.guard();
    await query("DROP TABLE IF EXISTS step_keys");
    await query(
      "CREATE TABLE step_keys(n DOUBLE PRECISION,p TEXT," +
        keyNames.map((name) => name + (array ? " TEXT[]" : " JSONB")).join(",") +
        ")",
    );
    for (let row = 0; row < rows; row++)
      await query(
        "INSERT INTO step_keys VALUES($1,$2," +
          keyNames.map((_, i) => "$" + String(i + 3) + (array ? "::text[]" : "::jsonb")).join(",") +
          ")",
        [row + 1, payload + String(row + 1), ...cells.map((column) => column[row]?.pg)],
      );
    const filter = data.filter ? " FILTER(WHERE n>" + String(Math.floor(rows / 2)) + ")" : "";
    sql = `SELECT json_agg(${data.payload ? "p" : "n"} ORDER BY n DESC)${filter}::text j FROM step_keys GROUP BY ${keyNames.join(",")}`;
  }
  const expected = (await query(sql)).rows;
  const prepare = core.executor.prepare.bind(core.executor);
  core.executor.prepare = (plan) => history.wrap(prepare(plan));
  const actual = await core.execute(sql);
  history.guard();
  if (!("batches" in actual)) throw new Error("Expected SELECT rows");
  expect(
    oracleRows(actual).sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))),
  ).toEqual(expected.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b))));
  expect(occupied).toBe(true);
  const spilled =
    core.lastProfile?.operators.reduce((bytes, op) => bytes + op.spilledBytes, 0) ?? 0;
  return { spilled, peak: governor.peakBytes };
}
