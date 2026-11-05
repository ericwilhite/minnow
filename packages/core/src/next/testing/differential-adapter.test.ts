import { describe, expect, it } from "vitest";
import { currentEngine } from "../../testing/differential-current.js";
import { compareEngines, type Workload } from "../../testing/differential-harness.js";
import type { OutputColumn } from "../ir/ir.js";
import { setAllValid, allocateValidity, setValid } from "../vector/validity.js";
import { VarlenBuilder } from "../vector/varlen.js";
import { allocateFlat, newDictionaryId, type Batch, type FlatVector } from "../vector/vector.js";
import { nextEngine, oracleRows, type NextCore, type NextRows } from "./differential-adapter.js";

function int32Vector(values: number[]): FlatVector {
  const vector = allocateFlat({ kind: "int32", bits: 32 }, values.length);
  if (vector.data.layout === "i32") vector.data.values.set(values);
  vector.length = values.length;
  return vector;
}

function textVector(values: string[]): FlatVector {
  const builder = new VarlenBuilder(values.length, {
    tryGrow: () => true,
    release: () => undefined,
  });
  for (const value of values) builder.appendString(value);
  return {
    form: "flat",
    type: { kind: "text" },
    length: values.length,
    validity: null,
    data: builder.data(),
  };
}

/** The literal row the toy core answers for one SELECT, built from vectors by hand. */
function literalRow(): NextRows {
  const columns: OutputColumn[] = [
    { name: "n", type: { kind: "int32", bits: 32 }, nullable: false },
    { name: "t", type: { kind: "text" }, nullable: false },
    { name: "d", type: { kind: "date" }, nullable: false },
    { name: "m", type: { kind: "decimal", precision: 5, scale: 2 }, nullable: false },
    { name: "b", type: { kind: "bool" }, nullable: false },
    { name: "z", type: { kind: "float64" }, nullable: true },
  ];
  const date = allocateFlat({ kind: "date" }, 1);
  if (date.data.layout === "i32") date.data.values[0] = 8_825; // 2024-02-29: days since 2000-01-01
  date.length = 1;
  const decimal = allocateFlat({ kind: "decimal", precision: 5, scale: 2 }, 1);
  if (decimal.data.layout === "decimal" && decimal.data.coefficients.repr === "safe")
    decimal.data.coefficients.values[0] = 150;
  decimal.length = 1;
  const bool = allocateFlat({ kind: "bool" }, 1);
  if (bool.data.layout === "bool") bool.data.values[0] = 1;
  bool.length = 1;
  const nullFloat = allocateFlat({ kind: "float64" }, 1);
  nullFloat.validity = allocateValidity(1);
  nullFloat.length = 1;
  const batch: Batch = {
    length: 1,
    selection: null,
    columns: [int32Vector([1]), textVector(["hi"]), date, decimal, bool, nullFloat],
  };
  return { columns, batches: [batch] };
}

const SQL =
  "SELECT 1 AS n, 'hi' AS t, CAST('2024-02-29' AS DATE) AS d, CAST(1.5 AS NUMERIC(5,2)) AS m, TRUE AS b, CAST(NULL AS DOUBLE PRECISION) AS z";

const toyCore = async (): Promise<NextCore> => ({
  createTable: () => Promise.resolve(),
  execute: async (sql) => {
    if (sql !== SQL) throw new Error("The toy core answers one statement");
    return { kind: "rows", ...literalRow() };
  },
  writeBatch: async () => ({}),
  deleteBatch: async () => ({}),
  reopen: () => Promise.resolve(),
  close: () => Promise.resolve(),
});

describe("next-core differential adapter", () => {
  it("runs a toy core against the current engine through the frozen harness", async () => {
    const workload: Workload = {
      name: "adapter-literals",
      operations: [{ kind: "sql", sql: SQL }],
    };
    const reference = await currentEngine("memory");
    const candidate = await nextEngine(toyCore)("memory");
    try {
      await compareEngines(workload, [reference, candidate]);
    } finally {
      await reference.close();
      await candidate.close();
    }
  });

  it("reads dictionary, constant and selected vectors", () => {
    const dictionary = textVector(["red", "green"]);
    const validity = allocateValidity(3);
    setAllValid(validity, 3);
    setValid(validity, 1, false);
    const codes = new Int32Array([1, 0, 0]);
    const constant = int32Vector([7]);
    const rows = oracleRows({
      columns: [
        { name: "c", type: { kind: "text" }, nullable: true },
        { name: "k", type: { kind: "int32", bits: 32 }, nullable: false },
      ],
      batches: [
        {
          length: 3,
          selection: { rows: new Uint16Array([0, 1]), count: 2 },
          columns: [
            {
              form: "dictionary",
              type: { kind: "text" },
              length: 3,
              codes,
              validity,
              dictionary,
              dictionaryId: newDictionaryId(),
            },
            { form: "constant", type: { kind: "int32", bits: 32 }, length: 3, value: constant },
          ],
        },
      ],
    });
    expect(rows).toEqual([
      { c: "green", k: 7 },
      { c: null, k: 7 },
    ]);
  });
});
