/** Charged binding lifecycle and retry: a below-cap result is never a SQL memory refusal. */
import { expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { allocateFlat } from "../vector/vector.js";
import { Catalog } from "../write/catalog.js";
import { column, tableInput, TEXT } from "../write/test-support.js";
import { compileTableRules } from "../exec/rules.js";
import { bindTableRules, tableRuleMinimumBytes } from "../exec/row-rules.js";
import { ExpressionMemoryError } from "../exec/kernels/vectors.js";
const bytes = new TextEncoder().encode("x".repeat(65536));
function program() {
  const catalog = Catalog.empty().change(
    {
      kind: "createTable",
      table: tableInput("t", [{ ...column("g", TEXT), generatedSql: "'x'" }], { primaryKey: null }),
    },
    1,
  ).catalog;
  const table = catalog.tables[0];
  if (!table) throw new Error("missing table");
  const literal = {
    ...allocateFlat(TEXT, 1),
    data: { layout: "varlen" as const, bytes, offsets: Int32Array.of(0, bytes.length) },
  };
  return compileTableRules(table, catalog.version, {
    checks: [],
    generated: [
      { columnId: table.columns[0]?.id ?? 0, expr: { op: "literal", type: TEXT, value: literal } },
    ],
  });
}
it("charges before growth, preserves the old output on denial, retries and closes idempotently", async () => {
  const p = program(),
    gov = new Governor(4 * 1024 * 1024),
    min = tableRuleMinimumBytes(p, 1);
  const g = await gov.admit({ label: "rules", minimumBytes: min, spillable: true });
  const r = bindTableRules(p, g, { capacity: 1 });
  expect(g.allocated).toBeGreaterThanOrEqual(bytes.length);
  const other = await gov.admit({
    label: "competitor",
    minimumBytes: gov.capBytes - gov.reserveBytes - g.held,
    spillable: false,
  });
  const target = allocateFlat(TEXT, 1),
    before = target.data;
  try {
    expect(() => r.generate({ length: 1, columns: [target], selection: null }, [])).toThrow(
      ExpressionMemoryError,
    );
    expect(target.data).toBe(before);
    other.close();
    await g.makeRoom?.(bytes.length * 4);
    r.generate({ length: 1, columns: [target], selection: null }, []);
    expect(target.data.layout).toBe("varlen");
    if (target.data.layout !== "varlen") throw new Error("wrong result layout");
    expect(target.data.offsets[1]).toBe(bytes.length);
    expect(g.allocated).toBeGreaterThanOrEqual(bytes.length);
    // Reusing a complete previous result must preserve inactive values too.
    r.generate({ length: 1, columns: [target], selection: null }, []);
    expect(target.data.bytes.subarray(0, bytes.length)).toEqual(bytes);
  } finally {
    other.close();
    r.close();
    r.close();
    expect(g.allocated).toBe(0);
    g.close();
    expect(gov.usedBytes).toBe(0);
  }
  expect(() => r.check({ length: 1, columns: [target], selection: null }, null)).toThrow("closed");
});
it("honors cancellation and frees a partially compiled binding", async () => {
  const p = program(),
    gov = new Governor(4 * 1024 * 1024);
  const grant = await gov.admit({
    label: "cancel",
    minimumBytes: tableRuleMinimumBytes(p, 1),
    spillable: true,
  });
  const abort = new AbortController();
  const r = bindTableRules(p, grant, { capacity: 1, signal: abort.signal });
  abort.abort(new Error("stop rules"));
  expect(() =>
    r.generate({ length: 1, columns: [allocateFlat(TEXT, 1)], selection: null }, []),
  ).toThrow("stop rules");
  r.close();
  expect(grant.allocated).toBe(0);
  grant.close();
  let allocated = 0;
  expect(() =>
    bindTableRules(
      p,
      {
        allocate: (n) => {
          if (allocated + n > 5) return false;
          allocated += n;
          return true;
        },
        free: (n) => {
          allocated -= n;
        },
      },
      { capacity: 1 },
    ),
  ).toThrow(ExpressionMemoryError);
  expect(allocated).toBe(0);
});
