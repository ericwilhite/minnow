/** Native window admission fixture; the competitor owns every unadmitted byte. */
import { StorageCore } from "./storage-core.js";
import { Governor, type Grant } from "../memory/governor.js";
import { encodeArrayValue } from "../vector/array-value.js";
import type { PhysicalType } from "../types.js";
import { oracleRows } from "./differential-adapter.js";
export async function peerPressure(array: boolean, width: number, pressure: boolean) {
  const core = await new StorageCore(16).open(),
    governor = new Governor(128 << 20);
  let blocker: Grant | undefined,
    occupied = 0;
  const label = "k".repeat(width),
    second = label.slice(0, -1) + "j";
  const element: PhysicalType = { kind: "text" };
  const type: PhysicalType = array ? { kind: "array", element } : { kind: "json", binary: true };
  let failure: Error | undefined;
  let outcome:
    | { rows: ReturnType<typeof oracleRows>; peak: number; occupied: number; values: string[] }
    | undefined;
  try {
    await core.createTable({
      name: "ordered_keys",
      columns: [
        { name: "n", type: "number" },
        {
          name: "k",
          type: "string",
          sqlDomain: array ? { kind: "array", element: "text" } : { kind: "jsonb" },
        },
      ],
    });
    const encode = (value: string) =>
      array
        ? encodeArrayValue(element, {
            dimensions: [1],
            lowerBounds: [1],
            elements: [new TextEncoder().encode(value)],
          })
        : new TextEncoder().encode(JSON.stringify(value));
    const a = encode(label),
      b = encode(second),
      bytes = new Uint8Array(a.length + b.length);
    bytes.set(a);
    bytes.set(b, a.length);
    const table = core.harness.path.catalog.tableByName("ordered_keys");
    if (table === undefined) throw Error("table");
    await core.insertVectors(table, {
      length: 2,
      selection: null,
      columns: [
        {
          form: "flat",
          type: { kind: "float64" },
          length: 2,
          validity: null,
          data: { layout: "f64", values: Float64Array.of(1, 2) },
        },
        {
          form: "flat",
          type,
          length: 2,
          validity: null,
          data: { layout: "varlen", bytes, offsets: Int32Array.of(0, a.length, bytes.length) },
        },
      ],
    });
    await core.harness.path.recycleLog();
    await core.harness.path.quiesce();
    const native = governor.admitGroup.bind(governor);
    const controlled = new Proxy(governor, {
      get(target, key) {
        if (key === "tryAdmit")
          return (o: Parameters<Governor["tryAdmit"]>[0]) =>
            pressure && o.label.endsWith("/fixed-buffers") ? null : target.tryAdmit(o);
        if (key === "admitGroup")
          return async (o: Parameters<Governor["admitGroup"]>[0]) => {
            if (o.label.endsWith("/fixed-buffers")) {
              blocker?.close();
              blocker = undefined;
            }
            const grants = await native(o);
            if (
              pressure &&
              o.label.endsWith("/fixed-buffers") &&
              o.grants.some((g) => /\/window#\d+\/minimum$/u.test(g.label) && g.minimumBytes > 0)
            ) {
              const n = governor.capBytes - governor.usedBytes;
              [blocker] = await native({
                label: "competitor",
                includesProgress: true,
                grants: [{ label: "competitor", minimumBytes: n, spillable: true }],
              });
              if (!blocker?.allocate(n)) throw Error("competitor allocation");
              occupied++;
            }
            return grants;
          };
        const value: unknown = Reflect.get(target, key, target);
        return typeof value === "function"
          ? (value as (...args: unknown[]) => unknown).bind(target)
          : value;
      },
    });
    const prepare = core.executor.prepare.bind(core.executor);
    core.executor.prepare = (p) => {
      const prepared = prepare(p);
      return { ...prepared, execute: (r) => prepared.execute({ ...r, governor: controlled }) };
    };
    const query = await core.execute(
      "SELECT n,row_number() OVER(ORDER BY k)::int4 rn FROM ordered_keys",
    );
    if (!("batches" in query)) throw Error("window query result");
    const result = oracleRows(query);
    outcome = {
      rows: result,
      peak: governor.peakBytes,
      occupied,
      values: [
        array ? `{"${label}"}` : JSON.stringify(label),
        array ? `{"${second}"}` : JSON.stringify(second),
      ],
    };
  } catch (error) {
    failure = error instanceof Error ? error : new Error("Window failed", { cause: error });
  } finally {
    blocker?.close();
    await core.close();
  }
  if (governor.usedBytes !== 0 || core.spill.bytesInUse !== 0)
    throw Error("Window retained memory/spill after close");
  if (failure !== undefined) throw failure;
  if (outcome === undefined) throw Error("Missing window outcome");
  return outcome;
}
