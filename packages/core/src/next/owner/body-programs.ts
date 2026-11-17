/** Immutable body preparation and private scratch schemas. Synthetic schemas never enter
 * MCAT/WAL, and their IDs cannot be used by the native accepted-row continuation. */
import type { CatalogView } from "../access/catalog.js";
import type { TriggerBody } from "../access/trigger-def.js";
import type { PhysicalPlan } from "../ir/ir.js";
import type { ByteCharge } from "../vector/varlen.js";
import { NativeInputProgram, nativeInputProgramBytes } from "./native-input.js";
import type { RegistryImageTable } from "./registry-images.js";
export interface NativeBodyPlan {
  readonly body: TriggerBody | null;
  readonly plan: PhysicalPlan;
}
export interface NativeBodyProgram extends NativeBodyPlan {
  readonly schema: RegistryImageTable;
  readonly input: NativeInputProgram;
}
export function nativeBodyProgramMinimumBytes(plans: readonly NativeBodyPlan[]): number {
  const bytes =
    4096 +
    plans.reduce(
      (n, { plan }) =>
        n +
        1024 +
        256 * (plan.root.op === "modify" ? plan.root.input.output.length : 0) +
        nativeInputProgramBytes(plan),
      0,
    );
  if (!Number.isSafeInteger(bytes)) throw new RangeError("Native body programs too large");
  return bytes;
}
export class NativeBodyPrograms {
  #programs: NativeBodyProgram[] = [];
  #charge: ByteCharge | null;
  readonly #bytes: number;
  constructor(
    readonly catalog: CatalogView,
    plans: readonly NativeBodyPlan[],
    charge: ByteCharge,
  ) {
    this.#bytes =
      4096 +
      plans.reduce(
        (n, { plan }) =>
          n + 1024 + 256 * (plan.root.op === "modify" ? plan.root.input.output.length : 0),
        0,
      );
    if (!charge.allocate(this.#bytes)) throw new RangeError("Native body schemas must be admitted");
    this.#charge = charge;
    try {
      let id = Number.MAX_SAFE_INTEGER;
      for (const entry of plans) {
        const root = entry.plan.root;
        if (
          root.op !== "modify" ||
          !["insert", "update", "delete"].includes(root.kind) ||
          root.onConflict ||
          root.returning ||
          entry.plan.columns.length
        )
          throw new TypeError("Invalid native body modification");
        if (
          entry.body &&
          (entry.body.targetTableId !== root.tableId || entry.body.kind !== root.kind)
        )
          throw new TypeError("Invalid native body definition");
        if (
          !root.uniqueTargets &&
          root.kind === "update" &&
          (root.input.op !== "project" || root.input.exprs[0]?.op !== "column")
        )
          throw new TypeError("Native body update needs pre-assignment locator");
        while (catalog.table(id) || this.#programs.some((p) => p.schema.id === id)) id--;
        const schema: RegistryImageTable = {
          id: id--,
          schemaVersion: 0,
          columns: root.input.output.map((c, i) => ({ id: i + 1, type: c.type })),
        };
        const input = new NativeInputProgram(entry.plan, charge);
        this.#programs.push({ ...entry, schema, input });
      }
    } catch (error) {
      // No program can be executing during preparation; close returns its charge synchronously
      // before the first await (NativeInputProgram.close has no active producer here).
      for (const p of this.#programs) void p.input.close();
      this.#programs = [];
      this.#charge = null;
      charge.free(this.#bytes);
      throw error;
    }
  }
  table(id: number): RegistryImageTable | undefined {
    return this.catalog.table(id) ?? this.#programs.find((p) => p.schema.id === id)?.schema;
  }
  get(plan: PhysicalPlan, body: TriggerBody | null): NativeBodyProgram {
    if (!this.#charge) throw new TypeError("Native body programs closed");
    const p = this.#programs.find((p) => p.plan === plan && p.body === body);
    if (!p) throw new TypeError("Foreign native body program");
    return p;
  }
  async close(): Promise<void> {
    for (const p of this.#programs) await p.input.close();
    this.#programs = [];
    if (this.#charge) {
      this.#charge.free(this.#bytes);
      this.#charge = null;
    }
  }
}
