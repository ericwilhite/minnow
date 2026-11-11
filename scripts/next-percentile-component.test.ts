/** Protocol checks only: tiny decoded fixtures, no CPU measurements. */
import { fileURLToPath } from "node:url";
import { expect, it, vi } from "vitest";
import { StorageCore } from "../packages/core/dist/next/testing/storage-core.js";
import type { InputValue } from "../packages/core/dist/next/testing/executor-input.js";
import { inputBatch } from "../packages/core/dist/next/testing/executor-input.js";
import { percentileComponent } from "./next-percentile-component.mts";
import { wideSpec, wideBatch } from "./next-executor-workload/gen.mjs";

const generate = (spec: unknown, first: number, rows: number, count: number) =>
  wideBatch(spec, first, rows, count) as Record<string, InputValue[]>;

it.each([0, 1, 7, 2049])(
  "checks the complete decoded percentile protocol (%i rows)",
  async (count) => {
    const core = await new StorageCore(8192).open(),
      spec = wideSpec(200);
    const cpu = vi.spyOn(process, "cpuUsage");
    const fixtures: Array<Awaited<ReturnType<typeof percentileComponent>>> = [];
    const keys = new Set<string>();
    try {
      await core.execute(
        `CREATE TABLE events_wide (${spec.map((column) => `${column.name} ${column.pg}`).join(",")})`,
      );
      const table = core.harness.path.catalog.tableByName("events_wide");
      if (!table) throw new Error("Missing fixture");
      const grant = await core.harness.governor.admit({
        label: "protocol/input",
        minimumBytes: 8 << 20,
        spillable: true,
      });
      try {
        for (let first = 0; first < count; first += 2048) {
          const raw = generate(spec, first, Math.min(2048, count - first), count);
          for (const key of raw.d02 ?? []) keys.add(String(key));
          const data = inputBatch(table, raw, grant);
          try {
            await core.insertVectors(table, data.batch);
          } finally {
            data.close();
          }
        }
      } finally {
        grant.close();
      }
      const root = fileURLToPath(new URL("..", import.meta.url)).replace(/\/$/u, "");
      for (const candidate of [false, true])
        fixtures.push(
          await percentileComponent(root, core, count, candidate, { wideBatch: generate }, spec),
        );
      const outputs = [];
      for (const fixture of fixtures) {
        const result = await fixture.run();
        outputs.push({ ...result, digest: fixture.digest() });
      }
      expect(outputs[1]).toEqual(outputs[0]);
      expect(outputs[0]?.rows).toBe(keys.size);
      expect(cpu).not.toHaveBeenCalled();
    } finally {
      for (const fixture of fixtures) fixture.close();
      cpu.mockRestore();
      await core.close();
    }
  },
  30000,
);
