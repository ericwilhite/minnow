/** Small, single-threaded whole-recipe component. Setup/preparation excluded;
 * admission, execution, exact three-field validation, frame consumption, and resource release included. */
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { writeFile } from "node:fs/promises";
import type { StorageCore } from "../packages/core/src/next/testing/storage-core.js";
import type { Governor } from "../packages/core/src/next/memory/governor.js";
import type { coefficientAt } from "../packages/core/src/next/owner/result-values.js";
interface Arm {
  core: StorageCore;
  coefficientAt: typeof coefficientAt;
  original: Governor;
  governor: Governor;
  prepared: ReturnType<StorageCore["prepare"]>;
  minimum(): number;
}
const arg = (name: string) => {
  const i = process.argv.indexOf(name);
  const value = process.argv[i + 1];
  if (i < 0 || !value) throw Error(`Missing ${name}`);
  return value;
};
const paths = [arg("--parent"), arg("--candidate")],
  output = arg("--output");
const arms: Arm[] = [];
for (const path of paths) {
  const module = (await import(
    pathToFileURL(resolve(path, "next/testing/storage-core.js")).href
  )) as typeof import("../packages/core/src/next/testing/storage-core.js");
  const memory = (await import(
    pathToFileURL(resolve(path, "next/memory/governor.js")).href
  )) as typeof import("../packages/core/src/next/memory/governor.js");
  const values = (await import(
    pathToFileURL(resolve(path, "next/owner/result-values.js")).href
  )) as typeof import("../packages/core/src/next/owner/result-values.js");
  const core = await new module.StorageCore().open();
  const original = core.harness.governor;
  await core.execute(
    "CREATE TABLE person_record(id INT PRIMARY KEY,first_name TEXT,amount NUMERIC)",
  );
  await core.execute("INSERT INTO person_record VALUES(1,'before',12.3400),(2,'later',22.0000)");
  const governor = new memory.Governor(64 << 20);
  let minimum = 0;
  const admit = governor.tryAdmit.bind(governor);
  governor.tryAdmit = (options) => {
    if (options.label.endsWith("/fixed-buffers")) minimum = options.minimumBytes;
    return admit(options);
  };
  Object.assign(core.harness, { governor });
  arms.push({
    core,
    coefficientAt: values.coefficientAt,
    original,
    governor,
    prepared: core.prepare("SELECT * FROM person_record ORDER BY id LIMIT $1"),
    minimum: () => minimum,
  });
}
const run = async (arm: (typeof arms)[number]) => {
  const execution = arm.core.start(arm.prepared, [1]);
  let rows = 0;
  try {
    for (;;) {
      const frame = await execution.execution.results.next();
      if (!frame) break;
      rows += frame.payload.rowCount;
      const [id, text, amount] = frame.payload.columns;
      if (
        frame.payload.rowCount > 0 &&
        (!id ||
          id.form !== "flat" ||
          id.data.layout !== "i32" ||
          id.data.values[0] !== 1 ||
          !text ||
          text.form !== "flat" ||
          text.data.layout !== "varlen" ||
          Buffer.from(
            text.data.bytes.subarray(text.data.offsets[0], text.data.offsets[1]),
          ).toString() !== "before" ||
          !amount ||
          amount.form !== "flat" ||
          amount.data.layout !== "decimal" ||
          (amount.data.scale ?? amount.data.scales?.[0]) !== 4 ||
          arm.coefficientAt(amount.data, 0) !== 123400n)
      )
        throw Error("three-field component result changed");
      frame.release();
    }
    await execution.execution.executed;
    await execution.execution.delivered;
    if (rows !== 1) throw Error("row count changed");
  } finally {
    execution.release();
  }
  if (arm.governor.usedBytes !== 0) throw Error("query credit retained");
};
const pairs = [];
try {
  for (let warm = 0; warm < 4; warm++) for (const arm of arms) await run(arm);
  for (let round = 0; round < 9; round++) {
    const order = round % 2 ? [1, 0] : [0, 1],
      values = [];
    for (const index of order) {
      const arm = arms[index];
      if (!arm) throw Error("arm");
      const before = process.cpuUsage();
      for (let repeat = 0; repeat < 32; repeat++) await run(arm);
      const cpu = process.cpuUsage(before);
      values.push({
        arm: index === 0 ? "parent" : "candidate",
        cpuMs: (cpu.user + cpu.system) / 32000,
        minimumBytes: arm.minimum(),
      });
    }
    pairs.push({ round, order: order.map((i) => (i === 0 ? "parent" : "candidate")), values });
  }
  await writeFile(
    output,
    JSON.stringify(
      {
        kind: "small recipe component; not hosted whole-cell",
        rows: 2,
        limit: 1,
        capBytes: 64 << 20,
        warmups: 4,
        rounds: 9,
        repeats: 32,
        runtimes: paths,
        pairs,
      },
      null,
      2,
    ),
  );
} finally {
  for (const arm of arms) {
    Object.assign(arm.core.harness, { governor: arm.original });
    await arm.core.close();
  }
}
