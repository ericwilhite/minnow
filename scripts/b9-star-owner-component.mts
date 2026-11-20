/** Ordinary owner / protocol / normalized TEXT entry on five actual W fact fields.
 * Both current-parent and candidate use their default planner and executor. Fixture
 * writing, lowering, warming and output checks are outside the process CPU clock.
 * The timed boundary includes owner binding, proof reads, atomic admission, native
 * execution, result delivery and cleanup. Raw rows are checked bit-for-bit outside
 * the clock, including every same-plan repeat. This is not the 200-column W1M cell.
 */
import { strict as assert } from "node:assert";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { wideSpec, value, dimTables } from "./next-executor-workload/gen.mjs";
import { W_QUERIES } from "./next-executor-workload/queries.mjs";
import { pairedCPU } from "./lib/paired-cpu.mts";
import type { ClientResult } from "../packages/core/dist/next/owner/client.js";
const args = process.argv.slice(2),
  option = (name: string, fallback: string) => {
    const i = args.indexOf(name);
    return i < 0 ? fallback : (args[i + 1] ?? fallback);
  },
  total = Number(option("--rows", "32768")),
  parent = option("--parent-core", ""),
  candidate = option("--candidate-core", "");
if (!parent || !candidate || !Number.isSafeInteger(total) || total < 2048 || total > 131072)
  throw Error("Two builds and bounded local rows required");
const spec = wideSpec(200).filter((c) => ["id", "d03", "d04", "d07", "f00"].includes(c.name)),
  facts = Array.from({ length: total }, (_, i) =>
    Object.fromEntries(spec.map((c) => [c.name, value(c, i, total)])),
  ),
  dims = dimTables().filter((d) => ["dim_d03", "dim_d04", "dim_d07"].includes(d.name)),
  query = W_QUERIES.find((q) => q.id === "join_star_3dims");
assert(query);
const sql = query.sql;
const bits = (r: ClientResult) =>
  r
    .rows()
    .map((row) =>
      row.map((v) =>
        typeof v === "number"
          ? { floatBits: Buffer.from(new Float64Array([v]).buffer).toString("hex") }
          : v,
      ),
    );
async function owner(root: string) {
  const url = (file: string) => pathToFileURL(resolve(root, file)).href,
    { StorageCore } = (await import(
      url("next/testing/storage-core.js")
    )) as typeof import("../packages/core/dist/next/testing/storage-core.js"),
    { composeEngine } = (await import(
      url("next/owner/compose.js")
    )) as typeof import("../packages/core/dist/next/owner/compose.js"),
    { OwnerServer } = (await import(
      url("next/owner/server.js")
    )) as typeof import("../packages/core/dist/next/owner/server.js"),
    { ProtocolClient } = (await import(
      url("next/owner/client.js")
    )) as typeof import("../packages/core/dist/next/owner/client.js"),
    { chunkReads } = (await import(
      url("next/exec/scan.js")
    )) as typeof import("../packages/core/dist/next/exec/scan.js"),
    { createExecutor } = (await import(
      url("next/exec/create.js")
    )) as typeof import("../packages/core/dist/next/exec/create.js"),
    core = await new StorageCore(8192).open();
  await core.execute(
    `CREATE TABLE events_wide (${spec.map((c) => `${c.name} ${c.pg}`).join(",")})`,
  );
  for (const d of dims) {
    await core.execute(
      `CREATE TABLE ${d.name}(k TEXT PRIMARY KEY,label TEXT,tier TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
    );
    await core.writeBatch(
      "insertBatch",
      d.name,
      d.rows.map((r) => ({
        k: r.key,
        label: r.label,
        tier: r.tier,
        weight: r.weight,
        active: r.active,
      })),
    );
  }
  await core.writeBatch("insertBatch", "events_wide", facts);
  await core.harness.path.recycleLog();
  await core.harness.path.quiesce();
  const factory = createExecutor();
  let delivery = Promise.resolve();
  const engine = composeEngine({
      planner: core.lowerer,
      writePath: core.harness.path,
      governor: core.harness.governor,
      spill: core.spill,
      reads: chunkReads,
      executor: {
        prepare(plan) {
          const prepared = factory.prepare(plan);
          return {
            plan: prepared.plan,
            columns: prepared.columns,
            kind: prepared.kind,
            usesSequences: prepared.usesSequences,
            bind: (values, grant) => prepared.bind(values, grant),
            execute(request) {
              const execution = prepared.execute(request);
              delivery = execution.delivered;
              return execution;
            },
          };
        },
      },
    }),
    plan = engine.plan(sql).lowered;
  assert.equal(plan.kind, "query");
  if (plan.kind !== "query") throw Error("Missing query");
  assert.equal(plan.plan.parameters.length, 1);
  assert.equal(plan.plan.parameters[0]?.kind, "text");
  const server = new OwnerServer(engine, {
      ownerId: "star-owner-component",
      epoch: 1,
      build: "component",
    }),
    channel = new MessageChannel(),
    client = new ProtocolClient();
  server.accept(channel.port2);
  await client.connect(channel.port1);
  return {
    core,
    async run() {
      const result = await client.query(sql, []);
      // Terminal done may precede the client's release reaching the server.
      // Include the documented result-grant/cleanup completion in every clock.
      await delivery;
      return result;
    },
    async close() {
      const draining = server.quiesce("closing");
      try {
        await draining.drained;
        client.close();
        await engine.close();
      } finally {
        await core.close();
        draining.finish();
        channel.port1.close();
        channel.port2.close();
      }
      assert.equal(core.harness.governor.usedBytes, 0);
      assert.equal(core.spill.bytesInUse, 0);
    },
  };
}
const before = await owner(parent);
let after: Awaited<ReturnType<typeof owner>> | undefined;
try {
  after = await owner(candidate);
  const { StarLookups } = (await import(
    pathToFileURL(resolve(candidate, "next/exec/join-star-lookups.js")).href
  )) as {
    StarLookups: { prototype: { replay: (...args: unknown[]) => unknown } };
  };
  const replay = StarLookups.prototype.replay;
  let lookups = 0;
  StarLookups.prototype.replay = function (...values: unknown[]) {
    lookups++;
    return replay.apply(this, values);
  };
  let expected: ReturnType<typeof bits>;
  try {
    expected = bits(await before.run());
    assert.equal(lookups, 0);
    assert.deepEqual(bits(await after.run()), expected);
    assert.equal(lookups, 1, "ordinary owner must select three dimension lookups");
  } finally {
    StarLookups.prototype.replay = replay;
  }
  const arms = [before, after],
    warmups = 8,
    repeats = 4;
  for (let i = 0; i < warmups; i++)
    for (const arm of i % 2 === 0 ? arms : [...arms].reverse())
      assert.deepEqual(bits(await arm.run()), expected);
  const pairs = [],
    controls: number[] = [],
    candidates: number[] = [];
  for (let round = 0; round < 9; round++) {
    const order = round % 2 === 0 ? [0, 1] : [1, 0],
      samples = [];
    for (const index of order) {
      const arm = arms[index];
      assert(arm);
      const cpu = process.cpuUsage(),
        results: ClientResult[] = [];
      for (let i = 0; i < repeats; i++) results.push(await arm.run());
      const elapsed = process.cpuUsage(cpu),
        cpuMs = (elapsed.user + elapsed.system) / 1000 / repeats;
      for (const result of results) assert.deepEqual(bits(result), expected);
      (index === 0 ? controls : candidates).push(cpuMs);
      samples.push({ arm: index === 0 ? "parent" : "candidate", cpuMs });
    }
    pairs.push({ round, order, samples });
  }
  console.log(
    JSON.stringify(
      {
        kind: "ordinary-owner normalized TEXT star",
        rows: total,
        fields: spec.map((c) => c.name),
        parent,
        candidate,
        warmups,
        repeats,
        pairs,
        route: { parameters: ["text"], candidateLookups: lookups, parentLookups: 0 },
        strictNativeResults: true,
        identicalBitsEveryRepeat: true,
        clock:
          "process user+system CPU, all threads and natural GC; 9 AB/BA pairs; no forced GC; fixture/warmup/validation excluded",
        acceptance: pairedCPU(controls, candidates),
        rawResults: expected,
        limits:
          "local five-field component, not 200-column W1M; ordinary-build prototype emission hashes must be checked before hosted timing",
      },
      (_, v: unknown) => (typeof v === "bigint" ? { bigint: v.toString() } : v),
      2,
    ),
  );
} finally {
  if (after) await after.close();
  await before.close();
}
