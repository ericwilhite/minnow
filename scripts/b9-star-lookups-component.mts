/** Actual W key representation, five demanded fact fields, resident join/group state.
 * Same ordinary-writer snapshot and compiled engine; starLookups:false is the
 * unchanged three-native-join control. This is not the 200-column W1M whole cell.
 * CPU covers prepared execution through released result frames, including a bounded
 * raw-frame copy. Fixture generation, compilation, decoding and PG checks are outside.
 * The PostgreSQL reference child exits before execution begins.
 */
import { strict as assert } from "node:assert";
import { execFileSync } from "node:child_process";
import { Session, type Profiler } from "node:inspector";
import { mkdir, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { PGlite } from "@electric-sql/pglite";
import { wideSpec, value, dimTables } from "./next-executor-workload/gen.mjs";
import { W_QUERIES } from "./next-executor-workload/queries.mjs";
import { pairedCPU } from "./lib/paired-cpu.mts";
import type { PreparedPlan } from "../packages/core/dist/next/exec/executor.js";
import type { WireFlat, FramePayload } from "../packages/core/dist/next/exec/result.js";
const args = process.argv.slice(2),
  option = (name: string, fallback: string) => {
    const i = args.indexOf(name);
    return i < 0 ? fallback : (args[i + 1] ?? fallback);
  },
  total = Number(option("--rows", "65536")),
  root = resolve(option("--core", "packages/core/dist"));
if (!Number.isSafeInteger(total) || total < 2048 || total > 131072)
  throw new Error("Bounded local component rows required");
const spec = wideSpec(200).filter((c) => ["id", "d03", "d04", "d07", "f00"].includes(c.name)),
  facts = Array.from({ length: total }, (_, i) =>
    Object.fromEntries(spec.map((c) => [c.name, value(c, i, total)])),
  ),
  dims = dimTables().filter((d) => ["dim_d03", "dim_d04", "dim_d07"].includes(d.name)),
  query = W_QUERIES.find((q) => q.id === "join_star_3dims");
if (!query) throw new Error("Missing W star query");
const sql = query.sql,
  factDDL = `CREATE TABLE events_wide (${spec.map((c) => `${c.name} ${c.pg}`).join(",")})`,
  dimDDL = (name: string) =>
    `CREATE TABLE ${name}(k TEXT PRIMARY KEY,label TEXT,tier TEXT,weight DOUBLE PRECISION,active BOOLEAN)`,
  dimRows = (d: (typeof dims)[number]) =>
    d.rows.map((r) => ({
      k: r.key,
      label: r.label,
      tier: r.tier,
      weight: r.weight,
      active: r.active,
    }));
type Row = Record<string, unknown>;
async function postgresReference(): Promise<Row[]> {
  const pg = new PGlite();
  await pg.waitReady;
  let reference: Row[];
  try {
    await pg.exec(factDDL);
    for (const d of dims) {
      await pg.exec(dimDDL(d.name));
      await pg.query(
        `INSERT INTO ${d.name} SELECT * FROM json_to_recordset($1::json) x(k text,label text,tier text,weight double precision,active boolean)`,
        [JSON.stringify(dimRows(d))],
      );
    }
    await pg.query(
      "INSERT INTO events_wide SELECT * FROM json_to_recordset($1::json) x(id bigint,d03 text,d04 text,d07 text,f00 double precision)",
      [JSON.stringify(facts)],
    );
    reference = (await pg.query<Row>(sql)).rows.map((row) => ({ ...row, n: Number(row.n) }));
  } finally {
    await pg.close();
  }

  return reference;
}
// The reference child exits before engine setup and timing. The timed process
// retains no PGlite database, keeping local component RSS bounded on the shared host.
if (args.includes("--reference-only")) {
  console.log(JSON.stringify(await postgresReference()));
} else {
  const reference = JSON.parse(
    execFileSync(
      process.execPath,
      [
        ...process.execArgv,
        fileURLToPath(import.meta.url),
        "--reference-only",
        "--rows",
        String(total),
      ],
      { encoding: "utf8", maxBuffer: 1024 * 1024 },
    ),
  ) as Row[];
  const url = (file: string) => pathToFileURL(resolve(root, file)).href,
    { StorageCore, expandTextDictionary } = (await import(
      url("next/testing/storage-core.js")
    )) as typeof import("../packages/core/dist/next/testing/storage-core.js"),
    { oracleValue } = (await import(
      url("next/testing/differential-adapter.js")
    )) as typeof import("../packages/core/dist/next/testing/differential-adapter.js"),
    { createExecutor } = (await import(
      url("next/exec/create.js")
    )) as typeof import("../packages/core/dist/next/exec/create.js"),
    { StarLookups } = (await import(
      url("next/exec/join-star-lookups.js")
    )) as typeof import("../packages/core/dist/next/exec/join-star-lookups.js"),
    core = await new StorageCore(8192).open();
  const bits = (rows: readonly Row[]) =>
    rows.map((row) =>
      Object.entries(row).map(([key, v]) => [
        key,
        typeof v === "number" ? Buffer.from(new Float64Array([v]).buffer).toString("hex") : v,
      ]),
    );
  function postgres(rows: readonly Row[]) {
    assert.equal(rows.length, reference.length);
    for (let i = 0; i < rows.length; i++) {
      const a = rows[i],
        b = reference[i];
      assert(a && b);
      assert.deepEqual(Object.keys(a), Object.keys(b));
      for (const key of Object.keys(b)) {
        const x: unknown = a[key],
          y: unknown = b[key];
        // Law: https://www.postgresql.org/docs/current/functions-aggregate.html
        // Unordered float8 SUM alone has unspecified input order. Specials/zero
        // and every other field remain exact. Engine controls are bit-exact below.
        if (
          key === "s" &&
          typeof x === "number" &&
          typeof y === "number" &&
          Number.isFinite(x) &&
          Number.isFinite(y) &&
          !(x === 0 && y === 0)
        )
          assert(Math.abs(x - y) <= Math.max(1, Math.abs(x), Math.abs(y)) * 1e-9);
        else assert(Object.is(x, y), `PostgreSQL ${key} differs at ${i}`);
      }
    }
  }
  async function run(prepared: PreparedPlan) {
    const frames: FramePayload[] = [],
      cpu = process.cpuUsage(),
      started = core.start(prepared),
      settled = Promise.allSettled([started.execution.executed, started.execution.delivered]);
    let summary;
    try {
      for (;;) {
        const frame = await started.execution.results.next();
        if (!frame) break;
        try {
          frames.push(structuredClone(frame.payload));
        } finally {
          frame.release();
        }
      }
      summary = await started.execution.executed;
      await started.execution.delivered;
    } catch (error) {
      started.execution.cancel(error);
      throw error;
    } finally {
      await settled;
      started.release();
    }
    const used = process.cpuUsage(cpu),
      dictionaries = new Map<number, WireFlat[]>(),
      rows: Row[] = [];
    for (const frame of frames) {
      for (const delta of frame.dictionaries) {
        const parts = dictionaries.get(delta.id) ?? [];
        assert.equal(
          delta.from,
          parts.reduce((n, p) => n + p.length, 0),
        );
        parts.push(delta.values);
        dictionaries.set(delta.id, parts);
      }
      const columns = frame.columns.map((v, i) => {
        const flat =
            v.form === "dictionary"
              ? expandTextDictionary(v, dictionaries.get(v.dictionary) ?? [])
              : v,
          type = prepared.columns[i]?.type;
        if (flat.form !== "flat" || !type) throw new Error("Invalid bounded star frame");
        return { ...flat, type };
      });
      for (let i = 0; i < frame.rowCount; i++)
        rows.push(
          Object.fromEntries(
            columns.map((column, c) => [
              prepared.columns[c]?.name ?? "missing",
              oracleValue(column, i),
            ]),
          ),
        );
    }
    assert.equal(core.spill.bytesInUse, 0);
    return { cpuMs: (used.user + used.system) / 1000, rows, profile: summary.profile };
  }
  try {
    await core.execute(factDDL);
    for (const d of dims) {
      await core.execute(dimDDL(d.name));
      await core.writeBatch("insertBatch", d.name, dimRows(d));
    }
    await core.writeBatch("insertBatch", "events_wide", facts);
    await core.harness.path.recycleLog();
    await core.harness.path.quiesce();
    const lowered = core.lowerer.lower(sql, core.context());
    if (lowered.kind !== "query") throw new Error("Expected W query");
    const control = createExecutor({ starLookups: false }).prepare(lowered.plan),
      candidate = createExecutor({ starLookups: true }).prepare(lowered.plan);
    let hits = 0;
    const original = StarLookups.prototype.replay;
    StarLookups.prototype.replay = function (...args: Parameters<typeof original>) {
      hits++;
      return original.apply(this, args);
    };
    let before: Awaited<ReturnType<typeof run>>, after: Awaited<ReturnType<typeof run>>;
    try {
      before = await run(control);
      assert.equal(hits, 0);
      after = await run(candidate);
      assert.equal(hits, 1, "Actual W SQL must reach the sink");
    } finally {
      StarLookups.prototype.replay = original;
    }
    postgres(before.rows);
    postgres(after.rows);
    const expected = bits(before.rows);
    assert.deepEqual(bits(after.rows), expected);
    const checked = async (plan: PreparedPlan) => {
      const result = await run(plan);
      assert.deepEqual(bits(result.rows), expected, "Native/fused or same-plan bits changed");
      return result.cpuMs;
    };
    for (let i = 0; i < 8; i++) {
      await checked(control);
      await checked(candidate);
    }
    if (args.includes("--profile-only")) {
      const directory = resolve(
        option("--profile-dir", resolve(homedir(), ".cache/minnow-b9-star-profiles")),
      );
      if (!directory.startsWith(resolve(homedir(), ".cache") + "/"))
        throw new Error("CPU profiles must stay under the cache");
      await mkdir(directory, { recursive: true });
      for (const [label, plan] of [
        ["control", control],
        ["candidate", candidate],
      ] as const) {
        const session = new Session();
        session.connect();
        try {
          await new Promise<void>((yes, no) =>
            session.post("Profiler.enable", (e) => (e ? no(e) : yes())),
          );
          await new Promise<void>((yes, no) =>
            session.post("Profiler.start", (e) => (e ? no(e) : yes())),
          );
          for (let i = 0; i < 12; i++) await checked(plan);
          const profile = await new Promise<Profiler.Profile>((yes, no) =>
            session.post("Profiler.stop", (e, result) => (e ? no(e) : yes(result.profile))),
          );
          await writeFile(resolve(directory, label + ".cpuprofile"), JSON.stringify(profile));
        } finally {
          session.disconnect();
        }
      }
      console.log(
        JSON.stringify({
          purpose: "CPU diagnosis only; no component timing claim",
          rows: total,
          runsPerArm: 12,
          directory,
          raw: { control: before.rows, candidate: after.rows },
        }),
      );
    } else {
      const block = async (plan: PreparedPlan) => {
          const runs = [];
          for (let i = 0; i < 4; i++) runs.push(await checked(plan));
          return { cpuMs: runs.reduce((n, v) => n + v, 0) / runs.length, runs };
        },
        samples = [];
      for (let round = 0; round < 9; round++) {
        let a, b;
        if (round % 2 === 0) {
          a = await block(control);
          b = await block(candidate);
        } else {
          b = await block(candidate);
          a = await block(control);
        }
        samples.push({ round, order: round % 2 === 0 ? "AB" : "BA", control: a, candidate: b });
      }
      const comparison = pairedCPU(
        samples.map((s) => s.control.cpuMs),
        samples.map((s) => s.candidate.cpuMs),
      );
      console.log(
        JSON.stringify({
          purpose:
            "Local actual-W dictionary/int32 star join/group component; five fact fields, not W1M whole-cell",
          rows: total,
          memtableRows: 8192,
          capBytes: core.harness.governor.capBytes,
          clock: "process user+system CPU through released copied frames",
          warmups: 8,
          executionsPerArm: 4,
          law: "PGlite unordered SUM finite1e-9; native/fused and same-plan output bits exact",
          raw: { postgres: reference, control: before.rows, candidate: after.rows },
          profiles: { control: before.profile, candidate: after.profile },
          samples,
          comparison,
          medianBeatsFullRange:
            -comparison.medianDeltaMs > comparison.rangeDeltaMs &&
            comparison.favoursCandidate >= comparison.requiredFavourable,
        }),
      );
    }
  } finally {
    await core.close();
    assert.equal(core.harness.governor.usedBytes, 0);
  }
}
