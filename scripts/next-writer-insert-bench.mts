/** Isolated native inserts, decoded inputs; process CPU includes all V8/GC threads.
 * Compile each runtime first. Example: node scripts/next-writer-insert-bench.mts
 * --control /path/to/parent --candidate /path/to/tip --rounds 7 --output /path/report.json
 * Schema/input construction, verification and close are outside the insert+durable+quiesce clock.
 */
import { execFileSync } from "node:child_process";
import { writeFile } from "node:fs/promises";
import { loadavg } from "node:os";
import { resolve, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
const option = (name: string, fallback = "") => {
  const at = process.argv.indexOf(name);
  return at < 0 ? fallback : (process.argv[at + 1] ?? fallback);
};
const shapes = {
  narrow: { rows: 8192, width: 8 },
  wide: { rows: 4096, width: 96 },
  wide_text: { rows: 4096, width: 96, text: true },
};
type Shape = keyof typeof shapes;
interface Sample {
  cpuMs: number;
  wallMs: number;
  load: number[];
}
if (process.argv.includes("--worker")) {
  const runtime = resolve(option("--worker")),
    shape = option("--shape") as Shape;
  if (!(shape in shapes)) throw new TypeError("Unknown insert shape");
  const native = (await import(
    pathToFileURL(join(runtime, "packages/core/dist/next/write/test-support.js")).href
  )) as typeof import("../packages/core/src/next/write/test-support.js");
  const specification = shapes[shape];
  const { rows, width } = specification;
  const text = "text" in specification && specification.text;
  let cpuMs = 0,
    wallMs = 0;
  for (let round = 0; round < 3; round++) {
    const h = new native.Harness("strict", "memory", 128),
      path = await h.open();
    try {
      await native.autocommit(path, (s) =>
        s.changeCatalog({
          kind: "createTable",
          table: native.tableInput(
            "ordinary",
            Array.from({ length: width }, (_, c) =>
              native.column(`c${String(c)}`, text ? native.TEXT : native.INT, false),
            ),
            { primaryKey: null },
          ),
        }),
      );
      await path.quiesce();
      const input = native.batch(
        ...Array.from({ length: width }, (_, c) =>
          text
            ? native.texts(
                Array.from({ length: rows }, (_, r) => `row:${String(r % 97)}:c${String(c)}`),
              )
            : native.ints(Array.from({ length: rows }, (_, r) => (r * 31 + c * 997) % 10007)),
        ),
      );
      const cpu = process.cpuUsage(),
        wall = performance.now();
      await native.autocommit(path, (s) => s.table(1).insert(input, null));
      await path.quiesce();
      const used = process.cpuUsage(cpu);
      cpuMs += (used.user + used.system) / 1000;
      wallMs += performance.now() - wall;
      const pin = path.pin({});
      try {
        const output = await native.rowsOf(pin, 1);
        if (output.length !== rows) throw new Error("Insert row count differs");
        for (let r = 0; r < rows; r++)
          for (let c = 0; c < width; c++)
            if (
              output[r]?.[c] !==
              (text ? `row:${String(r % 97)}:c${String(c)}` : (r * 31 + c * 997) % 10007)
            )
              throw new Error("Insert value differs");
      } finally {
        pin.release();
      }
    } finally {
      await h.close();
    }
    if (h.governor.usedBytes !== 0) throw new Error("Insert resources leaked");
  }
  console.log(
    JSON.stringify({ cpuMs: cpuMs / 3, wallMs: wallMs / 3, load: loadavg() } satisfies Sample),
  );
} else {
  const rounds = Number(option("--rounds", "7"));
  if (!Number.isSafeInteger(rounds) || rounds < 5)
    throw new TypeError("At least five pairs required");
  const control = resolve(option("--control")),
    candidate = resolve(option("--candidate"));
  const sample = (root: string, shape: Shape): Sample =>
    JSON.parse(
      execFileSync(
        process.execPath,
        [fileURLToPath(import.meta.url), "--worker", root, "--shape", shape],
        { encoding: "utf8", maxBuffer: 1 << 20 },
      ),
    ) as Sample;
  const quantile = (values: number[], p: number) => {
    const sorted = [...values].sort((a, b) => a - b),
      index = (sorted.length - 1) * p,
      at = Math.floor(index),
      fraction = index - at;
    return (
      (sorted[at] ?? 0) * (1 - fraction) +
      (sorted[Math.min(at + 1, sorted.length - 1)] ?? 0) * fraction
    );
  };
  const results = [];
  for (const shape of Object.keys(shapes) as Shape[]) {
    const pairs: Array<{ control: Sample; candidate: Sample; order: string }> = [];
    for (let round = 0; round < rounds; round++) {
      const order =
        round % 2 ? (["candidate", "control"] as const) : (["control", "candidate"] as const);
      const samples = {} as Record<"control" | "candidate", Sample>;
      for (const arm of order)
        samples[arm] = sample(arm === "control" ? control : candidate, shape);
      pairs.push({ ...samples, order: order.join("/") });
      console.log(JSON.stringify({ shape, round, ...pairs[pairs.length - 1] }));
    }
    const a = pairs.map((p) => p.control.cpuMs),
      b = pairs.map((p) => p.candidate.cpuMs),
      gains = pairs.map((p) => p.control.cpuMs - p.candidate.cpuMs),
      medianGain = quantile(gains, 0.5),
      iqrGain = quantile(gains, 0.75) - quantile(gains, 0.25);
    results.push({
      shape,
      pairs,
      cpu: {
        controlMedian: quantile(a, 0.5),
        controlIqr: quantile(a, 0.75) - quantile(a, 0.25),
        candidateMedian: quantile(b, 0.5),
        candidateIqr: quantile(b, 0.75) - quantile(b, 0.25),
        medianGain,
        iqrGain,
        favourable: gains.filter((gain) => gain > 0).length,
        noRegressionBeyondSpread: medianGain >= -iqrGain,
      },
    });
  }
  const report = {
    control,
    candidate,
    rounds,
    boundary:
      "native insert through durable quiescence; prebuilt input; verification and close outside clock",
    results,
  };
  await writeFile(
    resolve(option("--output", "writer-insert-results.json")),
    JSON.stringify(report, null, 2) + "\n",
  );
  console.log(JSON.stringify(report.results.map(({ shape, cpu }) => ({ shape, ...cpu }))));
}
