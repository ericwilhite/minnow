/** Key extraction, byte-table build and probe on the W join's actual flat TEXT
 * representation. Fixture generation is outside CPU timing; build allocations,
 * comparison, slicing accounting and table close are inside the build phase. */
import { strict as assert } from "node:assert";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve, join } from "node:path";
import { pathToFileURL } from "node:url";
import { createHash } from "node:crypto";
import { pairedCPU } from "./lib/paired-cpu.mts";
const option = (name, fallback) => {
  const at = process.argv.indexOf(name);
  if (at < 0 && fallback !== undefined) return fallback;
  const value = process.argv[at + 1];
  if (at < 0 || !value) throw Error(`Missing ${name}`);
  return value;
};
const rows = Number(option("--rows", "32768")),
  repetitions = Number(option("--repetitions", "4"));
assert(Number.isSafeInteger(rows) && rows >= 2048 && rows <= 131072);
assert(Number.isSafeInteger(repetitions) && repetitions > 0);
let seed = 0xb9f41;
const random = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0);
const values = Array.from({ length: rows }, (_, i) => {
  const suffix = Array.from({ length: 7 }, () => random().toString(16).padStart(8, "0")).join("");
  const id = i.toString(16).padStart(8, "0") + suffix;
  return `${id.slice(0, 8)}-${id.slice(8, 12)}-${id.slice(12, 16)}-${id.slice(16, 20)}-${id.slice(20, 32)}`;
});
const results = [];
for (const keyCase of ["flat36", "flat80", "duplicates36"]) {
  const buildValues = values.map((v, i) =>
    keyCase === "duplicates36" ? values[i >>> 2] : keyCase === "flat80" ? v + "x".repeat(44) : v,
  );
  const arms = [];
  for (const name of ["parent", "candidate"]) {
    const root = resolve(option(`--${name}`));
    const load = async (file) => import(pathToFileURL(join(root, `next/exec/${file}.js`)).href);
    const [keys, tables, stores, fixtures, slicing] = await Promise.all([
      load("join-keys"),
      load("join-table"),
      load("column-store"),
      load("join-fixtures"),
      load("slices"),
    ]);
    let charged = 0,
      peak = 0;
    const charge = {
      allocate(n) {
        charged += n;
        peak = Math.max(peak, charged);
        return true;
      },
      free(n) {
        charged -= n;
        assert(charged >= 0);
      },
    };
    const batches = [
      ...fixtures.batchesOf(
        [fixtures.TEXT],
        buildValues.map((v) => [v]),
      ),
    ];
    const store = new stores.ColumnStore([fixtures.TEXT], charge);
    for (const batch of batches) assert(store.append(batch.columns, null, 0, batch.length));
    const layout = new keys.KeyLayout([fixtures.TEXT], [false]);
    const scratch = new keys.KeyScratch(layout, 2048, charge);
    const slice = new slicing.Slice(Infinity);
    const heads = new Int32Array(2048);
    const build = () => tables.JoinTable.build(store, [0], layout, 0, charge, scratch);
    const table = build();
    assert(table);
    assert.equal(table.kind, "bytes");
    const run = (phase) => {
      let checksum = 0;
      for (let repeat = 0; repeat < repetitions; repeat++) {
        if (phase === "build") {
          const made = build();
          assert(made);
          checksum += made.inserted;
          made.close();
        } else {
          const work = new keys.KeyWork();
          for (const batch of batches) {
            const count = slicing.runToEnd(
              keys.extractKeysJob(batch.columns, null, 0, batch.length, scratch, work, slice),
            );
            checksum += slicing.runToEnd(table.lookupJob(scratch, count, heads, work, slice));
            for (let i = 0; i < count; i++) assert(heads[i] > 0);
          }
        }
      }
      assert.equal(checksum, rows * repetitions);
    };
    arms.push({
      name,
      root,
      run,
      close() {
        table.close();
        charge.free(keys.KeyScratch.bytes(layout, 2048));
        store.close();
        assert.equal(charged, 0);
      },
      get peak() {
        return peak;
      },
      hashes: Object.fromEntries(
        ["join-keys", "join-table"].map((file) => [
          file,
          createHash("sha256")
            .update(readFileSync(join(root, `next/exec/${file}.js`)))
            .digest("hex"),
        ]),
      ),
    });
  }
  for (const phase of ["hash-and-probe", "build"]) {
    for (let i = 0; i < 3; i++) for (const arm of arms) arm.run(phase);
    const samples = { parent: [], candidate: [] },
      raw = [];
    for (let round = 0; round < 9; round++) {
      for (const index of round % 2 ? [1, 0] : [0, 1]) {
        const arm = arms[index],
          start = process.cpuUsage();
        arm.run(phase);
        const used = process.cpuUsage(start),
          ms = (used.user + used.system) / 1000;
        samples[arm.name].push(ms);
        raw.push({ round, arm: arm.name, cpuMs: ms });
      }
    }
    const comparison = pairedCPU(samples.parent, samples.candidate);
    results.push({
      keyCase,
      phase,
      rows,
      repetitions,
      warmups: 3,
      rounds: 9,
      raw,
      comparison,
      modules: arms.map(({ name, root, hashes, peak }) => ({ name, root, hashes, peak })),
    });
    console.log(JSON.stringify({ keyCase, phase, ...comparison }));
  }
  for (const arm of arms) arm.close();
}
writeFileSync(
  resolve(option("--output")),
  JSON.stringify(
    {
      clock: "process user+system CPU; natural GC",
      scope:
        "isolated key/table work, no admission waits, scan/decode, payload gather, aggregate, owner or I/O",
      results,
    },
    null,
    2,
  ) + "\n",
);
