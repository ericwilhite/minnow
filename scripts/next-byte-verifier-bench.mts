/** Test-oracle CPU only; build core first. Scratch follows os.tmpdir(). */
import { cpuUsage } from "node:process";
import { mkdir, writeFile } from "node:fs/promises";
import { loadavg, tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { byteSliceEquals } from "../packages/core/dist/next/testing/byte-equality.test.fixture.js";
import { pairedCPU } from "./lib/paired-cpu.mts";

const size = 39806,
  rows = 1024,
  executions = 3;
const marker = Uint8Array.from({ length: size }, (_, i) => 97 + (i % 26));
const payload = new Uint8Array(size * rows);
for (let i = 0; i < rows; i++) payload.set(marker, i * size);
const control = () => {
  let checked = 0;
  for (let q = 0; q < executions; q++)
    for (let row = 0; row < rows; row++) {
      if (!payload.subarray(row * size, (row + 1) * size).every((b, i) => b === marker[i]))
        throw new Error("Bytewise verifier mismatch");
      checked++;
    }
  return checked;
};
const candidate = () => {
  let checked = 0;
  for (let q = 0; q < executions; q++)
    for (let row = 0; row < rows; row++) {
      if (!byteSliceEquals(payload, row * size, marker))
        throw new Error("Portable word verifier mismatch");
      checked++;
    }
  return checked;
};
control();
candidate();
const before: number[] = [],
  after: number[] = [],
  order: string[] = [],
  loads: number[][] = [];
for (let round = 0; round < 9; round++) {
  loads.push(loadavg());
  for (const name of round % 2 ? ["candidate", "control"] : ["control", "candidate"]) {
    const start = cpuUsage();
    if ((name === "control" ? control : candidate)() !== rows * executions)
      throw new Error("Verifier omitted a row");
    const used = cpuUsage(start);
    (name === "control" ? before : after).push((used.user + used.system) / 1000);
    order.push(name);
  }
}
const args = process.argv.slice(2),
  at = args.indexOf("--output");
const output = resolve(at < 0 ? join(tmpdir(), "minnow-byte-verifier.json") : (args[at + 1] ?? ""));
const result = {
  scope: "Exact portable test-oracle comparison only; no engine performance claim",
  size,
  rows,
  executions,
  bytesCompared: size * rows * executions,
  fixture: "marker[i]=97+i%26; repeat it for each output row; setup excluded",
  order,
  loads,
  node: process.version,
  control: before,
  candidate: after,
  paired: pairedCPU(before, after),
};
await mkdir(dirname(output), { recursive: true });
await writeFile(output, JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify(result));
