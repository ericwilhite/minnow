/** Isolated checksum CPU after native metadata training; setup is excluded.
 * Run with vite-node in each arm's worktree, fresh process, fixed two-core affinity.
 * Pair AB/BA >=9 times, run identical-build A/A first; never assert timing in unit tests.
 */
import { xxHash32 } from "../packages/core/src/next/hash/xxhash32.ts";
import {
  Harness,
  autocommit,
  batch,
  column,
  ints,
  INT,
  tableInput,
} from "../packages/core/src/next/write/test-support.ts";
import { SimDisk } from "../packages/core/src/next/storage/sim-disk.ts";
const disk = new SimDisk({ seed: 45759 }),
  h = new Harness("strict", "sim", undefined, disk);
try {
  const path = await h.open();
  await autocommit(path, (s) =>
    s.changeCatalog({
      kind: "createTable",
      table: tableInput("checksum_training", [column("n", INT)]),
    }),
  );
  await autocommit(path, (s) => s.table(1).insert(batch(ints([17, 29, 41])), null));
  await path.recycleLog();
} finally {
  await h.close();
}
if (h.governor.usedBytes !== 0) throw new Error("Training leaked governor credit");
// Read actual immutable metadata/data images, excluding random incarnation/owner headers.
const images = [...disk.files]
  .filter(
    ([name, state]) =>
      !name.startsWith("root-") &&
      !name.includes("epoch") &&
      !name.includes("log") &&
      state.current.length >= 256,
  )
  .map(([name, state]) => ({ name, bytes: state.current }));
if (images.length === 0) throw new Error("No native checksum inputs");
const bytesPerPass = images.reduce((n, entry) => n + entry.bytes.length, 0),
  passes = Math.max(1, Math.ceil((256 << 20) / bytesPerPass));
let sink = 0;
const begin = process.hrtime.bigint(),
  cpu = process.cpuUsage();
for (let turn = 0; turn < passes; turn++)
  for (const entry of images) sink ^= xxHash32(entry.bytes, turn);
const used = process.cpuUsage(cpu);
console.log(
  "B2_HASH_COMPONENT " +
    JSON.stringify({
      cpuMs: (used.user + used.system) / 1000,
      wallMs: Number(process.hrtime.bigint() - begin) / 1e6,
      sink,
      bytes: passes * bytesPerPass,
      images: images.map((entry) => ({ name: entry.name, length: entry.bytes.length })),
      peakGovernor: h.governor.peakBytes,
    }),
);
