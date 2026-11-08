/** Isolate the real dictionary byte-sizing kernel over a trusted synthetic uint8 page.
 * No IO, page parse, frame build or transfer. Setup and one warmup are outside each timer.
 */
import {
  varlenBytesOf,
  type PageView,
  type PageDictionary,
} from "../../packages/core/src/next/page/decode.js";
import { PageEncoding } from "../../packages/core/src/next/format/manifest.js";
import { ValueTag } from "../../packages/core/src/next/page/format.js";
export function dictionarySizing(sparse: boolean, timed = true) {
  const n = 65536,
    codes = Uint8Array.from({ length: n }, (_, r) => (Math.imul(r, 13) ^ (r >>> 8)) & 31),
    entries = new Int32Array(33);
  for (let k = 0; k < 32; k++) entries[k + 1] = (entries[k] ?? 0) + 3 + ((k * 37) % 123);
  const validity = sparse ? new Uint32Array(n / 32).fill(0xffffffff) : null;
  if (validity)
    for (let r = 0; r < n; r += 7) validity[r >>> 5] = (validity[r >>> 5] ?? 0) & ~(1 << (r & 31));
  const prefix = new Float64Array(n + 1);
  for (let r = 0; r < n; r++) {
    const c = codes[r] ?? 0;
    prefix[r + 1] =
      (prefix[r] ?? 0) + (sparse && r % 7 === 0 ? 0 : (entries[c + 1] ?? 0) - (entries[c] ?? 0));
  }
  const dict: PageDictionary = {
    tag: ValueTag.varlen,
    size: 32,
    bytes: new Uint8Array(entries[32] ?? 0),
    offsets: entries,
  };
  const view: PageView = {
    bytes: new Uint8Array(),
    kind: PageEncoding.dictionary,
    tag: ValueTag.varlen,
    flags: 0,
    width: 1,
    rowCount: n,
    aux: 0,
    nullCount: sparse ? Math.ceil(n / 7) : 0,
    validity,
    allNull: false,
    safe: false,
    boundSafe: false,
    specials: null,
    scale: 0,
    scales: null,
    large: null,
    start: 0,
    unsigned: codes,
    baseLow: 0,
    baseHigh: 0,
    baseNumber: 0,
    checkpoints: null,
    plain: null,
    data: null,
    patches: null,
    patchBytes: null,
  };
  const repeats = 512;
  const run = () => {
    let checksum = 0;
    for (let repeat = 0; repeat < repeats; repeat++)
      for (let from = 0; from < n; from += 1001)
        checksum += varlenBytesOf(view, dict, null, from, Math.min(n, from + 1001));
    return checksum;
  };
  run();
  const before = process.cpuUsage(),
    threadBefore = process.threadCpuUsage(),
    start = performance.now();
  const checksum = run(),
    wallMs = performance.now() - start,
    cpu = process.cpuUsage(before),
    thread = process.threadCpuUsage(threadBefore);
  if (checksum !== (prefix[n] ?? 0) * repeats)
    throw new Error("Dictionary sizing differs from source lengths");
  return {
    sparse,
    rows: n * repeats,
    checksum,
    cpuMs: (cpu.user + cpu.system) / 1000,
    threadCpuMs: (thread.user + thread.system) / 1000,
    wallMs,
    timed,
  };
}
