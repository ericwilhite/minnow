/** Test-only COW fork of a closed, never-crashed SimDisk. File images and operation payloads
 * are immutable under SimDisk's contract. Each fork gets private lists/handles and NO append
 * lineage, so its first append copies before using spare capacity. Preserve pending/queued
 * operations: taking only durable or current bytes changes the power-loss model.
 * A never-used crash RNG may be recreated with each cut's seed; a consumed RNG may not.
 */
import { createHash } from "node:crypto";
import { SimDisk, type SimDiskOptions, type SimFile } from "../storage/sim-disk.js";

interface ClosedDiskFixture {
  (options: SimDiskOptions): SimDisk;
  /** Check after EVERY case, including failed cases, before reusing the shared images. */
  verify(): void;
}

export function closedDiskFixture(disk: SimDisk): ClosedDiskFixture {
  if (disk.crashes !== 0) throw new TypeError("Cannot fork a consumed crash random stream");
  for (const file of disk.files.values())
    if (file.handles.size !== 0) throw new TypeError("Cannot fork live file handles");
  const epoch = disk.epoch;
  const files = [...disk.files].map(([name, file]) => ({
    name,
    current: file.current,
    durable: file.durable,
    pending: file.pending.slice(),
    queued: file.queued.slice(),
  }));
  // Hash framed values, not a concatenation: names, lengths, operation order and coordinates
  // are part of the fixture too. Stream image bytes without retaining another image copy.
  const fingerprint = () => {
    const hash = createHash("sha256"),
      word = new Uint8Array(8),
      view = new DataView(word.buffer);
    const number = (value: number) => {
      view.setFloat64(0, value, true);
      hash.update(word);
    };
    const bytes = (value: Uint8Array) => {
      number(value.byteLength);
      hash.update(value);
    };
    number(epoch);
    number(files.length);
    for (const file of files) {
      number(file.name.length);
      hash.update(file.name, "utf16le");
      bytes(file.current);
      bytes(file.durable);
      for (const operations of [file.pending, file.queued]) {
        number(operations.length);
        for (const operation of operations) {
          hash.update(operation.kind);
          if (operation.kind === "write") {
            number(operation.at);
            bytes(operation.bytes);
          } else number(operation.size);
        }
      }
    }
    return hash.digest("hex");
  };
  const baseline = fingerprint();
  const fork = (options: SimDiskOptions) => {
    const fork = new SimDisk(options);
    fork.epoch = epoch;
    for (const file of files)
      fork.files.set(file.name, {
        current: file.current,
        durable: file.durable,
        pending: file.pending.slice(),
        queued: file.queued.slice(),
        append: null,
        appendEnd: 0,
        handles: new Set<SimFile>(),
      });
    return fork;
  };
  return Object.assign(fork, {
    verify() {
      if (fingerprint() !== baseline) throw new Error("Shared closed disk fixture changed");
    },
  });
}
