/** Test-only COW fork of a closed, never-crashed SimDisk. File images and operation payloads
 * are immutable under SimDisk's contract. Each fork gets private lists/handles and NO append
 * lineage, so its first append copies before using spare capacity. Preserve pending/queued
 * operations: taking only durable or current bytes changes the power-loss model.
 * A never-used crash RNG may be recreated with each cut's seed; a consumed RNG may not.
 */
import { SimDisk, type SimDiskOptions, type SimFile } from "../storage/sim-disk.js";
export function closedDiskFixture(disk: SimDisk): (options: SimDiskOptions) => SimDisk {
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
  return (options) => {
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
}
