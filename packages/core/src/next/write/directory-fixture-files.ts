/** Raw native-file fixture host. No manifest/page codec or logical import/export path here. */
import { MemoryFile } from "../storage/memory-store.js";
import type { SyncFile } from "../storage/contract.js";
import { SimDisk } from "../storage/sim-disk.js";
export interface DirectoryImage {
  readonly epoch: number;
  readonly files: Record<string, string>;
}
interface Contents {
  bytes: Uint8Array;
  size: number;
  holder: MemoryFile | null;
}
export class DirectoryFiles {
  epoch = 0;
  readonly files = new Map<string, Contents>();
  open(name: string): SyncFile {
    let c = this.files.get(name);
    if (c === undefined) {
      c = { bytes: new Uint8Array(), size: 0, holder: null };
      this.files.set(name, c);
    }
    if (c.holder !== null)
      throw new DOMException("File already open", "NoModificationAllowedError");
    const f = new MemoryFile(c);
    c.holder = f;
    return f;
  }
  image(): DirectoryImage {
    return {
      epoch: this.epoch,
      files: Object.fromEntries(
        [...this.files].map(([name, c]) => [
          name,
          Buffer.from(c.bytes.subarray(0, c.size)).toString("base64"),
        ]),
      ),
    };
  }
  static load(image: DirectoryImage): DirectoryFiles {
    const disk = new DirectoryFiles();
    disk.epoch = image.epoch;
    for (const [name, b64] of Object.entries(image.files)) {
      const bytes = Uint8Array.from(Buffer.from(b64, "base64"));
      disk.files.set(name, { bytes, size: bytes.length, holder: null });
    }
    return disk;
  }
}
export function simulatedImage(
  image: DirectoryImage,
  options: ConstructorParameters<typeof SimDisk>[0] = {},
): SimDisk {
  const disk = new SimDisk(options);
  disk.epoch = image.epoch;
  for (const [name, b64] of Object.entries(image.files)) {
    const f = disk.open(name, false);
    f.write(Uint8Array.from(Buffer.from(b64, "base64")), { at: 0 });
    f.flush();
    f.close();
  }
  return disk;
}
