import { expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsTree } from "../opfs/files.js";
import { WalAcknowledgements } from "./wal-acknowledgement.js";
import { WalWriter, iterateWalFrames } from "./wal.js";

function present<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("Required fixture bytes are missing");
  return value;
}

it("rejects every truncation point before the durable boundary and permits a later torn append", async () => {
  const shim = new MemoryOpfs();
  const tree = new OpfsTree(shim.root);
  const handle = await tree.openHandle(["wal"], { create: true });
  const writer = new WalWriter(handle, 0);
  writer.append({ seq: 1, value: "acknowledged" }, true);
  const boundary = writer.byteLength;
  writer.append({ seq: 2, value: "unfinished" }, false);
  const original = present(shim.readFileBytes("wal"));
  handle.close();
  for (let cut = 0; cut < original.length; cut += 1) {
    shim.writeFileBytes("wal", original.slice(0, cut));
    const reader = await tree.openHandle(["wal"], { create: false });
    if (cut < boundary) expect(() => [...iterateWalFrames(reader, boundary)]).toThrow();
    else
      expect([...iterateWalFrames(reader, boundary)].map((frame) => frame.payload)).toEqual([
        { seq: 1, value: "acknowledged" },
      ]);
    reader.close();
  }
});

it("round-trips acknowledgement boundaries and refuses damaged copies instead of using an older slot", async () => {
  const shim = new MemoryOpfs();
  shim.setTransferLimit((_path, _operation, requested) => Math.min(3, requested));
  const handle = await new OpfsTree(shim.root).openHandle(["ack"], { create: true });
  const acknowledgements = new WalAcknowledgements(handle, true);
  expect(acknowledgements.latest).toEqual({ sequence: 0, endOffset: 0 });
  acknowledgements.publish(1, 100);
  acknowledgements.publish(2, 200);
  expect(new WalAcknowledgements(handle, false).latest).toEqual({ sequence: 2, endOffset: 200 });
  const original = present(shim.readFileBytes("ack"));
  handle.close();
  for (let offset = 0; offset < original.length; offset += 1) {
    const bytes = original.slice();
    bytes[offset] = present(bytes[offset]) ^ 0xff;
    shim.writeFileBytes("ack", bytes);
    const reader = await new OpfsTree(shim.root).openHandle(["ack"], { create: false });
    expect(() => new WalAcknowledgements(reader, false)).toThrow(/corrupt/);
    reader.close();
  }
  for (let cut = 0; cut < original.length; cut += 1) {
    shim.writeFileBytes("ack", original.slice(0, cut));
    const reader = await new OpfsTree(shim.root).openHandle(["ack"], { create: false });
    expect(() => new WalAcknowledgements(reader, false)).toThrow(/missing or truncated/);
    reader.close();
  }
});
