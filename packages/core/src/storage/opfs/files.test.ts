import { describe, expect, it } from "vitest";
import fc from "fast-check";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { decodeSegment, encodeSegment, OpfsTree } from "./files.js";

describe("segment encoding", () => {
  it("passes plain names through and escapes the rest", () => {
    expect(encodeSegment("segment")).toBe("segment");
    expect(encodeSegment("000001")).toBe("000001");
    expect(encodeSegment("")).toBe("%");
    expect(encodeSegment(".")).toBe("%2E");
    expect(encodeSegment("..")).toBe("%2E.");
    expect(encodeSegment("a b")).toBe("a%20b");
    expect(encodeSegment("100%")).toBe("100%25");
  });

  it("round-trips arbitrary segments, including the odd ones", () => {
    fc.assert(
      fc.property(fc.string(), (segment) => {
        const encoded = encodeSegment(segment);
        expect(decodeSegment(encoded)).toBe(segment);
        // The encoded form is a legal, unambiguous entry name.
        expect(encoded.length).toBeGreaterThan(0);
        expect(encoded.includes("/")).toBe(false);
        expect(encoded === "." || encoded === "..").toBe(false);
      }),
    );
  });
});

describe("OpfsTree", () => {
  it("writes, stats, reads, and deletes through nested directories", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const path = ["temp", encodeSegment("owner one"), encodeSegment("run/1"), "0"];
    await tree.writeFile(path, new Uint8Array([1, 2, 3]));
    expect([...((await tree.readFile(path)) ?? [])]).toEqual([1, 2, 3]);
    expect(await tree.deleteFile(path)).toBe(true);
    expect(await tree.deleteFile(path)).toBe(false);
    expect(await tree.readFile(path)).toBeUndefined();
  });

  it("walks files with sizes and lists names", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    await tree.writeFile(["extents", "a"], new Uint8Array(4));
    await tree.writeFile(["extents", "deep", "b"], new Uint8Array(2));
    const walked = (await tree.walkFiles(["extents"]))
      .map(({ path, size }) => [path.join("/"), size] as const)
      .sort((left, right) => left[0].localeCompare(right[0]));
    expect(walked).toEqual([
      ["a", 4],
      ["deep/b", 2],
    ]);
    expect(await tree.listNames(["missing"])).toEqual([]);
  });

  it("reads a locked file as absent only when asked", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    await tree.writeFile(["wal"], new Uint8Array([1]));
    const held = await tree.openHandle(["wal"], { create: false });
    await expect(tree.readFile(["wal"])).rejects.toMatchObject({
      name: "NoModificationAllowedError",
    });
    expect(await tree.readFile(["wal"], { lockedMeansAbsent: true })).toBeUndefined();
    held.close();
    expect([...((await tree.readFile(["wal"])) ?? [])]).toEqual([1]);
  });

  it("survives deleting a tree and recreating it (cache invalidation)", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    await tree.writeFile(["temp", "owner", "run", "0"], new Uint8Array([1]));
    await tree.deleteTree(["temp", "owner"]);
    expect(await tree.readFile(["temp", "owner", "run", "0"])).toBeUndefined();
    await tree.writeFile(["temp", "owner", "run", "0"], new Uint8Array([2]));
    expect([...((await tree.readFile(["temp", "owner", "run", "0"])) ?? [])]).toEqual([2]);
  });
});
