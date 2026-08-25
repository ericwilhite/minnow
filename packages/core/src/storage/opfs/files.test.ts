import { describe, expect, it } from "vitest";
import fc from "fast-check";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import {
  decodeSegment,
  encodeSegment,
  MAX_OPFS_DIRECTORY_HANDLE_CACHE_ENTRIES,
  MAX_OPFS_ENCODED_SEGMENT_CHARACTERS,
  MAX_OPFS_PATH_SEGMENTS,
  OpfsTree,
} from "./files.js";

async function collect<T>(items: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const item of items) values.push(item);
  return values;
}

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
      fc.property(fc.string({ unit: "grapheme" }), (segment) => {
        const encoded = encodeSegment(segment);
        expect(decodeSegment(encoded)).toBe(segment);
        // The encoded form is a legal, unambiguous entry name.
        expect(encoded.length).toBeGreaterThan(0);
        expect(encoded.includes("/")).toBe(false);
        expect(encoded === "." || encoded === "..").toBe(false);
      }),
    );
  });

  it("rejects lossy surrogate names instead of aliasing valid Unicode", () => {
    expect(encodeSegment("\uFFFD")).toBe("%EF%BF%BD");
    expect(() => encodeSegment("\uD800")).toThrow("unpaired surrogate");
    expect(() => encodeSegment("x\uDFFF")).toThrow("unpaired surrogate");
  });

  it("rejects malformed, invalid UTF-8, and non-canonical encoded names", () => {
    for (const encoded of ["", "%A", "%GG", "%ff", "%FF", "é", ".hidden", "%41"]) {
      expect(() => decodeSegment(encoded), encoded).toThrow(/Invalid|Non-canonical/);
    }
  });

  it("bounds encoded names with Unicode expansion accounted for", () => {
    const ascii = "a".repeat(MAX_OPFS_ENCODED_SEGMENT_CHARACTERS);
    expect(encodeSegment(ascii)).toBe(ascii);
    expect(() => encodeSegment(`${ascii}a`)).toThrow(RangeError);

    const unicode = "é".repeat(MAX_OPFS_ENCODED_SEGMENT_CHARACTERS / 6);
    const encoded = encodeSegment(unicode);
    expect(encoded).toHaveLength(MAX_OPFS_ENCODED_SEGMENT_CHARACTERS);
    expect(decodeSegment(encoded)).toBe(unicode);
    expect(() => encodeSegment(`${unicode}é`)).toThrow(RangeError);
    expect(() => decodeSegment(`${encoded}A`)).toThrow(RangeError);
  });
});

describe("OpfsTree", () => {
  it("completes partial whole-file writes", async () => {
    const shim = new MemoryOpfs();
    shim.setTransferLimit((_path, _operation, requested) => Math.min(2, requested));
    const tree = new OpfsTree(shim.root);
    const bytes = Uint8Array.from({ length: 33 }, (_, index) => index);
    await tree.writeFile(["control"], bytes, { flush: true });
    expect(await tree.readFile(["control"])).toEqual(bytes);
  });

  it("closes a whole-file handle whose write stops making progress", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    shim.setTransferLimit((_path, operation, requested, at) =>
      operation === "write" && at >= 3 ? 0 : Math.min(3, requested),
    );
    await expect(tree.writeFile(["temp", "stalled"], new Uint8Array(10))).rejects.toThrow(
      /no progress.*writing temp\/stalled/,
    );
    shim.setTransferLimit(null);
    await expect(tree.writeFile(["temp", "stalled"], Uint8Array.of(9))).resolves.toBeUndefined();
    expect(await tree.readFile(["temp", "stalled"])).toEqual(Uint8Array.of(9));
  });

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
    const walked = (await collect(tree.walkFiles(["extents"])))
      .map(({ path, size }) => [path.join("/"), size] as const)
      .sort((left, right) => left[0].localeCompare(right[0]));
    expect(walked).toEqual([
      ["a", 4],
      ["deep/b", 2],
    ]);
    expect(await collect(tree.iterateNames(["missing"]))).toEqual([]);
  });

  it("refuses oversized whole-file reads before allocation and bounds path depth", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    await tree.writeFile(["control"], new Uint8Array(5));
    await expect(tree.readFile(["control"], { maxBytes: 4 })).rejects.toThrow(RangeError);
    await expect(tree.readFile(["control"], { maxBytes: 5 })).resolves.toEqual(new Uint8Array(5));
    await expect(
      tree.writeFile(
        Array.from({ length: MAX_OPFS_PATH_SEGMENTS + 1 }, () => "x"),
        Uint8Array.of(),
      ),
    ).rejects.toThrow(RangeError);
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

  it("bounds directory handles under high path churn and reopens evicted paths", async () => {
    const shim = new MemoryOpfs();
    const tree = new OpfsTree(shim.root);
    const count = MAX_OPFS_DIRECTORY_HANDLE_CACHE_ENTRIES * 4;
    for (let index = 0; index < count; index += 1) {
      await tree.writeFile(["temp", `owner-${String(index)}`, "run", "0"], Uint8Array.of(index));
      expect(tree.directoryCacheSizeForTests).toBeLessThanOrEqual(
        MAX_OPFS_DIRECTORY_HANDLE_CACHE_ENTRIES,
      );
    }
    expect(await tree.readFile(["temp", "owner-0", "run", "0"])).toEqual(Uint8Array.of(0));
    expect(tree.directoryCacheSizeForTests).toBeLessThanOrEqual(
      MAX_OPFS_DIRECTORY_HANDLE_CACHE_ENTRIES,
    );
  });
});
