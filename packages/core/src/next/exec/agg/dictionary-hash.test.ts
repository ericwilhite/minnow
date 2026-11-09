/** A dictionary's codes reuse value ids: hashes are per observed entry, never per row. */
import { expect, it, vi } from "vitest";
import { xxHash32, xxHash32View } from "../../hash/xxhash32.js";
import { TEXT, UNCHARGED, dictionary, texts } from "../kernels/test-support.js";
import { GroupTable } from "./groups.js";
import { ScalarByteDistinct } from "./scalar-byte-distinct.js";

vi.mock("../../hash/xxhash32.js", async (original) => {
  const actual = await original<typeof import("../../hash/xxhash32.js")>();
  return { ...actual, xxHash32: vi.fn(actual.xxHash32), xxHash32View: vi.fn(actual.xxHash32View) };
});

it("hashes each observed dictionary entry once across batches and resets on a new dictionary", () => {
  const entries = texts(["b", "a", "b", "unused"]);
  const key = dictionary(
    entries,
    Array.from({ length: 2048 }, (_, i) => i % 3),
  );
  const ids = new Int32Array(2048);
  const table = new GroupTable([TEXT], UNCHARGED);
  vi.mocked(xxHash32).mockClear();
  table.assign([key], 2048, null, ids);
  table.assign([key], 2048, null, ids);
  expect(xxHash32).toHaveBeenCalledTimes(3);
  expect(table.groupCount).toBe(2);
  expect(ids[0]).toBe(ids[2]);
  const other = dictionary(texts(["a", "b"]), [0, 1]);
  table.assign([other], 2, null, ids);
  expect(xxHash32).toHaveBeenCalledTimes(5);
  expect(Array.from(ids.subarray(0, 2))).toEqual([1, 0]);
  table.close();
});

it("scalar byte membership hashes only observed entries, including an appended dictionary prefix", () => {
  const entries = texts(["b", "a", "b", "unused"]),
    key = dictionary(
      entries,
      Array.from({ length: 2048 }, (_, i) => i % 3),
    );
  if (key.form !== "dictionary") throw new Error("fixture");
  const set = new ScalarByteDistinct(TEXT, UNCHARGED),
    gids = new Int32Array(2048),
    out = { rows: new Uint16Array(2048), count: 0 };
  vi.mocked(xxHash32View).mockClear();
  try {
    set.filter(key, gids, 2048, null, true, out);
    expect(out.count).toBe(2);
    set.filter(key, gids, 2048, null, true, out);
    expect(out.count).toBe(0);
    expect(xxHash32View).toHaveBeenCalledTimes(3);
    const grown = dictionary(
      texts(["b", "a", "b", "unused", ...Array.from({ length: 701 }, (_, i) => `new${String(i)}`)]),
      [0, 4],
    );
    set.filter(
      { ...grown, dictionaryId: key.dictionaryId } as typeof key,
      gids,
      2,
      null,
      true,
      out,
    );
    expect(out.count).toBe(1);
    expect(xxHash32View).toHaveBeenCalledTimes(4);
    const other = dictionary(texts(["a", "b"]), [0, 1]);
    set.filter(other, gids, 2, null, true, out);
    expect(out.count).toBe(0);
    expect(xxHash32View).toHaveBeenCalledTimes(6);
    expect(set.count).toBe(3);
  } finally {
    set.close();
  }
});
