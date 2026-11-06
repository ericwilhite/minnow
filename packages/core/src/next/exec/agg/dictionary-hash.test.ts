/** A dictionary's codes reuse value ids: hashes are per observed entry, never per row. */
import { expect, it, vi } from "vitest";
import { xxHash32 } from "../../hash/xxhash32.js";
import { TEXT, UNCHARGED, dictionary, texts } from "../kernels/test-support.js";
import { GroupTable } from "./groups.js";

vi.mock("../../hash/xxhash32.js", async (original) => {
  const actual = await original<typeof import("../../hash/xxhash32.js")>();
  return { ...actual, xxHash32: vi.fn(actual.xxHash32) };
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
