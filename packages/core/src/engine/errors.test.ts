import { expect, it } from "vitest";
import { CompactionWriteAmplificationError } from "./errors.js";

it("reports compaction write-amplification limits as typed structured state", () => {
  const error = new CompactionWriteAmplificationError(2_048, 1_024);
  expect(error).toBeInstanceOf(Error);
  expect(error).toMatchObject({
    name: "CompactionWriteAmplificationError",
    outputBytes: 2_048,
    maximumOutputBytes: 1_024,
  });
  expect(error.message).toBe("Compaction output would use 2048 stored bytes; limit is 1024 bytes");
});
