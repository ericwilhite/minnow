/**
 * Each budget refusal of a read becomes the typed 53200, naming the read; anything else passes
 * through. Law: next/memory/README.md, every memory refusal is `MemoryAdmissionError` (SQLSTATE
 * 53200); the write path types its own reads' refusals (page/read-refusal.ts, interim).
 */
import { expect, it } from "vitest";
import { ParseBudgetError } from "../format/manifest-codec.js";
import { MemoryAdmissionError } from "../memory-values.js";
import { ReadDeniedError } from "../storage/extents.js";
import { readRefusal } from "./read-refusal.js";
import { PageBudgetError } from "./reader.js";

it("types each read refusal as 53200, naming the read", () => {
  for (const [error, read] of [
    [new ReadDeniedError(2_500_000), "a stored value or page lease"],
    [new PageBudgetError(6_061), "a chunk reader's pages"],
    [new ParseBudgetError(1_032), "a metadata parse"],
  ] as const) {
    const typed = readRefusal(error);
    expect(typed).toBeInstanceOf(MemoryAdmissionError);
    expect(typed).toMatchObject({ sqlState: "53200", bytes: error.bytes });
    expect((typed as Error).message).toContain(`write path read (${read})`);
  }
});

it("leaves every other error as it is", () => {
  const other = new TypeError("not a read refusal");
  expect(readRefusal(other)).toBe(other);
});
