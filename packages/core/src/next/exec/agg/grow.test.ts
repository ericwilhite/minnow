import { expect, it, vi } from "vitest";
import { TEXT, UNCHARGED, texts, valueAt } from "../kernels/test-support.js";
import { IDENTITY } from "../kernels/vectors.js";
import { accumulatorFor } from "./states.js";

it("MIN/MAX steady growth avoids rebuilding empty-value descriptors and still grows later", () => {
  const state = accumulatorFor("min", TEXT, TEXT, UNCHARGED);
  state.grow(1);
  state.update(texts(["saved"]), new Int32Array(1), IDENTITY, 1, false);
  const stringify = vi.spyOn(JSON, "stringify");
  let calls: number;
  try {
    for (let i = 0; i < 512; i++) state.grow(1);
    calls = stringify.mock.calls.length;
  } finally {
    stringify.mockRestore();
  }
  expect(calls).toBe(0);
  state.grow(2);
  expect(valueAt(state.emit(0, 2), 0)).toBe("saved");
  expect(valueAt(state.emit(0, 2), 1)).toBe(null);
  state.close();
});
