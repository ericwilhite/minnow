import { expect, it } from "vitest";
import { chaosPoster } from "./next-storage-post.js";

it("claims the prefix before posting, including interruption at postMessage return", () => {
  const counter = new Int32Array(new SharedArrayBuffer(4));
  const interruption = new Error("paused after postMessage, before the next instruction");
  const delivered: unknown[] = [];
  const post = chaosPoster(
    {
      postMessage(message: unknown) {
        delivered.push(message);
        throw interruption;
      },
    },
    counter,
  );
  try {
    post({ chaos: "began", sequence: 1 });
    expect.fail("not interrupted");
  } catch (error) {
    expect(error).toBe(interruption);
  }
  expect(delivered).toEqual([{ chaos: "began", sequence: 1, n: 1 }]);
  expect(Atomics.load(counter, 0)).toBe(1);
});
