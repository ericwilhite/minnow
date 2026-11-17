import { expect, it } from "vitest";
import { NativeLiveState } from "./state.js";
it("keeps stable loading/ready/error snapshots and isolates listener failures", () => {
  let bytes = 0,
    errors = 0,
    calls = 0;
  const state = NativeLiveState.create<number>(
    {
      allocate: (n) => {
        bytes += n;
        return true;
      },
      free: (n) => {
        bytes -= n;
      },
    },
    () => {
      errors++;
    },
  );
  const initial = state.getSnapshot();
  state.loading();
  expect(state.getSnapshot()).toBe(initial);
  const stop = state.subscribe(() => {
    calls++;
  });
  const bad = state.subscribe(() => {
    throw new Error("listener");
  });
  const rows = Object.freeze([1, 2]);
  state.ready(rows, 1, true);
  const ready = state.getSnapshot();
  state.ready(rows, 2, false);
  expect(state.getSnapshot()).toBe(ready);
  state.loading();
  expect(state.getSnapshot()).toMatchObject({ status: "loading", rows });
  const error = new Error("query");
  state.error(error);
  const failed = state.getSnapshot();
  expect(failed).toMatchObject({ status: "error", rows, error, version: 1 });
  state.error(error);
  expect(state.getSnapshot()).toBe(failed);
  state.ready(rows, 3, false);
  expect(state.getSnapshot().status).toBe("ready");
  expect(calls).toBe(4);
  expect(errors).toBe(4);
  stop();
  bad();
  state.close();
  expect(bytes).toBe(0);
  state.ready(rows, 4, true);
  expect(state.getSnapshot().status).toBe("loading");
});
