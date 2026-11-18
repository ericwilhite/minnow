import { createElement } from "react";
import { renderToString } from "react-dom/server";
import { expect, it } from "vitest";
import * as existing from "../../dist/index.js";
import * as native from "./index.js";

it("reuses the exact structural hook implementation qualified by the existing suite", () => {
  expect(native.useLiveQuery).toBe(existing.useLiveQuery);
  expect(native.useLiveSelector).toBe(existing.useLiveSelector);
  expect(native.useSuspenseLiveQuery).toBe(existing.useSuspenseLiveQuery);
  const snapshot = {
    status: "ready" as const,
    rows: [{ n: 9007199254740993n, t: "2000-02-29T12:34:56.123456" }],
  };
  const query = { getSnapshot: () => snapshot, subscribe: () => () => undefined };
  function Rows() {
    const current = native.useLiveQuery(query);
    return createElement("p", null, `${String(current.rows[0]?.n)} ${current.rows[0]?.t ?? ""}`);
  }
  expect(renderToString(createElement(Rows))).toContain(
    "9007199254740993 2000-02-29T12:34:56.123456",
  );
  expect(() => native.nativeLiveStore()).toThrow("not yet bound");
});
