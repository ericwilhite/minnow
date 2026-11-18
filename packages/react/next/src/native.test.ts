import { decodeResult } from "../../../core/dist/next/owner/result.js";
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

it("presents canonical duplicate-label object rows with last-wins semantics", () => {
  const result = decodeResult<{ x: number }>({
    columns: [1, 2].map(() => ({
      name: "x",
      type: { kind: "int32", bits: 32 },
      sqlType: "integer",
      nullable: false,
      render: null,
    })),
    frames: [
      {
        sequence: 0,
        rowCount: 1,
        dictionaries: [],
        columns: [1, 2].map((value) => ({
          form: "flat",
          length: 1,
          validity: null,
          data: { layout: "i32", values: Int32Array.of(value) },
        })),
      },
    ],
    summary: {
      command: "SELECT",
      rowCount: null,
      counts: null,
      lsn: null,
      replayed: false,
      closedEarly: false,
      transaction: "idle",
      catalogVersion: 1,
    },
  });
  const snapshot = { status: "ready" as const, rows: result.rows() };
  const query = { getSnapshot: () => snapshot, subscribe: () => () => undefined };
  function Rows() {
    const current = native.useLiveQuery(query);
    return createElement("p", null, current.rows[0]?.x);
  }
  expect(renderToString(createElement(Rows))).toBe("<p>2</p>");
});
