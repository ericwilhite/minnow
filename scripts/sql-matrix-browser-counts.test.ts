/** Matrix population changes must update the native-browser coverage assertion too. */
import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import matrix from "../packages/core/sql-feature-matrix.json" with { type: "json" };

it("keeps the browser's exact matrix population in step with the committed fixture", () => {
  const spec = readFileSync(
    new URL("../packages/core/browser-tests/sql-differential.spec.ts", import.meta.url),
    "utf8",
  );
  const assertion = /expect\(result\.matrix\)\.toEqual\(\{([\s\S]*?)\}\);/u.exec(spec)?.[1];
  if (assertion === undefined) throw new Error("Browser matrix assertion moved; update this check");
  const count = (key: string): number => {
    const match = new RegExp(`\\b${key}:\\s*(\\d+)\\s*,`, "u").exec(assertion);
    if (match === null) throw new Error(`Browser matrix count ${key} must remain explicit`);
    return Number(match[1]);
  };
  expect({
    entries: count("entries"),
    supported: count("supported"),
    unsupportedRejected: count("unsupportedRejected"),
  }).toEqual({
    entries: matrix.features.length,
    supported: matrix.features.filter((feature) => feature.status === "supported").length,
    unsupportedRejected: matrix.features.filter((feature) => feature.status === "unsupported")
      .length,
  });
});
