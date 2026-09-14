import { expect } from "@playwright/test";
import { test } from "./fixtures.js";

test("an aborted native store-choice transaction cannot authorize opening a database", async ({
  page,
}) => {
  await page.goto("/packages/core/browser/");
  const result = await page.evaluate(async () => {
    const url = "/packages/core/browser/auto-choice.ts";
    return (
      (await import(url)) as typeof import("../browser/auto-choice.js")
    ).abortChoiceAfterRequestSuccess();
  });
  expect(result).toEqual({
    requestSucceeded: true,
    transactionAborted: true,
    opened: false,
    errorName: "AbortError",
  });
});
