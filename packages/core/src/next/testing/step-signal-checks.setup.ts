/**
 * Unit-project setup: engine code opens no step signal while an abort of one of its sources is in
 * dispatch (step-signal.ts, "Contract": the one timing difference from `AbortSignal.any` needs
 * such an open). Every test file starts the check and must end with no such open recorded.
 * testing/step-signal.test.ts opens them on purpose and clears its own after each test.
 */
import { afterAll, beforeAll, expect } from "vitest";
import { checkStepOpensDuringAbort } from "../step-signal.js";

beforeAll(() => {
  checkStepOpensDuringAbort(true);
});

afterAll(() => {
  expect(checkStepOpensDuringAbort(false), "steps opened inside an abort dispatch").toEqual([]);
});
