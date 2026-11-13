/** The directory publication compatibility campaign's reach, asserted over the union of what its
 * four files' histories reached (testing/reach-union.ts). The test sequencer keeps those files in
 * the same run part as this one, and the `reach` project runs it after them. */
import { it } from "vitest";
import { DIRECTORY_TRANSITION_REACH } from "../../testing/reach.js";
import { assertReachUnion } from "../../testing/reach-union.js";

it("reached packing transition failure and concurrency states", () =>
  assertReachUnion("next-directory-transition", DIRECTORY_TRANSITION_REACH));
