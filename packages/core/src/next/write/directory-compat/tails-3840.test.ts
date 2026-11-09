import { createReach, DIRECTORY_COMPAT_TAILS } from "../../../testing/reach.js";
import { registerDirectoryTails } from "../directory-tails-test-support.js";
registerDirectoryTails(
  3840,
  createReach("next-directory-tails-3840", [
    ...DIRECTORY_COMPAT_TAILS.slice(3840, 3968),
    "current to baseline continuation",
    "baseline to current continuation",
    "every value preserved",
    "exact cleanup",
    "range end reopened",
  ]),
);
