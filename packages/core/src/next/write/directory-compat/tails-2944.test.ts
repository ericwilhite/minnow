import { createReach, DIRECTORY_COMPAT_TAILS } from "../../../testing/reach.js";
import { registerDirectoryTails } from "../directory-tails-test-support.js";
registerDirectoryTails(
  2944,
  createReach("next-directory-tails-2944", [
    ...DIRECTORY_COMPAT_TAILS.slice(2944, 3072),
    "current to baseline continuation",
    "baseline to current continuation",
    "every value preserved",
    "exact cleanup",
    "range end reopened",
  ]),
);
