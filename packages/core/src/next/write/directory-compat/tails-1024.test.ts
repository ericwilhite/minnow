import { createReach, DIRECTORY_COMPAT_TAILS } from "../../../testing/reach.js";
import { registerDirectoryTails } from "../directory-tails-test-support.js";
registerDirectoryTails(
  1024,
  createReach("next-directory-tails-1024", [
    ...DIRECTORY_COMPAT_TAILS.slice(1024, 1152),
    "current to baseline continuation",
    "baseline to current continuation",
    "every value preserved",
    "exact cleanup",
    "range end reopened",
  ]),
);
