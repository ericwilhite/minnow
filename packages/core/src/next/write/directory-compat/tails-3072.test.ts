import { createReach, DIRECTORY_COMPAT_TAILS } from "../../../testing/reach.js";
import { registerDirectoryTails } from "../directory-tails-test-support.js";
registerDirectoryTails(
  3072,
  createReach("next-directory-tails-3072", [
    ...DIRECTORY_COMPAT_TAILS.slice(3072, 3200),
    "current to baseline continuation",
    "baseline to current continuation",
    "every value preserved",
    "exact cleanup",
    "range end reopened",
  ]),
);
