import { createReach, DIRECTORY_COMPAT_TAILS } from "../../../testing/reach.js";
import { registerDirectoryTails } from "../directory-tails-test-support.js";
registerDirectoryTails(
  512,
  createReach("next-directory-tails-512", [
    ...DIRECTORY_COMPAT_TAILS.slice(512, 640),
    "current to baseline continuation",
    "baseline to current continuation",
    "every value preserved",
    "exact cleanup",
    "range end reopened",
  ]),
);
