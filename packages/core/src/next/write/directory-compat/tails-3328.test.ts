import { createReach, DIRECTORY_COMPAT_TAILS } from "../../../testing/reach.js";
import { registerDirectoryTails } from "../directory-tails-test-support.js";
registerDirectoryTails(
  3328,
  createReach("next-directory-tails-3328", [
    ...DIRECTORY_COMPAT_TAILS.slice(3328, 3456),
    "current to peer continuation",
    "peer to current continuation",
    "every value preserved",
    "exact cleanup",
    "range end reopened",
  ]),
);
