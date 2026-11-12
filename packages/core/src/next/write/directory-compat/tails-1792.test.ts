import { createReach, DIRECTORY_COMPAT_TAILS } from "../../../testing/reach.js";
import { registerDirectoryTails } from "../directory-tails-test-support.js";
registerDirectoryTails(
  1792,
  createReach("next-directory-tails-1792", [
    ...DIRECTORY_COMPAT_TAILS.slice(1792, 1920),
    "current to peer continuation",
    "peer to current continuation",
    "every value preserved",
    "exact cleanup",
    "range end reopened",
  ]),
);
