import { createReach, DIRECTORY_COMPAT_TAILS } from "../../../testing/reach.js";
import { registerDirectoryTails } from "../directory-tails-test-support.js";
registerDirectoryTails(
  2304,
  createReach("next-directory-tails-2304", [
    ...DIRECTORY_COMPAT_TAILS.slice(2304, 2432),
    "current to peer continuation",
    "peer to current continuation",
    "every value preserved",
    "exact cleanup",
    "range end reopened",
  ]),
);
