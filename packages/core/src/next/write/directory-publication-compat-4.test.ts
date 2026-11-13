/** Part 4 of 4 of the directory publication compatibility campaign: its fixtures and cases are
 * in directory-publication-compat.test-support.ts, and its reach is asserted with the other parts'
 * in directory-publication-compat.reach.test.ts. */
import { DIRECTORY_COMPAT_SEEDS } from "../../testing/directory-compat-seeds.test-support.js";
import { createReach, DIRECTORY_TRANSITION_REACH } from "../../testing/reach.js";
import { recordReach } from "../../testing/reach-union.js";
import { directoryCompatPart } from "./directory-publication-compat.test-support.js";

directoryCompatPart(
  4,
  recordReach(
    "next-directory-transition",
    createReach("next-directory-transition", DIRECTORY_TRANSITION_REACH),
    import.meta.url,
  ),
  DIRECTORY_COMPAT_SEEDS,
);
