/** Shared native FTS fixtures live in the composition layer; differential campaigns
 * keep this import path without creating an owner -> testing dependency. */
export {
  ftsBudget,
  openFtsHarness,
  setupFts,
  ftsParts,
  lastFtsCommit,
} from "../owner/fts-transaction.test-support.js";
