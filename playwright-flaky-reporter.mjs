// @ts-check
/**
 * Turns a run with flaky tests red.
 *
 * A retry keeps one noisy sample from blocking a merge, but a test that needed the retry is a
 * test that fails sometimes, and Playwright reports the run green all the same. This lists every
 * test whose outcome was "flaky" once the run ends and fails the run, so a flake is fixed or
 * quarantined instead of retried forever.
 *
 * @typedef {import("@playwright/test/reporter").Reporter} Reporter
 * @implements {Reporter}
 */
export default class FlakyReporter {
  /** @type {import("@playwright/test/reporter").Suite | undefined} */
  #suite;

  /**
   * @param {import("@playwright/test/reporter").FullConfig} _config
   * @param {import("@playwright/test/reporter").Suite} suite
   */
  onBegin(_config, suite) {
    this.#suite = suite;
  }

  /** @returns {Promise<{ status: "failed" } | undefined>} */
  async onEnd() {
    const flaky = (this.#suite?.allTests() ?? []).filter((test) => test.outcome() === "flaky");
    if (flaky.length === 0) return undefined;
    console.error(`\n${String(flaky.length)} flaky test(s) passed only on retry:`);
    for (const test of flaky) console.error(`  ${test.titlePath().filter(Boolean).join(" > ")}`);
    process.exitCode = 1;
    return { status: "failed" };
  }
}
