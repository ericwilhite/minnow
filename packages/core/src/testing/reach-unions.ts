/**
 * Reach campaigns split across several test files (./reach-union.ts): their files, repository-
 * relative, and the `*.reach.test.ts` file that asserts their union. No imports, so the test
 * sequencer (scripts/test-shards.ts) can read it to keep each campaign's files in one run part.
 * Excluded from the published tarball.
 */
export const REACH_UNIONS = {
  "next-directory-transition": {
    files: [
      "packages/core/src/next/write/directory-publication-compat-1.test.ts",
      "packages/core/src/next/write/directory-publication-compat-2.test.ts",
      "packages/core/src/next/write/directory-publication-compat-3.test.ts",
      "packages/core/src/next/write/directory-publication-compat-4.test.ts",
    ],
    union: "packages/core/src/next/write/directory-publication-compat.reach.test.ts",
  },
} as const satisfies Record<string, { files: readonly string[]; union: string }>;

