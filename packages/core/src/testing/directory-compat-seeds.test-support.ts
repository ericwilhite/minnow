/**
 * The directory publication compatibility campaign's seeds
 * (next/write/directory-publication-compat-1..4.test.ts), drawn once here so the campaign stays
 * the registry's single consumer of its suite (seeds.test.ts); each part runs its slot of them.
 */
import { seedsFor } from "./seeds.js";

export const DIRECTORY_COMPAT_SEEDS = seedsFor(
  "next-directory-publication-compat",
  [28011, 28012, 28013, 28014],
);
