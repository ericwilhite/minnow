import { describe, it } from "vitest";
import { blockFormatProperties } from "./property-campaign.js";
import { seedsFor } from "../testing/seeds.js";

describe("block format properties", () => {
  for (const seed of seedsFor("block-format-properties", [0xb10c])) {
    for (const [name, run] of Object.entries(blockFormatProperties)) {
      it(`${name} (seed ${String(seed)})`, () => run(seed));
    }
  }
});
