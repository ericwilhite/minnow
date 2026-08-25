import { describe, expect, it } from "vitest";
import { buildTimeVersions, hrefFor, ordered } from "./versions";

describe("documentation major lines", () => {
  it("lists each major once using a stable label and path", () => {
    const versions = ordered(buildTimeVersions);
    const majors = versions.map((entry) => Number(entry.version.split(".")[0]));

    expect(new Set(majors).size).toBe(majors.length);
    expect(buildTimeVersions.current.path).toBe("/");
    expect(buildTimeVersions.current.label).toBe(`${String(majors[0])}.x`);

    for (const [index, archived] of buildTimeVersions.archived.entries()) {
      const major = majors[index + 1];
      expect(archived.label).toBe(`${String(major)}.x`);
      expect(archived.path).toBe(`/v${String(major)}/`);
    }
  });

  it("keeps the same page when moving between major lines", () => {
    expect(hrefFor({ version: "0.4.0", label: "0.x", path: "/v0/" }, "/docs/sql/")).toBe(
      "/v0/docs/sql/",
    );
    expect(hrefFor(buildTimeVersions.current, "/")).toBe("/");
  });
});
