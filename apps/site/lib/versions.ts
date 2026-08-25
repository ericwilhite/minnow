import versionsJson from "@/public/versions.json";

/**
 * Which documentation a URL points at.
 *
 * The current release is served unprefixed: minnowdb.com/docs/... is always the docs for the
 * version on npm today, so a link written anywhere keeps pointing at documentation that matches
 * the package a reader is about to install. At a major boundary, the previous line is built from
 * its final tag with `SITE_BASE_PATH` set and committed under the next site's public directory —
 * minnowdb.com/v0/docs/... — so one Vercel project serves current and frozen documentation.
 *
 * public/versions.json is the list of what exists, and it is served at /versions.json — the site
 * root, not this build's base path — so that an archived build, frozen at a tag that predates
 * every later release, still reads the current list and offers versions it has never heard of.
 * The copy compiled into the build is the fallback for when that fetch fails.
 */
export interface DocsVersion {
  /** What the picker shows, e.g. `0.x`. */
  label: string;
  /** The full version this documentation was built from, e.g. `0.1.0`. */
  version: string;
  /** Where it is published, with both slashes: `/` for current, `/v0/` for an archived major. */
  path: string;
}

export interface VersionList {
  current: DocsVersion;
  archived: DocsVersion[];
}

/** Where the site the browser is reading was published. Empty for the current release. */
export const basePath = process.env.NEXT_PUBLIC_SITE_BASE_PATH ?? "";

/**
 * The list as it stood when this build was made. The cast is the shape the JSON always has;
 * TypeScript reads the checked-in `archived: []` as an array of nothing.
 */
export const buildTimeVersions = versionsJson as VersionList;

/** True when this build is an archived release rather than the current one. */
export const isArchived = basePath !== "";

/**
 * Where the list lives, from the origin's root. Root-relative rather than absolute: an archive is
 * published under a prefix of the same site, so this reaches the live list from either, and a
 * local build reads its own instead of asking production across an origin it cannot.
 */
export const versionsUrl = "/versions.json";

/** The version this build documents. */
export const buildVersion: DocsVersion = isArchived
  ? (buildTimeVersions.archived.find((entry) => entry.path === `${basePath}/`) ?? {
      label: `${basePath.replace("/v", "")}.x`,
      version: buildTimeVersions.current.version,
      path: `${basePath}/`,
    })
  : buildTimeVersions.current;

/** Current first, then newest archive first, which is the order a picker should list them in. */
export function ordered(list: VersionList): DocsVersion[] {
  return [list.current, ...list.archived];
}

/**
 * The same page in another version. Next strips the base path from `usePathname`, so the path
 * here is version-independent and only the prefix changes.
 */
export function hrefFor(version: DocsVersion, pathname: string): string {
  const prefix = version.path.replace(/\/$/, "");
  return `${prefix}${pathname === "/" ? "" : pathname}` || "/";
}
