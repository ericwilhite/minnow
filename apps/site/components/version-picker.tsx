"use client";

import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import {
  buildTimeVersions,
  buildVersion,
  hrefFor,
  isArchived,
  ordered,
  versionsUrl,
  type VersionList,
} from "@/lib/versions";

/**
 * Which version's documentation this is, and a way to any other.
 *
 * The list starts as the one compiled into this build and is replaced by the site root's
 * /versions.json when that is reachable, so an archived build offers releases that did not exist
 * when it was frozen. Switching versions is a full navigation rather than a client route change:
 * archived major lines are frozen static builds under their own prefix.
 */
export function VersionPicker() {
  const pathname = usePathname();
  const [list, setList] = useState<VersionList>(buildTimeVersions);

  useEffect(() => {
    const controller = new AbortController();
    fetch(versionsUrl, { signal: controller.signal })
      .then((response) => (response.ok ? (response.json() as Promise<VersionList>) : null))
      .then((fetched) => {
        if (fetched?.current !== undefined) setList(fetched);
      })
      .catch(() => {
        // Offline, or a deployment without the file: the compiled-in list still works.
      });
    return () => {
      controller.abort();
    };
  }, []);

  const known = ordered(list);
  const options = known.some((version) => version.path === buildVersion.path)
    ? known
    : [...known, buildVersion];

  return (
    <div className="flex flex-col gap-1.5">
      <select
        aria-label="Documentation version"
        className="w-full rounded-lg border border-fd-border bg-fd-card px-2.5 py-1.5 text-sm text-fd-foreground"
        value={buildVersion.path}
        onChange={(event) => {
          const target = options.find((version) => version.path === event.target.value);
          if (target !== undefined) window.location.href = hrefFor(target, pathname);
        }}
      >
        {options.map((version) => (
          <option key={version.path} value={version.path}>
            {version.label}
            {version.path === list.current.path ? " (latest)" : ""}
          </option>
        ))}
      </select>
      {isArchived ? (
        <a
          className="text-xs text-fd-muted-foreground underline underline-offset-2"
          href={hrefFor(list.current, pathname)}
        >
          {list.current.label} is the latest release
        </a>
      ) : null}
    </div>
  );
}
