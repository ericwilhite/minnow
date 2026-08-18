"use client";

import { useEffect, useState } from "react";

/**
 * The 404 as this site would answer it: a query with no rows.
 *
 * The path is read after mount rather than rendered into the page, because there is only one
 * 404.html in a static export — it is served for every path that misses, and so cannot have any
 * of them baked in.
 */
export function MissingQuery() {
  const [path, setPath] = useState<string>();

  useEffect(() => {
    setPath(window.location.pathname);
  }, []);

  return (
    <div className="not-prose mx-auto mt-8 w-full max-w-xl overflow-hidden rounded-xl border border-fd-border text-left">
      <pre className="overflow-x-auto px-4 py-3 text-sm leading-relaxed">
        <code>
          <span className="text-fd-muted-foreground">SELECT</span> *{" "}
          <span className="text-fd-muted-foreground">FROM</span> pages{" "}
          <span className="text-fd-muted-foreground">WHERE</span> path ={" "}
          <span className="text-fd-primary">{`'${path ?? "…"}'`}</span>
        </code>
      </pre>
      <p className="border-t border-fd-border px-4 py-2 text-xs text-fd-muted-foreground">
        0 rows · the fish ate it
      </p>
    </div>
  );
}
