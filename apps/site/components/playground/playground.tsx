"use client";
/**
 * The playground's entry point. The console is loaded only in the browser: it constructs a
 * worker, opens IndexedDB, and mounts a CodeMirror editor, none of which exist while the page is
 * being prerendered.
 */
import dynamic from "next/dynamic";

const Console = dynamic(async () => (await import("./console")).PlaygroundConsole, {
  ssr: false,
  loading: () => (
    <div className="minnow-placeholder flex items-center justify-center" style={{ height: 620 }}>
      <p className="text-sm text-fd-muted-foreground">Loading the console…</p>
    </div>
  ),
});

export function Playground({ height }: { height?: number }) {
  return <Console {...(height === undefined ? {} : { height })} />;
}
