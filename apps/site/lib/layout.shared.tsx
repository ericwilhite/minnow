import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { Logo } from "@/components/logo";

/**
 * Shared chrome for every layout: the wordmark, the top-level links, and the repository.
 *
 * Which version a reader is on is the version picker's job, at the top of the docs sidebar, so
 * that it sits with the pages it applies to rather than over the console and the benchmarks —
 * which are always the current release.
 */
export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <>
          <Logo size={24} />
          <span className="font-semibold">Minnow</span>
        </>
      ),
      transparentMode: "top",
    },
    links: [
      { text: "Docs", url: "/docs", active: "nested-url" },
      { text: "Console", url: "/#console" },
      { text: "Benchmarks", url: "/benchmarks" },
      { text: "AI & LLMs", url: "/docs/reference/agents" },
    ],
    githubUrl: "https://github.com/ericwilhite/minnow",
  };
}
