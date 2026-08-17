import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { Logo } from "@/components/logo";

/** Shared chrome for every layout: the wordmark, the top-level links, and the repository. */
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
      { text: "Playground", url: "/playground" },
      { text: "Benchmarks", url: "/benchmarks" },
    ],
    githubUrl: "https://github.com/minnowdb/minnow",
  };
}
