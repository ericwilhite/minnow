export interface NavItem {
  title: string;
  href: string;
}

export interface NavSection {
  title: string;
  items: NavItem[];
}

export const docsNav: NavSection[] = [
  {
    title: "Start here",
    items: [
      { title: "Overview", href: "/docs/" },
      { title: "Getting started", href: "/docs/getting-started/" },
      { title: "Playground", href: "/docs/playground/" },
      { title: "Best practices", href: "/docs/best-practices/" },
    ],
  },
  {
    title: "Guides",
    items: [
      { title: "Schema & migrations", href: "/docs/schema/" },
      { title: "Typed queries", href: "/docs/queries/" },
      { title: "Writes & transactions", href: "/docs/writes/" },
      { title: "Live queries", href: "/docs/live-queries/" },
      { title: "Workers & multi-tab", href: "/docs/workers/" },
      { title: "Devtools", href: "/docs/devtools/" },
      { title: "Architecture", href: "/docs/architecture/" },
    ],
  },
  {
    title: "Reference",
    items: [
      { title: "API reference", href: "/docs/api/" },
      { title: "SQL support", href: "/docs/sql/" },
      { title: "Benchmarks", href: "/benchmarks/" },
    ],
  },
];
