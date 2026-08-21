import Link from "next/link";
import { Playground } from "@/components/playground/playground";
import { HeroPond } from "@/components/pond/pond";

const FACTS = [
  {
    title: "Real SQL, our own engine",
    body: "Parser, planner, optimizer, and a vectorized executor written here. Joins, CTEs, window functions, set operations, grouping sets, full-text search. No SQLite or DuckDB underneath.",
  },
  {
    title: "No WebAssembly to download",
    body: "Plain JavaScript, around 185 KB gzipped. Nothing is fetched and compiled before the first query answers.",
  },
  {
    title: "Columnar and durable",
    body: "Immutable compressed column blocks on IndexedDB or OPFS, read through snapshots. Writes publish atomically; another tab sees the old version or the new one, never half of one.",
  },
  {
    title: "Bounded memory",
    body: "Execution works in batches under a budget you set and spills when it has to. A whole table is never required to be resident.",
  },
];

export default function HomePage() {
  return (
    <main className="flex flex-1 flex-col">
      <section className="mx-auto w-full max-w-6xl px-4 pt-10 pb-8 text-center">
        {/* The mark, at size: one minnow in a pond that swims away from whoever reaches for it. */}
        <div className="mb-8 h-[190px] sm:h-[240px] md:h-[280px]">
          <HeroPond />
        </div>
        <h1 className="text-balance text-4xl font-semibold tracking-tight sm:text-5xl">
          A SQL engine that runs in the browser
        </h1>
        <p className="mx-auto mt-4 max-w-2xl text-balance text-lg text-fd-muted-foreground">
          Minnow is a columnar SQL database for the browser. Real queries over immutable snapshots,
          durable on IndexedDB or OPFS, with no server and nothing to compile before the first
          answer.
        </p>
        <div className="mt-7 flex flex-wrap items-center justify-center gap-3">
          <Link
            href="/docs"
            className="rounded-lg bg-fd-primary px-4 py-2 font-medium text-fd-primary-foreground"
          >
            Read the docs
          </Link>
          <Link
            href="#console"
            className="rounded-lg border border-fd-border px-4 py-2 font-medium hover:bg-fd-accent"
          >
            Open the console
          </Link>
          <Link
            href="/benchmarks"
            className="rounded-lg border border-fd-border px-4 py-2 font-medium hover:bg-fd-accent"
          >
            Run the benchmarks
          </Link>
          <code className="rounded-lg border border-fd-border px-4 py-2 text-sm">
            npm install @minnowdb/core
          </code>
        </div>
      </section>

      {/*
        The console is the argument. It is not a screenshot or a canned response: the page builds
        a real database in this browser and answers whatever gets typed into it — in SQL, or in
        TypeScript through the typed client, over the same database either way.

        It carries the id every link that used to point at a separate playground page now uses.
      */}
      <section id="console" className="mx-auto w-full max-w-6xl px-4 pb-16 scroll-mt-20">
        <Playground />
      </section>

      <section className="mx-auto w-full max-w-6xl px-4 pb-20">
        <div className="grid gap-4 sm:grid-cols-2">
          {FACTS.map((fact) => (
            <div key={fact.title} className="rounded-xl border border-fd-border p-5">
              <h2 className="mb-1.5 font-semibold">{fact.title}</h2>
              <p className="text-sm text-fd-muted-foreground">{fact.body}</p>
            </div>
          ))}
        </div>
      </section>
    </main>
  );
}
