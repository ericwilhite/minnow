import Link from "next/link";
import { Playground } from "@/components/playground/playground";
import { HeroPond } from "@/components/pond/pond";

const HOME_LINKS = [
  {
    href: "/docs",
    title: "Get started",
    body: "Install Minnow and run your first query",
  },
  {
    href: "/docs/sql/feature-matrix",
    title: "SQL compatibility",
    body: "See every supported PostgreSQL form",
  },
  {
    href: "/docs/adapters",
    title: "Client adapters",
    body: "Use Kysely with Minnow's SQL engine",
  },
  {
    href: "/docs/comparison",
    title: "Why Minnow",
    body: "Compare the browser database options",
  },
  {
    href: "/benchmarks",
    title: "Live benchmarks",
    body: "Run every suite in your own browser",
  },
];

const FEATURES = [
  {
    title: "PostgreSQL-style SQL",
    body: "Use familiar parameters, joins, CTEs, windows, grouping sets, upserts, RETURNING, triggers, exact decimals, JSON, arrays, enums, and savepoints.",
  },
  {
    title: "Direct SQL or Kysely",
    body: "Use the engine's direct SQL API or connect Kysely through its PostgreSQL compiler. Both paths use the same engine and data.",
  },
  {
    title: "Columnar and durable",
    body: "Compressed columns make scans and aggregates efficient. IndexedDB and OPFS keep writes durable and publish each commit atomically.",
  },
  {
    title: "No server or Wasm",
    body: "Minnow is plain JavaScript, about 335 KB gzipped including durable storage. There is no database process, Wasm module, compile delay, or special hosting setup.",
  },
  {
    title: "Safe across workers and tabs",
    body: "Run queries off the UI thread. Snapshot reads stay consistent while other tabs write, and live queries refresh after relevant commits.",
  },
  {
    title: "Search without index setup",
    body: "Run full-text MATCH and BM25 queries on any column. Minnow builds and maintains the full-text index in the background when it helps.",
  },
];

export default function HomePage() {
  return (
    <main className="flex flex-1 flex-col">
      <section className="mx-auto grid w-full max-w-[112rem] gap-12 px-4 pt-12 pb-20 sm:px-6 lg:grid-cols-[minmax(18rem,0.7fr)_minmax(0,1.3fr)] lg:gap-14 lg:px-8 lg:pt-16 xl:gap-20 2xl:px-12">
        <header className="lg:pt-8">
          <p className="text-sm font-semibold tracking-wide text-fd-primary uppercase">
            Minnow database
          </p>
          <h1 className="mt-4 text-balance text-4xl leading-[1.05] font-semibold tracking-tight sm:text-5xl xl:text-6xl">
            A browser native SQL database
          </h1>
          <p className="mt-6 max-w-xl text-pretty text-lg leading-8 text-fd-muted-foreground">
            Durable, columnar application data with no server and no WebAssembly module. Query it
            directly with SQL or through Kysely.
          </p>

          <div className="mt-7 flex flex-wrap items-center gap-3">
            <Link
              href="/docs"
              className="rounded-lg bg-fd-primary px-4 py-2 font-medium text-fd-primary-foreground"
            >
              Read the docs
            </Link>
            <code className="rounded-lg border border-fd-border px-4 py-2 text-sm">
              npm install @minnowdb/core
            </code>
          </div>

          <nav aria-label="Explore Minnow" className="mt-10 border-y border-fd-border">
            <ul className="divide-y divide-fd-border">
              {HOME_LINKS.map((item) => (
                <li key={item.href} className="py-3.5 leading-7">
                  <Link
                    href={item.href}
                    className="font-semibold text-fd-primary underline decoration-fd-primary/35 underline-offset-4 hover:decoration-fd-primary"
                  >
                    {item.title}
                  </Link>
                  <span className="text-fd-muted-foreground"> — {item.body}</span>
                </li>
              ))}
            </ul>
          </nav>
        </header>

        <section id="console" className="min-w-0 scroll-mt-20" aria-labelledby="console-title">
          <div className="mb-4">
            <p className="text-sm font-medium text-fd-primary">The real thing</p>
            <h2 id="console-title" className="mt-1 text-2xl font-semibold tracking-tight">
              Query a live database
            </h2>
          </div>
          <Playground />
        </section>
      </section>

      <section className="border-t border-fd-border bg-fd-muted/45">
        <div className="mx-auto w-full max-w-6xl px-4 py-20 sm:px-6 lg:px-8">
          <div className="max-w-3xl">
            <p className="text-sm font-medium text-fd-primary">
              The whole database, in the browser
            </p>
            <h2 className="mt-3 text-balance text-3xl font-semibold tracking-tight sm:text-4xl">
              Familiar tools for local data that needs to last.
            </h2>
          </div>

          <div className="mt-10 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {FEATURES.map((feature) => (
              <div
                key={feature.title}
                className="rounded-xl border border-fd-border bg-fd-card p-5"
              >
                <h3 className="mb-1.5 font-semibold">{feature.title}</h3>
                <p className="text-sm leading-6 text-fd-muted-foreground">{feature.body}</p>
              </div>
            ))}
          </div>

          <div className="mt-8 rounded-xl border border-fd-border bg-fd-card p-6 text-left sm:p-8">
            <p className="mb-2 text-sm font-medium text-fd-primary">Clear compatibility</p>
            <h2 className="text-balance text-2xl font-semibold tracking-tight">
              Know which PostgreSQL features work
            </h2>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-fd-muted-foreground">
              Every documented SQL form has a runnable example and a PostgreSQL classification.
              Supported reads and writes are compared with PGlite, and every deliberate difference
              or exclusion is listed plainly.
            </p>
            <div className="mt-5 flex flex-wrap gap-3">
              <Link
                href="/docs/sql/feature-matrix"
                className="rounded-lg bg-fd-primary px-4 py-2 text-sm font-medium text-fd-primary-foreground"
              >
                See PostgreSQL compatibility
              </Link>
              <Link
                href="/docs/conformance"
                className="rounded-lg border border-fd-border px-4 py-2 text-sm font-medium hover:bg-fd-accent"
              >
                See how it is tested
              </Link>
            </div>
          </div>
        </div>
      </section>

      <section
        className="mx-auto w-full max-w-[112rem] px-4 py-20 sm:px-6 lg:px-8 2xl:px-12"
        aria-label="Minnow tank"
      >
        <div className="h-[220px] sm:h-[280px] md:h-[340px]">
          <HeroPond />
        </div>
      </section>
    </main>
  );
}
