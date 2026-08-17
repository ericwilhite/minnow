import type { Metadata } from "next";
import { BenchRunner } from "@/components/bench/runner";

export const metadata: Metadata = {
  title: "Benchmarks",
  description:
    "Run Minnow against SQLite WASM and PGlite in your own browser, on a dataset size you choose.",
};

export default function BenchmarksPage() {
  return (
    <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-8 px-4 py-10">
      <header className="max-w-3xl">
        <h1 className="text-3xl font-semibold tracking-tight">Benchmarks</h1>
        <p className="mt-3 text-fd-muted-foreground">
          There are no published numbers on this page. Pick the engines, the suites, and the dataset
          size, and all three databases are built and measured here, in your browser, on your
          machine — which is the only place a browser database&rsquo;s performance means anything.
        </p>
        <p className="mt-3 text-fd-muted-foreground">
          Reads and writes stay split by OLTP and OLAP throughout, because a blended score hides the
          trade-off: Minnow leads on scans and bulk loads, SQLite leads on single-key lookups and
          small writes. Every result is checked against an independent oracle before its timing
          counts, and an engine that got the wrong answer reports no number at all.
        </p>
      </header>

      <BenchRunner />

      <section className="max-w-3xl text-sm text-fd-muted-foreground">
        <h2 className="mb-2 text-base font-semibold text-fd-foreground">How it works</h2>
        <p className="mb-2">
          The dataset is a deterministic 50-table commerce schema generated from a closed-form
          function of the row index, which is what makes the oracles possible: every expected answer
          is recomputed in JavaScript from the same inputs the engines were given, rather than read
          back out of one of them.
        </p>
        <p className="mb-2">
          Every engine runs its shipped defaults — no pragmas, no tuning. Each database persists to
          the storage its own documentation recommends, named per engine above and reported exactly
          as the engine installed it once a run has finished. Only the engine&rsquo;s own call is
          timed; reshaping rows into the form each API wants is the harness&rsquo;s cost and is
          excluded.
        </p>
        <p className="mb-2">
          Timings are taken by the batch. The browser&rsquo;s clock ticks every 5µs on this page and
          every 100µs on an origin that is not cross-origin isolated, which is coarser than most of
          what is measured here — so anything quicker than the clock is executed many times inside
          one timed window and divided back down. A lookup that costs 150µs is reported as 150µs
          rather than rounded to the nearest tick, which is what made every fast case look
          identical.
        </p>
        <p>
          Running a suite writes real data to your browser&rsquo;s storage for this origin. Use one
          tab at a time — PGlite allows only one open instance per data directory.
        </p>
      </section>
    </main>
  );
}
