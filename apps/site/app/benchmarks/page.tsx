import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Benchmarks",
  description:
    "Run Minnow against SQLite WASM and PGlite in your own browser, on a dataset size you choose.",
};

export default function BenchmarksPage() {
  return (
    <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-6 px-4 py-10">
      <header>
        <h1 className="text-3xl font-semibold tracking-tight">Benchmarks</h1>
        <p className="mt-2 max-w-3xl text-fd-muted-foreground">
          Pick the engines, the suites, and the dataset size; everything runs here, in your browser,
          on your machine.
        </p>
      </header>
    </main>
  );
}
