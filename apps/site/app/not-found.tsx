import type { Metadata } from "next";
import Link from "next/link";
import { HomeLayout } from "fumadocs-ui/layouts/home";
import { MissingQuery } from "@/components/missing-query";
import { HeroPond } from "@/components/pond/pond";
import { baseOptions } from "@/lib/layout.shared";

export const metadata: Metadata = {
  title: "Nothing swimming here",
  robots: { index: false, follow: true },
};

const ELSEWHERE = [
  { href: "/docs", label: "Read the docs" },
  { href: "/#console", label: "Open the console" },
  { href: "/benchmarks", label: "Run the benchmarks" },
  { href: "/", label: "Back to the surface" },
];

/**
 * The page for a path that does not exist. It carries the nav, so nobody is stranded, and the
 * pond, because a page that got away is the one thing this site already has a picture of — the
 * minnow swims away from whoever reaches for it.
 */
export default function NotFound() {
  return (
    <HomeLayout {...baseOptions()}>
      <main className="mx-auto flex w-full max-w-3xl flex-1 flex-col px-4 pt-10 pb-20 text-center">
        <div className="mb-8 h-[190px] sm:h-[240px]">
          <HeroPond />
        </div>
        <p className="text-sm font-medium tracking-[0.2em] text-fd-muted-foreground">404</p>
        <h1 className="mt-2 text-balance text-4xl font-semibold tracking-tight sm:text-5xl">
          This one got away
        </h1>
        <p className="mx-auto mt-4 max-w-xl text-balance text-lg text-fd-muted-foreground">
          There is no page at that path. The minnow above has not seen it either, and it is not
          telling — though you are welcome to try catching it.
        </p>
        <MissingQuery />
        <div className="mt-8 flex flex-wrap items-center justify-center gap-3">
          {ELSEWHERE.map((link, index) => (
            <Link
              key={link.href}
              href={link.href}
              className={
                index === 0
                  ? "rounded-lg bg-fd-primary px-4 py-2 font-medium text-fd-primary-foreground"
                  : "rounded-lg border border-fd-border px-4 py-2 font-medium hover:bg-fd-accent"
              }
            >
              {link.label}
            </Link>
          ))}
        </div>
      </main>
    </HomeLayout>
  );
}
