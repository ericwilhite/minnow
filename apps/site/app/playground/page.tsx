import type { Metadata } from "next";
import { Playground } from "@/components/playground/playground";

export const metadata: Metadata = {
  title: "Playground",
  description:
    "A real Minnow database, generated and stored in your browser. Write SQL against a retailer's point-of-sale data.",
};

export default function PlaygroundPage() {
  return (
    <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-6 px-4 py-10">
      <header>
        <h1 className="text-3xl font-semibold tracking-tight">Playground</h1>
        <p className="mt-2 max-w-3xl text-fd-muted-foreground">
          A specialty coffee retailer&rsquo;s point-of-sale data: stores, staff, a product
          catalogue, customers, orders, order lines, and returns. It is generated in this browser
          and stored in IndexedDB, so it survives a reload and nothing is sent anywhere.
        </p>
      </header>
      <Playground height={720} />
    </main>
  );
}
