import type { ReactNode } from "react";
import type { Metadata } from "next";
import { Providers } from "@/components/providers";
import { isArchived } from "@/lib/versions";
import "./global.css";

export const metadata: Metadata = {
  metadataBase: new URL("https://minnowdb.com"),
  title: {
    default: "Minnow — A browser native SQL database",
    template: "%s · Minnow",
  },
  description:
    "A columnar SQL database for the browser. Run PostgreSQL-style SQL over durable IndexedDB or OPFS data with no server or WebAssembly module.",
  icons: { icon: "/favicon.svg" },
  // An archived release documents a version that is no longer the one npm installs. It stays
  // reachable and linkable, but it should never be the result a search engine offers first.
  ...(isArchived ? { robots: { index: false, follow: true } } : {}),
};

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="flex min-h-screen flex-col">
        <Providers>{children}</Providers>
      </body>
    </html>
  );
}
