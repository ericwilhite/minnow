import type { ReactNode } from "react";
import type { Metadata } from "next";
import { RootProvider } from "fumadocs-ui/provider/next";
import SearchDialog from "@/components/search";
import "./global.css";

export const metadata: Metadata = {
  metadataBase: new URL("https://minnowdb.com"),
  title: {
    default: "Minnow — a SQL engine that runs in the browser",
    template: "%s · Minnow",
  },
  description:
    "A columnar SQL engine that runs entirely in the browser. Real SQL over immutable snapshots, durable on IndexedDB, with no WebAssembly to download.",
  icons: { icon: "/favicon.svg" },
};

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="flex min-h-screen flex-col">
        <RootProvider search={{ SearchDialog }}>{children}</RootProvider>
      </body>
    </html>
  );
}
