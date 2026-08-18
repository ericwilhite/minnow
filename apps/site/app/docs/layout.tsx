import type { ReactNode } from "react";
import { DocsLayout } from "fumadocs-ui/layouts/notebook";
import { baseOptions } from "@/lib/layout.shared";
import { source } from "@/lib/source";
import { VersionPicker } from "@/components/version-picker";

export default function Layout({ children }: { children: ReactNode }) {
  return (
    <DocsLayout
      tree={source.getPageTree()}
      // Keyed because fumadocs renders the banner inside an array beside the sidebar's own
      // header, and React asks every child of one for a key.
      sidebar={{ banner: <VersionPicker key="version-picker" /> }}
      {...baseOptions()}
    >
      {children}
    </DocsLayout>
  );
}
