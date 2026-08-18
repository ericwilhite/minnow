"use client";
/**
 * Search, computed in the browser.
 *
 * The site is a static export with no server to query, so the build writes the whole index to
 * `/static.json` and this dialog searches it locally. Nothing about a visitor's search leaves
 * their machine.
 *
 * The index has to be named: the client otherwise looks for `/api/search`, a route a static
 * export never emits. It is named through the base path too, so an archived version searches its
 * own documentation rather than the current release's.
 */
import {
  SearchDialog,
  SearchDialogClose,
  SearchDialogContent,
  SearchDialogHeader,
  SearchDialogIcon,
  SearchDialogInput,
  SearchDialogList,
  SearchDialogOverlay,
  type SharedProps,
} from "fumadocs-ui/components/dialog/search";
import { useDocsSearch } from "fumadocs-core/search/client";
import { staticClient } from "fumadocs-core/search/client/orama-static";
import { basePath } from "@/lib/versions";

export default function DefaultSearchDialog(props: SharedProps) {
  const { search, setSearch, query } = useDocsSearch({
    client: staticClient({ from: `${basePath}/static.json` }),
  });

  return (
    <SearchDialog search={search} onSearchChange={setSearch} isLoading={query.isLoading} {...props}>
      <SearchDialogOverlay />
      <SearchDialogContent>
        <SearchDialogHeader>
          <SearchDialogIcon />
          <SearchDialogInput />
          <SearchDialogClose />
        </SearchDialogHeader>
        <SearchDialogList items={query.data !== "empty" ? query.data : null} />
      </SearchDialogContent>
    </SearchDialog>
  );
}
