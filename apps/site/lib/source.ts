import { loader } from "fumadocs-core/source";
// fumadocs-mdx generates `.source` from `source.config.ts`; `server` is its server-side entry.
import { docs } from "@/.source/server";

export const source = loader({
  baseUrl: "/docs",
  source: docs.toFumadocsSource(),
});
