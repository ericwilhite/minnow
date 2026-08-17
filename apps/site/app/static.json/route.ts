import { createFromSource } from "fumadocs-core/search/server";
import { source } from "@/lib/source";

/**
 * The search index, written once at build time. `output: 'export'` has no request-time server,
 * so the index has to be a file the browser fetches; `revalidate = false` is what makes Next
 * emit it as one.
 */
export const revalidate = false;

export const { staticGET: GET } = createFromSource(source);
