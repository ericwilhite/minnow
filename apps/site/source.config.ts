import { defineConfig, defineDocs } from "fumadocs-mdx/config";
import { pageSchema } from "fumadocs-core/source/schema";

export const docs = defineDocs({
  dir: "content/docs",
  docs: {
    schema: pageSchema,
    // Rename drafts only at enablement: they must not become routes or search entries.
    files: ["**/!(*.draft).{md,mdx}"],
  },
});

export default defineConfig({
  mdxOptions: {
    rehypeCodeOptions: {
      themes: { light: "github-light", dark: "github-dark" },
    },
  },
});
