import { defineConfig, defineDocs } from "fumadocs-mdx/config";
import { pageSchema } from "fumadocs-core/source/schema";

export const docs = defineDocs({
  dir: "content/docs",
  docs: { schema: pageSchema },
});

export default defineConfig({
  mdxOptions: {
    rehypeCodeOptions: {
      themes: { light: "github-light", dark: "github-dark" },
    },
  },
});
