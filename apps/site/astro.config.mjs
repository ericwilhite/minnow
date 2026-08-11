// @ts-check
import mdx from "@astrojs/mdx";
import { defineConfig } from "astro/config";

export default defineConfig({
  site: "https://minnowdb.dev",
  integrations: [mdx()],
  prefetch: { prefetchAll: true },
  markdown: {
    shikiConfig: {
      themes: { light: "github-light", dark: "github-dark" },
      defaultColor: false,
    },
  },
  server: {
    port: Number(process.env.PORT ?? 4321),
  },
  vite: {
    worker: { format: "es" },
  },
});
