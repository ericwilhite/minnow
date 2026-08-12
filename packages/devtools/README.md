# @minnowdb/devtools

An embeddable SQL console for a Minnow database. A floating panel during development, an inline
playground wherever you want one.

```bash
npm install @minnowdb/devtools
```

```ts
import { mountMinnowDevtools } from "@minnowdb/devtools";

if (import.meta.env.DEV) mountMinnowDevtools(db);
```

A launcher appears in the corner; click it or press `Cmd/Ctrl + Shift + D`.

- **Attaches to what you already have** — a `MinnowDatabase`, a `MinnowDatabaseClient`, or the
  `Minnow` facade over either.
- **Framework-agnostic** — also ships as `<minnow-devtools>`, a custom element with a shadow root,
  so it works unchanged in React, Vue, Svelte, Solid, Astro, and plain HTML.
- **A window, not a modal** — no backdrop, no focus trap; the page underneath stays interactive.
  Drag by the title bar, resize from the corner grip, and it reopens where you left it.
- **Changes are confirmed first** — the prompt names the table, the operation, and the statement.
  An `UPDATE` or `DELETE` with no `WHERE` is called out as hitting every row.
- **`permissions: { write: false }`** refuses statements that change data before they reach the
  database.

Keep the mount behind a development check. This is a separate package so it never reaches a
production bundle unless you put it there.

Full guide: [`apps/site/src/content/docs/devtools.mdx`](../../apps/site/src/content/docs/devtools.mdx).

## Demo

`packages/devtools/demo/` is a working page with an in-memory database. Serve the repository root
with vite and open `/packages/devtools/demo/`.
