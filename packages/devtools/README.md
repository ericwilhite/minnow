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

- **Browse your data** — tables and columns down the left, a windowed grid on the right with
  sortable headers and typed filters. Rows load as you scroll, by cursor rather than offset
  wherever the table allows it, so reading deep into a table stays as fast as reading the start.
- **Attaches to what you already have** — a `MinnowDatabase`, a `MinnowDatabaseClient`, or the
  `Minnow` facade over either.
- **Framework-agnostic** — also ships as `<minnow-devtools>`, a custom element with a shadow root,
  so it works unchanged in React, Vue, Svelte, Solid, Astro, and plain HTML.
- **A window, not a modal** — no backdrop, no focus trap; the page underneath stays interactive.
  Drag by the title bar, resize from the corner grip, and it reopens where you left it.
- **Edit records** — double-click a cell, select a row to delete it, or add one through a typed
  form. Values are validated against the column's type before anything is confirmed.
- **Query with completion** — a CodeMirror console whose completion comes from your own catalog,
  loaded only when the query tab is first opened. The last 50 runs are kept beside it.
- **Diagnostics as you type** — the engine's own compiler runs in the page, so bad SQL is
  underlined on the exact token without a round trip or a query, and failures that name an
  unsupported feature say what stands in for it. A **Plan** tab shows what the optimizer made of
  the statement.
- **Changes are confirmed first** — the prompt names the table, the key, and the before and after
  values. An `UPDATE` or `DELETE` with no `WHERE` is called out as hitting every row.
- **`permissions: { write: false }`** refuses statements that change data before they reach the
  database.

Keep the mount behind a development check. This is a separate package so it never reaches a
production bundle unless you put it there.

Full guide: [Devtools](https://minnowdb.dev/docs/devtools/).

## Demo

`packages/devtools/demo/` is a working page with an in-memory database. Serve the repository root
with vite and open `/packages/devtools/demo/`.
