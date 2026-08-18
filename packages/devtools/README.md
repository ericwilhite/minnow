# @minnowdb/devtools

An embeddable SQL console and data browser for a [Minnow](https://minnowdb.com) database. A
floating panel during development, an inline playground wherever you want one.

**[minnowdb.com](https://minnowdb.com)** — documentation, a live playground, and benchmarks you
run yourself. The [devtools guide](https://minnowdb.com/docs/devtools/) opens this panel over the
page, so you can try it before installing anything.

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
- **Embeds anywhere** — `mode: "inline"` puts the panel in your layout, where it fills its
  container. `theme` follows your page's light/dark switch, and the whole palette is `--mdt-*`
  custom properties you can override from outside the shadow root.
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
- **Download the database** — the whole thing as one snapshot file, and a matching restore that
  describes the file it picked before loading it. Progress is reported throughout, and the bytes
  leave a worker in slices so the page keeps painting.
- **`permissions: { write: false }`** refuses statements that change data before they reach the
  database.

- **Inline or floating** — `mode: "inline"` renders the panel in your layout instead of over the
  page, where it sits in the page's own stacking order rather than above it.

Keep the mount behind a development check. This is a separate package so it never reaches a
production bundle unless you put it there.

Full guide: [Devtools](https://minnowdb.com/docs/devtools/).

Every `@minnowdb` package shares a major version and moves independently inside it, so install
this on the same major as the engine. See
[Versioning](https://minnowdb.com/docs/reference/versioning/).

## Demo

`packages/devtools/demo/` is a working page with an in-memory database. Serve the repository root
with vite and open `/packages/devtools/demo/`.

## License

MIT
