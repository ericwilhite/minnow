# @minnowdb/devtools

An embeddable SQL console and database explorer for Minnow. Mount it as a floating development
panel or an inline playground.

```bash
npm install @minnowdb/devtools
```

```ts
import { mountMinnowDevtools } from "@minnowdb/devtools";

if (import.meta.env.DEV) mountMinnowDevtools(db);
```

The panel can attach to `MinnowDatabase` or `MinnowDatabaseClient`.

- Browse tables and views: columns with their declared SQL types, keys, indexes, foreign keys,
  checks, and triggers.
- Filter and sort with typed controls, cursor paging, and a live mode that reloads on every
  commit.
- Follow a foreign key from a cell to its parent row, or from a row to the rows that reference
  it, and read any row in full beside the grid.
- Insert, edit, duplicate, and delete rows, with enum and boolean menus, and a confirmation that
  spells out every change first.
- Run SQL with catalog completion, inline diagnostics, scripts, run-the-selection, a row cap,
  cancel, and query plans.
- Copy or download results as CSV, JSON, or INSERT statements; save queries beside the history.
- Read storage and maintenance statistics, and export or restore database snapshots.
- Use `permissions: { write: false }` for a read-only panel.
- Mount in React, Vue, Svelte, Solid, Astro, or plain HTML through the
  `<minnow-devtools>` custom element.

Keep the floating mount behind a development check so the package stays out of production builds.
Use `mode: "inline"` when the panel is part of the application or documentation.

Full guide and live demo: [minnowdb.com/docs/devtools](https://minnowdb.com/docs/devtools/).

## Demo

`packages/devtools/demo/` contains a small page backed by an in-memory database.

## License

MIT
