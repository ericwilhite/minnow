# @minnowdb/devtools

An embeddable SQL console and data browser for Minnow. Mount it as a floating development panel or
an inline playground.

```bash
npm install @minnowdb/devtools
```

```ts
import { mountMinnowDevtools } from "@minnowdb/devtools";

if (import.meta.env.DEV) mountMinnowDevtools(db);
```

The panel can attach to `MinnowDatabase` or `MinnowDatabaseClient`.

- Browse tables, columns, indexes, and rows.
- Filter and sort with typed controls and cursor-based paging where the table supports it.
- Insert, edit, and delete rows with confirmation before each write.
- Run SQL with catalog completion, inline diagnostics, history, and query plans.
- Inspect `RETURNING` rows and statement-specific results.
- Export and restore database snapshots with progress reporting.
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
