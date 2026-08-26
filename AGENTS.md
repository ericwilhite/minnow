# AGENTS.md

## Principles

- Be clear and use simple language.
- Ask when uncertain.
- Prioritize correctness and speed/performance.
- Finish work completely. Leave no loose ends.
- Keep code, modules, and files clean and organized.

## Responses

- Format responses to be scanned: bullets, code snippets, short headers, bold labels.
- Lead with the outcome. Show real output instead of describing it.
- Avoid long paragraphs.

## Changes

- Test all changes before committing.
- Check for regressions.
- Do not commit broken or unverified work.

## Documentation

- Update public documentation for every relevant change.
- Write public documentation in clear, simple language. This includes the README.
- The docs site (`apps/site`) is the single source of truth for all user documentation: guides,
  API reference, and the SQL feature matrix page. Do not create parallel docs directories.
- Every published package version must appear in `apps/site/content/docs/changelog.mdx`, with its
  migration and stored-format impact. `npm run version:check` enforces the version entries.
- Benchmarks are not published numbers. `/benchmarks` runs the suites live in the visitor's
  browser from `apps/site/bench`, so there are no capture files to regenerate and no results to
  keep in step with the code.
- The console on the home page is the runnable example. `apps/site/lib/dataset` declares a
  retailer's schema with the core DSL and generates its data in the browser; the docs query it,
  so prose examples should use that schema and a reader can paste them straight in. There is no
  separate playground page — `/#console` is the link.
- The console has two tabs over one database. SQL is the shipped devtools panel; TypeScript is a
  Monaco editor running the real language service against the `.d.ts` files in `packages/*/dist`,
  collected by `apps/site/scripts/generate-playground-types.mjs`. Both are handed the same
  worker-hosted client, so a row written in one is visible in the other.
- The README is a feature list plus install and development pointers. It links to the docs site
  rather than repeating anything from it. Package READMEs are short summaries with the same rule.
- Documentation is part of the change, not a follow-up. A change that alters behaviour and leaves
  the docs describing the old behaviour is unfinished, however green the tests are.
- The machine-readable set — `/llms.txt`, `/llms-full.txt`, the `.md` twin of every page,
  `/agent-rules.md` — is generated from the MDX by `apps/site/scripts/generate-llms.mjs`. Update
  the page and those follow. Never edit the generated files.

## Claims go stale silently

Most of this documentation describes what Minnow _is_: what it requires, what it supports, what it
refuses. Prose is not tested, so when a change moves one of those lines, the sentence that said
otherwise stays perfectly convincing and wrong.

So when a change alters a capability or a requirement, find every sentence that claimed the old
one and update all of them — not the nearest page. Grep for the claim itself, across the docs, both
README layers, and the agents page:

```bash
rg -i "indexeddb" apps/site/content README.md packages/*/README.md
```

The places that make claims of this kind:

- **`apps/site/content/docs/reference/agents.mdx`** — its rules block is published as
  `/agent-rules.md` and pasted into other people's repositories, where it long outlives this
  release. It states the environment ("needs IndexedDB and `CompressionStream`", "no Node build"),
  the API shape, and the SQL forms to avoid.
- **`apps/site/content/docs/installation.mdx`** — the entry-point table and "Where it runs".
- **`apps/site/content/docs/storage/`** — what each block store costs and guarantees.
- **`README.md`** — the feature list, including sizes and the comparisons around them.
- **Package `README.md` files** — the one-paragraph summary of what each package is.

Adding an OPFS block store, for one example, is never one file: the agent rules say the engine
needs IndexedDB, installation says "Anything with IndexedDB and `CompressionStream`", the storage
guide lists the adapters, the README says blocks live on IndexedDB, and the rules tell a model to
reach for `MemoryBlockStore` in tests. Every one of them is then wrong, and no test says so.

What the suites do and do not cover:

- **Covered.** The SELECT guide's SQL runs against the retail dataset (`docs-sql.test.ts`), and
  so do the console's SQL chips (`queries.test.ts`). The console's TypeScript snippets are both
  typechecked against the published declarations and executed (`snippets.test.ts`) — that suite
  also proves the whole editor pipeline offline, so a broken `paths` map or a declaration the
  resolver cannot follow fails in Vitest rather than in a browser. The SQL feature matrix is a
  fixture the engine is tested against, so that page cannot drift. `generate-llms.mjs` fails the
  build on a component it cannot render as markdown, and `npm run version:check` fails on a
  version claim that no longer matches the manifests or changelog.
- **Not covered.** Every sentence of prose, every capability claim, every number in the README,
  and the rules block. Those are yours to keep true.
