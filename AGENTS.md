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
  API reference, the SQL feature matrix page, and benchmark results (capture JSONs live in
  `apps/site/src/data/benchmarks/`). Do not create parallel docs or results directories.
- The README is a feature list plus install and development pointers. It links to the docs site
  rather than repeating anything from it. Package READMEs are short summaries with the same rule.
