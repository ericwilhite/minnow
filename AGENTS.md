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
- Benchmarks are not published numbers. `/benchmarks` runs the suites live in the visitor's
  browser from `apps/site/bench`, so there are no capture files to regenerate and no results to
  keep in step with the code.
- The playground is the runnable example. `apps/site/lib/dataset` generates a retailer's data in
  the browser and the docs query it; prose examples should use that schema so a reader can paste
  them straight into the console.
- The README is a feature list plus install and development pointers. It links to the docs site
  rather than repeating anything from it. Package READMEs are short summaries with the same rule.
