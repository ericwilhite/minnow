/**
 * The machine-readable half of the docs site, written into `public/` before the site is built or
 * served — so the dev server hands out the same files as the deployment, and a link to one of
 * them can be followed on a local build.
 *
 * A language model reading these pages through HTML is reading a navigation tree, a search
 * dialog, and a syntax highlighter's markup around the sentences it wanted. Every page here is
 * therefore published twice: once as the page, and once as the markdown it was written in, at the
 * same path with `.md` on the end. Two indexes sit above them, following llmstxt.org:
 *
 *   /llms.txt        every page, titled and described, linked to its markdown
 *   /llms-full.txt   the whole documentation set in one file
 *
 * The custom components the pages use are expanded rather than dropped, because what they render
 * is content: a callout becomes a blockquote, and the SQL feature matrix becomes a table of all
 * 190 forms. An unrecognised component stops the build — a `.md` twin with raw JSX in it would be
 * a page that quietly lies about what the site says.
 *
 * Everything is written under the build's base path, so an archived version has its own
 * llms.txt describing its own release rather than pointing at the current one.
 */
import { copyFileSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const siteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const contentRoot = path.join(siteRoot, "content/docs");
const publicRoot = path.join(siteRoot, "public");
const matrixFile = path.join(siteRoot, "../../packages/core/sql-feature-matrix.json");
const corePackageFile = path.join(siteRoot, "../../packages/core/package.json");

const ORIGIN = "https://minnowdb.com";
const basePath = process.env.SITE_BASE_PATH ?? "";
const version = JSON.parse(readFileSync(corePackageFile, "utf8")).version;
const matrix = JSON.parse(readFileSync(matrixFile, "utf8"));

/**
 * A link to something else on this site, from the root, carrying the base path this build was
 * given. Site-relative rather than absolute so that a local build reads correctly: following a
 * link out of a file served from localhost should stay on localhost, not jump to production.
 */
function url(pathname) {
  return `${basePath}${pathname}`;
}

// --- reading the pages -------------------------------------------------------------------------

/** The frontmatter fields the site uses, and the body below them. */
function parsePage(raw) {
  const match = /^---\n([\s\S]*?)\n---\n?/.exec(raw);
  if (match === null) return { title: "", description: "", body: raw };
  const fields = {};
  for (const line of match[1].split("\n")) {
    const field = /^(\w+):\s*(.*)$/.exec(line);
    if (field !== null) fields[field[1]] = field[2].replace(/^["']|["']$/g, "");
  }
  return {
    title: fields.title ?? "",
    description: fields.description ?? "",
    body: raw.slice(match[0].length),
  };
}

/**
 * The pages in sidebar order, grouped the way meta.json groups them. Walking the meta files
 * rather than the directory keeps one order across the sidebar, llms.txt, and llms-full.txt.
 */
function collect(directory, slugPrefix) {
  const meta = JSON.parse(readFileSync(path.join(directory, "meta.json"), "utf8"));
  const pages = [];
  const groups = [];

  for (const entry of meta.pages ?? []) {
    if (entry.startsWith("---")) continue; // a separator in the sidebar, not a page
    const asFile = path.join(directory, `${entry}.mdx`);
    const asDirectory = path.join(directory, entry);

    if (readdirSync(directory).includes(`${entry}.mdx`)) {
      const slug = entry === "index" ? slugPrefix : [...slugPrefix, entry];
      const { title, description, body } = parsePage(readFileSync(asFile, "utf8"));
      pages.push({ slug, title, description, body });
    } else {
      const nested = collect(asDirectory, [...slugPrefix, entry]);
      groups.push({ title: nested.title, pages: nested.pages });
      groups.push(...nested.groups);
    }
  }

  return { title: meta.title ?? "Documentation", pages, groups };
}

// --- turning MDX into markdown -----------------------------------------------------------------

function featureTable(status) {
  const selected = matrix.features.filter((feature) => feature.status === status);
  const lines = [
    status === "supported"
      ? `${selected.length} forms, each executed on every test run and diffed against SQLite and PostgreSQL wherever the three agree on what the answer should be.`
      : `${selected.length} forms, each checked on every test run to still fail with the error below.`,
    "",
    `| Feature | SQL:2023 | Example | ${status === "supported" ? "Notes" : "Rejected with"} |`,
    "| --- | --- | --- | --- |",
  ];
  const cell = (value) => (value ?? "").replaceAll("|", "\\|");
  for (const feature of selected) {
    const last = status === "supported" ? feature.notes : feature.error;
    lines.push(
      `| \`${feature.id}\` | ${feature.feature === "minnow" ? "Minnow extension" : feature.feature} | \`${cell(feature.example)}\` | ${last === undefined ? "" : cell(last)} |`,
    );
  }
  lines.push("", `The same data as JSON: ${url("/sql-feature-matrix.json")}`);
  return lines.join("\n");
}

function calloutToBlockquote(_match, attributes, inner) {
  const title = /title="([^"]*)"/.exec(attributes ?? "")?.[1];
  const type = /type="([^"]*)"/.exec(attributes ?? "")?.[1];
  const heading = title ?? (type === "warn" ? "Warning" : "Note");
  const body = inner.trim().split("\n");
  return [`> **${heading}**`, ">", ...body.map((line) => `> ${line}`.trimEnd())].join("\n");
}

/** Site-relative links become absolute, and links into the docs point at the markdown twin. */
function absoluteLinks(markdown) {
  return markdown.replace(/\]\((\/[^)\s]*)\)/g, (_match, target) => {
    const [pathname, hash] = target.split("#");
    if (!pathname.startsWith("/docs"))
      return `](${url(pathname)}${hash === undefined ? "" : `#${hash}`})`;
    const trimmed = pathname.replace(/\/$/, "");
    const markdownPath = trimmed === "/docs" ? "/docs/index.md" : `${trimmed}.md`;
    return `](${url(markdownPath)}${hash === undefined ? "" : `#${hash}`})`;
  });
}

/** Fenced blocks, then double-backtick spans (which may contain a backtick), then plain spans. */
const CODE = /(```[\s\S]*?```|``[^\n]*?``|`[^`\n]*`)/g;

/**
 * Apply a transformation to prose only. Code is left exactly as written: a fenced TypeScript
 * block is full of things that look like components — `InferDatabase<DB>` for one — and rewriting
 * inside it would corrupt an example a reader is meant to paste.
 */
function outsideCode(markdown, transform) {
  return markdown
    .split(CODE)
    .map((segment, index) => (index % 2 === 1 ? segment : transform(segment)))
    .join("");
}

/** The same text with every code block and code span removed, for checks that must not see code. */
function proseOnly(markdown) {
  return markdown.replace(CODE, "");
}

function toMarkdown(page) {
  let body = page.body;
  body = body.replace(/<Callout([^>]*)>([\s\S]*?)<\/Callout>/g, calloutToBlockquote);
  body = body.replace(/<\/?Steps>\n?/g, "").replace(/<\/?Step>\n?/g, "");
  body = body.replace(/<SqlFeatureMatrix\s+status="(\w+)"\s*\/>/g, (_match, status) =>
    featureTable(status),
  );
  body = body.replace(
    /<Playground[^>]*\/>/g,
    `_An interactive console runs here on the site, in SQL and in TypeScript: ${url("/#console")}_`,
  );
  body = body.replace(
    /<DevtoolsDemo[^>]*\/>/g,
    `_A button here on the site mounts the floating panel over the page: ${url("/docs/devtools/")}_`,
  );
  body = outsideCode(body, absoluteLinks);

  const leftover = /<\/?[A-Z][A-Za-z]*/.exec(proseOnly(body));
  if (leftover !== null) {
    throw new Error(
      `${page.slug.join("/") || "index"}: no markdown for the component at "${leftover[0]}". ` +
        "Teach scripts/generate-llms.mjs how to render it before publishing the page.",
    );
  }

  return [
    `# ${page.title}`,
    "",
    page.description === "" ? null : `> ${page.description}`,
    page.description === "" ? null : "",
    body.trim(),
    "",
    "---",
    "",
    `Minnow ${version} · this page on the site: ${url(pagePath(page))}`,
    "",
  ]
    .filter((line) => line !== null)
    .join("\n");
}

// --- writing -----------------------------------------------------------------------------------

function pagePath(page) {
  return page.slug.length === 0 ? "/docs/" : `/docs/${page.slug.join("/")}/`;
}

function markdownPath(page) {
  return page.slug.length === 0 ? "/docs/index.md" : `/docs/${page.slug.join("/")}.md`;
}

function write(pathname, contents) {
  const file = path.join(publicRoot, pathname.replace(/^\//, ""));
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, contents);
}

const root = collect(contentRoot, []);
const sections = [{ title: root.title, pages: root.pages }, ...root.groups].filter(
  (section) => section.pages.length > 0,
);
const everyPage = sections.flatMap((section) => section.pages);

const rendered = new Map();
for (const page of everyPage) {
  const markdown = toMarkdown(page);
  rendered.set(page, markdown);
  write(markdownPath(page), markdown);
}

// The rules block on the agents page, published on its own so it can be fetched straight into a
// project's AGENTS.md. It lives in the page rather than beside it so that what the site shows and
// what an agent downloads cannot drift apart.
const agentsPage = everyPage.find((page) => page.slug.join("/") === "reference/agents");
if (agentsPage === undefined) {
  throw new Error("reference/agents is missing, and /agent-rules.md is generated from it.");
}
const rules = /```md\n([\s\S]*?)```/.exec(agentsPage.body);
if (rules === null) {
  throw new Error("reference/agents has no ```md block to publish as /agent-rules.md.");
}
write("/agent-rules.md", rules[1]);

const summary =
  "Minnow is a columnar SQL engine that runs entirely in the browser. It parses, plans, and " +
  "executes SQL itself over immutable columnar blocks stored in IndexedDB, with no server and no " +
  "WebAssembly to download.";

write(
  "/llms.txt",
  [
    "# Minnow",
    "",
    `> ${summary}`,
    "",
    `Published at ${ORIGIN}. Every link below is site-relative, so this file reads the same from ` +
      "a local build as from the live site.",
    "",
    `Engine version ${version}. \`@minnowdb/core\`, \`@minnowdb/client\`, and \`@minnowdb/devtools\` ` +
      "share a major version and move independently inside it; install them on the same major.",
    "",
    `Every page below is the markdown the site was written from. The whole set in one file is at ${url("/llms-full.txt")}, ` +
      `and the SQL surface as data is at ${url("/sql-feature-matrix.json")}.`,
    "",
    `Writing Minnow code? ${url("/agent-rules.md")} is a short rules file covering the API, the ` +
      "traps, and the SQL forms this engine rejects.",
    "",
    ...sections.flatMap((section) => [
      `## ${section.title}`,
      "",
      ...section.pages.map(
        (page) =>
          `- [${page.title}](${url(markdownPath(page))})${page.description === "" ? "" : `: ${page.description}`}`,
      ),
      "",
    ]),
    "## Optional",
    "",
    `- [Console](${url("/#console")}): a live database of ~590,000 rows, generated in the browser, that any SQL can be run against — or queried through the typed client, in a TypeScript editor with the published declarations loaded.`,
    `- [Benchmarks](${url("/benchmarks/")}): Minnow against SQLite Wasm and PGlite, run in the visitor's browser.`,
    `- [Source](https://github.com/ericwilhite/minnow): the repository, MIT licensed.`,
    "",
  ].join("\n"),
);

write(
  "/llms-full.txt",
  [
    "# Minnow",
    "",
    `> ${summary}`,
    "",
    `Complete documentation for Minnow ${version}, one page after another, in the order the site presents them.`,
    "",
    ...everyPage.flatMap((page) => ["---", "", rendered.get(page)]),
  ].join("\n"),
);

const staticPages = ["/", "/benchmarks/", ...everyPage.map(pagePath)];
write(
  "/sitemap.xml",
  [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...staticPages.map((pathname) => `  <url><loc>${ORIGIN}${url(pathname)}</loc></url>`),
    "</urlset>",
    "",
  ].join("\n"),
);

// Only the current release publishes a robots.txt: a crawler reads the one at the origin's root,
// and an archived build is not served from there. Archived pages carry `noindex` in their head.
if (basePath === "") {
  write(
    "/robots.txt",
    ["User-agent: *", "Allow: /", "", `Sitemap: ${ORIGIN}${url("/sitemap.xml")}`, ""].join("\n"),
  );
}

copyFileSync(matrixFile, path.join(publicRoot, "sql-feature-matrix.json"));

console.log(
  `llms.txt, llms-full.txt, agent-rules.md, sitemap.xml, and ${everyPage.length} markdown pages ` +
    `written for ${basePath === "" ? "the current release" : basePath}.`,
);
