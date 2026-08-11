/**
 * Shared page shell: header navigation and footer, injected at startup so twelve HTML
 * entries don't each repeat the markup. The active link comes from `<body data-page>`;
 * docs pages use `data-page="docs/<name>"` and get `../`-prefixed links plus a docs
 * sub-navigation.
 */

const pages = [
  ["index", "index.html", "Overview"],
  ["docs", "docs/getting-started.html", "Docs"],
  ["sql", "sql.html", "SQL support"],
  ["datasets", "datasets.html", "Datasets"],
  ["query", "query.html", "Query"],
  ["suites", "suites.html", "Benchmarks"],
] as const;

export const docPages = [
  ["getting-started", "Getting started"],
  ["worker-client", "Run it in a worker"],
  ["write-api", "Write API"],
  ["schema-migrations", "Schema & migrations"],
  ["orm", "Query builder"],
  ["live-queries", "Live queries"],
  ["architecture", "Architecture"],
] as const;

export function initShell(): void {
  const page = document.body.dataset.page ?? "";
  const inDocs = page.startsWith("docs/");
  const prefix = inDocs ? "../" : "";

  const header = document.createElement("header");
  header.className = "site-header";
  const links = pages
    .map(([id, href, label]) => {
      const active = id === "docs" ? inDocs : page === id;
      return `<a href="${prefix}${href}"${active ? ' aria-current="page"' : ""}>${label}</a>`;
    })
    .join("");
  header.innerHTML = `<nav><a class="brand" href="${prefix}index.html">MinnowDatabase</a>${links}</nav>`;
  document.body.prepend(header);

  if (inDocs) {
    const docId = page.slice("docs/".length);
    const docsNav = document.createElement("nav");
    docsNav.className = "docs-nav";
    docsNav.innerHTML = docPages
      .map(
        ([id, label]) =>
          `<a href="${id}.html"${id === docId ? ' aria-current="page"' : ""}>${label}</a>`,
      )
      .join("");
    document.querySelector("main")?.prepend(docsNav);
  }

  const footer = document.createElement("footer");
  footer.className = "site-footer";
  footer.innerHTML = `<div>MinnowDatabase — a persistent columnar SQL database for the browser. Local workspace build.</div>`;
  document.body.append(footer);
}
