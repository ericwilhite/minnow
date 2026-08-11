/**
 * Shared page shell: header navigation and footer, injected at startup so the HTML
 * entries don't each repeat the markup. The active link comes from `<body data-page>`.
 * Docs live on the public site (apps/site); this harness keeps only the lab pages.
 */

const pages = [
  ["index", "index.html", "Overview"],
  ["sql", "sql.html", "SQL support"],
  ["datasets", "datasets.html", "Datasets"],
  ["query", "query.html", "Query"],
  ["suites", "suites.html", "Benchmarks"],
] as const;

export function initShell(): void {
  const page = document.body.dataset.page ?? "";

  const header = document.createElement("header");
  header.className = "site-header";
  const links = pages
    .map(([id, href, label]) => {
      const active = page === id;
      return `<a href="${href}"${active ? ' aria-current="page"' : ""}>${label}</a>`;
    })
    .join("");
  header.innerHTML = `<nav><a class="brand" href="index.html">Minnow bench</a>${links}</nav>`;
  document.body.prepend(header);

  const footer = document.createElement("footer");
  footer.className = "site-footer";
  footer.innerHTML = `<div>Minnow bench — the internal benchmark and test harness. Local workspace build.</div>`;
  document.body.append(footer);
}
