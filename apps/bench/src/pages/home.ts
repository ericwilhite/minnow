import "../style.css";
import { showcaseExamples } from "../content/examples.js";
import { escapeHtml, required } from "../ui/format.js";
import { highlight, highlightAll } from "../ui/highlight.js";
import { initShell } from "../ui/shell.js";

initShell();

const showcase = required<HTMLElement>("#showcase");
showcase.innerHTML = showcaseExamples
  .map(
    (example) => `<article class="showcase" id="example-${example.id}">
      <h3>${escapeHtml(example.title)}</h3>
      <pre><code data-lang="ts">${escapeHtml(example.code)}</code></pre>
      <div class="actions">
        <button data-run="${example.id}">Run</button>
      </div>
      <div data-output="${example.id}"></div>
    </article>`,
  )
  .join("");
void highlightAll(showcase);

for (const button of Array.from(showcase.querySelectorAll<HTMLButtonElement>("[data-run]"))) {
  button.addEventListener("click", () => {
    const example = showcaseExamples.find((candidate) => candidate.id === button.dataset.run);
    const output = showcase.querySelector<HTMLElement>(
      `[data-output="${button.dataset.run ?? ""}"]`,
    );
    if (example === undefined || output === null) return;
    button.disabled = true;
    output.innerHTML = `<p class="note">Running…</p>`;
    example
      .run()
      .then(async (result) => {
        output.innerHTML = await highlight(result, "json");
      })
      .catch((error: unknown) => {
        output.innerHTML = `<p class="status fail">${escapeHtml(
          error instanceof Error ? error.message : String(error),
        )}</p>`;
      })
      .finally(() => {
        button.disabled = false;
      });
  });
}
