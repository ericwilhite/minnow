/**
 * Syntax highlighting via shiki's fine-grained core: the JavaScript regex engine (no
 * oniguruma wasm, which matters under the site's COEP headers) and only the grammars the
 * site actually renders — sql, typescript, json.
 */
import { createHighlighterCore, type HighlighterCore } from "@shikijs/core";
import { createJavaScriptRegexEngine } from "@shikijs/engine-javascript";
import langJson from "@shikijs/langs/json";
import langSql from "@shikijs/langs/sql";
import langTs from "@shikijs/langs/typescript";
import themeGithubLight from "@shikijs/themes/github-light";

export type HighlightLang = "sql" | "ts" | "json";

let highlighterPromise: Promise<HighlighterCore> | undefined;

function highlighter(): Promise<HighlighterCore> {
  highlighterPromise ??= createHighlighterCore({
    themes: [themeGithubLight],
    langs: [langSql, langTs, langJson],
    engine: createJavaScriptRegexEngine(),
  });
  return highlighterPromise;
}

/** Returns a complete `<pre class="shiki">…</pre>` block for dynamic rendering. */
export async function highlight(code: string, lang: HighlightLang): Promise<string> {
  const core = await highlighter();
  return core.codeToHtml(code, {
    lang: lang === "ts" ? "typescript" : lang,
    theme: "github-light",
  });
}

/**
 * Replaces every `pre > code[data-lang]` under `root` with highlighted markup. Authored
 * blocks stay readable as plain escaped text until this resolves.
 */
export async function highlightAll(root: ParentNode = document): Promise<void> {
  const blocks = Array.from(root.querySelectorAll<HTMLElement>("pre > code[data-lang]"));
  for (const block of blocks) {
    const lang = block.dataset.lang;
    if (lang !== "sql" && lang !== "ts" && lang !== "json") continue;
    const pre = block.parentElement;
    if (pre === null) continue;
    const html = await highlight(block.textContent, lang);
    const template = document.createElement("template");
    template.innerHTML = html;
    const replacement = template.content.firstElementChild;
    if (replacement !== null) pre.replaceWith(replacement);
  }
}
