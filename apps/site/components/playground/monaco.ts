/**
 * The editor behind the TypeScript console, and the type checker behind that.
 *
 * The whole editor is imported but only one language is. The package's entry point,
 * `editor.main`, also registers about ninety languages and pulls in the CSS, HTML, and JSON
 * language services, each with a worker of its own — none of which a TypeScript box has any use
 * for. What is left is the editor itself plus TypeScript.
 *
 * The editor comes in through `features/register.all` **and** the list below it. That list is not
 * a matter of taste: `register.all` is missing eight modules that `editor.main` imports directly,
 * and one of them is the suggest widget — so with `register.all` alone the language service
 * answers completion requests that nothing ever draws. The comment on each says what it is for.
 * `apps/site/tests/site.spec.ts` opens the completion list and a hover for real, so a monaco
 * upgrade that moves any of these fails there rather than in front of a reader.
 *
 * All of it is loaded on demand — nothing here is fetched until a reader picks the TypeScript
 * tab. That is deliberate on a page whose first claim is that it downloads nothing to answer a
 * query: a type checker is a large thing, and a visitor who never opens the tab never pays for it.
 */
import * as monaco from "monaco-editor/editor/editor.api";
import "monaco-editor/features/register.all";
import "monaco-editor/editor/browser/coreCommands"; // cursor and typing commands
import "monaco-editor/editor/contrib/suggest/browser/suggestController"; // the completion list
import "monaco-editor/editor/contrib/find/browser/findController"; // ⌘F inside the editor
import "monaco-editor/editor/contrib/gotoSymbol/browser/goToCommands"; // go to definition
import "monaco-editor/editor/contrib/gotoError/browser/markerSelectionStatus";
import "monaco-editor/editor/contrib/caretOperations/browser/caretOperations";
import "monaco-editor/editor/contrib/dropOrPasteInto/browser/copyPasteContribution";
import "monaco-editor/editor/contrib/semanticTokens/browser/documentSemanticTokens";
import "monaco-editor/languages/definitions/typescript/register";
import {
  ModuleKind,
  ModuleResolutionKind,
  ScriptTarget,
  getTypeScriptWorker,
  typescriptDefaults,
  type CompilerOptions,
} from "monaco-editor/languages/features/typescript/register";
import { basePath } from "@/lib/versions";

export type Monaco = typeof monaco;

/**
 * Monaco's declared options predate `moduleDetection`, but the compiler behind them understands
 * it. Naming it here keeps the value typed rather than asserted past the interface.
 */
interface ConsoleCompilerOptions extends CompilerOptions {
  moduleDetection?: number;
}

/** What `generate-playground-types.mjs` wrote: the packages' declarations and how to find them. */
interface TypeBundle {
  paths: Record<string, string[]>;
  files: Record<string, string>;
}

/**
 * The palette, taken from the devtools panel so the two consoles are visibly one console.
 *
 * The completion list is spelled out rather than left to inherit. Overriding only its background
 * is what makes it unreadable: the base themes pair a saturated selected row with white text, so
 * a paler selected background inherits white-on-near-white. Selected background and selected
 * foreground have to move together, and the highlight colours — the matched characters of what
 * has been typed — with them.
 */
const THEMES = {
  light: {
    base: "vs" as const,
    colors: {
      "editor.background": "#ffffff",
      "editor.foreground": "#37352f",
      "editorLineNumber.foreground": "#b4b2ad",
      "editorLineNumber.activeForeground": "#37352f",
      "editor.lineHighlightBackground": "#f7f6f3",
      "editor.selectionBackground": "#c5dcf5",
      "editorIndentGuide.background1": "#eceae5",
      "editorWidget.background": "#ffffff",
      "editorWidget.foreground": "#37352f",
      "editorWidget.border": "#e3e1dc",
      "editorSuggestWidget.background": "#ffffff",
      "editorSuggestWidget.border": "#e3e1dc",
      "editorSuggestWidget.foreground": "#37352f",
      "editorSuggestWidget.selectedBackground": "#e8eef7",
      "editorSuggestWidget.selectedForeground": "#37352f",
      "editorSuggestWidget.selectedIconForeground": "#1667c0",
      "editorSuggestWidget.highlightForeground": "#1667c0",
      "editorSuggestWidget.focusHighlightForeground": "#1667c0",
      "editorHoverWidget.background": "#ffffff",
      "editorHoverWidget.foreground": "#37352f",
      "editorHoverWidget.border": "#e3e1dc",
      "editorHoverWidget.statusBarBackground": "#f7f6f3",
      "editorError.foreground": "#c4433f",
      "editorWarning.foreground": "#8a5511",
    },
    tokens: { keyword: "1667c0", string: "1c7c54", number: "8a5511", comment: "8f8d87" },
  },
  dark: {
    base: "vs-dark" as const,
    colors: {
      "editor.background": "#1e1e1e",
      "editor.foreground": "#d4d4d3",
      "editorLineNumber.foreground": "#5a5a59",
      "editorLineNumber.activeForeground": "#d4d4d3",
      "editor.lineHighlightBackground": "#262626",
      "editor.selectionBackground": "#2f4d63",
      "editorIndentGuide.background1": "#2d2d2d",
      "editorWidget.background": "#202020",
      "editorWidget.foreground": "#d4d4d3",
      "editorWidget.border": "#333333",
      "editorSuggestWidget.background": "#202020",
      "editorSuggestWidget.border": "#333333",
      "editorSuggestWidget.foreground": "#d4d4d3",
      "editorSuggestWidget.selectedBackground": "#31424d",
      "editorSuggestWidget.selectedForeground": "#ffffff",
      "editorSuggestWidget.selectedIconForeground": "#8cc2e6",
      "editorSuggestWidget.highlightForeground": "#8cc2e6",
      "editorSuggestWidget.focusHighlightForeground": "#a8d4f2",
      "editorHoverWidget.background": "#202020",
      "editorHoverWidget.foreground": "#d4d4d3",
      "editorHoverWidget.border": "#333333",
      "editorHoverWidget.statusBarBackground": "#262626",
      "editorError.foreground": "#ff7369",
      "editorWarning.foreground": "#d9a33f",
    },
    tokens: { keyword: "529cca", string: "4dab7f", number: "d9a33f", comment: "8b8b8a" },
  },
};

/** The editor is a singleton: its workers and its type checker are shared by every model. */
let started: Promise<Monaco> | undefined;

export function loadMonaco(declarations: string): Promise<Monaco> {
  started ??= start(declarations);
  return started;
}

async function start(declarations: string): Promise<Monaco> {
  // Monaco finds its workers through a global rather than an import, so this has to be set before
  // the first model is created. Turbopack rewrites both URLs at build time.
  (globalThis as { MonacoEnvironment?: monaco.Environment }).MonacoEnvironment = {
    getWorker(_id, label) {
      return label === "typescript" || label === "javascript"
        ? new Worker(new URL("monaco-editor/language/typescript/ts.worker.js", import.meta.url), {
            type: "module",
          })
        : new Worker(new URL("monaco-editor/editor/editor.worker.js", import.meta.url), {
            type: "module",
          });
    },
  };

  for (const [name, theme] of Object.entries(THEMES)) {
    monaco.editor.defineTheme(`minnow-${name}`, {
      base: theme.base,
      inherit: true,
      colors: theme.colors,
      rules: [
        { token: "keyword", foreground: theme.tokens.keyword },
        { token: "keyword.flow", foreground: theme.tokens.keyword },
        { token: "string", foreground: theme.tokens.string },
        { token: "number", foreground: theme.tokens.number },
        { token: "comment", foreground: theme.tokens.comment, fontStyle: "italic" },
      ],
    });
  }

  const response = await fetch(`${basePath}/playground-types.json`);
  if (!response.ok)
    throw new Error(`The type declarations did not load (${String(response.status)})`);
  const bundle = (await response.json()) as TypeBundle;

  const options: ConsoleCompilerOptions = {
    // Monaco's declared enum stops at ES2020; the compiler behind it does not. `ESNext` asks
    // for the newest it has, which is what a snippet with a top-level `await` needs.
    target: ScriptTarget.ESNext,
    module: ModuleKind.ESNext,
    // Only Classic and Node are on offer here, which is why the generator strips the `.js` from
    // every relative specifier in the declarations it collects.
    moduleResolution: ModuleResolutionKind.NodeJs,
    // A snippet awaits at the top level without importing anything. Forcing module detection is
    // what lets it, and it leaves the ambient declarations below global because they are `.d.ts`.
    moduleDetection: 3,
    // Full file names, not the short forms `tsconfig.json` takes: the worker resolves these
    // against the set of lib files it has bundled, and an unknown name silently contributes
    // nothing — which reads as `console` not existing.
    lib: ["lib.es2022.d.ts", "lib.dom.d.ts", "lib.dom.iterable.d.ts"],
    strict: true,
    exactOptionalPropertyTypes: true,
    allowNonTsExtensions: true,
    baseUrl: "file:///",
    paths: bundle.paths,
  };
  typescriptDefaults.setCompilerOptions(options);

  typescriptDefaults.setDiagnosticsOptions({
    noSemanticValidation: false,
    noSyntaxValidation: false,
  });
  // The console compiles the model the reader is looking at, so the worker has to be holding it.
  typescriptDefaults.setEagerModelSync(true);
  typescriptDefaults.setExtraLibs([
    ...Object.entries(bundle.files).map(([filePath, content]) => ({ content, filePath })),
    { content: declarations, filePath: "file:///playground.d.ts" },
  ]);

  return monaco;
}

/**
 * Compiles what is in the editor. Diagnostics come back first: a snippet that does not typecheck
 * is not run, because running it would say the type checking was decorative.
 */
export async function compile(
  model: monaco.editor.ITextModel,
): Promise<{ javascript: string } | { errors: string[] }> {
  const worker = await (await getTypeScriptWorker())(model.uri);
  const uri = model.uri.toString();
  const [semantic, syntactic] = await Promise.all([
    worker.getSemanticDiagnostics(uri),
    worker.getSyntacticDiagnostics(uri),
  ]);

  const errors = [...syntactic, ...semantic].map((diagnostic) => {
    const at = model.getPositionAt(diagnostic.start ?? 0);
    return `Line ${String(at.lineNumber)}: ${flatten(diagnostic.messageText)}`;
  });
  if (errors.length > 0) return { errors };

  const emitted = await worker.getEmitOutput(uri);
  const javascript = emitted.outputFiles.find((file: { name: string }) =>
    file.name.endsWith(".js"),
  )?.text;
  return javascript === undefined
    ? { errors: ["The snippet produced nothing to run."] }
    : { javascript };
}

function flatten(message: string | { messageText: string; next?: unknown[] } | undefined): string {
  if (message === undefined) return "";
  return typeof message === "string" ? message : message.messageText;
}
