"use client";
/**
 * Kysely, over the database this page already built.
 *
 * The SQL console beside it proves the engine answers queries. This one proves the other half of
 * the claim: that the query you write is checked against the schema before it runs. So the editor
 * is not a highlighted textarea — it is the real TypeScript language service, holding the same
 * `.d.ts` files npm publishes, and the Run button refuses a snippet that does not typecheck.
 *
 * It shares the worker and the database with the SQL console rather than opening its own. That is
 * not only cheaper: a row inserted here is a row the SQL tab can then select, which is the point
 * of the two being one console.
 */
import { useCallback, useEffect, useRef, useState } from "react";
import type { editor } from "monaco-editor/editor/editor.api";
import type { MinnowDatabaseClient } from "@minnowdb/core/client";
import { retailSchema } from "@/lib/dataset/retail";
import { SETUP_SNIPPET, playgroundDeclarations } from "./declarations";
import { compile, loadMonaco, type Monaco } from "./monaco";
import { Output } from "./output";
import { runSnippet, unsupportedRuntimeImports, type RunResult } from "./run";
import { PLAYGROUND_RUNTIME_MODULES, type PlaygroundRuntimeModule } from "./runtime-modules";
import { defaultSnippet, snippets } from "./snippets";

const STORAGE_KEY = "minnow-playground-typescript";

type Status =
  | { kind: "loading" }
  | { kind: "ready" }
  | { kind: "failed"; message: string }
  /** A snippet the compiler refused. Nothing ran, so there is nothing to show below. */
  | { kind: "rejected"; errors: string[] };

export function TypeScriptConsole({
  client,
  height,
  theme,
}: {
  client: MinnowDatabaseClient;
  height: number;
  theme: "light" | "dark";
}) {
  const mount = useRef<HTMLDivElement>(null);
  const editorRef = useRef<editor.IStandaloneCodeEditor | undefined>(undefined);
  const api = useRef<Monaco | undefined>(undefined);
  const [status, setStatus] = useState<Status>({ kind: "loading" });
  const [result, setResult] = useState<RunResult>();
  const [running, setRunning] = useState(false);

  // The client is read through a ref so a snippet always runs against the live database, without
  // the editor being torn down and rebuilt whenever the host hands over a new one.
  const database = useRef(client);
  useEffect(() => {
    database.current = client;
  }, [client]);

  const run = useCallback(async (): Promise<void> => {
    const current = editorRef.current;
    const monaco = api.current;
    if (current === undefined || monaco === undefined) return;
    const model = current.getModel();
    if (model === null) return;

    setRunning(true);
    setResult(undefined);
    try {
      const compiled = await compile(model);
      if ("errors" in compiled) {
        setStatus({ kind: "rejected", errors: compiled.errors });
        return;
      }
      setStatus({ kind: "ready" });

      const unsupported = unsupportedRuntimeImports(
        compiled.javascript,
        PLAYGROUND_RUNTIME_MODULES,
      );
      if (unsupported.length > 0) {
        setStatus({
          kind: "rejected",
          errors: unsupported.map(
            (specifier) =>
              `The console can type-check declarations from ${specifier}, but cannot execute that module.`,
          ),
        });
        return;
      }

      const [{ createKysely, search }, kysely, core, coreClient] = await Promise.all([
        import("@minnowdb/kysely"),
        import("kysely"),
        import("@minnowdb/core"),
        import("@minnowdb/core/client"),
      ]);
      const db = createKysely({
        driver: database.current,
        schema: (await import("@/lib/dataset/retail")).retailDefinition,
      });
      setResult(
        await runSnippet(compiled.javascript, {
          modules: {
            "@minnowdb/kysely": { createKysely, search },
            kysely: { sql: kysely.sql },
            "@minnowdb/core": core,
            "@minnowdb/core/client": coreClient,
          } satisfies Record<PlaygroundRuntimeModule, unknown>,
          globals: { db, database: database.current },
        }),
      );
    } finally {
      setRunning(false);
    }
  }, []);

  // `run` is read through a ref by the editor's keybinding, which is registered once: rebinding
  // it on every render would leave the editor holding a closure over a stale database.
  const latestRun = useRef(run);
  useEffect(() => {
    latestRun.current = run;
  }, [run]);

  useEffect(() => {
    let cancelled = false;
    const stopped = (): boolean => cancelled;

    async function start(): Promise<void> {
      const monaco = await loadMonaco(playgroundDeclarations(retailSchema));
      if (stopped()) return;
      const container = mount.current;
      if (container === null) return;

      const stored = window.localStorage.getItem(STORAGE_KEY);
      const model =
        monaco.editor.getModel(monaco.Uri.parse("file:///snippet.ts")) ??
        monaco.editor.createModel(
          stored ?? defaultSnippet,
          "typescript",
          monaco.Uri.parse("file:///snippet.ts"),
        );

      const instance = monaco.editor.create(container, {
        model,
        theme: `minnow-${theme}`,
        automaticLayout: true,
        fontSize: 13,
        fontFamily: 'ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, monospace',
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        padding: { top: 10, bottom: 10 },
        renderLineHighlight: "line",
        tabSize: 2,
        smoothScrolling: true,
        scrollbar: { alwaysConsumeMouseWheel: false },
        /*
         * Suggestions offer themselves rather than waiting to be asked for. `strings` is the one
         * that matters and it is off by default: in this API every table and every column is
         * written as a string literal, so with the default a reader gets no help at exactly the
         * moment they are wondering what the tables are called. Someone who already knows the
         * shortcut loses nothing; someone meeting Minnow for the first time is why this is here.
         */
        quickSuggestions: { other: true, comments: false, strings: true },
        quickSuggestionsDelay: 0,
        suggestOnTriggerCharacters: true,
        // The identifiers already in the buffer are noise beside a real completion list, and they
        // are what shows up in a string when the language service has nothing to say.
        wordBasedSuggestions: "off",
        suggest: { showWords: false, preview: true },
        // The completion list and the hover are taller than the space above the line that
        // summoned them, and this console is a short box with `overflow: hidden` and rounded
        // corners. Left inside it they are cut off at the top. Fixed widgets are positioned
        // against the viewport from a container on the body instead, so they escape the box.
        fixedOverflowWidgets: true,
      });
      instance.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
        void latestRun.current();
      });

      /*
       * Opening a quote asks for the list.
       *
       * The TypeScript language service is only consulted on a typed `.` — that is the adapter's
       * one trigger character — and the editor's own quick suggestions wait for a word character.
       * Between them, `selectFrom("` offers nothing until a letter is guessed, which is precisely
       * the moment a reader who does not know the tables needs to be shown them. Every table and
       * column in this API is named inside quotes, so the quote is the trigger that matters.
       */
      instance.onDidChangeModelContent((event) => {
        if (event.isFlush) return;
        // An auto-closed quote arrives as the pair, so both lengths count.
        if (!/^["'`]{1,2}$/.test(event.changes.at(-1)?.text ?? "")) return;
        void instance.getAction("editor.action.triggerSuggest")?.run();
      });

      model.onDidChangeContent(() => {
        window.localStorage.setItem(STORAGE_KEY, model.getValue());
      });

      api.current = monaco;
      editorRef.current = instance;
      setStatus({ kind: "ready" });
    }

    start().catch((error: unknown) => {
      if (stopped()) return;
      setStatus({
        kind: "failed",
        message: error instanceof Error ? error.message : String(error),
      });
    });

    return () => {
      cancelled = true;
      editorRef.current?.dispose();
      editorRef.current = undefined;
    };
    // The editor is built once, with no dependencies: the theme is applied below and the client
    // is read through a ref, so neither rebuilds it.
  }, []);

  useEffect(() => {
    api.current?.editor.setTheme(`minnow-${theme}`);
  }, [theme]);

  const load = useCallback((code: string) => {
    editorRef.current?.getModel()?.setValue(code);
    editorRef.current?.focus();
    setResult(undefined);
    setStatus({ kind: "ready" });
  }, []);

  return (
    <div className="flex flex-col gap-3">
      <div
        className="flex flex-col overflow-hidden rounded-lg border border-fd-border"
        style={{ height }}
      >
        <div className="flex items-center gap-2 border-b border-fd-border bg-fd-muted/40 px-3 py-1.5 text-sm">
          <button
            type="button"
            onClick={() => {
              void run();
            }}
            disabled={running || status.kind === "loading" || status.kind === "failed"}
            className="rounded-md bg-fd-primary px-3 py-1 text-xs font-medium text-fd-primary-foreground disabled:opacity-50"
          >
            {running ? "Running…" : "Run"}
          </button>
          <span className="text-xs text-fd-muted-foreground">⌘↵</span>
          <details className="ml-auto text-xs">
            <summary className="cursor-pointer text-fd-muted-foreground hover:text-fd-foreground">
              Where <code>db</code> came from
            </summary>
            <pre className="absolute right-4 z-10 mt-2 max-w-[min(38rem,90vw)] overflow-x-auto rounded-lg border border-fd-border bg-fd-background p-3 text-xs shadow-lg">
              {SETUP_SNIPPET}
            </pre>
          </details>
        </div>

        <div className="relative min-h-0 flex-[3]">
          {status.kind === "loading" ? (
            <div className="minnow-placeholder absolute inset-0 flex items-center justify-center">
              <p className="text-sm text-fd-muted-foreground">Loading the type checker…</p>
            </div>
          ) : null}
          {status.kind === "failed" ? (
            <div className="absolute inset-0 flex items-center justify-center p-6 text-center">
              <p className="max-w-md text-sm text-red-500">
                The editor could not start: {status.message}
              </p>
            </div>
          ) : null}
          <div ref={mount} className="h-full" />
        </div>

        <div className="min-h-0 flex-[2] overflow-auto border-t border-fd-border">
          {status.kind === "rejected" ? (
            <div className="flex flex-col gap-1 p-3">
              <p className="text-xs text-fd-muted-foreground">
                Nothing ran — the compiler refused it:
              </p>
              {status.errors.map((error) => (
                <pre
                  key={error}
                  className="overflow-x-auto font-mono text-xs whitespace-pre-wrap text-red-500"
                >
                  {error}
                </pre>
              ))}
            </div>
          ) : (
            <Output
              entries={result?.output ?? []}
              {...(result?.failure === undefined ? {} : { failure: result.failure })}
              {...(result === undefined ? {} : { elapsedMs: result.elapsedMs })}
              idle="Press Run. Whatever the snippet logs is drawn here — an array of rows becomes a table."
            />
          )}
        </div>
      </div>

      <div className="flex flex-wrap gap-1.5 text-xs">
        {snippets.map((snippet) => (
          <button
            key={snippet.id}
            type="button"
            title={snippet.note}
            onClick={() => {
              load(snippet.code);
            }}
            className="rounded-full border border-fd-border px-2.5 py-1 text-fd-muted-foreground hover:bg-fd-accent hover:text-fd-foreground"
          >
            {snippet.label}
          </button>
        ))}
      </div>
    </div>
  );
}
