import { diagnose, explainUnsupported } from "./diagnostics.js";
import { spaced, type EditorSchema, type SqlEditor, type SqlEditorDeps } from "./editor.js";

export interface CodeMirrorDeps extends SqlEditorDeps {
  /**
   * The shadow root the editor lives in. CodeMirror needs it to find the active element and to
   * place its own stylesheet; without it, selection and completion break inside a shadow tree.
   */
  root: ShadowRoot;
  schema: EditorSchema;
}

/**
 * Colours come from the panel's own tokens rather than a bundled theme, so the editor tracks the
 * light and dark palettes with everything else and adds no colour decisions of its own.
 */
const themeSpec = {
  "&": {
    height: "100%",
    fontSize: "12.5px",
    backgroundColor: "var(--mdt-bg)",
    color: "var(--mdt-text)",
  },
  "&.cm-focused": { outline: "none" },
  ".cm-content": {
    fontFamily: "var(--mdt-mono)",
    padding: "8px 0",
    caretColor: "var(--mdt-accent)",
  },
  ".cm-gutters": {
    backgroundColor: "var(--mdt-bg)",
    color: "var(--mdt-text-faint)",
    border: "none",
    borderRight: "1px solid var(--mdt-border)",
    fontFamily: "var(--mdt-mono)",
    fontSize: "10.5px",
  },
  ".cm-activeLine": { backgroundColor: "var(--mdt-bg-hover)" },
  ".cm-activeLineGutter": { backgroundColor: "transparent", color: "var(--mdt-text-secondary)" },
  // drawSelection paints the selection as its own layer, so this is the colour that shows.
  ".cm-selectionBackground, &.cm-focused .cm-selectionBackground": {
    backgroundColor: "var(--mdt-selection)",
  },
  "&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground": {
    backgroundColor: "var(--mdt-selection)",
  },
  // The base theme re-enables the native selection on focused content with `highlight !important`
  // — the system colour, which is near-white and buries the text on a dark panel. The drawn layer
  // is already showing, so the native one is turned off in every state it tries to appear in.
  [[
    ".cm-content ::selection",
    ".cm-line ::selection",
    ".cm-content :focus::selection",
    ".cm-content :focus ::selection",
  ].join(", ")]: { backgroundColor: "transparent !important" },
  ".cm-cursor, .cm-dropCursor": { borderLeftColor: "var(--mdt-accent)" },
  ".cm-tooltip": {
    backgroundColor: "var(--mdt-bg)",
    border: "1px solid var(--mdt-border-strong)",
    borderRadius: "7px",
    boxShadow: "var(--mdt-shadow)",
    overflow: "hidden",
  },
  ".cm-tooltip-autocomplete ul li": {
    fontFamily: "var(--mdt-mono)",
    fontSize: "11.5px",
    padding: "3px 9px",
    color: "var(--mdt-text)",
  },
  ".cm-tooltip-autocomplete ul li[aria-selected]": {
    backgroundColor: "var(--mdt-accent-bg)",
    color: "var(--mdt-accent)",
  },
  ".cm-completionIcon": { display: "none" },
  ".cm-diagnostic": {
    fontFamily: "var(--mdt-mono)",
    fontSize: "11.5px",
    padding: "5px 8px",
    whiteSpace: "pre-wrap",
  },
  ".cm-diagnostic-error": { borderLeft: "3px solid var(--mdt-danger)" },
  ".cm-lintRange-error": {
    backgroundImage: "none",
    borderBottom: "2px solid var(--mdt-danger)",
  },
  ".cm-gutter-lint .cm-gutterElement": { padding: "0 2px" },
  ".cm-completionDetail": {
    fontStyle: "normal",
    color: "var(--mdt-text-faint)",
    marginLeft: "8px",
  },
};

/** The panel's own resolution order: the host's `theme` attribute, else the viewer's setting. */
function prefersDark(root: ShadowRoot): boolean {
  const theme = root.host.getAttribute("theme");
  if (theme === "dark") return true;
  if (theme === "light") return false;
  return globalThis.matchMedia("(prefers-color-scheme: dark)").matches;
}

/**
 * Loads CodeMirror and builds the editor. Everything here is behind one dynamic import, so the
 * launcher and the first paint of the panel never pay for it — the chunk is fetched the first time
 * the query tab is looked at.
 */
export async function loadCodeMirrorEditor(deps: CodeMirrorDeps): Promise<SqlEditor> {
  const [state, view, language, langSql, autocomplete, commands, highlight, lint] =
    await Promise.all([
      import("@codemirror/state"),
      import("@codemirror/view"),
      import("@codemirror/language"),
      import("@codemirror/lang-sql"),
      import("@codemirror/autocomplete"),
      import("@codemirror/commands"),
      import("@lezer/highlight"),
      import("@codemirror/lint"),
    ]);

  const { Compartment, EditorState } = state;
  const { EditorView, keymap, lineNumbers, highlightActiveLine, drawSelection } = view;
  const { HighlightStyle, syntaxHighlighting } = language;
  const { tags } = highlight;

  const syntax = HighlightStyle.define([
    { tag: tags.keyword, color: "var(--mdt-accent)" },
    { tag: tags.string, color: "var(--mdt-ok)" },
    { tag: [tags.number, tags.bool, tags.null], color: "var(--mdt-warn)" },
    { tag: tags.function(tags.variableName), color: "var(--mdt-warn)" },
    { tag: tags.comment, color: "var(--mdt-text-faint)", fontStyle: "italic" },
    { tag: tags.operator, color: "var(--mdt-text-secondary)" },
  ]);

  // The schema lives in a compartment so the catalog can be swapped in without rebuilding the
  // editor — the document, history, and caret all survive a reconfigure.
  const schemaCompartment = new Compartment();
  const sqlExtension = (schema: EditorSchema): ReturnType<typeof langSql.sql> =>
    langSql.sql({ schema, upperCaseKeywords: true });

  const editorView = new EditorView({
    // Without this, CodeMirror looks at `document` for the focused element and the selection and
    // completion misbehave inside a shadow tree.
    root: deps.root,
    state: EditorState.create({
      doc: deps.initial,
      extensions: [
        lineNumbers(),
        drawSelection(),
        highlightActiveLine(),
        commands.history(),
        autocomplete.autocompletion({ activateOnTyping: true, icons: false }),
        schemaCompartment.of(sqlExtension(deps.schema)),
        // Compiles on idle, in the page. The engine's own parser decides what is wrong and
        // exactly where, so the squiggle sits under the token rather than the whole line.
        lint.linter((editor) => diagnose(editor.state.doc.toString(), explainUnsupported), {
          delay: 350,
        }),
        lint.lintGutter(),
        syntaxHighlighting(syntax),
        EditorState.allowMultipleSelections.of(true),
        EditorView.lineWrapping,
        EditorView.contentAttributes.of({ "aria-label": "SQL" }),
        keymap.of([
          {
            key: "Mod-Enter",
            preventDefault: true,
            run: () => {
              deps.onRun();
              return true;
            },
          },
          ...commands.defaultKeymap,
          ...commands.historyKeymap,
          ...autocomplete.completionKeymap,
        ]),
        // Tells CodeMirror which half of its base theme to apply. Every colour that matters is
        // overridden above, but the base theme still decides things like the tooltip's own
        // borders, and it defaults to light.
        EditorView.theme(themeSpec, { dark: prefersDark(deps.root) }),
      ],
    }),
  });

  return {
    node: editorView.dom,
    value: () => editorView.state.doc.toString(),
    setValue: (text) => {
      editorView.dispatch({
        changes: { from: 0, to: editorView.state.doc.length, insert: text },
      });
    },
    focus: () => {
      editorView.focus();
    },
    selectRange: (from, to) => {
      const end = Math.min(to, editorView.state.doc.length);
      editorView.focus();
      editorView.dispatch({
        selection: { anchor: Math.min(from, end), head: end },
        scrollIntoView: true,
      });
    },
    insert: (text) => {
      const { from, to } = editorView.state.selection.main;
      const insert = spaced(editorView.state.doc.sliceString(Math.max(0, from - 1), from), text);
      editorView.focus();
      editorView.dispatch({
        changes: { from, to, insert },
        selection: { anchor: from + insert.length },
        scrollIntoView: true,
      });
    },
    setSchema: (schema) => {
      editorView.dispatch({ effects: schemaCompartment.reconfigure(sqlExtension(schema)) });
    },
    destroy: () => {
      editorView.destroy();
    },
  };
}
