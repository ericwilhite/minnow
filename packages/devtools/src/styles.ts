/**
 * The panel's entire stylesheet, adopted into its shadow root. Nothing here reaches the host page
 * and nothing on the host page reaches in; the panel therefore looks the same in every app it is
 * dropped into. Tokens follow the Minnow docs palette and track the viewer's colour scheme, with
 * `theme="light" | "dark"` on the element overriding it.
 */
export const styles = `
:host {
  --mdt-bg: #ffffff;
  --mdt-bg-secondary: #f7f7f5;
  --mdt-bg-hover: rgba(55, 53, 47, 0.06);
  --mdt-bg-active: rgba(55, 53, 47, 0.1);
  --mdt-bg-code: #f7f6f3;
  --mdt-text: rgb(55, 53, 47);
  --mdt-text-secondary: rgba(55, 53, 47, 0.65);
  --mdt-text-faint: rgba(55, 53, 47, 0.45);
  --mdt-border: rgba(55, 53, 47, 0.12);
  --mdt-border-strong: rgba(55, 53, 47, 0.2);
  --mdt-accent: #2383e2;
  --mdt-accent-bg: rgba(35, 131, 226, 0.09);
  --mdt-selection: rgba(35, 131, 226, 0.28);
  --mdt-danger: #c4433f;
  --mdt-danger-bg: rgba(235, 87, 87, 0.1);
  --mdt-warn: #a86a1c;
  --mdt-warn-bg: rgba(203, 145, 47, 0.14);
  --mdt-ok: #1c7c54;
  --mdt-ok-bg: rgba(28, 124, 84, 0.1);
  --mdt-shadow: rgba(15, 15, 15, 0.05) 0 0 0 1px, rgba(15, 15, 15, 0.1) 0 3px 6px,
    rgba(15, 15, 15, 0.2) 0 9px 24px;
  --mdt-sans: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial,
    sans-serif;
  --mdt-header-h: 33px;
  --mdt-mono: ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;

  font-family: var(--mdt-sans);
  font-size: 14px;
  line-height: 1.5;
  color: var(--mdt-text);
}

@media (prefers-color-scheme: dark) {
  :host(:not([theme="light"])) {
    --mdt-bg: #1e1e1e;
    --mdt-bg-secondary: #202020;
    --mdt-bg-hover: rgba(255, 255, 255, 0.055);
    --mdt-bg-active: rgba(255, 255, 255, 0.09);
    --mdt-bg-code: #262626;
    --mdt-text: rgba(255, 255, 255, 0.81);
    --mdt-text-secondary: rgba(255, 255, 255, 0.46);
    --mdt-text-faint: rgba(255, 255, 255, 0.3);
    --mdt-border: rgba(255, 255, 255, 0.11);
    --mdt-border-strong: rgba(255, 255, 255, 0.2);
    --mdt-accent: #529cca;
    --mdt-accent-bg: rgba(82, 156, 202, 0.14);
    --mdt-selection: rgba(82, 156, 202, 0.42);
    --mdt-danger: #ff7369;
    --mdt-danger-bg: rgba(255, 115, 105, 0.14);
    --mdt-warn: #d9a33f;
    --mdt-warn-bg: rgba(217, 163, 63, 0.15);
    --mdt-ok: #4dab7f;
    --mdt-ok-bg: rgba(77, 171, 127, 0.14);
    --mdt-shadow: rgba(15, 15, 15, 0.3) 0 0 0 1px, rgba(15, 15, 15, 0.5) 0 3px 6px,
      rgba(15, 15, 15, 0.7) 0 9px 24px;
  }
}

:host([theme="dark"]) {
  --mdt-bg: #1e1e1e;
  --mdt-bg-secondary: #202020;
  --mdt-bg-hover: rgba(255, 255, 255, 0.055);
  --mdt-bg-active: rgba(255, 255, 255, 0.09);
  --mdt-bg-code: #262626;
  --mdt-text: rgba(255, 255, 255, 0.81);
  --mdt-text-secondary: rgba(255, 255, 255, 0.46);
  --mdt-text-faint: rgba(255, 255, 255, 0.3);
  --mdt-border: rgba(255, 255, 255, 0.11);
  --mdt-border-strong: rgba(255, 255, 255, 0.2);
  --mdt-accent: #529cca;
  --mdt-accent-bg: rgba(82, 156, 202, 0.14);
  --mdt-danger: #ff7369;
  --mdt-danger-bg: rgba(255, 115, 105, 0.14);
  --mdt-warn: #d9a33f;
  --mdt-warn-bg: rgba(217, 163, 63, 0.15);
  --mdt-ok: #4dab7f;
  --mdt-ok-bg: rgba(77, 171, 127, 0.14);
  --mdt-shadow: rgba(15, 15, 15, 0.3) 0 0 0 1px, rgba(15, 15, 15, 0.5) 0 3px 6px,
    rgba(15, 15, 15, 0.7) 0 9px 24px;
}

*, *::before, *::after { box-sizing: border-box; }

/* Every layout rule below sets display, which would otherwise beat the hidden attribute. */
[hidden] { display: none !important; }

/*
 * Selected text keeps the panel's own foreground. Left to the host page or the browser default,
 * a dark panel gets a light selection behind light text and the words disappear.
 */
::selection { background: var(--mdt-selection); color: var(--mdt-text); }

button { font: inherit; color: inherit; }

:focus-visible { outline: 2px solid var(--mdt-accent); outline-offset: 1px; }

.launcher {
  position: fixed;
  width: 40px;
  height: 40px;
  border-radius: 10px;
  border: 1px solid var(--mdt-border-strong);
  background: var(--mdt-bg);
  color: var(--mdt-accent);
  box-shadow: var(--mdt-shadow);
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
}
.launcher:hover { background: var(--mdt-bg-hover); }
.launcher svg { width: 21px; height: 21px; }

.panel {
  display: flex;
  flex-direction: column;
  background: var(--mdt-bg);
  border: 1px solid var(--mdt-border-strong);
  border-radius: 10px;
  box-shadow: var(--mdt-shadow);
  overflow: hidden;
}
.panel.floating { position: fixed; }
.panel.inline { position: relative; width: 100%; height: 520px; }

.titlebar {
  flex: none;
  display: flex;
  align-items: center;
  gap: 8px;
  height: 38px;
  padding: 0 8px 0 12px;
  background: var(--mdt-bg-secondary);
  border-bottom: 1px solid var(--mdt-border);
}
.panel.floating .titlebar { cursor: grab; user-select: none; }
.panel.floating .titlebar.dragging { cursor: grabbing; }
.mark { display: flex; color: var(--mdt-accent); }
.mark svg { width: 17px; height: 17px; }
.title { font-size: 12.5px; font-weight: 500; }
.spacer { flex: 1; }

.badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-family: var(--mdt-mono);
  font-size: 10.5px;
  padding: 2.5px 7px;
  border-radius: 5px;
  white-space: nowrap;
  background: var(--mdt-bg-hover);
  color: var(--mdt-text-secondary);
}
.badge.ok { background: var(--mdt-ok-bg); color: var(--mdt-ok); }
.badge.warn { background: var(--mdt-warn-bg); color: var(--mdt-warn); }
.badge .dot { width: 5px; height: 5px; border-radius: 50%; background: currentColor; }

.winbtn {
  width: 24px;
  height: 24px;
  border: none;
  border-radius: 5px;
  background: transparent;
  color: var(--mdt-text-faint);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
}
.winbtn:hover { background: var(--mdt-bg-hover); color: var(--mdt-text); }
.winbtn svg { width: 14px; height: 14px; }

.body { flex: 1; display: flex; flex-direction: column; min-height: 0; }
/* Size containers, so the sidebars react to the panel's width rather than the browser's. */
.panel-body { flex: 1; display: flex; min-height: 0; container: mdt-panel / inline-size; }
.views { flex: 1; display: flex; flex-direction: column; min-width: 0; min-height: 0; }
.views > * { flex: 1; min-height: 0; }

.tabs { display: flex; gap: 2px; margin-left: 4px; }
.tab {
  font-size: 12px;
  padding: 3px 9px;
  border: none;
  border-radius: 5px;
  background: transparent;
  color: var(--mdt-text-secondary);
  cursor: pointer;
}
.tab:hover { background: var(--mdt-bg-hover); }
.tab.on { background: var(--mdt-bg-active); color: var(--mdt-text); }

/* --- sidebars ------------------------------------------------------------------------------- */

.side { flex: none; display: flex; flex-direction: column; min-height: 0; }
.side-head {
  flex: none;
  display: flex;
  align-items: center;
  gap: 6px;
  height: var(--mdt-header-h);
  padding: 0 4px 0 10px;
  border-bottom: 1px solid var(--mdt-border);
}
.side-title {
  font-size: 10.5px;
  text-transform: uppercase;
  letter-spacing: 0.07em;
  color: var(--mdt-text-faint);
}
.side-toggle {
  flex: none;
  width: 22px;
  height: 22px;
  border: none;
  border-radius: 5px;
  background: transparent;
  color: var(--mdt-text-faint);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
}
.side-toggle:hover { background: var(--mdt-bg-hover); color: var(--mdt-text); }
.side-toggle svg { width: 14px; height: 14px; }

/* Collapsed, a sidebar keeps a narrow strip so the way back is always visible. */
.side-stub {
  display: none;
  height: var(--mdt-header-h);
  align-items: center;
  justify-content: center;
  border-bottom: 1px solid var(--mdt-border);
}
.side.collapsed { width: 30px; }
.side.collapsed > .side-head, .side.collapsed > .rail-list, .side.collapsed > .hlist { display: none; }
.side.collapsed > .side-stub { display: flex; }

/*
 * Narrow enough and a sidebar stops earning its width, so it leaves entirely — a collapsed strip
 * would only be a control that cannot be used. The table picker in the Data toolbar is what keeps
 * tables reachable while the rail is away.
 */
@container mdt-panel (max-width: 600px) {
  .rail { display: none; }
}
@container mdt-console (max-width: 620px) {
  .history { display: none; }
}

/* --- explorer ------------------------------------------------------------------------------- */

.explorer-main { flex: 1; display: flex; flex-direction: column; min-width: 0; min-height: 0; }

.rail { width: 210px; border-right: 1px solid var(--mdt-border); }
.rail-search {
  flex: 1;
  min-width: 0;
  font: inherit;
  font-family: var(--mdt-mono);
  font-size: 11.5px;
  padding: 3px 7px;
  border-radius: 5px;
  color: var(--mdt-text);
  background: var(--mdt-bg-code);
  border: 1px solid var(--mdt-border);
}
.rail-list { flex: 1; overflow: auto; padding-bottom: 10px; }
.rail-group {
  font-size: 10.5px;
  text-transform: uppercase;
  letter-spacing: 0.07em;
  color: var(--mdt-text-faint);
  padding: 8px 12px 4px;
}
.rail-empty { padding: 12px; font-size: 12px; color: var(--mdt-text-faint); }

/* Expanding and opening are separate targets: a table can be inspected without being loaded. */
.tnode { display: flex; align-items: stretch; width: 100%; color: var(--mdt-text-secondary); }
.tnode-toggle {
  flex: none;
  width: 22px;
  border: none;
  background: transparent;
  color: var(--mdt-text-secondary);
  cursor: pointer;
  font-size: 13px;
  line-height: 1;
  padding: 0;
}
.tnode-toggle:hover { color: var(--mdt-text); background: var(--mdt-bg-hover); }
.tnode-open {
  display: flex;
  align-items: center;
  gap: 6px;
  flex: 1;
  min-width: 0;
  padding: 4px 10px 4px 2px;
  border: none;
  background: transparent;
  cursor: pointer;
  font-size: 12.5px;
  color: inherit;
  text-align: left;
}
.tnode-open:hover { background: var(--mdt-bg-hover); }
.tnode.on { background: var(--mdt-accent-bg); color: var(--mdt-accent); }
.tnode-icon { display: flex; flex: none; color: var(--mdt-text-faint); }
.tnode-icon svg { width: 14px; height: 14px; stroke-width: 1.6; }
.tnode-open:hover .tnode-icon { color: var(--mdt-text-secondary); }
.tnode.on .tnode-icon { color: var(--mdt-accent); }
.tnode-name { font-family: var(--mdt-mono); font-size: 12px; flex: 1; overflow: hidden; text-overflow: ellipsis; }
.tnode-meta { font-family: var(--mdt-mono); font-size: 10.5px; color: var(--mdt-text-faint); }
.tnode.on .tnode-meta { color: inherit; opacity: 0.7; }
.tnode-badge {
  font-family: var(--mdt-mono);
  font-size: 9.5px;
  padding: 1px 4px;
  border-radius: 4px;
  background: var(--mdt-warn-bg);
  color: var(--mdt-warn);
}

.cols { padding: 1px 0 6px; }
.col {
  display: flex;
  align-items: center;
  gap: 6px;
  width: 100%;
  padding: 2.5px 10px 2.5px 26px;
  border: none;
  background: transparent;
  cursor: pointer;
  font-family: var(--mdt-mono);
  font-size: 11.5px;
  color: var(--mdt-text-secondary);
  text-align: left;
}
.col:hover { background: var(--mdt-bg-hover); color: var(--mdt-text); }
.col-name { flex: 1; overflow: hidden; text-overflow: ellipsis; }
.col-type { font-size: 10.5px; color: var(--mdt-text-faint); }
.col-key { font-size: 9.5px; color: var(--mdt-warn); }

.crumb { font-family: var(--mdt-mono); font-size: 12.5px; display: flex; align-items: center; gap: 8px; }
.crumb-meta { font-family: var(--mdt-mono); font-size: 11px; color: var(--mdt-text-faint); }
.table-picker { max-width: 180px; }

.filters {
  flex: none;
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
  padding: 6px 10px;
  border-bottom: 1px solid var(--mdt-border);
  background: var(--mdt-bg-secondary);
}
.chips { display: contents; }
.chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-family: var(--mdt-mono);
  font-size: 11px;
  background: var(--mdt-bg);
  border: 1px solid var(--mdt-border-strong);
  border-radius: 6px;
  padding: 2px 4px 2px 8px;
  color: var(--mdt-text-secondary);
}
.chip-x {
  border: none;
  background: transparent;
  color: var(--mdt-text-faint);
  cursor: pointer;
  font-size: 13px;
  line-height: 1;
  padding: 0 3px;
  border-radius: 4px;
}
.chip-x:hover { background: var(--mdt-bg-active); color: var(--mdt-danger); }
.btn.mini { font-size: 11px; padding: 2px 8px; }
.btn.dashed { border-style: dashed; color: var(--mdt-text-faint); }
.filter-editor { display: flex; align-items: center; gap: 4px; flex-wrap: wrap; }
select.mini, input.mini {
  font: inherit;
  font-family: var(--mdt-mono);
  font-size: 11px;
  padding: 2px 5px;
  border-radius: 5px;
  color: var(--mdt-text);
  background: var(--mdt-bg);
  border: 1px solid var(--mdt-border-strong);
}
input.mini.value { width: 110px; }

/* --- grid ----------------------------------------------------------------------------------- */

.grid { flex: 1; display: flex; flex-direction: column; min-height: 0; overflow: hidden; }
.grid-head {
  position: sticky;
  top: 0;
  z-index: 1;
  display: grid;
  background: var(--mdt-bg);
  border-bottom: 1px solid var(--mdt-border-strong);
}
.grid-th {
  display: flex;
  align-items: baseline;
  gap: 5px;
  padding: 6px 10px;
  font-family: var(--mdt-mono);
  font-size: 11.5px;
  font-weight: 500;
  color: var(--mdt-text-secondary);
  white-space: nowrap;
  overflow: hidden;
}
.grid-th.sortable { cursor: pointer; }
.grid-th.sortable:hover { background: var(--mdt-bg-hover); color: var(--mdt-text); }
.grid-th.sorted { color: var(--mdt-text); }
.grid-name { overflow: hidden; text-overflow: ellipsis; }
.grid-type { font-size: 10px; color: var(--mdt-text-faint); font-weight: 400; }
.grid-sort { color: var(--mdt-accent); font-size: 10px; }

.grid-viewport { flex: 1; overflow: auto; min-height: 0; }
.grid-surface { position: relative; width: max-content; min-width: 100%; }
.grid-sizer { position: relative; }
.grid-row {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 26px;
  display: grid;
  border-bottom: 1px solid var(--mdt-border);
  will-change: transform;
}
.grid-row:hover { background: var(--mdt-bg-hover); }
.cell {
  padding: 0 10px;
  font-family: var(--mdt-mono);
  font-size: 11.5px;
  line-height: 25px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  color: var(--mdt-text);
}
.grid-row.sel { background: var(--mdt-accent-bg); }
.grid-row.sel:hover { background: var(--mdt-accent-bg); }
.grid-row.editing { background: var(--mdt-bg); z-index: 2; border-bottom-color: transparent; }
.cell-edit { display: flex; align-items: center; padding: 0 4px; }
.cell-input {
  width: 100%;
  font: inherit;
  font-family: var(--mdt-mono);
  font-size: 11.5px;
  padding: 2px 5px;
  color: var(--mdt-text);
  background: var(--mdt-bg);
  border: 1.5px solid var(--mdt-accent);
  border-radius: 4px;
  box-shadow: 0 0 0 3px var(--mdt-accent-bg);
}
.cell-input:focus { outline: none; }

.cell.number { text-align: right; font-variant-numeric: tabular-nums; }
.cell.null { color: var(--mdt-text-faint); font-style: italic; }
.cell.boolean { color: var(--mdt-text-secondary); }
.grid-message { padding: 18px 12px; font-size: 12.5px; color: var(--mdt-text-faint); }

/* --- editing -------------------------------------------------------------------------------- */

.banner {
  flex: none;
  padding: 6px 11px;
  background: var(--mdt-warn-bg);
  color: var(--mdt-warn);
  font-size: 11.5px;
  border-bottom: 1px solid var(--mdt-border);
}

.insert-sheet {
  position: absolute;
  inset: 0;
  z-index: 3;
  display: flex;
  flex-direction: column;
  background: var(--mdt-bg);
}
.explorer-main { position: relative; }
.insert-head { padding: 10px 12px 6px; border-bottom: 1px solid var(--mdt-border); }
.insert-title { margin: 0; font-size: 13px; font-weight: 600; }
.insert-fields {
  flex: 1;
  overflow: auto;
  padding: 10px 12px;
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 8px 12px;
  align-content: start;
}
.insert-field { display: flex; flex-direction: column; gap: 3px; }
.insert-label { display: flex; align-items: baseline; gap: 6px; }
.insert-name { font-family: var(--mdt-mono); font-size: 11.5px; }
.insert-type { font-family: var(--mdt-mono); font-size: 10px; color: var(--mdt-text-faint); }
.insert-input {
  font: inherit;
  font-family: var(--mdt-mono);
  font-size: 11.5px;
  padding: 4px 7px;
  border-radius: 5px;
  color: var(--mdt-text);
  background: var(--mdt-bg-code);
  border: 1px solid var(--mdt-border-strong);
}
.insert-error {
  flex: none;
  padding: 6px 12px;
  background: var(--mdt-danger-bg);
  color: var(--mdt-danger);
  font-family: var(--mdt-mono);
  font-size: 11.5px;
}
.insert-foot {
  flex: none;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-top: 1px solid var(--mdt-border);
  background: var(--mdt-bg-secondary);
}
.insert-note { font-size: 11px; color: var(--mdt-text-faint); }

.toolbar {
  flex: none;
  display: flex;
  align-items: center;
  gap: 8px;
  height: var(--mdt-header-h);
  padding: 0 10px;
  border-bottom: 1px solid var(--mdt-border);
}

.btn {
  font-size: 12px;
  color: var(--mdt-text-secondary);
  background: transparent;
  border: 1px solid var(--mdt-border-strong);
  border-radius: 6px;
  padding: 4px 10px;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  white-space: nowrap;
}
.btn:hover { background: var(--mdt-bg-hover); color: var(--mdt-text); }
.btn svg { width: 12px; height: 12px; }
.btn.primary { background: var(--mdt-accent); border-color: var(--mdt-accent); color: #ffffff; }
.btn.primary:hover { filter: brightness(1.07); color: #ffffff; }
.btn.danger { color: var(--mdt-danger); border-color: var(--mdt-danger); }
.btn.danger:hover { background: var(--mdt-danger-bg); }
.btn:disabled { opacity: 0.45; cursor: not-allowed; }
.hint { font-family: var(--mdt-mono); font-size: 10.5px; color: var(--mdt-text-faint); }

.console { display: flex; min-height: 0; container: mdt-console / inline-size; }
.console-main { flex: 1; display: flex; flex-direction: column; min-width: 0; min-height: 0; }

.editor-slot {
  flex: none;
  height: 32%;
  min-height: 100px;
  display: flex;
  overflow: hidden;
  border-bottom: 1px solid var(--mdt-border);
}
.editor-slot > * { flex: 1; min-width: 0; }

.editor {
  width: 100%;
  resize: none;
  border: none;
  padding: 10px 12px;
  background: var(--mdt-bg);
  color: var(--mdt-text);
  font-family: var(--mdt-mono);
  font-size: 12.5px;
  line-height: 1.6;
  tab-size: 2;
}
.editor:focus { outline: none; box-shadow: inset 0 0 0 2px var(--mdt-accent-bg); }

/* --- history ---------------------------------------------------------------------------------- */

.history { width: 214px; border-left: 1px solid var(--mdt-border); }
.hclear {
  border: none;
  background: transparent;
  color: var(--mdt-text-faint);
  cursor: pointer;
  font-size: 10.5px;
  text-transform: none;
  letter-spacing: 0;
  padding: 2px 5px;
  border-radius: 4px;
}
.hclear:hover { background: var(--mdt-bg-hover); color: var(--mdt-text); }
.hlist { flex: 1; overflow: auto; min-height: 0; }
.hempty { padding: 12px 10px; font-size: 11.5px; color: var(--mdt-text-faint); line-height: 1.5; }
.hitem {
  display: flex;
  flex-direction: column;
  gap: 3px;
  width: 100%;
  padding: 6px 10px;
  border: none;
  border-bottom: 1px solid var(--mdt-border);
  background: transparent;
  cursor: pointer;
  text-align: left;
}
.hitem:hover { background: var(--mdt-bg-hover); }
.hitem.on { background: var(--mdt-accent-bg); }
.hsql {
  font-family: var(--mdt-mono);
  font-size: 10.5px;
  color: var(--mdt-text);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 100%;
}
.hmeta {
  display: flex;
  align-items: baseline;
  gap: 6px;
  font-family: var(--mdt-mono);
  font-size: 9.5px;
  color: var(--mdt-text-faint);
}
/* The outcome gives way; the age never wraps, so a long error message cannot break the line. */
.houtcome { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.hmeta > span:last-child { flex: none; white-space: nowrap; }
.hitem.failed .houtcome { color: var(--mdt-danger); }

.notice {
  flex: none;
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 7px 12px;
  border-bottom: 1px solid var(--mdt-border);
  font-family: var(--mdt-mono);
  font-size: 11.5px;
  white-space: pre-wrap;
  word-break: break-word;
}
.notice.error { background: var(--mdt-danger-bg); color: var(--mdt-danger); }
.notice.blocked { background: var(--mdt-warn-bg); color: var(--mdt-warn); }
.notice.done { background: var(--mdt-ok-bg); color: var(--mdt-ok); }
.notice svg { width: 13px; height: 13px; flex: none; margin-top: 2px; }

.results { flex: 1; overflow: auto; min-height: 0; }
.empty { padding: 18px 12px; font-size: 12.5px; color: var(--mdt-text-faint); }

table {
  border-collapse: separate;
  border-spacing: 0;
  width: 100%;
  font-family: var(--mdt-mono);
  font-size: 11.5px;
}
th {
  position: sticky;
  top: 0;
  z-index: 1;
  text-align: left;
  font-weight: 500;
  background: var(--mdt-bg);
  border-bottom: 1px solid var(--mdt-border-strong);
  padding: 6px 10px;
  white-space: nowrap;
  color: var(--mdt-text-secondary);
}
td {
  padding: 5px 10px;
  border-bottom: 1px solid var(--mdt-border);
  white-space: nowrap;
  max-width: 280px;
  overflow: hidden;
  text-overflow: ellipsis;
}
td.number { text-align: right; font-variant-numeric: tabular-nums; }
td.null { color: var(--mdt-text-faint); font-style: italic; }
tr:hover td { background: var(--mdt-bg-hover); }

.statusbar {
  flex: none;
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 5px 10px;
  border-top: 1px solid var(--mdt-border);
  background: var(--mdt-bg-secondary);
  font-family: var(--mdt-mono);
  font-size: 10.5px;
  color: var(--mdt-text-faint);
}

.grip {
  position: absolute;
  right: 1px;
  bottom: 1px;
  width: 16px;
  height: 16px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: var(--mdt-text-faint);
  cursor: nwse-resize;
  touch-action: none;
}
.grip svg { width: 12px; height: 12px; }
.panel.inline .grip { display: none; }

/*
 * Invisible strips over each boundary. They sit just inside the panel so the whole edge is a
 * target without the window needing a visible frame; the corner grip stays as the obvious one.
 */
.resize-edge { position: absolute; touch-action: none; z-index: 4; }
.resize-n, .resize-s { left: 8px; right: 8px; height: 6px; cursor: ns-resize; }
.resize-e, .resize-w { top: 8px; bottom: 8px; width: 6px; cursor: ew-resize; }
.resize-n { top: 0; }
.resize-s { bottom: 0; }
.resize-e { right: 0; }
.resize-w { left: 0; }
.resize-ne, .resize-nw, .resize-sw { width: 12px; height: 12px; }
.resize-ne { top: 0; right: 0; cursor: nesw-resize; }
.resize-nw { top: 0; left: 0; cursor: nwse-resize; }
.resize-sw { bottom: 0; left: 0; cursor: nesw-resize; }
.panel.inline .resize-edge { display: none; }

.scrim {
  position: absolute;
  inset: 0;
  z-index: 2;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 16px;
  background: rgba(15, 15, 15, 0.3);
}

.dialog {
  width: 100%;
  max-width: 440px;
  background: var(--mdt-bg);
  border: 1px solid var(--mdt-border-strong);
  border-radius: 10px;
  box-shadow: var(--mdt-shadow);
  overflow: hidden;
}
.dialog h2 { margin: 0; font-size: 14px; font-weight: 600; }
.dialog-head { display: flex; align-items: center; gap: 9px; padding: 13px 16px 0; }
.dialog-body { padding: 10px 16px 14px; display: flex; flex-direction: column; gap: 10px; }
.dialog-warning {
  display: flex;
  gap: 7px;
  align-items: flex-start;
  font-size: 12px;
  color: var(--mdt-danger);
}
.dialog-warning svg { width: 13px; height: 13px; flex: none; margin-top: 2px; }

.plate {
  background: var(--mdt-bg-code);
  border: 1px solid var(--mdt-border);
  border-radius: 7px;
  font-family: var(--mdt-mono);
  font-size: 11.5px;
  overflow: hidden;
}
.plate-row { display: flex; gap: 10px; padding: 5px 10px; border-bottom: 1px solid var(--mdt-border); }
.plate-row:last-child { border-bottom: none; }
.plate-key { width: 74px; flex: none; color: var(--mdt-text-faint); }
.plate-value { flex: 1; word-break: break-word; }
.plate pre {
  margin: 0;
  padding: 8px 10px;
  white-space: pre-wrap;
  word-break: break-word;
  font: inherit;
}

.dialog-foot {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 16px;
  border-top: 1px solid var(--mdt-border);
  background: var(--mdt-bg-secondary);
}
.dialog-note { font-size: 11px; color: var(--mdt-text-faint); }

@media (prefers-reduced-motion: reduce) {
  * { transition: none !important; animation: none !important; }
}
`;
