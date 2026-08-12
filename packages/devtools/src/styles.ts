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

/* --- explorer ------------------------------------------------------------------------------- */

.explorer { flex: 1; display: flex; min-height: 0; }
.explorer-main { flex: 1; display: flex; flex-direction: column; min-width: 0; min-height: 0; }

.rail {
  width: 210px;
  flex: none;
  display: flex;
  flex-direction: column;
  min-height: 0;
  border-right: 1px solid var(--mdt-border);
}
.rail-top { padding: 8px; border-bottom: 1px solid var(--mdt-border); }
.rail-search {
  width: 100%;
  font: inherit;
  font-family: var(--mdt-mono);
  font-size: 12px;
  padding: 5px 8px;
  border-radius: 6px;
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

.tnode {
  display: flex;
  align-items: center;
  gap: 6px;
  width: 100%;
  padding: 4px 10px 4px 8px;
  border: none;
  background: transparent;
  cursor: pointer;
  font-size: 12.5px;
  color: var(--mdt-text-secondary);
  text-align: left;
}
.tnode:hover { background: var(--mdt-bg-hover); }
.tnode.on { background: var(--mdt-accent-bg); color: var(--mdt-accent); }
.tnode-chev { width: 10px; flex: none; font-size: 9px; opacity: 0.7; }
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
  padding: 2.5px 10px 2.5px 26px;
  font-family: var(--mdt-mono);
  font-size: 11.5px;
  color: var(--mdt-text-secondary);
}
.col-name { flex: 1; overflow: hidden; text-overflow: ellipsis; }
.col-type { font-size: 10.5px; color: var(--mdt-text-faint); }
.col-key { font-size: 9.5px; color: var(--mdt-warn); }

.crumb { font-family: var(--mdt-mono); font-size: 12.5px; display: flex; align-items: center; gap: 8px; }
.crumb-meta { font-size: 11px; color: var(--mdt-text-faint); }

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
.cell.number { text-align: right; font-variant-numeric: tabular-nums; }
.cell.null { color: var(--mdt-text-faint); font-style: italic; }
.cell.boolean { color: var(--mdt-text-secondary); }
.grid-message { padding: 18px 12px; font-size: 12.5px; color: var(--mdt-text-faint); }

.toolbar {
  flex: none;
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 7px 10px;
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

.editor {
  flex: none;
  height: 34%;
  min-height: 96px;
  width: 100%;
  resize: none;
  border: none;
  border-bottom: 1px solid var(--mdt-border);
  padding: 10px 12px;
  background: var(--mdt-bg);
  color: var(--mdt-text);
  font-family: var(--mdt-mono);
  font-size: 12.5px;
  line-height: 1.6;
  tab-size: 2;
}
.editor:focus { outline: none; box-shadow: inset 0 0 0 2px var(--mdt-accent-bg); }

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
