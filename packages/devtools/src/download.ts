import { el } from "./dom.js";

/**
 * Hands bytes to the browser's own download. The anchor goes in the host document rather than
 * the panel's shadow root, which is where a download has to be started from, and the object URL
 * is released on a later task — revoking it in the same turn as the click cancels the download.
 */
export function downloadBlob(blob: Blob, fileName: string): void {
  const url = URL.createObjectURL(blob);
  const link = el("a", { attrs: { href: url, download: fileName } });
  link.hidden = true;
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => {
    URL.revokeObjectURL(url);
  }, 0);
}

export function downloadText(text: string, fileName: string, type: string): void {
  downloadBlob(new Blob([text], { type }), fileName);
}
