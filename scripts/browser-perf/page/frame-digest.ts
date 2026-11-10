/** Untimed correctness diagnostic, never enabled for performance acceptance.
 * Chain SHA-256 over frame metadata and every native byte (including NULL masks/padding).
 * The temporary packed hash input is receiver-only scratch, never a result or transfer buffer.
 */
export class FrameDigest {
  #previous: Uint8Array<ArrayBuffer> = new Uint8Array(32);
  async add(frame: unknown): Promise<void> {
    const views: Array<Uint8Array<ArrayBufferLike>> = [];
    const json = JSON.stringify(frame, (_key: string, value: unknown): unknown => {
      if (ArrayBuffer.isView(value)) {
        views.push(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
        return { native: value.constructor.name, bytes: value.byteLength };
      }
      if (value instanceof ArrayBuffer) {
        views.push(new Uint8Array(value));
        return { native: "ArrayBuffer", bytes: value.byteLength };
      }
      return typeof value === "bigint" ? { bigint: value.toString() } : value;
    });
    const header = new TextEncoder().encode(json);
    const packed = new Uint8Array(36 + header.length + views.reduce((n, v) => n + v.length, 0));
    packed.set(this.#previous);
    new DataView(packed.buffer).setUint32(32, header.length, true);
    packed.set(header, 36);
    let at = 36 + header.length;
    for (const view of views) {
      packed.set(view, at);
      at += view.length;
    }
    this.#previous = new Uint8Array(await crypto.subtle.digest("SHA-256", packed));
  }
  get hex(): string {
    return Array.from(this.#previous, (b) => b.toString(16).padStart(2, "0")).join("");
  }
}
