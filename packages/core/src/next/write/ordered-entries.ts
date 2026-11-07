/** Write-maintained order: bounded leaf moves, no read-time cardinality-sized permutation. */
import { charged, need, type Charge } from "./bytes.js";

const LEAF = 64;
const LEAF_BYTES = LEAF * 4 + 64;
interface Leaf {
  readonly entries: Int32Array;
  count: number;
}
export class OrderedEntries {
  #leaves: Array<Leaf | undefined> = [];
  #capacity = 0;
  #count = 0;
  #linear = 0;
  version = 0;
  constructor(
    readonly charge: Charge,
    readonly compare: (a: number, b: number) => number,
  ) {}

  #leaf(): Leaf {
    charged(this.charge, LEAF_BYTES);
    try {
      return { entries: new Int32Array(LEAF), count: 0 };
    } catch (error) {
      this.charge.free(LEAF_BYTES);
      throw error;
    }
  }
  #grow(): void {
    if (this.#count < this.#capacity) return;
    const capacity = Math.max(4, this.#capacity * 2),
      bytes = 64 + capacity * 8;
    charged(this.charge, bytes);
    try {
      const grown = new Array<Leaf | undefined>(capacity);
      for (let i = 0; i < this.#count; i++) grown[i] = this.#leaves[i];
      this.#leaves = grown;
    } catch (error) {
      this.charge.free(bytes);
      throw error;
    }
    if (this.#capacity) this.charge.free(64 + this.#capacity * 8);
    this.#capacity = capacity;
  }
  /** First entry for which cmp(entry) >= 0; sentinel after the last leaf at EOF. */
  #tailSeek(cmp: (entry: number) => number): number {
    let lo = 0,
      hi = this.#count;
    while (lo < hi) {
      const m = (lo + hi) >>> 1,
        leaf = need(this.#leaves[m]);
      if (cmp(leaf.entries[leaf.count - 1] ?? 0) < 0) lo = m + 1;
      else hi = m;
    }
    if (lo === this.#count) return lo * LEAF;
    const leaf = need(this.#leaves[lo]);
    let a = 0,
      b = leaf.count;
    while (a < b) {
      const m = (a + b) >>> 1;
      if (cmp(leaf.entries[m] ?? 0) < 0) a = m + 1;
      else b = m;
    }
    return lo * LEAF + a;
  }
  #tailAt(position: number): number {
    if (position < 0) return -1;
    const leaf = this.#leaves[Math.floor(position / LEAF)],
      at = position % LEAF;
    return leaf === undefined || at >= leaf.count ? -1 : (leaf.entries[at] ?? -1);
  }
  #tailStep(position: number, direction: number): number {
    const i = Math.floor(position / LEAF),
      at = position % LEAF;
    if (direction < 0) {
      if (at > 0) return position - 1;
      return i > 0 ? (i - 1) * LEAF + need(this.#leaves[i - 1]).count - 1 : -1;
    }
    return at + 1 < (this.#leaves[i]?.count ?? 0) ? position + 1 : (i + 1) * LEAF;
  }
  #tailFirst(direction: number): number {
    return direction > 0
      ? 0
      : this.#count === 0
        ? -1
        : (this.#count - 1) * LEAF + need(this.#leaves[this.#count - 1]).count - 1;
  }
  /** Negative positions −(entry+2) name the implicit prefix; nonnegative positions name leaves. */
  /** An already ordered append prefix is implicit entry ids, requiring no per-entry storage. */
  at(position: number): number {
    if (position < -1) {
      const e = -position - 2;
      return e < this.#linear ? e : -1;
    }
    return this.#tailAt(position);
  }
  #prefixSeek(cmp: (entry: number) => number): number {
    let lo = 0,
      hi = this.#linear;
    while (lo < hi) {
      const m = (lo + hi) >>> 1;
      if (cmp(m) < 0) lo = m + 1;
      else hi = m;
    }
    return lo < this.#linear ? -lo - 2 : -1;
  }
  #choose(a: number, b: number, direction: number): number {
    const x = this.at(a),
      y = this.at(b);
    if (x < 0) return y < 0 ? -1 : b;
    if (y < 0) return a;
    return this.compare(x, y) * direction < 0 ? a : b;
  }
  seek(cmp: (entry: number) => number): number {
    return this.#choose(this.#prefixSeek(cmp), this.#tailSeek(cmp), 1);
  }
  first(direction: number): number {
    return this.#choose(
      this.#linear === 0 ? -1 : direction > 0 ? -2 : -this.#linear - 1,
      this.#tailFirst(direction),
      direction,
    );
  }
  step(position: number, direction: number): number {
    if (position === -1) return direction < 0 ? this.first(-1) : -1;
    const entry = this.at(position);
    if (entry < 0) return direction < 0 ? this.first(-1) : -1;
    const cmp = (e: number) => {
      const c = this.compare(e, entry);
      return direction > 0 && c === 0 ? -1 : c;
    };
    let a: number, b: number;
    if (position < -1) {
      const e = entry + direction;
      a = e >= 0 && e < this.#linear ? -e - 2 : -1;
      if (this.#count === 0) return a;
      b = this.#tailSeek(cmp);
      if (direction < 0) b = this.#tailStep(b, -1);
    } else {
      b = this.#tailStep(position, direction);
      if (this.#linear === 0) return this.#tailAt(b) < 0 ? -1 : b;
      a = this.#prefixSeek(cmp);
      if (direction < 0) {
        const e = a === -1 ? this.#linear - 1 : -a - 3;
        a = e >= 0 ? -e - 2 : -1;
      }
    }
    return this.#choose(a, b, direction);
  }
  add(entry: number): void {
    if (
      this.#count === 0 &&
      entry === this.#linear &&
      (entry === 0 || this.compare(entry - 1, entry) < 0)
    ) {
      this.#linear++;
      this.version++;
      return;
    }
    const last = this.#leaves[this.#count - 1];
    const position =
      last !== undefined && this.compare(last.entries[last.count - 1] ?? 0, entry) < 0
        ? (this.#count - 1) * LEAF + last.count
        : this.#tailSeek((e) => this.compare(e, entry));
    let i = Math.floor(position / LEAF),
      at = position % LEAF;
    if (i === this.#count) {
      const last = this.#leaves[i - 1];
      if (last !== undefined && last.count < LEAF) {
        i--;
        at = last.count;
      } else {
        this.#grow();
        const leaf = this.#leaf();
        this.#leaves[this.#count++] = leaf;
      }
    }
    let leaf = need(this.#leaves[i]);
    if (leaf.count === LEAF) {
      this.#grow();
      const right = this.#leaf();
      right.entries.set(leaf.entries.subarray(LEAF / 2));
      right.count = LEAF / 2;
      for (let k = this.#count; k > i + 1; k--) this.#leaves[k] = this.#leaves[k - 1];
      this.#leaves[i + 1] = right;
      this.#count++;
      leaf.count = LEAF / 2;
      if (at >= LEAF / 2) {
        leaf = right;
        at -= LEAF / 2;
      }
    }
    leaf.entries.copyWithin(at + 1, at, leaf.count);
    leaf.entries[at] = entry;
    leaf.count++;
    this.version++;
  }
  remove(entry: number): void {
    if (entry < this.#linear) {
      if (entry !== this.#linear - 1)
        throw new TypeError("An implicit prefix is popped newest first");
      this.#linear--;
      this.version++;
      return;
    }
    const position = this.#tailSeek((e) => this.compare(e, entry));
    if (this.#tailAt(position) !== entry) throw new TypeError("Ordered delta entry is missing");
    const i = Math.floor(position / LEAF),
      at = position % LEAF,
      leaf = need(this.#leaves[i]);
    leaf.entries.copyWithin(at, at + 1, leaf.count);
    leaf.count--;
    if (leaf.count === 0) {
      for (let k = i; k + 1 < this.#count; k++) this.#leaves[k] = this.#leaves[k + 1];
      this.#leaves[--this.#count] = undefined;
      this.charge.free(LEAF_BYTES);
    }
    this.version++;
  }
}
