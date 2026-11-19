import type { RegexInput } from "./input.js";

/** ARE submatch dissection after the bounded VM has selected the whole match.
 * No pattern backreferences: membership is independent of captures. See PostgreSQL
 * regcomp.c's positive-repeat prefix/final-atom transformation and regexec.c dissection. */
export type RegexNode =
  | { kind: "character"; matches: (value: string) => boolean }
  | { kind: "assert"; accepts: (input: RegexInput, position: number) => boolean }
  | { kind: "sequence"; children: RegexNode[] }
  | { kind: "alternative"; children: RegexNode[] }
  | { kind: "group"; child: RegexNode; group: number }
  | { kind: "repeat"; child: RegexNode; min: number; max: number };
export type RegexInstruction =
  | { kind: "character"; matches: (value: string) => boolean; next: number }
  | { kind: "assert"; accepts: (input: RegexInput, position: number) => boolean; next: number }
  | { kind: "save"; slot: number; next: number }
  | { kind: "split"; first: number; second: number }
  | { kind: "match" };
export interface RegexRange {
  readonly start: number;
  readonly stop: number;
}
export interface RegexNodeRange extends RegexRange {
  readonly captures: boolean;
  readonly minimum: number;
  readonly firstGroup: number;
  readonly lastGroup: number;
  /** Union of consuming predicates; rejection proves a prefix cannot extend. */
  readonly alphabet: (value: string, spend: () => void) => boolean;
  /** Fixed Unicode-code-point width, or null when variable. */
  readonly width: number | null;
  readonly children: readonly RegexRange[];
  readonly prefix: RegexRange | null;
}
/** A valid whole-match path. It is evidence for membership, never submatch priority. */
export interface MatchTrace {
  readonly predecessors: Map<number, number>;
  last: number;
}
function previous(input: RegexInput, at: number): number {
  return input.previous(at);
}
/** All memo cells/VM arrivals use the unchanged match state/work ceilings. No host
 * backtracking, input-sized copies, recursive sequence walk or uncapped repetition.
 * For a fixed pattern, dissection is at most cubic in match length before these
 * ceilings: child visits partition intervals (an empty iteration terminates), so
 * there are O(n) splits per AST node. Each split pins one prefix walk and probes
 * at most n suffix starts; each walk visits O(n) VM states for a fixed program.
 * No candidate endpoint restarts its prefix. Cache eviction cannot unpin it. */
export function captureSpans(
  root: RegexNode,
  ranges: ReadonlyMap<RegexNode, RegexNodeRange>,
  program: readonly RegexInstruction[],
  input: RegexInput,
  begin: number,
  end: number,
  slots: number,
  spend: () => void,
  trace?: MatchTrace,
): number[] {
  const memo = new Map<string, { ends: ReadonlySet<number>; maximum: number }>(),
    captures = new Array<number>(slots).fill(-1);
  // Index just one accepted path in one backwards pass. Shared suffix proofs
  // avoid re-running the full tail at every nested sequence boundary.
  const ordinals = new Map<number, number>(),
    occurrences = new Map<number, number[]>();
  if (trace) {
    let key = trace.last,
      ordinal = 0;
    while (key >= 0) {
      spend();
      const pc = key % 65536,
        list = occurrences.get(pc) ?? [];
      list.push(key);
      occurrences.set(pc, list);
      ordinals.set(key, ordinal++);
      const parent = trace.predecessors.get(key);
      if (parent === undefined) throw new Error("Missing regex match predecessor");
      trace.predecessors.delete(key);
      key = parent;
    }
    trace.predecessors.clear();
  }
  const witness = (part: RegexRange, from: number): number | undefined => {
    if (part.start === part.stop) return from;
    const ordinal = ordinals.get(from * 65536 + part.start),
      list = occurrences.get(part.stop);
    if (ordinal === undefined || !list) return undefined;
    let lo = 0,
      hi = list.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1,
        at = list[mid];
      if (at === undefined) throw new Error("Missing regex witness occurrence");
      const order = ordinals.get(at);
      if (order === undefined) throw new Error("Missing regex witness ordinal");
      if (order <= ordinal) lo = mid + 1;
      else hi = mid;
    }
    const key = list[lo - 1];
    return key === undefined ? undefined : Math.floor(key / 65536);
  };
  const range = (node: RegexNode) => {
    const value = ranges.get(node);
    if (!value) throw new Error("Missing regex dissection range");
    return value;
  };
  // Cache endpoints by range/start, not (range,start,candidate end). A greedy
  // split can probe thousands of decreasing ends; each shares this one walk.
  let memoCells = 0;
  let pinned: string | null = null;
  const room = (working: number): void => {
    while (memoCells + working > 65536 && memo.size) {
      let removed = false;
      for (const [key, answer] of memo)
        if (key !== pinned) {
          memo.delete(key);
          memoCells -= answer.ends.size + 1;
          removed = true;
          break;
        }
      if (!removed) break;
    }
    if (memoCells + working > 65536)
      throw new RangeError("Regular expression match state limit exceeded");
  };
  const keyOf = (part: RegexRange, from: number): string =>
    `${String(part.start)}:${String(part.stop)}:${String(from)}`;
  const endpoints = (part: RegexRange, from: number) => {
    spend();
    const key = keyOf(part, from),
      cached = memo.get(key);
    if (cached) return cached;
    let maximum = -1;
    const visited = new Set<number>(),
      ends = new Set<number>(),
      stack = [{ pc: part.start, position: from }];
    while (stack.length) {
      const state = stack.pop();
      if (!state) throw new Error("Missing regex state");
      let { pc, position } = state;
      for (;;) {
        spend();
        if (pc === part.stop) {
          if (!ends.has(position)) {
            room(visited.size + ends.size + 1);
            ends.add(position);
            maximum = Math.max(maximum, position);
          }
          break;
        }
        const cell = position * 65536 + pc;
        if (visited.has(cell)) break;
        room(visited.size + ends.size + 1);
        visited.add(cell);
        const op = program[pc];
        if (!op) throw new Error("Missing regex instruction");
        if (op.kind === "match") break;
        if (op.kind === "save") {
          pc = op.next;
          continue;
        }
        if (op.kind === "split") {
          if (stack.length >= 65536)
            throw new RangeError("Regular expression branch limit exceeded");
          stack.push({ pc: op.second, position });
          pc = op.first;
          continue;
        }
        if (op.kind === "assert") {
          if (!op.accepts(input, position)) break;
          pc = op.next;
          continue;
        }
        const point = input.codePointAt(position);
        if (point === undefined || position >= end) break;
        const next = input.advance(position, point);
        if (next > end || !op.matches(String.fromCodePoint(point))) break;
        position = next;
        pc = op.next;
      }
    }
    // Evict optional old answers before refusing the active walk. Count retained
    // endpoints and keys, not just the number of sets, under the same state bound.
    visited.clear();
    room(ends.size + 1);
    const answer = { ends, maximum };
    memo.set(key, answer);
    memoCells += ends.size + 1;
    return answer;
  };
  const exact = (part: RegexRange, from: number, to: number): boolean => {
    spend();
    if (from > to || to > end) return false;
    if (part.start === part.stop) return from === to;
    return witness(part, from) === to || endpoints(part, from).ends.has(to);
  };
  const cannotExtend = (node: RegexNode | undefined, at: number, to: number): boolean => {
    if (!node || at >= to) return false;
    const predicate = range(node).alphabet;
    const point = input.codePointAt(at);
    return point === undefined || !predicate(String.fromCodePoint(point), spend);
  };
  const split = (
    part: RegexRange,
    tail: RegexRange,
    from: number,
    to: number,
    node?: RegexNode,
  ): number | null => {
    spend();
    const known = witness(part, from);
    if (
      known !== undefined &&
      (known === to || cannotExtend(node, known, to)) &&
      witness(tail, known) === to
    )
      return known;
    const answer = endpoints(part, from);
    pinned = keyOf(part, from);
    try {
      let middle = Math.min(to, answer.maximum);
      while (middle >= from) {
        spend();
        if (answer.ends.has(middle) && (witness(tail, middle) === to || exact(tail, middle, to)))
          return middle;
        if (middle === from) break;
        middle = previous(input, middle);
      }
      return null;
    } finally {
      pinned = null;
    }
  };
  const clear = (info: RegexNodeRange): void => {
    spend();
    // Group IDs are contiguous within a subtree. Never walk capture-free AST
    // siblings just to clear at most 32 slots.
    for (let group = info.firstGroup; group > 0 && group <= info.lastGroup; group++)
      if (captures[group * 2] !== -1 || captures[group * 2 + 1] !== -1) {
        spend();
        captures[group * 2] = -1;
        captures[group * 2 + 1] = -1;
      }
  };
  const visit = (node: RegexNode, from: number, to: number): void => {
    spend();
    const info = range(node);
    if (!info.captures) return;
    if (node.kind === "character" || node.kind === "assert") return;
    if (node.kind === "group") {
      if (range(node.child).captures) visit(node.child, from, to);
      captures[node.group * 2] = from;
      captures[node.group * 2 + 1] = to;
      return;
    }
    if (node.kind === "alternative") {
      for (const child of node.children)
        if (exact(range(child), from, to)) {
          visit(child, from, to);
          return;
        }
    } else if (node.kind === "sequence") {
      let position = from;
      for (let i = 0; i < node.children.length; i++) {
        const child = node.children[i],
          part = info.children[i],
          tail = info.children[i + 1]?.start ?? info.stop;
        if (!child || !part) throw new Error("Missing regex sequence part");
        const fixed = range(child).width;
        let middle: number;
        if (fixed !== null) {
          middle = position;
          for (let n = 0; n < fixed; n++) {
            spend();
            const point = input.codePointAt(middle);
            if (point === undefined || middle >= to)
              throw new Error("Invalid fixed regex dissection");
            middle = input.advance(middle, point);
          }
        } else {
          const selected = split(part, { start: tail, stop: info.stop }, position, to, child);
          if (selected === null) throw new Error("Invalid regex sequence dissection");
          middle = selected;
        }
        if (range(child).captures) visit(child, position, middle);
        position = middle;
      }
      return;
    } else if (node.min > 0) {
      const fixed = range(node.child).width;
      if (fixed !== null && fixed > 0) {
        let last = to;
        for (let i = 0; i < fixed; i++) {
          spend();
          last = previous(input, last);
        }
        if (last < from) throw new Error("Invalid fixed regex repeat dissection");
        clear(info);
        visit(node.child, last, to);
        return;
      }
      // For one-or-more unbounded atoms, an accepted loop boundary at the
      // child's minimum width is the greatest possible prefix. Its path proves
      // both memberships, so do not replay the whole prefix to rediscover it.
      const minimum = range(node.child).minimum;
      if (node.min === 1 && node.max === Infinity && minimum > 0 && witness(info, from) === to) {
        let last = to;
        for (let i = 0; i < minimum; i++) {
          spend();
          last = previous(input, last);
        }
        const loop = info.prefix === null ? undefined : program[info.prefix.start];
        if (
          last >= from &&
          loop?.kind === "split" &&
          info.prefix !== null &&
          (last === from || witness({ start: loop.first, stop: info.prefix.start }, last) === to)
        ) {
          clear(info);
          visit(node.child, last, to);
          return;
        }
      }
      // Put capture-free repetitions before the final capturing atom. The prefix
      // takes priority; captures belong only to that final atom, including NULLs.
      const prefix = info.prefix;
      if (!prefix) throw new Error("Missing regex repeat prefix");
      const middle = split(prefix, range(node.child), from, to);
      if (middle === null) throw new Error("Invalid regex repeat dissection");
      clear(info);
      visit(node.child, middle, to);
      return;
    } else {
      clear(info);
      if (node.max === 0 || (from === to && range(node.child).minimum > 0)) return;
      const fixed = range(node.child).width;
      // A fixed-width atom has exactly one possible final iteration. Previous
      // iterations cannot contribute subcaptures, and need not be dissected.
      if (fixed !== null && fixed > 0 && from < to) {
        let last = to;
        for (let i = 0; i < fixed; i++) {
          spend();
          last = previous(input, last);
        }
        visit(node.child, last, to);
        return;
      }
      let position = from,
        pc = info.start;
      for (let count = 0; count < node.max; count++) {
        const choice = program[pc];
        if (choice?.kind !== "split") break;
        const remaining = node.max === Infinity ? pc : choice.second;
        const part = { start: choice.first, stop: remaining },
          tail = { start: remaining, stop: info.stop };
        const middle = split(part, tail, position, to);
        if (middle === null) return;
        clear(info);
        visit(node.child, position, middle);
        position = middle;
        if (position === to) return;
        pc = remaining;
      }
      if (position === to) return;
    }
    throw new Error("Invalid regex submatch dissection");
  };
  captures[0] = begin;
  captures[1] = end;
  visit(root, begin, end);
  return captures;
}
