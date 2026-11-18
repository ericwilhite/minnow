/** ARE submatch dissection after the bounded VM has selected the whole match.
 * No pattern backreferences: membership is independent of captures. See PostgreSQL
 * regcomp.c's positive-repeat prefix/final-atom transformation and regexec.c dissection. */
export type RegexNode =
  | { kind: "character"; matches: (value: string) => boolean }
  | { kind: "assert"; accepts: (input: string, position: number) => boolean }
  | { kind: "sequence"; children: RegexNode[] }
  | { kind: "alternative"; children: RegexNode[] }
  | { kind: "group"; child: RegexNode; group: number }
  | { kind: "repeat"; child: RegexNode; min: number; max: number };
export type RegexInstruction =
  | { kind: "character"; matches: (value: string) => boolean; next: number }
  | { kind: "assert"; accepts: (input: string, position: number) => boolean; next: number }
  | { kind: "save"; slot: number; next: number }
  | { kind: "split"; first: number; second: number }
  | { kind: "match" };
export interface RegexRange {
  readonly start: number;
  readonly stop: number;
}
export interface RegexNodeRange extends RegexRange {
  readonly captures: boolean;
  /** Fixed Unicode-code-point width, or null when variable. */
  readonly width: number | null;
  readonly children: readonly RegexRange[];
  readonly prefix: RegexRange | null;
}
function previous(input: string, at: number): number {
  const low = input.charCodeAt(at - 1),
    high = input.charCodeAt(at - 2);
  return at - (low >= 0xdc00 && low <= 0xdfff && high >= 0xd800 && high <= 0xdbff ? 2 : 1);
}
/** All memo cells/VM arrivals use the unchanged match state/work ceilings. No host
 * backtracking, input-sized copies, recursive sequence walk or uncapped repetition. */
export function captureSpans(
  root: RegexNode,
  ranges: ReadonlyMap<RegexNode, RegexNodeRange>,
  program: readonly RegexInstruction[],
  input: string,
  begin: number,
  end: number,
  slots: number,
  spend: () => void,
): number[] {
  const memo = new Map<string, boolean>(),
    captures = new Array<number>(slots).fill(-1);
  const range = (node: RegexNode) => {
    const value = ranges.get(node);
    if (!value) throw new Error("Missing regex dissection range");
    return value;
  };
  const exact = (part: RegexRange, from: number, to: number): boolean => {
    spend();
    const key = `${String(part.start)}:${String(part.stop)}:${String(from)}:${String(to)}`;
    const cached = memo.get(key);
    if (cached !== undefined) return cached;
    const visited = new Set<number>(),
      stack = [{ pc: part.start, position: from }];
    let found = false;
    search: while (stack.length) {
      const state = stack.pop();
      if (!state) throw new Error("Missing regex state");
      let { pc, position } = state;
      for (;;) {
        spend();
        if (pc === part.stop) {
          if (position === to) {
            found = true;
            break search;
          }
          break;
        }
        const cell = position * 65536 + pc;
        if (visited.has(cell)) break;
        if (visited.size + memo.size >= 65536)
          throw new RangeError("Regular expression match state limit exceeded");
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
        if (point === undefined || position >= to) break;
        const character = String.fromCodePoint(point);
        if (position + character.length > to || !op.matches(character)) break;
        position += character.length;
        pc = op.next;
      }
    }
    if (visited.size + memo.size >= 65536)
      throw new RangeError("Regular expression match state limit exceeded");
    memo.set(key, found);
    return found;
  };
  const clear = (node: RegexNode): void => {
    spend();
    if (node.kind === "group") {
      captures[node.group * 2] = -1;
      captures[node.group * 2 + 1] = -1;
      clear(node.child);
    } else if (node.kind === "repeat") clear(node.child);
    else if (node.kind === "sequence" || node.kind === "alternative")
      for (const child of node.children) clear(child);
  };
  const visit = (node: RegexNode, from: number, to: number): void => {
    spend();
    const info = range(node);
    if (!info.captures) return;
    if (node.kind === "character" || node.kind === "assert") return;
    if (node.kind === "group") {
      visit(node.child, from, to);
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
        let middle = to;
        for (;;) {
          if (exact(part, position, middle) && exact({ start: tail, stop: info.stop }, middle, to))
            break;
          if (middle === position) throw new Error("Invalid regex sequence dissection");
          middle = previous(input, middle);
        }
        visit(child, position, middle);
        position = middle;
      }
      return;
    } else if (node.min > 0) {
      // Put capture-free repetitions before the final capturing atom. The prefix
      // takes priority; captures belong only to that final atom, including NULLs.
      const prefix = info.prefix;
      if (!prefix) throw new Error("Missing regex repeat prefix");
      let middle = to;
      for (;;) {
        if (exact(prefix, from, middle) && exact(range(node.child), middle, to)) break;
        if (middle === from) throw new Error("Invalid regex repeat dissection");
        middle = previous(input, middle);
      }
      clear(node.child);
      visit(node.child, middle, to);
      return;
    } else {
      clear(node.child);
      if (node.max === 0) return;
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
        let middle = to;
        for (;;) {
          if (exact(part, position, middle) && exact(tail, middle, to)) break;
          if (middle === position) return;
          middle = previous(input, middle);
        }
        clear(node.child);
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
