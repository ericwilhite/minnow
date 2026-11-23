/** SCRATCH (never commit): a cheap ring buffer of state transitions, dumped on a stall. */
const SIZE = 8192;
const ring: string[] = new Array<string>(SIZE);
let at = 0;
export function trace(event: string): void {
  ring[at % SIZE] = `${performance.now().toFixed(1)} ${event}`;
  at++;
}
export function dumpTrace(): string {
  const out: string[] = [];
  for (let i = Math.max(0, at - SIZE); i < at; i++) out.push(ring[i % SIZE] ?? "");
  return out.join("\n");
}
export function resetTrace(): void {
  at = 0;
}
