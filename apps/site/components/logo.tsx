/**
 * The minnow's geometry, in the 100×62 box the mark is drawn in. The wordmark renders these as
 * SVG and the hero's pond draws the same paths onto a canvas, so there is only ever one fish.
 */
export const MINNOW = {
  /** Body: a lozenge that reads as a fish at 16 pixels as well as at 160. */
  body: "M30 31 C 38 17, 62 15, 74 24 C 80 28, 82 31, 82 31 C 82 31, 80 34, 74 38 C 62 47, 38 45, 30 31 Z",
  tail: "M30 31 L 14 20 L 18 31 L 14 42 Z",
  /** Where the tail joins the body, and so what it swings around. */
  pivot: { x: 30, y: 31 },
  nose: { x: 82, y: 31 },
  eye: { x: 70, y: 28, r: 2.1 },
  /** Midpoint of tail tip and nose, used to place the fish by its middle. */
  center: { x: 48, y: 31 },
  /** Tail tip to nose. */
  length: 68,
};

/**
 * The mark itself. A fish drawn once and reused at every size — the wordmark in the navbar and
 * the favicon both render this. `swim` animates the tail.
 */
export function Logo({ size = 26, swim = true }: { size?: number; swim?: boolean }) {
  return (
    <svg
      width={size}
      height={size * 0.62}
      viewBox="0 0 100 62"
      fill="none"
      aria-hidden="true"
      className={swim ? "minnow-swim" : undefined}
    >
      <g fill="currentColor">
        <path d={MINNOW.body} />
        {/* Tail, animated from its own origin so the body stays still. */}
        <path className="minnow-tail" d={MINNOW.tail} />
      </g>
      <circle cx={MINNOW.eye.x} cy={MINNOW.eye.y} r={MINNOW.eye.r} className="minnow-eye" />
    </svg>
  );
}
