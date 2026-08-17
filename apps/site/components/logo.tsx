/**
 * The minnow. A fish drawn once and reused at every size — the wordmark in the navbar, the hero,
 * and the favicon all render this. `swim` animates the tail; `water` adds the surface it swims
 * under, which only the hero asks for.
 */
export function Logo({
  size = 26,
  swim = true,
  water = false,
}: {
  size?: number;
  swim?: boolean;
  water?: boolean;
}) {
  return (
    <svg
      width={size}
      height={size * 0.62}
      viewBox="0 0 100 62"
      fill="none"
      aria-hidden="true"
      className={swim ? "minnow-swim" : undefined}
    >
      {water ? (
        <g className="minnow-water" stroke="currentColor" strokeOpacity="0.18" fill="none">
          <path d="M2 10 q 10 -6 20 0 t 20 0 t 20 0 t 20 0 t 16 0" strokeWidth="1.5" />
          <path d="M2 18 q 12 -5 24 0 t 24 0 t 24 0 t 24 0" strokeWidth="1" strokeOpacity="0.1" />
        </g>
      ) : null}
      <g fill="currentColor">
        {/* Body: a lozenge that reads as a fish at 16 pixels as well as at 160. */}
        <path d="M30 31 C 38 17, 62 15, 74 24 C 80 28, 82 31, 82 31 C 82 31, 80 34, 74 38 C 62 47, 38 45, 30 31 Z" />
        {/* Tail, animated from its own origin so the body stays still. */}
        <path className="minnow-tail" d="M30 31 L 14 20 L 18 31 L 14 42 Z" />
      </g>
      <circle cx="70" cy="28" r="2.1" className="minnow-eye" />
    </svg>
  );
}
