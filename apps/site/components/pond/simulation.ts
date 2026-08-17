/**
 * The pond, without anything that draws. A fish wanders, reads the banks, and breaks away from
 * the net; the net lags behind the pointer that swings it. Keeping this apart from the canvas is
 * what lets the one rule the whole thing rests on — the fish is never caught — be tested.
 */

export interface Net {
  /** Where the pointer is, and where the hoop has actually got to. It lags, which is the point. */
  toX: number;
  toY: number;
  x: number;
  y: number;
  /** How the hoop is travelling, in pixels a second. The fish reads this and leads its escape. */
  vx: number;
  vy: number;
  speed: number;
  tilt: number;
  radius: number;
  /** 0 out of the water, 1 fully in, moving between the two when the pointer enters or leaves. */
  depth: number;
  wanted: number;
  /** A press makes the net lunge; it decays back to zero. */
  lunge: number;
  trail: number;
}

export interface Fish {
  x: number;
  y: number;
  vx: number;
  vy: number;
  /** How steeply it is tipped, in radians. A minnow stays close to level. */
  angle: number;
  /**
   * Which way it is pointing, easing from 1 to -1 through 0. A fish turns by coming about, so it
   * narrows to nothing and opens out the other way rather than pivoting through nose-up.
   */
  facing: number;
  /** Nose to tail tip, in pixels. */
  length: number;
  cruise: number;
  dash: number;
  /** How far off it notices the net. */
  fear: number;
  /** 0 at the front of the pond, 1 at the back — drives size, speed and how solidly it is drawn. */
  distance: number;
  /** Tail beat phase, and the slower one behind the burst-and-glide of cruising. */
  tail: number;
  pulse: number;
  /** 1 just after a fright, falling back to 0. */
  startle: number;
  targetX: number;
  targetY: number;
  targetFor: number;
  wake: number;
}

export interface Ripple {
  x: number;
  y: number;
  r: number;
  grow: number;
  life: number;
  span: number;
}
export interface Bubble {
  x: number;
  y: number;
  r: number;
  rise: number;
  sway: number;
  phase: number;
}

export interface World {
  w: number;
  h: number;
  time: number;
  net: Net;
  hero: Fish;
  school: Fish[];
  ripples: Ripple[];
  bubbles: Bubble[];
  nextBubble: number;
}

export const TAU = Math.PI * 2;

/** How far back the shy ones hang. Five of them is a pond; more is an aquarium. */
const SCHOOL = [0.55, 0.72, 0.85, 0.62, 0.9];

export function clamp(value: number, low: number, high: number) {
  return value < low ? low : value > high ? high : value;
}

/** Move `angle` toward `want` the short way round, by at most `step`. */
export function turnToward(angle: number, want: number, step: number) {
  const delta = ((want - angle + Math.PI * 3) % TAU) - Math.PI;
  return angle + clamp(delta, -step, step);
}

function makeFish(w: number, h: number, distance: number): Fish {
  return {
    x: w * (0.2 + Math.random() * 0.6),
    y: h * (0.25 + Math.random() * 0.55),
    vx: 0,
    vy: 0,
    angle: 0,
    facing: Math.random() < 0.5 ? 1 : -1,
    length: h * 0.3 * (1 - distance * 0.62),
    cruise: h * (0.3 - distance * 0.13),
    dash: h * (2.4 - distance * 1.2),
    fear: h * (1.1 - distance * 0.45),
    distance,
    tail: Math.random() * TAU,
    pulse: Math.random() * TAU,
    startle: 0,
    targetX: w * 0.5,
    targetY: h * 0.5,
    targetFor: 0,
    wake: 0,
  };
}

export function createWorld(w: number, h: number): World {
  const hero = makeFish(w, h, 0);
  // The hero starts mid-pond on a lazy crossing, so the first thing seen is a fish swimming.
  hero.x = w * 0.62;
  hero.y = h * 0.5;
  hero.vx = -hero.cruise;
  hero.facing = -1;
  return {
    w,
    h,
    time: 0,
    net: {
      toX: w * 0.5,
      toY: h * 0.5,
      x: w * 0.5,
      y: h * 0.5,
      vx: 0,
      vy: 0,
      speed: 0,
      tilt: 0,
      radius: netRadius(h),
      depth: 0,
      wanted: 0,
      lunge: 0,
      trail: 0,
    },
    hero,
    // A narrow pond gets fewer of them, so a phone shows a pond rather than a shoal.
    school: SCHOOL.slice(0, w < 520 ? 3 : SCHOOL.length).map((distance) =>
      makeFish(w, h, distance),
    ),
    ripples: [],
    bubbles: [],
    nextBubble: 0.8,
  };
}

function netRadius(h: number) {
  return Math.max(26, h * 0.17);
}

/** A resize keeps everything where it was in the pond, proportionally, and scales it with depth. */
export function resizeWorld(world: World, w: number, h: number) {
  const sx = w / world.w;
  const sy = h / world.h;
  const grow = h / world.h;
  for (const fish of [world.hero, ...world.school]) {
    fish.x *= sx;
    fish.y *= sy;
    fish.targetX *= sx;
    fish.targetY *= sy;
    fish.length *= grow;
    fish.cruise *= grow;
    fish.dash *= grow;
    fish.fear *= grow;
  }
  world.net.x *= sx;
  world.net.y *= sy;
  world.net.toX *= sx;
  world.net.toY *= sy;
  world.net.radius = netRadius(h);
  world.w = w;
  world.h = h;
}

/** Somewhere new to be: off the banks, and — while a net is about — away from the net. */
function retarget(fish: Fish, world: World) {
  const margin = fish.length * 0.9;
  let bestX = world.w * 0.5;
  let bestY = world.h * 0.5;
  let bestScore = -Infinity;
  // Fish travel far across a pond and only drift up and down it, so a new place to be is mostly
  // somewhere else along the water rather than somewhere else in the depth of it.
  const rise = world.h * 0.3;
  for (let i = 0; i < 5; i += 1) {
    const x = margin + Math.random() * (world.w - margin * 2);
    const y = clamp(
      fish.y + (Math.random() - 0.5) * 2 * rise,
      margin,
      Math.max(margin, world.h - margin),
    );
    const fromHere = Math.hypot(x - fish.x, y - fish.y);
    const fromNet = world.net.depth > 0.05 ? Math.hypot(x - world.net.x, y - world.net.y) : 0;
    const score = fromHere + fromNet * world.net.depth * 1.4;
    if (score > bestScore) {
      bestX = x;
      bestY = y;
      bestScore = score;
    }
  }
  fish.targetX = bestX;
  fish.targetY = bestY;
  fish.targetFor = 4 + Math.random() * 5;
}

/**
 * Which way to cut. A startled fish does not reverse away in a straight line, it breaks sideways,
 * so take the side it is already turning toward unless that side runs it into the bank.
 */
function pickSide(fish: Fish, awayX: number, awayY: number, world: World) {
  const px = -awayY;
  const py = awayX;
  let side = fish.vx * px + fish.vy * py >= 0 ? 1 : -1;
  const probe = fish.length * 2.4;
  const openness = (s: number) => {
    const x = fish.x + px * s * probe;
    const y = fish.y + py * s * probe;
    return Math.min(x, world.w - x, y, world.h - y);
  };
  if (openness(side) < fish.length && openness(-side) > openness(side)) side = -side;
  return side;
}

export function swim(fish: Fish, world: World, dt: number) {
  const { w, h, net } = world;

  fish.targetFor -= dt;
  fish.pulse += dt * (1.1 + fish.distance * 0.4);
  if (fish.targetFor <= 0 || Math.hypot(fish.targetX - fish.x, fish.targetY - fish.y) < fish.length)
    retarget(fish, world);

  // Cruising is bursts and glides rather than a constant crawl.
  const glide = 0.72 + 0.5 * (0.5 + 0.5 * Math.sin(fish.pulse));
  const dx = fish.targetX - fish.x;
  const dy = fish.targetY - fish.y;
  const d = Math.hypot(dx, dy) || 1;
  let wantX = (dx / d) * fish.cruise * glide;
  // Easier along the pond than up and down it, so a crossing is a long shallow line.
  let wantY = (dy / d) * fish.cruise * glide * 0.6;
  let agility = 1.5;
  let top = fish.cruise * 1.4;
  let fleeing = 0;

  if (net.depth > 0.02) {
    const nd = Math.hypot(fish.x - net.x, fish.y - net.y) || 1;
    const reach = fish.fear * (1 + net.lunge * 0.35);
    if (nd < reach) {
      const urgency = clamp((1 - nd / reach) * net.depth, 0, 1);
      const ax = (fish.x - net.x) / nd;
      const ay = (fish.y - net.y) / nd;
      const side = pickSide(fish, ax, ay, world);
      // Nothing outswims something coming straight down the same line, so the more directly the
      // net is bearing down, the more of the escape is a break across it rather than ahead of it.
      const closing = clamp((net.vx * -ax + net.vy * -ay) / Math.max(net.speed, 1), 0, 1);
      const mix = clamp(0.45 + 0.5 * urgency - 0.5 * closing, 0.1, 1);
      const ex = ax * mix + -ay * side * (1 - mix) * 1.5;
      const ey = ay * mix + ax * side * (1 - mix) * 1.5;
      const el = Math.hypot(ex, ey) || 1;
      // Whatever speed the net is swung at, the fish leaves at more than it.
      const own = fish.cruise + (fish.dash - fish.cruise) * urgency ** 0.35;
      const speed = Math.max(own, urgency > 0.2 ? net.speed * 1.3 : 0);
      wantX = (ex / el) * speed;
      wantY = (ey / el) * speed;
      agility = 2 + 18 * urgency;
      top = Math.max(fish.dash, net.speed * 1.5);
      fleeing = urgency;
      fish.startle = Math.max(fish.startle, urgency);
      fish.targetFor = Math.min(fish.targetFor, 0.35);
    }
  }

  // The banks: read early and turned away from, not bounced off.
  const margin = fish.length * 1.3;
  const push = h * (1.1 + fleeing);
  if (fish.x < margin) wantX += (1 - fish.x / margin) * push;
  if (fish.x > w - margin) wantX -= (1 - (w - fish.x) / margin) * push;
  if (fish.y < margin * 0.9) wantY += (1 - fish.y / (margin * 0.9)) * push;
  if (fish.y > h - margin * 0.9) wantY -= (1 - (h - fish.y) / (margin * 0.9)) * push;

  const ease = 1 - Math.exp(-agility * dt);
  fish.vx += (wantX - fish.vx) * ease;
  fish.vy += (wantY - fish.vy) * ease;
  let speed = Math.hypot(fish.vx, fish.vy);
  if (speed > top) {
    fish.vx *= top / speed;
    fish.vy *= top / speed;
    speed = top;
  }
  fish.x += fish.vx * dt;
  fish.y += fish.vy * dt;

  const edge = fish.length * 0.5;
  const lowY = edge * 0.8;
  if (fish.x < edge) {
    fish.x = edge;
    fish.vx = Math.abs(fish.vx) * 0.35;
  }
  if (fish.x > w - edge) {
    fish.x = w - edge;
    fish.vx = -Math.abs(fish.vx) * 0.35;
  }
  if (fish.y < lowY) {
    fish.y = lowY;
    fish.vy = Math.abs(fish.vy) * 0.35;
  }
  if (fish.y > h - lowY) {
    fish.y = h - lowY;
    fish.vy = -Math.abs(fish.vy) * 0.35;
  }

  // A pointer can be flicked faster than anything can swim. If the hoop has come down over the
  // fish anyway, it slips out of the rim. Straight away is tried first and then to either side,
  // so it leaves by the nearest opening and a bank behind it is no trap.
  if (net.depth > 0.4) {
    const nx = fish.x - net.x;
    const ny = fish.y - net.y;
    const nd = Math.hypot(nx, ny);
    const safe = net.radius + fish.length * 0.4;
    if (nd < safe) {
      const away = nd > 0.001 ? Math.atan2(ny, nx) : fish.angle + Math.PI;
      let bestX = fish.x;
      let bestY = fish.y;
      let bestGap = nd;
      for (let i = 0; i < 16 && bestGap < safe - 0.01; i += 1) {
        const angle = away + (((i + 1) >> 1) * TAU * (i % 2 === 0 ? 1 : -1)) / 16;
        const x = clamp(net.x + Math.cos(angle) * safe, edge, w - edge);
        const y = clamp(net.y + Math.sin(angle) * safe, lowY, h - lowY);
        const gap = Math.hypot(x - net.x, y - net.y);
        if (gap > bestGap) {
          bestGap = gap;
          bestX = x;
          bestY = y;
        }
      }
      const ox = bestX - fish.x;
      const oy = bestY - fish.y;
      const ol = Math.hypot(ox, oy) || 1;
      fish.x = bestX;
      fish.y = bestY;
      fish.vx += (ox / ol) * fish.dash * 0.5;
      fish.vy += (oy / ol) * fish.dash * 0.5;
      fish.startle = 1;
    }
  }

  const beat = clamp(speed / fish.dash, 0, 1);
  if (speed > 1) {
    // It tips to climb or dive, and tips further when bolting, but never stands on its nose.
    const limit = 0.5 + 0.6 * fleeing;
    const pitch = clamp(Math.atan2(fish.vy, Math.abs(fish.vx)), -limit, limit);
    fish.angle = turnToward(fish.angle, pitch, (5 + 12 * fleeing) * dt);
    if (Math.abs(fish.vx) > fish.cruise * 0.15) {
      const want = fish.vx >= 0 ? 1 : -1;
      const rate = (3.5 + 4 * fleeing) * dt;
      fish.facing = clamp(fish.facing + Math.sign(want - fish.facing) * rate, -1, 1);
    }
  }
  fish.tail += dt * (2.2 + 9 * beat) * TAU;
  fish.startle = Math.max(0, fish.startle - dt * 0.85);

  // A hard turn leaves something behind it in the water.
  fish.wake -= dt;
  if (fish.startle > 0.35 && fish.wake <= 0) {
    fish.wake = 0.12;
    const back = Math.hypot(fish.vx, fish.vy) || 1;
    world.ripples.push({
      x: fish.x - (fish.vx / back) * fish.length * 0.5,
      y: fish.y - (fish.vy / back) * fish.length * 0.5,
      r: fish.length * 0.12,
      grow: fish.length * 1.1,
      life: 1,
      span: 0.9,
    });
    if (Math.random() < 0.35 * fish.startle)
      world.bubbles.push({
        x: fish.x,
        y: fish.y,
        r: 1 + Math.random() * 2,
        rise: 22 + Math.random() * 26,
        sway: 4 + Math.random() * 8,
        phase: Math.random() * TAU,
      });
  }
}

export function step(world: World, dt: number) {
  const { net } = world;
  world.time += dt;

  const fromX = net.x;
  const fromY = net.y;
  const follow = 1 - Math.exp(-22 * dt);
  net.x += (net.toX - net.x) * follow;
  net.y += (net.toY - net.y) * follow;
  const moved = Math.hypot(net.x - fromX, net.y - fromY);
  const settle = 1 - Math.exp(-12 * dt);
  net.vx += ((net.x - fromX) / dt - net.vx) * settle;
  net.vy += ((net.y - fromY) / dt - net.vy) * settle;
  net.speed = Math.hypot(net.vx, net.vy);
  net.depth += (net.wanted - net.depth) * (1 - Math.exp(-9 * dt));
  net.lunge = Math.max(0, net.lunge - dt * 2.2);
  net.tilt += (clamp((net.x - fromX) / dt / 900, -0.5, 0.5) - net.tilt) * (1 - Math.exp(-7 * dt));

  // The net drags a trail of disturbed water behind it.
  net.trail += moved;
  if (net.depth > 0.2 && net.trail > 48) {
    net.trail = 0;
    world.ripples.push({
      x: net.x + (Math.random() - 0.5) * net.radius,
      y: net.y + net.radius * 0.5,
      r: net.radius * 0.15,
      grow: net.radius * 1.1,
      life: 1,
      span: 0.8,
    });
  }

  swim(world.hero, world, dt);
  for (const fish of world.school) swim(fish, world, dt);

  for (let i = world.ripples.length - 1; i >= 0; i -= 1) {
    const ripple = world.ripples[i];
    if (ripple === undefined) continue;
    ripple.life -= dt / ripple.span;
    ripple.r += ripple.grow * dt;
    if (ripple.life <= 0) world.ripples.splice(i, 1);
  }

  world.nextBubble -= dt;
  if (world.nextBubble <= 0) {
    world.nextBubble = 1.2 + Math.random() * 2.6;
    world.bubbles.push({
      x: world.w * (0.1 + Math.random() * 0.8),
      y: world.h * (0.85 + Math.random() * 0.15),
      r: 1.2 + Math.random() * 2.6,
      rise: 16 + Math.random() * 22,
      sway: 5 + Math.random() * 9,
      phase: Math.random() * TAU,
    });
  }
  for (let i = world.bubbles.length - 1; i >= 0; i -= 1) {
    const bubble = world.bubbles[i];
    if (bubble === undefined) continue;
    bubble.y -= bubble.rise * dt;
    bubble.phase += dt * 2.4;
    if (bubble.y < 6) world.bubbles.splice(i, 1);
  }
}
