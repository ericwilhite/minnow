"use client";
/**
 * The hero's pond: the minnow from the wordmark, given water to swim in. Inside the water the
 * pointer is a net, and the fish is quicker than whoever is holding it.
 *
 * This half draws. Colours are read from the CSS variables on the container so the scene follows
 * the theme, and the loop runs only while the pond is on screen and the visitor has not asked for
 * less motion. `simulation.ts` holds everything that moves.
 */
import { useEffect, useRef, useState } from "react";
import { MINNOW } from "../logo";
import {
  clamp,
  createWorld,
  resizeWorld,
  step,
  TAU,
  type Fish,
  type Net,
  type World,
} from "./simulation";

/** Path2D exists only in the browser, so the fish is built the first time one is drawn. */
let paths: { body: Path2D; tail: Path2D } | null = null;
function minnowPaths() {
  paths ??= { body: new Path2D(MINNOW.body), tail: new Path2D(MINNOW.tail) };
  return paths;
}

interface Palette {
  surface: string;
  deep: string;
  floor: string;
  fish: string;
  school: string;
  weed: string;
  weedBack: string;
  glow: string;
  glowFade: string;
  net: string;
  foam: string;
  eye: string;
}

/**
 * Kelp, rooted along the floor. Most of it stands behind the fish; the last stand is in front, so
 * there is something for the minnow to pass behind. Positions are fractions of the pond, so a
 * resize moves the plants with the water rather than stranding them.
 */
const WEEDS = [
  { at: 0.07, height: 0.62, blades: 6, spread: 0.05, front: false },
  { at: 0.23, height: 0.34, blades: 4, spread: 0.03, front: false },
  { at: 0.46, height: 0.22, blades: 3, spread: 0.025, front: false },
  { at: 0.71, height: 0.45, blades: 5, spread: 0.04, front: false },
  { at: 0.93, height: 0.7, blades: 7, spread: 0.055, front: true },
];

/** Repeatable jitter, so every blade differs but none of them move when the page reloads. */
function scatter(seed: number) {
  const n = Math.sin(seed * 12.9898) * 43758.5453;
  return n - Math.floor(n);
}

function readPalette(el: HTMLElement): Palette {
  const style = getComputedStyle(el);
  const read = (name: string) => style.getPropertyValue(name).trim();
  return {
    surface: read("--pond-surface"),
    deep: read("--pond-deep"),
    floor: read("--pond-floor"),
    fish: read("--pond-fish"),
    school: read("--pond-school"),
    weed: read("--pond-weed"),
    weedBack: read("--pond-weed-back"),
    glow: read("--pond-glow"),
    glowFade: read("--pond-glow-fade"),
    net: read("--pond-net"),
    foam: read("--pond-foam"),
    eye: read("--pond-eye"),
  };
}

/** One stand of kelp, leaning on the current and pushed aside by anything that passes close. */
function drawWeed(
  ctx: CanvasRenderingContext2D,
  world: World,
  weed: (typeof WEEDS)[number],
  colour: string,
  alpha: number,
) {
  const { w, h, time } = world;
  ctx.fillStyle = colour;
  for (let i = 0; i < weed.blades; i += 1) {
    const seed = weed.at * 100 + i;
    const offset = weed.blades === 1 ? 0 : i / (weed.blades - 1) - 0.5;
    const x = w * (weed.at + offset * weed.spread);
    const length = h * weed.height * (0.55 + scatter(seed) * 0.55);
    const width = Math.max(3, h * 0.028 * (0.7 + scatter(seed + 7) * 0.6));
    // Each blade grows with a lean of its own, and the water moves the whole stand together.
    const curl = (scatter(seed + 11) - 0.5) * length * 0.55;
    const sway = Math.sin(time * (0.45 + scatter(seed + 3) * 0.3) + seed) * length * 0.18;

    // Something swimming through drags the blades over as it goes.
    let shove = 0;
    for (const passer of [
      { x: world.net.x, y: world.net.y, force: world.net.depth * length * 0.3 },
      { x: world.hero.x, y: world.hero.y, force: length * 0.18 },
    ]) {
      const near =
        1 - clamp(Math.hypot(passer.x - x, passer.y - (h - length * 0.5)) / length, 0, 1);
      shove += Math.sign(x - passer.x) * near * near * passer.force;
    }

    const bend = curl + sway + shove;
    const tipX = x + bend;
    const tipY = h - length;
    // The control point sits off the straight line, so a blade arcs over instead of standing up
    // like a blade of grass.
    const midX = x + bend * 0.15;
    const midY = h - length * 0.6;
    ctx.globalAlpha = alpha * (0.8 + scatter(seed + 5) * 0.4);
    ctx.beginPath();
    ctx.moveTo(x - width * 0.5, h);
    ctx.quadraticCurveTo(midX - width * 0.6, midY, tipX, tipY);
    ctx.quadraticCurveTo(midX + width * 0.7, midY, x + width * 0.5, h);
    ctx.closePath();
    ctx.fill();
  }
  ctx.globalAlpha = 1;
}

function drawFish(ctx: CanvasRenderingContext2D, fish: Fish, colour: string, eye: string) {
  const shape = minnowPaths();
  const scale = fish.length / MINNOW.length;
  const beat = clamp(Math.hypot(fish.vx, fish.vy) / fish.dash, 0, 1);
  const swing = Math.sin(fish.tail) * (0.2 + 0.36 * beat + 0.14 * fish.startle);

  ctx.save();
  ctx.translate(fish.x, fish.y);
  // Coming about is drawn as the fish narrowing and opening out the other way, so it never has to
  // rotate through nose-up to face the other bank. A floor keeps the matrix from collapsing.
  const facing = Math.abs(fish.facing) < 0.08 ? (fish.facing < 0 ? -0.08 : 0.08) : fish.facing;
  ctx.scale(facing, 1);
  ctx.rotate(fish.angle);
  ctx.scale(scale, scale);
  ctx.translate(-MINNOW.center.x, -MINNOW.center.y);
  ctx.fillStyle = colour;

  ctx.save();
  ctx.translate(MINNOW.pivot.x, MINNOW.pivot.y);
  ctx.rotate(swing);
  ctx.translate(-MINNOW.pivot.x, -MINNOW.pivot.y);
  ctx.fill(shape.tail);
  ctx.restore();

  // The head yaws a little against the tail, which is what reads as swimming rather than sliding.
  ctx.save();
  ctx.translate(MINNOW.nose.x, MINNOW.nose.y);
  ctx.rotate(swing * -0.16);
  ctx.translate(-MINNOW.nose.x, -MINNOW.nose.y);
  ctx.fill(shape.body);
  ctx.fillStyle = eye;
  ctx.beginPath();
  ctx.arc(MINNOW.eye.x, MINNOW.eye.y, MINNOW.eye.r, 0, TAU);
  ctx.fill();
  ctx.restore();

  ctx.restore();
}

function drawNet(ctx: CanvasRenderingContext2D, net: Net, colour: string, foam: string) {
  const r = net.radius * (1 + net.lunge * 0.12);
  const rim = r * 0.4;
  ctx.save();
  ctx.translate(net.x, net.y);
  ctx.rotate(net.tilt);
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  ctx.strokeStyle = colour;

  // Handle, running out of the top of the frame.
  ctx.globalAlpha = net.depth;
  ctx.lineWidth = Math.max(2, r * 0.1);
  ctx.beginPath();
  ctx.moveTo(0, -rim);
  ctx.lineTo(r * 0.5, -r * 3.6);
  ctx.stroke();

  // The mesh: strands from the rim down into the bag, and hoops across them.
  ctx.globalAlpha = net.depth * 0.45;
  ctx.lineWidth = Math.max(1, r * 0.035);
  for (let i = -2; i <= 2; i += 1) {
    const t = (i / 2) * (Math.PI / 2);
    const sx = Math.sin(t) * r;
    const sy = Math.cos(t) * rim;
    ctx.beginPath();
    ctx.moveTo(sx, sy);
    ctx.quadraticCurveTo(sx * 0.5, r * 0.8, 0, r * 1.1);
    ctx.stroke();
  }
  for (let i = 1; i <= 3; i += 1) {
    const k = i / 4;
    ctx.beginPath();
    ctx.ellipse(0, r * k * 1.0, r * (1 - k * 0.8), rim * (1 - k * 0.55), 0, 0, Math.PI);
    ctx.stroke();
  }

  // Rim last, over the mesh, with a highlight where the light would catch the wet side of it.
  ctx.globalAlpha = net.depth;
  ctx.lineWidth = Math.max(1.8, r * 0.085);
  ctx.beginPath();
  ctx.ellipse(0, 0, r, rim, 0, 0, TAU);
  ctx.stroke();
  ctx.strokeStyle = foam;
  ctx.globalAlpha = net.depth * 0.7;
  ctx.lineWidth = Math.max(1, r * 0.04);
  ctx.beginPath();
  ctx.ellipse(0, 0, r * 0.94, rim * 0.86, 0, Math.PI * 1.05, Math.PI * 1.75);
  ctx.stroke();
  ctx.restore();
}

function drawWater(ctx: CanvasRenderingContext2D, world: World, colours: Palette) {
  const { w, h, time } = world;
  const body = ctx.createLinearGradient(0, 0, 0, h);
  body.addColorStop(0, colours.surface);
  body.addColorStop(0.55, colours.deep);
  body.addColorStop(1, colours.floor);
  ctx.fillStyle = body;
  ctx.fillRect(0, 0, w, h);

  // Light coming in from above, leaning as the surface moves. Both stops of the gradient are the
  // same colour so a shaft fades out rather than through grey, which is what a stop of
  // `transparent` — transparent black — would do. A faint wide pass under a brighter narrow one
  // stands in for the soft edge such a shaft has in water.
  const shaft = ctx.createLinearGradient(0, 0, 0, h);
  shaft.addColorStop(0, colours.glow);
  shaft.addColorStop(1, colours.glowFade);
  ctx.fillStyle = shaft;
  for (let i = 0; i < 3; i += 1) {
    const x = w * (0.18 + i * 0.31) + Math.sin(time * (0.09 + i * 0.03) + i * 2.1) * w * 0.03;
    const lean = h * (0.32 + i * 0.06);
    for (const { spread, alpha } of [
      { spread: 2.1, alpha: 0.35 },
      { spread: 1, alpha: 0.75 },
    ]) {
      const top = w * 0.045 * spread;
      const foot = w * 0.1 * spread;
      ctx.globalAlpha = alpha;
      ctx.beginPath();
      ctx.moveTo(x - top * 0.5, 0);
      ctx.lineTo(x + top * 0.5, 0);
      ctx.lineTo(x + lean + foot * 0.5, h);
      ctx.lineTo(x + lean - foot * 0.5, h);
      ctx.closePath();
      ctx.fill();
    }
  }

  // The surface itself: a band with a moving edge, so the top of the pond is water and not a line.
  const wave = (x: number) =>
    5 +
    Math.sin(x * 0.021 + time * 0.9) * 2.6 +
    Math.sin(x * 0.047 - time * 1.35) * 1.4 +
    Math.sin(x * 0.008 + time * 0.4) * 2;
  ctx.fillStyle = colours.foam;
  ctx.globalAlpha = 0.45;
  ctx.beginPath();
  ctx.moveTo(0, 0);
  ctx.lineTo(w, 0);
  for (let x = w; x >= 0; x -= 6) ctx.lineTo(x, wave(x));
  ctx.closePath();
  ctx.fill();

  ctx.strokeStyle = colours.foam;
  ctx.globalAlpha = 0.7;
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  for (let x = 0; x <= w; x += 6) ctx.lineTo(x, wave(x));
  ctx.stroke();

  // A second line further down, for the depth between it and the surface.
  ctx.globalAlpha = 0.3;
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let x = 0; x <= w; x += 6)
    ctx.lineTo(x, h * 0.13 + Math.sin(x * 0.016 - time * 0.7) * 3 + Math.sin(time * 0.5) * 2);
  ctx.stroke();
  ctx.globalAlpha = 1;
}

function draw(ctx: CanvasRenderingContext2D, world: World, colours: Palette) {
  drawWater(ctx, world, colours);

  for (const weed of WEEDS) if (!weed.front) drawWeed(ctx, world, weed, colours.weedBack, 0.55);

  ctx.strokeStyle = colours.foam;
  ctx.lineWidth = 1.2;
  for (const ripple of world.ripples) {
    ctx.globalAlpha = 0.22 * ripple.life * ripple.life;
    ctx.beginPath();
    ctx.arc(ripple.x, ripple.y, ripple.r, 0, TAU);
    ctx.stroke();
  }

  for (const fish of world.school) {
    ctx.globalAlpha = 0.32 - fish.distance * 0.2;
    drawFish(ctx, fish, colours.school, colours.school);
  }

  ctx.globalAlpha = 0.4;
  ctx.strokeStyle = colours.foam;
  ctx.lineWidth = 1;
  for (const bubble of world.bubbles) {
    ctx.beginPath();
    ctx.arc(bubble.x + Math.sin(bubble.phase) * bubble.sway, bubble.y, bubble.r, 0, TAU);
    ctx.stroke();
  }

  ctx.globalAlpha = 1;
  ctx.save();
  ctx.shadowColor = colours.floor;
  ctx.shadowBlur = world.hero.length * 0.28;
  ctx.shadowOffsetY = world.hero.length * 0.08;
  drawFish(ctx, world.hero, colours.fish, colours.eye);
  ctx.restore();

  // The near stand last, so the fish can pass behind it.
  for (const weed of WEEDS) if (weed.front) drawWeed(ctx, world, weed, colours.weed, 0.85);

  if (world.net.depth > 0.01) drawNet(ctx, world.net, colours.net, colours.foam);
}

export function HeroPond() {
  const wrapRef = useRef<HTMLDivElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const worldRef = useRef<World | null>(null);
  const [dipped, setDipped] = useState(false);

  useEffect(() => {
    const wrap = wrapRef.current;
    const canvas = canvasRef.current;
    if (wrap === null || canvas === null) return;
    const ctx = canvas.getContext("2d");
    if (ctx === null) return;

    let colours = readPalette(wrap);
    let frame = 0;
    let last = 0;
    let running = false;
    let visible = document.visibilityState === "visible";
    let onScreen = true;
    const still = window.matchMedia("(prefers-reduced-motion: reduce)");

    const size = () => {
      const rect = wrap.getBoundingClientRect();
      const w = Math.max(1, Math.round(rect.width));
      const h = Math.max(1, Math.round(rect.height));
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.round(w * dpr);
      canvas.height = Math.round(h * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      const world = worldRef.current;
      if (world === null) worldRef.current = createWorld(w, h);
      else resizeWorld(world, w, h);
    };

    const paint = () => {
      const world = worldRef.current;
      if (world !== null) draw(ctx, world, colours);
    };

    const render = (now: number) => {
      const world = worldRef.current;
      if (world === null) return;
      // A tab that was in the background hands back an enormous gap; the pond skips it rather
      // than teleporting everything across the water.
      const dt = last === 0 ? 1 / 60 : Math.min((now - last) / 1000, 1 / 20);
      last = now;
      step(world, dt);
      draw(ctx, world, colours);
      frame = requestAnimationFrame(render);
    };

    const start = () => {
      if (running || still.matches || !visible || !onScreen) return;
      running = true;
      last = 0;
      frame = requestAnimationFrame(render);
    };
    const stop = () => {
      if (!running) return;
      running = false;
      cancelAnimationFrame(frame);
    };

    size();
    paint();

    const resizes = new ResizeObserver(() => {
      size();
      if (!running) paint();
    });
    resizes.observe(wrap);

    const shows = new IntersectionObserver(
      (entries) => {
        onScreen = entries.some((entry) => entry.isIntersecting);
        if (onScreen) start();
        else stop();
      },
      { threshold: 0 },
    );
    shows.observe(wrap);

    const onVisibility = () => {
      visible = document.visibilityState === "visible";
      if (visible) start();
      else stop();
    };
    document.addEventListener("visibilitychange", onVisibility);

    // The theme is a class on <html>, so the colours are read back when it changes.
    const themes = new MutationObserver(() => {
      colours = readPalette(wrap);
      if (!running) paint();
    });
    themes.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });

    const onMotion = () => {
      if (still.matches) stop();
      else start();
    };
    still.addEventListener("change", onMotion);

    start();

    return () => {
      stop();
      resizes.disconnect();
      shows.disconnect();
      themes.disconnect();
      still.removeEventListener("change", onMotion);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, []);

  const aim = (event: React.PointerEvent<HTMLDivElement>) => {
    const world = worldRef.current;
    const wrap = wrapRef.current;
    if (world === null || wrap === null) return;
    const rect = wrap.getBoundingClientRect();
    world.net.toX = event.clientX - rect.left;
    world.net.toY = event.clientY - rect.top;
    world.net.wanted = 1;
    if (!dipped) setDipped(true);
  };

  return (
    <div
      ref={wrapRef}
      className="minnow-pond relative h-full w-full overflow-hidden rounded-2xl border border-fd-border"
      data-net={dipped ? "in" : "out"}
      onPointerEnter={aim}
      onPointerMove={aim}
      onPointerDown={(event) => {
        aim(event);
        const world = worldRef.current;
        if (world !== null) world.net.lunge = 1;
      }}
      onPointerLeave={() => {
        const world = worldRef.current;
        if (world !== null) world.net.wanted = 0;
      }}
    >
      <canvas ref={canvasRef} className="block h-full w-full" aria-hidden="true" />
      <p className="minnow-pond-hint pointer-events-none absolute top-3 right-4 text-xs">
        Try to catch it
      </p>
    </div>
  );
}
