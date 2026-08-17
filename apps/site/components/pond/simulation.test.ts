import { describe, expect, it } from "vitest";
import { createWorld, resizeWorld, step, type World } from "./simulation";

/**
 * The pond makes one promise — the net never closes over the fish — and a promise that is only
 * true at reasonable pointer speeds is not one. These drive the net the way a visitor would, and
 * then the way nobody could, and check the fish is still swimming and still inside the water.
 */

const W = 900;
const H = 280;
const FRAME = 1 / 60;

function pond(): World {
  const world = createWorld(W, H);
  world.net.wanted = 1;
  return world;
}

/** How far the fish is from the middle of the hoop, in hoop radii. Below 1 it has been caught. */
function clearance(world: World) {
  const { hero, net } = world;
  return Math.hypot(hero.x - net.x, hero.y - net.y) / net.radius;
}

function inWater(world: World) {
  const { hero } = world;
  return hero.x >= 0 && hero.x <= world.w && hero.y >= 0 && hero.y <= world.h;
}

/** Run the net straight at wherever the fish is, for `seconds`, and report the closest it came. */
function chase(world: World, seconds: number, pointerSpeed: number) {
  let closest = Infinity;
  for (let t = 0; t < seconds; t += FRAME) {
    const dx = world.hero.x - world.net.toX;
    const dy = world.hero.y - world.net.toY;
    const d = Math.hypot(dx, dy) || 1;
    const reach = Math.min(d, pointerSpeed * FRAME);
    world.net.toX += (dx / d) * reach;
    world.net.toY += (dy / d) * reach;
    step(world, FRAME);
    expect(inWater(world)).toBe(true);
    if (t > 0.5) closest = Math.min(closest, clearance(world));
  }
  return closest;
}

describe("the pond", () => {
  it("keeps the fish out of the net at any speed the pointer can be swung", () => {
    for (const speed of [300, 900, 2400, 9000]) {
      const world = pond();
      expect(chase(world, 12, speed)).toBeGreaterThan(1);
    }
  });

  it("keeps the fish out of the net when it is cornered", () => {
    // The net is parked in the corner and the fish is dropped on top of it: the worst case the
    // escape has to handle, because open water is only in one direction.
    const world = pond();
    world.net.toX = 0;
    world.net.toY = 0;
    world.net.x = 0;
    world.net.y = 0;
    world.net.depth = 1;
    world.hero.x = world.hero.length * 0.5;
    world.hero.y = world.hero.length * 0.4;
    for (let t = 0; t < 6; t += FRAME) {
      step(world, FRAME);
      expect(inWater(world)).toBe(true);
      if (t > 0.2) expect(clearance(world)).toBeGreaterThan(1);
    }
  });

  it("swims on its own, and covers the water rather than circling one spot", () => {
    const world = createWorld(W, H);
    let low = Infinity;
    let high = -Infinity;
    let distance = 0;
    let { x, y } = world.hero;
    for (let t = 0; t < 60; t += FRAME) {
      step(world, FRAME);
      distance += Math.hypot(world.hero.x - x, world.hero.y - y);
      ({ x, y } = world.hero);
      low = Math.min(low, x);
      high = Math.max(high, x);
      expect(inWater(world)).toBe(true);
    }
    // A minute of cruising is a long way, over most of the width of the pond.
    expect(distance).toBeGreaterThan(W * 3);
    expect(high - low).toBeGreaterThan(W * 0.5);
  });

  it("bolts when the net comes at it, and settles again once it leaves", () => {
    const world = pond();
    world.net.depth = 1;
    world.net.x = world.hero.x + 30;
    world.net.y = world.hero.y;
    world.net.toX = world.net.x;
    world.net.toY = world.net.y;
    for (let t = 0; t < 0.5; t += FRAME) step(world, FRAME);
    const bolting = Math.hypot(world.hero.vx, world.hero.vy);
    expect(bolting).toBeGreaterThan(world.hero.cruise * 2);

    world.net.wanted = 0;
    for (let t = 0; t < 4; t += FRAME) step(world, FRAME);
    expect(world.hero.startle).toBe(0);
    expect(Math.hypot(world.hero.vx, world.hero.vy)).toBeLessThan(world.hero.cruise * 1.5);
  });

  it("carries the pond over a resize without stranding anything outside it", () => {
    const world = pond();
    for (let t = 0; t < 3; t += FRAME) step(world, FRAME);
    const across = world.hero.x / world.w;
    resizeWorld(world, 420, 190);
    expect(world.hero.x / world.w).toBeCloseTo(across, 5);
    for (let t = 0; t < 10; t += FRAME) {
      step(world, FRAME);
      expect(inWater(world)).toBe(true);
    }
  });
});
