/**
 * The playground dataset: a specialty coffee retailer's point-of-sale data — stores, staff, a
 * product catalogue, customers, orders, order lines, and returns — generated deterministically
 * in the browser.
 *
 * Nothing is downloaded. The same seed always produces the same rows, so a query quoted in the
 * docs keeps returning the numbers the docs say it does, and a visitor who reloads sees the
 * database they already had.
 *
 * Generation is deliberately structured, not uniform-random. Real retail data has shape, and a
 * query engine only looks honest on data that has it too:
 *
 * - Customer spend follows a power law. A few hundred customers drive a visible share of
 *   revenue, most order once or twice, and some never come back.
 * - Orders carry weekly, annual, and growth seasonality — weekends and December are busy, and
 *   the business grows year over year.
 * - Product popularity is Zipf-distributed, so a top-N by units sold has a real head and tail.
 * - Prices sit in category-appropriate bands and end on realistic charm-pricing digits.
 * - Names, cities, and product names come from combinatorial pools (see `./pools.ts`), so
 *   GROUP BY over any of them produces thousands of buckets rather than a dozen.
 */
import type { AnyTable, SchemaColumnType } from "@minnowdb/core";
import {
  BRANDS,
  CATEGORIES,
  CITIES,
  EMAIL_DOMAINS,
  EMPLOYEE_ROLES,
  FIRST_NAMES,
  LAST_NAMES,
  PAYMENT_METHODS,
  RETURN_REASONS,
  STORE_FORMATS,
  type City,
} from "./pools";
import { retailTables } from "./schema";

export { retailDefinition, retailTables } from "./schema";

export type ColumnType = SchemaColumnType;

export interface ColumnSpec {
  name: string;
  type: ColumnType;
  nullable?: boolean;
}

export interface TableSpec {
  name: string;
  uniqueKey: string;
  columns: readonly ColumnSpec[];
  /** One line of prose for the schema panel beside the console. */
  description: string;
}

export type Value = boolean | number | string | Date | null;
export type Row = Record<string, Value>;

/**
 * The schema itself lives in `./schema`, re-exported above; `retailSchema` below reduces it to
 * the plain shape the loader and the dataset tests read.
 */

/**
 * What each table holds, in one line. Read by the Kysely console, which prints them as doc
 * comments on the generated row types — so hovering `orders` in the editor says what an order is.
 */
export const retailDescriptions: Record<string, string> = {
  stores: "Physical locations, each with a city, a format, and an opening date.",
  employees: "Staff assigned to a store, with a role and an hourly rate.",
  products: "The catalogue: SKU, category, brand, cost and list price.",
  customers: "People, with a home city, a signup date, and a loyalty tier.",
  orders: "One basket: who, where, when, how it was paid for, and what it came to.",
  order_items: "Order lines: the product, how many, and what was actually charged.",
  returns: "Refunded lines, with a reason and the amount given back.",
};

/** The declaration above, flattened — the shape the loader and the dataset tests read. */
export const retailSchema: readonly TableSpec[] = retailTables.map((definition: AnyTable) => {
  const entries = Object.entries(definition.columns);
  const unique = entries.find(([, builder]) => builder.isUnique);
  if (unique === undefined) throw new Error(`${definition.name} declares no unique key`);
  return {
    name: definition.name,
    uniqueKey: unique[0],
    columns: entries.map(([name, builder]) => ({
      name,
      type: builder.type,
      ...(builder.isNullable ? { nullable: true } : {}),
    })),
    description: retailDescriptions[definition.name] ?? "",
  };
});

/** How many rows each size produces, and roughly how long a browser spends building it. */
export interface RetailSize {
  id: string;
  label: string;
  /** Multiplier over the base counts below. */
  scale: number;
  description: string;
}

export const retailSizes: readonly RetailSize[] = [
  { id: "small", label: "Small", scale: 0.25, description: "About 150,000 rows." },
  { id: "medium", label: "Medium", scale: 1, description: "About 590,000 rows." },
  { id: "large", label: "Large", scale: 3, description: "About 1.8 million rows." },
];

/**
 * Base row counts at scale 1. The customer-to-order ratio is the number that decides whether
 * the data reads as retail: at roughly 2.5 orders per customer the distribution has its mode at
 * a single purchase, a long tail of regulars, and a real population who signed up and never came
 * back — which is what a cohort or retention query needs in order to say anything.
 */
const BASE = {
  stores: 42,
  employees: 640,
  products: 1_600,
  customers: 60_000,
  orders: 150_000,
} as const;

/** Three full years of trading, ending at a fixed date so every visitor sees the same window. */
const START = Date.UTC(2023, 0, 2);
const END = Date.UTC(2026, 0, 1);
const DAYS = Math.round((END - START) / 86_400_000);

export interface RetailCounts {
  stores: number;
  employees: number;
  products: number;
  customers: number;
  orders: number;
  /** Order lines and returns are emergent, so these are the exact values only after a build. */
  order_items: number;
  returns: number;
}

const scaled = (base: number, scale: number): number => Math.max(1, Math.round(base * scale));

/** Row counts for the fixed tables; lines and returns are counted as they are produced. */
export function retailPlan(scale: number): Omit<RetailCounts, "order_items" | "returns"> {
  return {
    stores: scaled(BASE.stores, Math.min(scale, 2)),
    employees: scaled(BASE.employees, Math.min(scale, 2)),
    products: scaled(BASE.products, Math.min(scale, 2)),
    customers: scaled(BASE.customers, scale),
    orders: scaled(BASE.orders, scale),
  };
}

/** Estimated total rows, used for a progress bar before a single row exists. */
export function retailEstimatedRows(scale: number): number {
  const plan = retailPlan(scale);
  const items = Math.round(plan.orders * 2.6);
  return (
    plan.stores +
    plan.employees +
    plan.products +
    plan.customers +
    plan.orders +
    items +
    Math.round(items * 0.021)
  );
}

// A small, fast, well-distributed PRNG. sfc32 rather than a single-word generator because the
// generator is asked for tens of millions of values and low-order correlation would show up as
// visible banding in the aggregates.
function sfc32(a: number, b: number, c: number, d: number): () => number {
  return () => {
    a >>>= 0;
    b >>>= 0;
    c >>>= 0;
    d >>>= 0;
    let t = (a + b) | 0;
    a = b ^ (b >>> 9);
    b = (c + (c << 3)) | 0;
    c = (c << 21) | (c >>> 11);
    d = (d + 1) | 0;
    t = (t + d) | 0;
    c = (c + t) | 0;
    return (t >>> 0) / 4294967296;
  };
}

class Random {
  readonly #next: () => number;

  constructor(seed: number) {
    this.#next = sfc32(0x9e3779b9, seed ^ 0x85ebca6b, seed ^ 0xc2b2ae35, 0x27d4eb2f);
    for (let index = 0; index < 12; index += 1) this.#next();
  }

  next(): number {
    return this.#next();
  }

  /** Uniform integer in [min, max]. */
  int(min: number, max: number): number {
    return min + Math.floor(this.#next() * (max - min + 1));
  }

  pick<T>(items: readonly T[]): T {
    const value = items[Math.floor(this.#next() * items.length)];
    if (value === undefined) throw new Error("Cannot pick from an empty pool");
    return value;
  }

  bool(probability: number): boolean {
    return this.#next() < probability;
  }

  /**
   * Draws an index from a cumulative weight table. Callers build the table once, because the
   * hot paths draw from the same distribution millions of times.
   */
  weighted(cumulative: readonly number[]): number {
    const target = this.#next() * (cumulative[cumulative.length - 1] ?? 1);
    let low = 0;
    let high = cumulative.length - 1;
    while (low < high) {
      const middle = (low + high) >> 1;
      if ((cumulative[middle] ?? 0) < target) low = middle + 1;
      else high = middle;
    }
    return low;
  }

  /** Standard normal, via Box-Muller. Used to give prices and baskets a believable spread. */
  normal(): number {
    const u = 1 - this.#next();
    const v = this.#next();
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
  }

  /** Log-normal on [min, max], skewed toward the cheap end the way a real catalogue is. */
  logUniform(min: number, max: number): number {
    const value = Math.exp(Math.log(min) + this.#next() * (Math.log(max) - Math.log(min)));
    return Math.min(max, Math.max(min, value));
  }
}

function cumulativeWeights(weights: readonly number[]): number[] {
  const cumulative: number[] = [];
  let total = 0;
  for (const weight of weights) {
    total += weight;
    cumulative.push(total);
  }
  return cumulative;
}

const money = (value: number): number => Math.round(value * 100) / 100;

/** Charm pricing: real shelf prices end on .95, .99, .50 or .00 far more often than at random. */
function shelfPrice(random: Random, value: number): number {
  const whole = Math.floor(value);
  const ending = random.weighted(CHARM_CUMULATIVE);
  const cents = CHARM_ENDINGS[ending] ?? 99;
  return money(Math.max(1, whole) + cents / 100);
}

const CHARM_ENDINGS = [99, 95, 50, 0, 49, 75] as const;
const CHARM_CUMULATIVE = cumulativeWeights([44, 22, 12, 10, 7, 5]);

function postalCode(random: Random, template: string): string {
  let out = "";
  for (const character of template) {
    if (character === "#") out += String(random.int(0, 9));
    else if (character === "A") out += String.fromCharCode(65 + random.int(0, 25));
    else out += character;
  }
  return out;
}

function slug(value: string): string {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ".")
    .replace(/^\.|\.$/g, "");
}

const DAY = 86_400_000;
const dateAt = (millis: number): Date => new Date(millis);

/**
 * How busy a given day is, relative to the average. Three effects multiply: a linear growth
 * trend across the window, an annual cycle peaking through the holiday season, and a weekly
 * cycle peaking at the weekend.
 */
function dayIntensity(dayIndex: number): number {
  const progress = dayIndex / DAYS;
  const growth = 0.72 + 0.56 * progress;
  const dayOfYear = (dayIndex + 1) % 365;
  // Peaks in late November through December, troughs in February.
  const annual = 1 + 0.34 * Math.cos((2 * Math.PI * (dayOfYear - 340)) / 365);
  const weekday = new Date(START + dayIndex * DAY).getUTCDay();
  const weekly = [0.92, 0.86, 0.88, 0.94, 1.08, 1.24, 1.18][weekday] ?? 1;
  return growth * annual * weekly;
}

/** Opening hours: a morning rush, a lunch bump, and a quiet evening. */
const HOUR_CUMULATIVE = cumulativeWeights([
  0, 0, 0, 0, 0, 1, 4, 12, 26, 34, 30, 26, 30, 28, 22, 20, 18, 16, 12, 7, 4, 2, 1, 0,
]);

interface GeneratedProduct {
  id: number;
  listPrice: number;
  launchedAt: number;
  discontinued: boolean;
}

export interface RetailBatch {
  table: string;
  rows: Row[];
  /** Rows emitted so far across every table, for a progress bar. */
  emitted: number;
}

export interface RetailOptions {
  scale: number;
  seed?: number;
  /** Rows per emitted batch. Larger batches load faster; smaller ones update progress sooner. */
  batchRows?: number;
}

/**
 * Produces the dataset one batch at a time. A generator rather than one big array: a million
 * rows materialized at once is hundreds of megabytes of JavaScript objects, where batching keeps
 * peak memory to one batch and lets the caller report progress and yield to the event loop.
 */
export function* retailBatches(options: RetailOptions): Generator<RetailBatch> {
  const scale = options.scale;
  const batchRows = options.batchRows ?? 20_000;
  const random = new Random(options.seed ?? 0x5eed_c0de);
  const plan = retailPlan(scale);
  let emitted = 0;

  function* emit(table: string, rows: Row[]): Generator<RetailBatch> {
    emitted += rows.length;
    yield { table, rows, emitted };
  }

  // ---- stores -------------------------------------------------------------------------------
  const cityCumulative = cumulativeWeights(CITIES.map((city) => city.weight));
  const storeCities: City[] = [];
  const storeRows: Row[] = [];
  for (let index = 0; index < plan.stores; index += 1) {
    const city = CITIES[random.weighted(cityCumulative)] ?? CITIES[0];
    if (city === undefined) throw new Error("No cities configured");
    storeCities.push(city);
    const format = random.pick(STORE_FORMATS);
    storeRows.push({
      store_id: index + 1,
      code: `${city.country}-${String(index + 1).padStart(3, "0")}`,
      name: `${city.name} ${format}`,
      city: city.name,
      region: city.region,
      country: city.country,
      format,
      floor_sqm: random.int(45, 420),
      // Stores open across the years before and during the trading window, so a store's age
      // and its revenue are correlated the way they would be.
      opened_on: dateAt(START - random.int(0, 2200) * DAY),
    });
  }
  yield* emit("stores", storeRows);

  // ---- employees ----------------------------------------------------------------------------
  const roleCumulative = cumulativeWeights(EMPLOYEE_ROLES.map((role) => role.weight));
  const employeesByStore: number[][] = Array.from({ length: plan.stores }, () => []);
  const employeeRows: Row[] = [];
  for (let index = 0; index < plan.employees; index += 1) {
    const storeIndex = random.int(0, plan.stores - 1);
    const role = EMPLOYEE_ROLES[random.weighted(roleCumulative)] ?? EMPLOYEE_ROLES[0];
    const [low, high] = role.rate;
    employeesByStore[storeIndex]?.push(index + 1);
    employeeRows.push({
      employee_id: index + 1,
      store_id: storeIndex + 1,
      name: `${random.pick(FIRST_NAMES)} ${random.pick(LAST_NAMES)}`,
      role: role.name,
      hourly_rate: money(low + random.next() * (high - low)),
      hired_on: dateAt(START - random.int(0, 1500) * DAY),
      active: random.bool(0.86),
    });
    if (employeeRows.length >= batchRows) yield* emit("employees", employeeRows.splice(0));
  }
  if (employeeRows.length > 0) yield* emit("employees", employeeRows);

  // ---- products -----------------------------------------------------------------------------
  const categoryCumulative = cumulativeWeights(CATEGORIES.map((category) => category.weight));
  const products: GeneratedProduct[] = [];
  const productRows: Row[] = [];
  const usedNames = new Set<string>();
  for (let index = 0; index < plan.products; index += 1) {
    const category = CATEGORIES[random.weighted(categoryCumulative)] ?? CATEGORIES[0];
    if (category === undefined) throw new Error("No categories configured");
    const subcategory = random.pick(category.subcategories);
    const brand = random.pick(BRANDS);
    // Four independent pools multiply out well past the catalogue size, so names stay
    // distinct without a counter suffix giving the generator away.
    let name = `${brand} ${random.pick(category.qualifiers)} ${random.pick(category.nouns)} ${random.pick(category.variants)}`;
    while (usedNames.has(name)) {
      name = `${brand} ${random.pick(category.qualifiers)} ${random.pick(category.nouns)} ${random.pick(category.variants)}`;
    }
    usedNames.add(name);
    const listPrice = shelfPrice(random, random.logUniform(category.price[0], category.price[1]));
    // Margin varies by price point: cheap consumables carry a fatter multiple than equipment.
    const margin = listPrice < 25 ? 0.42 + random.next() * 0.22 : 0.24 + random.next() * 0.2;
    const launchedAt = START - random.int(0, 1400) * DAY + random.int(0, DAYS) * DAY * 0.4;
    const discontinued = random.bool(0.08);
    products.push({ id: index + 1, listPrice, launchedAt, discontinued });
    productRows.push({
      product_id: index + 1,
      sku: `${category.name.slice(0, 3).toUpperCase()}-${subcategory.slice(0, 2).toUpperCase()}-${String(index + 1).padStart(5, "0")}`,
      name,
      category: category.name,
      subcategory,
      brand,
      unit_cost: money(listPrice * (1 - margin)),
      list_price: listPrice,
      launched_on: dateAt(launchedAt),
      discontinued,
    });
    if (productRows.length >= batchRows) yield* emit("products", productRows.splice(0));
  }
  if (productRows.length > 0) yield* emit("products", productRows);

  // Product popularity is Zipf: rank 1 sells roughly `products` times as often as the last.
  // Shuffling the ranks keeps popularity uncorrelated with product_id, so an ORDER BY on one
  // says nothing about the other.
  const popularity = products.map((_, index) => 1 / (index + 1));
  for (let index = popularity.length - 1; index > 0; index -= 1) {
    const swap = random.int(0, index);
    const held = popularity[index] ?? 0;
    popularity[index] = popularity[swap] ?? 0;
    popularity[swap] = held;
  }
  // Then damp by price. Unit sales fall off steeply with price in retail, and leaving the two
  // independent puts an 800-unit espresso machine in the same sales band as a bag of beans —
  // which made equipment eighty per cent of revenue and every GROUP BY answer the same way.
  // Damped, high-ticket lines stay the minority they are while still leading on revenue.
  const productCumulative = cumulativeWeights(
    products.map((product, index) => (popularity[index] ?? 0) / Math.pow(product.listPrice, 0.75)),
  );

  // ---- customers ----------------------------------------------------------------------------
  // Each customer gets a latent propensity to buy, drawn from a heavy-tailed distribution. It
  // decides how many orders they place and, with tenure, which loyalty tier they hold — so tier,
  // order count, and lifetime spend all agree with each other, as they would in a real system.
  const propensity = new Float64Array(plan.customers);
  const signedUpAt = new Float64Array(plan.customers);
  const customerRows: Row[] = [];
  const emailCounts = new Map<string, number>();
  for (let index = 0; index < plan.customers; index += 1) {
    const first = random.pick(FIRST_NAMES);
    const last = random.pick(LAST_NAMES);
    const city = CITIES[random.weighted(cityCumulative)] ?? CITIES[0];
    if (city === undefined) throw new Error("No cities configured");
    // Pareto-ish: most customers near 1, a long tail an order of magnitude above.
    const draw = Math.pow(1 - random.next(), -1 / 1.6);
    propensity[index] = Math.min(draw, 60);
    // Signups spread across the window with a bias toward earlier, so tenure varies.
    const signup = START - random.int(0, 900) * DAY + Math.floor(random.next() ** 1.7 * DAYS) * DAY;
    signedUpAt[index] = signup;

    const base = `${slug(first)}.${slug(last)}`;
    const seen = emailCounts.get(base) ?? 0;
    emailCounts.set(base, seen + 1);
    const email = `${base}${seen === 0 ? "" : String(seen + 1)}@${random.pick(EMAIL_DOMAINS)}`;

    const tenureYears = Math.max(0, (END - signup) / (365 * DAY));
    const standing = (propensity[index] ?? 1) * Math.min(1.5, 0.5 + tenureYears);
    const tier =
      standing > 14 ? "platinum" : standing > 6 ? "gold" : standing > 2.4 ? "silver" : "bronze";

    customerRows.push({
      customer_id: index + 1,
      name: `${first} ${last}`,
      email,
      city: city.name,
      region: city.region,
      country: city.country,
      postal_code: postalCode(random, city.postal),
      loyalty_tier: tier,
      birth_year: 2026 - Math.round(18 + Math.abs(random.normal()) * 16 + random.next() * 6),
      marketing_opt_in: random.bool(0.38),
      signed_up_on: dateAt(signup),
    });
    if (customerRows.length >= batchRows) yield* emit("customers", customerRows.splice(0));
  }
  if (customerRows.length > 0) yield* emit("customers", customerRows);

  // ---- orders, lines, returns ---------------------------------------------------------------
  // Orders are drawn per day so the seasonality above actually lands on the dates, and each
  // order's customer is drawn by propensity so the spend distribution comes out heavy-tailed.
  const customerCumulative = cumulativeWeights([...propensity]);
  const paymentCumulative = cumulativeWeights(PAYMENT_METHODS.map((method) => method.weight));
  const reasonCumulative = cumulativeWeights(RETURN_REASONS.map((reason) => reason.weight));
  const dayWeights = Array.from({ length: DAYS }, (_, day) => dayIntensity(day));
  const dayCumulative = cumulativeWeights(dayWeights);

  let orderId = 0;
  let orderItemId = 0;
  let returnId = 0;
  const orderRows: Row[] = [];
  const itemRows: Row[] = [];
  const returnRows: Row[] = [];

  for (let index = 0; index < plan.orders; index += 1) {
    const day = random.weighted(dayCumulative);
    const hour = random.weighted(HOUR_CUMULATIVE);
    const placedAt = START + day * DAY + hour * 3_600_000 + random.int(0, 3_599) * 1000;

    // Draw a customer by propensity, then reject anyone who had not signed up yet — which is
    // what keeps `placed_at >= signed_up_on` true for every row without post-processing.
    let customerIndex = random.weighted(customerCumulative);
    for (
      let attempt = 0;
      attempt < 6 && (signedUpAt[customerIndex] ?? 0) > placedAt;
      attempt += 1
    ) {
      customerIndex = random.weighted(customerCumulative);
    }
    if ((signedUpAt[customerIndex] ?? 0) > placedAt) continue;

    const storeIndex = random.int(0, plan.stores - 1);
    const online = random.bool(0.34);
    const staff = employeesByStore[storeIndex] ?? [];
    orderId += 1;

    // Basket size: mostly one or two lines, occasionally a big stock-up.
    const lines = Math.max(1, Math.min(12, Math.round(1 + Math.abs(random.normal()) * 1.8)));
    let subtotal = 0;
    let discount = 0;
    const chosen = new Set<number>();
    const orderLines: Array<{ id: number; productId: number; quantity: number; total: number }> =
      [];

    for (let line = 0; line < lines; line += 1) {
      let productIndex = random.weighted(productCumulative);
      // No duplicate lines, and nothing sold before it launched or after it was dropped.
      for (let attempt = 0; attempt < 8; attempt += 1) {
        const candidate = products[productIndex];
        if (
          candidate !== undefined &&
          !chosen.has(productIndex) &&
          candidate.launchedAt <= placedAt &&
          !(candidate.discontinued && placedAt > candidate.launchedAt + 500 * DAY)
        ) {
          break;
        }
        productIndex = random.weighted(productCumulative);
      }
      const product = products[productIndex];
      if (product === undefined || chosen.has(productIndex)) continue;
      chosen.add(productIndex);

      // Nobody buys three espresso machines; multiples belong to consumables.
      const quantity = product.listPrice > 90 ? 1 : random.next() < 0.76 ? 1 : random.int(2, 6);
      // Promotions are occasional and shallow, with the odd deep clearance.
      const lineDiscountRate = random.next() < 0.17 ? (random.next() < 0.15 ? 0.4 : 0.12) : 0;
      const unitPrice = product.listPrice;
      const gross = money(unitPrice * quantity);
      const lineDiscount = money(gross * lineDiscountRate);
      const lineTotal = money(gross - lineDiscount);
      orderItemId += 1;
      subtotal += gross;
      discount += lineDiscount;
      orderLines.push({
        id: orderItemId,
        productId: product.id,
        quantity,
        total: lineTotal,
      });
      itemRows.push({
        order_item_id: orderItemId,
        order_id: orderId,
        product_id: product.id,
        quantity,
        unit_price: unitPrice,
        discount: lineDiscount,
        line_total: lineTotal,
      });
    }

    if (orderLines.length === 0) {
      orderId -= 1;
      continue;
    }

    subtotal = money(subtotal);
    discount = money(discount);
    const net = money(subtotal - discount);
    const tax = money(net * 0.0875);
    const shipping = online ? (net > 45 ? 0 : money(3.5 + random.next() * 4.5)) : 0;
    const total = money(net + tax + shipping);
    const roll = random.next();
    const status =
      roll < 0.928
        ? "completed"
        : roll < 0.962
          ? "refunded"
          : roll < 0.986
            ? "cancelled"
            : "pending";

    orderRows.push({
      order_id: orderId,
      customer_id: customerIndex + 1,
      store_id: storeIndex + 1,
      employee_id: online || staff.length === 0 ? null : random.pick(staff),
      channel: online ? (random.bool(0.24) ? "pickup" : "online") : "in_store",
      status,
      // Cash only happens at a counter, so an online order redraws rather than paying in notes.
      payment_method: (() => {
        const method = PAYMENT_METHODS[random.weighted(paymentCumulative)]?.name ?? "card";
        return online && method === "cash" ? "card" : method;
      })(),
      item_count: orderLines.length,
      subtotal,
      discount,
      tax,
      shipping,
      total,
      placed_at: dateAt(placedAt),
    });

    // Returns hang off individual lines, and are far likelier on an order already marked
    // refunded — so `returns` and `orders.status` tell a consistent story.
    const returnRate = status === "refunded" ? 0.62 : status === "completed" ? 0.014 : 0;
    for (const line of orderLines) {
      if (!random.bool(returnRate)) continue;
      returnId += 1;
      returnRows.push({
        return_id: returnId,
        order_item_id: line.id,
        order_id: orderId,
        product_id: line.productId,
        quantity: line.quantity === 1 ? 1 : random.int(1, line.quantity),
        reason: RETURN_REASONS[random.weighted(reasonCumulative)]?.name ?? "changed_mind",
        refund_amount: line.total,
        returned_at: dateAt(placedAt + random.int(1, 28) * DAY),
      });
    }

    if (orderRows.length >= batchRows) yield* emit("orders", orderRows.splice(0));
    if (itemRows.length >= batchRows) yield* emit("order_items", itemRows.splice(0));
    if (returnRows.length >= batchRows) yield* emit("returns", returnRows.splice(0));
  }

  if (orderRows.length > 0) yield* emit("orders", orderRows);
  if (itemRows.length > 0) yield* emit("order_items", itemRows);
  if (returnRows.length > 0) yield* emit("returns", returnRows);
}
