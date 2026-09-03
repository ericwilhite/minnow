/**
 * The playground's schema, in dependency order — every table's foreign keys point at a table
 * already created.
 *
 * This is the single declaration, and it is a module of its own on purpose: the TypeScript
 * console ships this file to the editor as `./schema`, so the types it checks snippets against
 * — the Kysely `DB` and the schema-typed `database` — come from the same value `migrate()`
 * runs. Keep it free of imports other than `@minnowdb/core`, the only package the editor can
 * resolve, and free of anything but the declaration itself.
 */
import { column, schema, table } from "@minnowdb/core";

export const retailTables = [
  table("stores", {
    store_id: column.number().unique(),
    code: column.string(),
    name: column.string(),
    city: column.string(),
    region: column.string(),
    country: column.string(),
    format: column.string(),
    floor_sqm: column.number(),
    opened_on: column.datetime(),
  }),
  table("employees", {
    employee_id: column.number().unique(),
    store_id: column.number(),
    name: column.string(),
    role: column.string(),
    hourly_rate: column.number(),
    hired_on: column.datetime(),
    active: column.boolean(),
  }),
  table("products", {
    product_id: column.number().unique(),
    sku: column.string(),
    name: column.string(),
    category: column.string(),
    subcategory: column.string(),
    brand: column.string(),
    unit_cost: column.number(),
    list_price: column.number(),
    launched_on: column.datetime(),
    discontinued: column.boolean(),
  }),
  table("customers", {
    customer_id: column.number().unique(),
    name: column.string(),
    email: column.string(),
    city: column.string(),
    region: column.string(),
    country: column.string(),
    postal_code: column.string(),
    loyalty_tier: column.string(),
    birth_year: column.number(),
    marketing_opt_in: column.boolean(),
    signed_up_on: column.datetime(),
  }),
  table("orders", {
    order_id: column.number().unique(),
    customer_id: column.number(),
    store_id: column.number(),
    employee_id: column.number().nullable(),
    channel: column.string(),
    status: column.string(),
    payment_method: column.string(),
    item_count: column.number(),
    subtotal: column.number(),
    discount: column.number(),
    tax: column.number(),
    shipping: column.number(),
    total: column.number(),
    placed_at: column.datetime(),
  }),
  table("order_items", {
    order_item_id: column.number().unique(),
    order_id: column.number(),
    product_id: column.number(),
    quantity: column.number(),
    unit_price: column.number(),
    discount: column.number(),
    line_total: column.number(),
  }),
  table("returns", {
    return_id: column.number().unique(),
    order_item_id: column.number(),
    order_id: column.number(),
    product_id: column.number(),
    quantity: column.number(),
    reason: column.string(),
    refund_amount: column.number(),
    returned_at: column.datetime(),
  }),
] as const;

/** The schema `migrate()` takes, and the value Kysely infers its DB map from. */
export const retailDefinition = schema(retailTables);
