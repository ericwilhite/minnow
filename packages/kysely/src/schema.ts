import type { AnyTable, AnyView, PrimaryKeyKeys, SchemaDefinition } from "@minnowdb/core";
import type {
  BinaryOperator,
  ColumnType,
  ComparisonOperator,
  ComparisonOperatorExpression,
  ExtractTypeFromStringReference,
  SelectType,
  SqlBool,
  StringReference,
  ValueExpressionOrList,
} from "kysely";

type Simplify<T> = { [K in keyof T]: T[K] } & {};
type ColumnMetadata<TColumn> = TColumn extends {
  readonly "~types"?: infer TMetadata extends { readonly select: unknown; readonly input: unknown };
}
  ? NonNullable<TMetadata>
  : never;

type SelectValue<TColumn> = TColumn extends { readonly isNullable: infer TNullable }
  ? TNullable extends true
    ? ColumnMetadata<TColumn>["select"] | null
    : ColumnMetadata<TColumn>["select"]
  : never;

type InsertValue<TColumn> = TColumn extends {
  readonly isNullable: infer TNullable;
  readonly hasDefault: infer THasDefault;
}
  ? | ColumnMetadata<TColumn>["input"]
    | (TNullable extends true ? null | undefined : never)
    | (THasDefault extends true ? undefined : never)
  : never;

type UpdateValue<TColumn> = TColumn extends { readonly isNullable: infer TNullable }
  ? ColumnMetadata<TColumn>["input"] | (TNullable extends true ? null : never)
  : never;

/**
 * Kysely has select, insert, and update channels, but comparisons normally use the select type.
 * Minnow keeps the SQL parameter boundary alongside those channels so exact NUMERIC columns can
 * select as lossless strings while naturally accepting either strings or numbers in predicates.
 */
export type MinnowColumnType<TSelect, TInsert, TUpdate, TOperand> = ColumnType<
  TSelect,
  TInsert,
  TUpdate
> & { readonly __minnow_operand__: TOperand };

export type MinnowOperandType<DB, TB extends keyof DB, RE extends StringReference<DB, TB>> =
  ExtractTypeFromStringReference<DB, TB, RE> extends {
    readonly __minnow_operand__: infer TOperand;
  }
    ? TOperand
    : never;

type MinnowOperandExpression<
  DB,
  TB extends keyof DB,
  RE extends StringReference<DB, TB>,
> = ValueExpressionOrList<DB, TB, MinnowOperandType<DB, TB, RE> | null>;

type MinnowFilterObject<DB, TB extends keyof DB> = {
  [RE in StringReference<DB, TB>]?: MinnowOperandExpression<DB, TB, RE>;
};

/** One schema-declared table in Kysely's select/insert/update column format. */
export type InferKyselyTable<TTable extends AnyTable> = Simplify<{
  [K in keyof TTable["columns"]]: MinnowColumnType<
    SelectValue<TTable["columns"][K]>,
    InsertValue<TTable["columns"][K]>,
    K extends PrimaryKeyKeys<TTable> ? never : UpdateValue<TTable["columns"][K]>,
    UpdateValue<TTable["columns"][K]>
  >;
}>;

/** Views select normally, while their columns accept no insert or update value. */
export type InferKyselyView<TView extends AnyView> = Simplify<{
  [K in keyof TView["columns"]]: MinnowColumnType<
    SelectValue<TView["columns"][K]>,
    never,
    never,
    UpdateValue<TView["columns"][K]>
  >;
}>;

/**
 * Derives Kysely's complete `DB` map from a Minnow schema declaration. Defaults and nullable
 * columns become optional inserts, enum literals stay narrow, logical SQL domains keep their
 * boundary types, primary keys are read-only on update, and views are read-only.
 */
export type InferKyselyDatabase<TSchema extends SchemaDefinition<readonly AnyTable[]>> = Simplify<
  {
    [TTable in TSchema["tables"][number] as TTable["name"]]: InferKyselyTable<TTable>;
  } & {
    [TView in TSchema["views"][number] as TView["name"]]: InferKyselyView<TView>;
  }
>;

declare module "kysely" {
  interface SelectQueryBuilder<DB, TB extends keyof DB, O> {
    where<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): SelectQueryBuilder<DB, TB, O>;
    having<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): SelectQueryBuilder<DB, TB, O>;
  }

  interface UpdateQueryBuilder<DB, UT extends keyof DB, TB extends keyof DB, O> {
    where<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): UpdateQueryBuilder<DB, UT, TB, O>;
  }

  interface DeleteQueryBuilder<DB, TB extends keyof DB, O> {
    where<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): DeleteQueryBuilder<DB, TB, O>;
  }

  interface JoinBuilder<DB, TB extends keyof DB> {
    on<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): JoinBuilder<DB, TB>;
  }

  interface OnConflictBuilder<DB, TB extends keyof DB> {
    where<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): OnConflictBuilder<DB, TB>;
  }

  interface OnConflictUpdateBuilder<DB, TB extends keyof DB> {
    where<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): OnConflictUpdateBuilder<DB, TB>;
  }

  interface ExpressionBuilder<DB, TB extends keyof DB> {
    <RE extends StringReference<DB, TB>, OP extends BinaryOperator>(
      lhs: RE,
      op: OP,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): ExpressionWrapper<
      DB,
      TB,
      OP extends ComparisonOperator
        ? SqlBool
        : SelectType<ExtractTypeFromStringReference<DB, TB, RE>>
    >;
    and(exprs: Readonly<MinnowFilterObject<DB, TB>>): ExpressionWrapper<DB, TB, SqlBool>;
    or(exprs: Readonly<MinnowFilterObject<DB, TB>>): ExpressionWrapper<DB, TB, SqlBool>;
  }

  interface ExpressionWrapper<DB, TB extends keyof DB, T> {
    or<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): T extends SqlBool
      ? OrWrapper<DB, TB, SqlBool>
      : import("kysely").KyselyTypeError<"or() method can only be called on boolean expressions">;
    and<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): T extends SqlBool
      ? AndWrapper<DB, TB, SqlBool>
      : import("kysely").KyselyTypeError<"and() method can only be called on boolean expressions">;
  }

  interface OrWrapper<DB, TB extends keyof DB, T extends SqlBool> {
    or<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): OrWrapper<DB, TB, T>;
  }

  interface AndWrapper<DB, TB extends keyof DB, T extends SqlBool> {
    and<RE extends StringReference<DB, TB>>(
      lhs: RE,
      op: ComparisonOperatorExpression,
      rhs: MinnowOperandExpression<DB, TB, RE>,
    ): AndWrapper<DB, TB, T>;
  }
}
