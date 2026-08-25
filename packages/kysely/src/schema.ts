import type { AnyTable, AnyView, PrimaryKeyKeys, SchemaDefinition } from "@minnowdb/core";
import type {
  AggregateFunctionBuilder,
  BinaryOperator,
  ColumnDataType,
  ColumnType,
  ComparisonOperator,
  ComparisonOperatorExpression,
  ExtractTypeFromReferenceExpression,
  ExtractTypeFromStringReference,
  ReferenceExpression,
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

type DatabaseColumnType<DB> = {
  [T in keyof DB]: DB[T][keyof DB[T]];
}[keyof DB];

type HasMinnowColumns<DB> = [
  Extract<DatabaseColumnType<DB>, { readonly __minnow_operand__: unknown }>,
] extends [never]
  ? false
  : true;

/** Keep Kysely's portable union unless this DB contains schema-derived Minnow columns. */
type MinnowCountOutput<DB> = HasMinnowColumns<DB> extends true ? number : number | string | bigint;

type NonNullReferenceValue<DB, TB extends keyof DB, RE> = Exclude<
  ExtractTypeFromReferenceExpression<DB, TB, RE>,
  null
>;

type MinnowNumericAggregateOutput<DB, TB extends keyof DB, RE, TPortable> =
  HasMinnowColumns<DB> extends true
    ? | ([NonNullReferenceValue<DB, TB, RE>] extends [never]
          ? number | string
          : NonNullReferenceValue<DB, TB, RE> extends number
            ? number
            : NonNullReferenceValue<DB, TB, RE> extends string
              ? string
              : number | string)
      | null
    : TPortable;

type MinnowFixedScalarFunction =
  | "round"
  | "date_trunc"
  | "date_add"
  | "upper"
  | "lower"
  | "length"
  | "abs"
  | "trim"
  | "ltrim"
  | "rtrim"
  | "substr"
  | "replace"
  | "instr"
  | "floor"
  | "ceil"
  | "mod"
  | "power"
  | "sqrt"
  | "extract"
  | "octet_length"
  | "lpad"
  | "rpad"
  | "overlay"
  | "current_date"
  | "current_timestamp"
  | "localtime"
  | "grouping"
  | "json_value"
  | "json_query"
  | "json_exists"
  | "json_object"
  | "json_array"
  | "array"
  | "nextval"
  | "currval"
  | "random"
  | "gen_random_uuid";

type MinnowFixedScalarFunctionSpelling =
  MinnowFixedScalarFunction | Uppercase<MinnowFixedScalarFunction>;

type MinnowFixedScalarBaseOutput<TName extends MinnowFixedScalarFunctionSpelling> =
  Uppercase<TName> extends
    | "ROUND"
    | "LENGTH"
    | "ABS"
    | "FLOOR"
    | "CEIL"
    | "MOD"
    | "POWER"
    | "SQRT"
    | "INSTR"
    | "EXTRACT"
    | "OCTET_LENGTH"
    | "GROUPING"
    | "NEXTVAL"
    | "CURRVAL"
    | "RANDOM"
    ? number
    : Uppercase<TName> extends "DATE_TRUNC" | "DATE_ADD" | "CURRENT_DATE" | "CURRENT_TIMESTAMP"
      ? Date
      : Uppercase<TName> extends "JSON_EXISTS"
        ? SqlBool
        : string;

type MinnowAlwaysNonNullScalarFunction =
  | "CURRENT_DATE"
  | "CURRENT_TIMESTAMP"
  | "LOCALTIME"
  | "GROUPING"
  | "JSON_OBJECT"
  | "JSON_ARRAY"
  | "ARRAY"
  | "NEXTVAL"
  | "CURRVAL"
  | "RANDOM"
  | "GEN_RANDOM_UUID";

type MinnowAlwaysNullableScalarFunction = "JSON_VALUE" | "JSON_QUERY";

type MinnowValueScalarFunction = "coalesce" | "nullif" | "greatest" | "least";

type MinnowValueScalarFunctionSpelling =
  MinnowValueScalarFunction | Uppercase<MinnowValueScalarFunction>;

type ReferenceValues<
  DB,
  TB extends keyof DB,
  TArgs extends readonly unknown[],
> = ExtractTypeFromReferenceExpression<DB, TB, TArgs[number]>;

type HasRequiredReferenceValue<
  DB,
  TB extends keyof DB,
  TArgs extends readonly unknown[],
> = TArgs extends readonly [infer THead, ...infer TTail]
  ? null extends ExtractTypeFromReferenceExpression<DB, TB, THead>
    ? HasRequiredReferenceValue<DB, TB, TTail>
    : true
  : false;

type MinnowValueScalarOutput<
  DB,
  TB extends keyof DB,
  TName extends MinnowValueScalarFunctionSpelling,
  TArgs extends readonly unknown[],
> =
  HasMinnowColumns<DB> extends true
    ? Uppercase<TName> extends "NULLIF"
      ? ExtractTypeFromReferenceExpression<DB, TB, TArgs[0]> | null
      : | Exclude<ReferenceValues<DB, TB, TArgs>, null>
        | (HasRequiredReferenceValue<DB, TB, TArgs> extends true ? never : null)
    : unknown;

type MinnowFixedScalarOutput<
  DB,
  TB extends keyof DB,
  TName extends MinnowFixedScalarFunctionSpelling,
  RE,
> =
  HasMinnowColumns<DB> extends true
    ? | MinnowFixedScalarBaseOutput<TName>
      | (Uppercase<TName> extends MinnowAlwaysNullableScalarFunction
          ? null
          : Uppercase<TName> extends MinnowAlwaysNonNullScalarFunction
            ? never
            : null extends ExtractTypeFromReferenceExpression<DB, TB, RE>
              ? null
              : never)
    : unknown;

type MinnowAggregateFunction =
  "count" | "sum" | "avg" | "min" | "max" | "json_arrayagg" | "string_agg";

type MinnowAggregateFunctionSpelling = MinnowAggregateFunction | Uppercase<MinnowAggregateFunction>;

type MinnowAggregateFunctionOutput<
  DB,
  TB extends keyof DB,
  TName extends MinnowAggregateFunctionSpelling,
  RE,
> =
  HasMinnowColumns<DB> extends true
    ? Uppercase<TName> extends "COUNT"
      ? number
      : Uppercase<TName> extends "SUM" | "AVG"
        ? MinnowNumericAggregateOutput<DB, TB, RE, never>
        : Uppercase<TName> extends "MIN" | "MAX"
          ? ExtractTypeFromReferenceExpression<DB, TB, RE> | null
          : string | null
    : unknown;

type MinnowCastDataType =
  | "boolean"
  | "integer"
  | "smallint"
  | "bigint"
  | "real"
  | "double precision"
  | "text"
  | "varchar"
  | `varchar(${number})`
  | "char"
  | `char(${number})`
  | "numeric"
  | `numeric(${number}, ${number})`
  | "decimal"
  | `decimal(${number}, ${number})`
  | "date"
  | "datetime"
  | `datetime(${number})`
  | "timestamp"
  | `timestamp(${number})`
  | "timestamptz"
  | `timestamptz(${number})`
  | "time"
  | "uuid"
  | "json"
  | "jsonb";

type MinnowCastBaseOutput<TDataType extends MinnowCastDataType> = TDataType extends "boolean"
  ? boolean
  : TDataType extends "integer" | "smallint" | "bigint" | "real" | "double precision"
    ? number
    : TDataType extends
          | "date"
          | "datetime"
          | `datetime(${number})`
          | "timestamp"
          | `timestamp(${number})`
          | "timestamptz"
          | `timestamptz(${number})`
      ? Date
      : string;

type MinnowCastOutput<DB, TB extends keyof DB, RE, TDataType extends MinnowCastDataType> =
  HasMinnowColumns<DB> extends true
    ? | MinnowCastBaseOutput<TDataType>
      | (null extends ExtractTypeFromReferenceExpression<DB, TB, RE> ? null : never)
    : unknown;

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
  interface FunctionModule<DB, TB extends keyof DB> {
    <
      TName extends MinnowValueScalarFunctionSpelling,
      TArgs extends readonly [ReferenceExpression<DB, TB>, ...Array<ReferenceExpression<DB, TB>>],
    >(
      name: TName,
      args: TArgs,
    ): ExpressionWrapper<DB, TB, MinnowValueScalarOutput<DB, TB, TName, TArgs>>;
    <
      TName extends MinnowFixedScalarFunctionSpelling,
      RE extends ReferenceExpression<DB, TB> = ReferenceExpression<DB, TB>,
    >(
      name: TName,
      args?: readonly RE[],
    ): ExpressionWrapper<DB, TB, MinnowFixedScalarOutput<DB, TB, TName, RE>>;
    agg<
      TName extends MinnowAggregateFunctionSpelling,
      RE extends ReferenceExpression<DB, TB> = ReferenceExpression<DB, TB>,
    >(
      name: TName,
      args?: readonly RE[],
    ): AggregateFunctionBuilder<DB, TB, MinnowAggregateFunctionOutput<DB, TB, TName, RE>>;
    avg<RE extends ReferenceExpression<DB, TB>>(
      expr: RE,
    ): AggregateFunctionBuilder<DB, TB, MinnowNumericAggregateOutput<DB, TB, RE, number | string>>;
    count(
      expr: ReferenceExpression<DB, TB>,
    ): AggregateFunctionBuilder<DB, TB, MinnowCountOutput<DB>>;
    countAll(table: TB): AggregateFunctionBuilder<DB, TB, MinnowCountOutput<DB>>;
    countAll(): AggregateFunctionBuilder<DB, TB, MinnowCountOutput<DB>>;
    max<RE extends ReferenceExpression<DB, TB>>(
      expr: RE,
    ): AggregateFunctionBuilder<
      DB,
      TB,
      HasMinnowColumns<DB> extends true
        ? ExtractTypeFromReferenceExpression<DB, TB, RE> | null
        : ExtractTypeFromReferenceExpression<DB, TB, RE, number | string | Date | bigint>
    >;
    min<RE extends ReferenceExpression<DB, TB>>(
      expr: RE,
    ): AggregateFunctionBuilder<
      DB,
      TB,
      HasMinnowColumns<DB> extends true
        ? ExtractTypeFromReferenceExpression<DB, TB, RE> | null
        : ExtractTypeFromReferenceExpression<DB, TB, RE, number | string | Date | bigint>
    >;
    sum<RE extends ReferenceExpression<DB, TB>>(
      expr: RE,
    ): AggregateFunctionBuilder<
      DB,
      TB,
      MinnowNumericAggregateOutput<DB, TB, RE, number | string | bigint>
    >;
  }

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
    cast<RE extends ReferenceExpression<DB, TB>, TDataType extends MinnowCastDataType>(
      expr: RE,
      dataType: TDataType & ColumnDataType,
    ): ExpressionWrapper<DB, TB, MinnowCastOutput<DB, TB, RE, TDataType>>;
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
