"""Composable data-quality engine for timeseries fact tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql import types as T


_TIMESTAMP_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy/MM/dd HH:mm:ss",
    "dd.MM.yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
]


@dataclass(frozen=True)
class DqRule:
    rule_id: str
    rule_name: str
    target_column: str
    rule_type: str
    severity: str
    params: dict[str, Any]
    enabled: bool = True
    is_blocking: bool = False


DEFAULT_TIMESERIES_RULES = [
    DqRule(
        rule_id="ts_timestamp_not_null",
        rule_name="Timestamp must be present",
        target_column="timestamp",
        rule_type="not_null",
        severity="high",
        params={},
    ),
    DqRule(
        rule_id="ts_timestamp_parseable",
        rule_name="Timestamp must be parseable",
        target_column="timestamp",
        rule_type="timestamp_parseable",
        severity="high",
        params={"formats": _TIMESTAMP_FORMATS},
    ),
    DqRule(
        rule_id="ts_unique_point",
        rule_name="Timeseries point key must be unique",
        target_column="channel_id",
        rule_type="no_duplicate_key",
        severity="high",
        params={"key_columns": ["uuid", "group", "sample_offset", "channel_id"]},
    ),
]


def _apply_selector(df: DataFrame, rule: DqRule) -> DataFrame:
    selected = df
    channel_col = rule.params.get("channel_column", "channel_id")
    channel_equals = rule.params.get("channel_equals")
    if channel_equals:
        selected = selected.filter(F.col(channel_col) == channel_equals)
    channel_regex = rule.params.get("channel_regex")
    if channel_regex:
        selected = selected.filter(F.col(channel_col).rlike(channel_regex))
    return selected


def _empty_failures_df(df: DataFrame) -> DataFrame:
    schema = T.StructType(df.schema.fields + [
        T.StructField("rule_id", T.StringType(), False),
        T.StructField("rule_name", T.StringType(), False),
        T.StructField("severity", T.StringType(), False),
        T.StructField("is_blocking", T.BooleanType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("evaluated_at", T.TimestampType(), False),
    ])
    return df.sparkSession.createDataFrame([], schema)


def _parse_timestamp(raw_col, formats: list[str]) -> F.Column:
    parsed = None
    for fmt in formats:
        candidate = F.to_timestamp(raw_col, fmt)
        parsed = candidate if parsed is None else F.coalesce(parsed, candidate)
    return F.coalesce(parsed, F.to_timestamp(raw_col))


def evaluate_rule(df: DataFrame, rule: DqRule) -> DataFrame:
    scoped = _apply_selector(df, rule)

    if rule.rule_type == "not_null":
        failed = scoped.filter(F.col(rule.target_column).isNull())
    elif rule.rule_type == "timestamp_parseable":
        formats = rule.params.get("formats") or [rule.params.get("format", "yyyy-MM-dd HH:mm:ss")]
        parsed_col = _parse_timestamp(F.col(rule.target_column), formats)
        failed = scoped.filter(
            F.col(rule.target_column).isNotNull()
            & parsed_col.isNull()
        )
    elif rule.rule_type == "no_duplicate_key":
        keys = rule.params["key_columns"]
        dupes = scoped.groupBy(*keys).count().filter(F.col("count") > 1).drop("count")
        failed = scoped.join(dupes, on=keys, how="inner")
    elif rule.rule_type == "monotonic_increasing":
        partition_cols = rule.params.get("partition_by", ["uuid", "group"])
        order_col = rule.params.get("order_by", "sample_offset")
        parsed_col = F.to_timestamp(F.col(rule.target_column), rule.params.get("format", "yyyy-MM-dd HH:mm:ss"))
        window = Window.partitionBy(*partition_cols).orderBy(order_col)
        failed = (
            scoped.withColumn("_parsed_value", parsed_col)
            .withColumn("_prev_value", F.lag("_parsed_value").over(window))
            .filter(F.col("_prev_value").isNotNull() & (F.col("_parsed_value") < F.col("_prev_value")))
            .drop("_parsed_value", "_prev_value")
        )
    else:
        raise ValueError(f"Unsupported rule type: {rule.rule_type}")

    return (
        failed.withColumn("rule_id", F.lit(rule.rule_id))
        .withColumn("rule_name", F.lit(rule.rule_name))
        .withColumn("severity", F.lit(rule.severity))
        .withColumn("is_blocking", F.lit(rule.is_blocking))
        .withColumn("status", F.lit("FAILED"))
        .withColumn("evaluated_at", F.current_timestamp())
    )


def evaluate_rules(df: DataFrame, rules: list[DqRule]) -> DataFrame:
    active_rules = [rule for rule in rules if rule.enabled]
    if not active_rules:
        return _empty_failures_df(df)

    failures = None
    for rule in active_rules:
        rule_failures = evaluate_rule(df, rule)
        failures = rule_failures if failures is None else failures.unionByName(rule_failures, allowMissingColumns=True)
    return failures if failures is not None else _empty_failures_df(df)


def summarize_failures(failures: DataFrame) -> DataFrame:
    return failures.groupBy("rule_id", "rule_name", "severity", "is_blocking").agg(
        F.count("*").alias("total_failed"),
        F.countDistinct("uuid").alias("affected_files"),
        F.current_timestamp().alias("summarized_at"),
    )
