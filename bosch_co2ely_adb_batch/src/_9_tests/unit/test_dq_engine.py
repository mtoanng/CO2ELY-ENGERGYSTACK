"""Unit tests for the silver DQ engine."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

from pyspark.sql import types as T

_ENRICH_DIR = Path(__file__).resolve().parent.parent.parent / "_2_b2s"
sys.path.insert(0, str(_ENRICH_DIR))

from dq_engine import DqRule, _empty_failures_df, evaluate_rules  # noqa: E402


class TestDqEngine:
    def test_not_null_rule_flags_missing_timestamp(self):
        df = MagicMock()
        selected_df = MagicMock()
        failed_df = MagicMock()

        df.filter.return_value = selected_df
        selected_df.filter.return_value = failed_df
        failed_df.withColumn.return_value = failed_df

        rule = DqRule(
            rule_id="ts_timestamp_not_null",
            rule_name="Timestamp must be present",
            target_column="value_str",
            rule_type="not_null",
            severity="high",
            params={"channel_equals": "timestamp"},
        )

        result = evaluate_rules(df, [rule])
        assert result is failed_df
        df.filter.assert_called()
        selected_df.filter.assert_called()
        assert failed_df.withColumn.call_count == 6

    def test_duplicate_rule_flags_duplicate_points(self):
        df = MagicMock()
        grouped = MagicMock()
        counted = MagicMock()
        filtered_dupes = MagicMock()
        deduped = MagicMock()
        joined = MagicMock()
        final_df = MagicMock()

        df.groupBy.return_value = grouped
        grouped.count.return_value = counted
        counted.filter.return_value = filtered_dupes
        filtered_dupes.drop.return_value = deduped
        df.join.return_value = joined
        joined.withColumn.side_effect = [joined, joined, joined, joined, joined, final_df]

        rule = DqRule(
            rule_id="ts_unique_point",
            rule_name="Timeseries point key must be unique",
            target_column="channel",
            rule_type="no_duplicate_key",
            severity="high",
            params={"key_columns": ["uuid", "group", "sample_offset", "channel"]},
        )

        result = evaluate_rules(df, [rule])
        assert result is final_df
        df.groupBy.assert_called_once_with("uuid", "group", "sample_offset", "channel")
        df.join.assert_called_once()

    def test_no_enabled_rules_returns_empty_failure_schema(self):
        spark = MagicMock()
        df = MagicMock()
        df.schema.fields = [
            T.StructField("uuid", T.StringType(), True),
            T.StructField("group", T.StringType(), True),
        ]
        df.sparkSession = spark

        rules = [
            DqRule(
                rule_id="disabled_rule",
                rule_name="Disabled rule",
                target_column="value_str",
                rule_type="not_null",
                severity="low",
                params={"channel_equals": "timestamp"},
                enabled=False,
            )
        ]

        evaluate_rules(df, rules)
        schema = spark.createDataFrame.call_args.args[1]
        assert spark.createDataFrame.call_args.args[0] == []
        assert "rule_id" in schema.fieldNames()
        assert "status" in schema.fieldNames()
        assert "evaluated_at" in schema.fieldNames()

    def test_empty_failures_schema_extends_source_schema(self):
        spark = MagicMock()
        df = MagicMock()
        df.schema.fields = [T.StructField("uuid", T.StringType(), True)]
        df.sparkSession = spark

        _empty_failures_df(df)
        schema = spark.createDataFrame.call_args.args[1]
        assert schema.fieldNames()[0] == "uuid"
        assert schema.fieldNames()[-1] == "evaluated_at"
