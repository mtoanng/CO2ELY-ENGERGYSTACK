"""Unit tests for silver_timeseries_enriched.py."""

from unittest.mock import MagicMock, patch

from silver_timeseries_enriched import build_enriched_timeseries_df


class TestSilverTimeseriesEnriched:
    def test_joins_mapping_and_metadata_then_selects_serving_columns(self):
        timeseries_df = MagicMock(name="timeseries_df")
        channel_df = MagicMock(name="channel_df")
        filemeta_df = MagicMock(name="filemeta_df")
        mapping_df = MagicMock(name="mapping_df")

        channel_meta = MagicMock(name="channel_meta")
        file_series = MagicMock(name="file_series")
        joined_channel = MagicMock(name="joined_channel")
        joined_file = MagicMock(name="joined_file")
        joined_mapping = MagicMock(name="joined_mapping")
        after_std = MagicMock(name="after_std")
        after_event_raw = MagicMock(name="after_event_raw")
        after_event_ts = MagicMock(name="after_event_ts")
        after_valid = MagicMock(name="after_valid")
        after_elapsed = MagicMock(name="after_elapsed")
        final_df = MagicMock(name="final_df")

        channel_df.select.return_value = channel_meta
        filemeta_df.select.return_value = file_series
        timeseries_df.join.return_value = joined_channel
        joined_channel.join.return_value = joined_file
        joined_file.join.return_value = joined_mapping
        joined_mapping.withColumn.return_value = after_std
        after_std.withColumn.return_value = after_event_raw
        after_event_raw.withColumn.return_value = after_event_ts
        after_event_ts.withColumn.return_value = after_valid
        after_valid.withColumn.return_value = after_elapsed
        after_elapsed.withColumn.return_value = after_elapsed
        after_elapsed.select.return_value = final_df

        with patch("silver_timeseries_enriched._parse_timestamp"):
            result = build_enriched_timeseries_df(timeseries_df, channel_df, filemeta_df, mapping_df)

        assert result is final_df
        channel_df.select.assert_called_once_with("uuid", "group", "channel_id", "raw_channel", "unit")
        filemeta_df.select.assert_called_once_with("uuid", "series")
        timeseries_df.join.assert_called_once()
        joined_channel.join.assert_called_once()
        joined_file.join.assert_called_once()
        assert after_elapsed.select.call_args.args[0] == "series"
