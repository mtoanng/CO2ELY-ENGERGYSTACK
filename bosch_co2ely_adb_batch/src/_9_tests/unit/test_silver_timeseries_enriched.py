"""Unit tests for silver_timeseries_enriched.py."""

from unittest.mock import MagicMock, patch

from silver_timeseries_enriched import build_enriched_timeseries_df


class TestSilverTimeseriesEnriched:
    def test_promotes_real_time_and_time_channels(self):
        timeseries_df = MagicMock(name="timeseries_df")
        channel_df = MagicMock(name="channel_df")
        channel_roles = MagicMock(name="channel_roles")
        joined = MagicMock(name="joined")
        event_rows = MagicMock(name="event_rows")
        event_select = MagicMock(name="event_select")
        event_grouped = MagicMock(name="event_grouped")
        event_agg = MagicMock(name="event_agg")
        elapsed_rows = MagicMock(name="elapsed_rows")
        elapsed_select = MagicMock(name="elapsed_select")
        elapsed_grouped = MagicMock(name="elapsed_grouped")
        elapsed_final = MagicMock(name="elapsed_final")
        measurements = MagicMock(name="measurements")
        joined_with_event = MagicMock(name="joined_with_event")
        joined_with_elapsed = MagicMock(name="joined_with_elapsed")
        final_after_coalesce = MagicMock(name="final_after_coalesce")
        final_df = MagicMock(name="final_df")

        timeseries_df.join.return_value = joined
        joined.filter.side_effect = [event_rows, elapsed_rows, measurements]

        event_rows.select.return_value = event_select
        event_select.groupBy.return_value = event_grouped
        event_grouped.agg.return_value = event_agg
        event_agg.withColumn.return_value = event_agg

        elapsed_rows.select.return_value = elapsed_select
        elapsed_select.groupBy.return_value = elapsed_grouped
        elapsed_grouped.agg.return_value = elapsed_final

        measurements.join.return_value = joined_with_event
        joined_with_event.join.return_value = joined_with_elapsed
        joined_with_elapsed.withColumn.return_value = final_after_coalesce
        final_after_coalesce.select.return_value = final_df

        with patch("silver_timeseries_enriched._detect_channel_roles", return_value=channel_roles), patch(
            "silver_timeseries_enriched._parse_timestamp"
        ):
            result = build_enriched_timeseries_df(timeseries_df, channel_df)

        assert result is final_df
        timeseries_df.join.assert_called_once()
        assert joined.filter.call_count == 3
        event_grouped.agg.assert_called_once()
        elapsed_grouped.agg.assert_called_once()
        measurements.join.assert_called_once()
        joined_with_event.join.assert_called_once()
        final_after_coalesce.select.assert_called_once()

    def test_excludes_structural_time_channels_from_measurements(self):
        timeseries_df = MagicMock(name="timeseries_df")
        channel_df = MagicMock(name="channel_df")
        channel_roles = MagicMock(name="channel_roles")
        joined = MagicMock(name="joined")
        event_rows = MagicMock(name="event_rows")
        elapsed_rows = MagicMock(name="elapsed_rows")
        measurements = MagicMock(name="measurements")
        event_select = MagicMock(name="event_select")
        event_grouped = MagicMock(name="event_grouped")
        event_agg = MagicMock(name="event_agg")
        elapsed_select = MagicMock(name="elapsed_select")
        elapsed_grouped = MagicMock(name="elapsed_grouped")
        elapsed_agg = MagicMock(name="elapsed_agg")
        joined_after_event = MagicMock(name="joined_after_event")
        joined_after_elapsed = MagicMock(name="joined_after_elapsed")
        final_after_coalesce = MagicMock(name="final_after_coalesce")
        final_df = MagicMock(name="final_df")

        timeseries_df.join.return_value = joined
        joined.filter.side_effect = [event_rows, elapsed_rows, measurements]

        event_rows.select.return_value = event_select
        event_select.groupBy.return_value = event_grouped
        event_grouped.agg.return_value = event_agg
        event_agg.withColumn.return_value = event_agg

        elapsed_rows.select.return_value = elapsed_select
        elapsed_select.groupBy.return_value = elapsed_grouped
        elapsed_grouped.agg.return_value = elapsed_agg

        measurements.join.return_value = joined_after_event
        joined_after_event.join.return_value = joined_after_elapsed
        joined_after_elapsed.withColumn.return_value = final_after_coalesce
        final_after_coalesce.select.return_value = final_df

        with patch("silver_timeseries_enriched._detect_channel_roles", return_value=channel_roles), patch(
            "silver_timeseries_enriched._parse_timestamp"
        ):
            result = build_enriched_timeseries_df(timeseries_df, channel_df)

        assert result is final_df
        joined.filter.assert_called()
        measurements.join.assert_called_once()
        joined_after_event.join.assert_called_once()
        final_after_coalesce.select.assert_called_once()
