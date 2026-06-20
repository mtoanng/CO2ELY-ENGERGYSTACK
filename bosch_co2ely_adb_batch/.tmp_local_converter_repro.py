import io
import logging
from pathlib import Path

import pyarrow.parquet as pq

from xlsx_converter import convert
from common import logger

sample_path = Path(r"c:\Users\got4hc\CO2ELY_ENERGYSTACK\CO2ELY_ENERGYSTACK\bosch_co2ely_adb_batch\sys_files\test_data\test\PoC Stack VI\PoC VI Stack Measurement.xlsx")
relative_path = "test/PoC Stack VI/PoC VI Stack Measurement.xlsx"
abfss_path = "abfss://local@test.dfs.core.windows.net/" + relative_path.replace("\\", "/")

for handler in logger.handlers:
    handler.setLevel(logging.INFO)
logger.setLevel(logging.INFO)

print("=" * 100)
print(f"Local converter repro for: {sample_path}")
print(f"File exists: {sample_path.exists()}")
print(f"File size: {sample_path.stat().st_size if sample_path.exists() else 'n/a'}")
print("=" * 100)

file_bytes = sample_path.read_bytes()
results = convert(
    file_bytes,
    relative_path,
    len(file_bytes),
    None,
    abfss_file_path=abfss_path,
)

print("=" * 100)
print(f"Conversion returned {len(results)} result group(s)")
print("=" * 100)

for idx, result in enumerate(results, start=1):
    print(f"\n[GROUP {idx}] group_name={result.group_name!r} n_rows={result.n_rows} n_channels={result.n_channels}")

    channel_table = result.tables["channel"]
    channel_df = channel_table.to_pandas()
    print("\nCHANNEL TABLE:")
    print(channel_df.to_string(index=False))

    if result.timeseries_buffer is not None:
        ts_table = pq.read_table(io.BytesIO(result.timeseries_buffer.getvalue()))
    else:
        ts_table = result.tables["timeseries"]

    ts_df = ts_table.to_pandas()
    print("\nTIMESERIES HEAD (first 120 rows):")
    print(ts_df.head(120).to_string(index=False))

    if "channel" in ts_df.columns:
        ts_only = ts_df[ts_df["channel"].astype(str).str.contains("timestamp|real time", case=False, regex=True, na=False)]
        print("\nTIMESTAMP/REAL-TIME ROWS (first 200):")
        if len(ts_only) == 0:
            print("<none>")
        else:
            print(ts_only.head(200).to_string(index=False))

print("\n" + "=" * 100)
print("END OF LOCAL CONVERTER REPRO")
print("=" * 100)
