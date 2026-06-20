import io
from pathlib import Path

import pyarrow.parquet as pq

from xlsx_converter import convert

sample_path = Path(r"c:\Users\got4hc\CO2ELY_ENERGYSTACK\CO2ELY_ENERGYSTACK\bosch_co2ely_adb_batch\sys_files\test_data\test\PoC Stack VI\PoC VI Stack Measurement.xlsx")
relative_path = "test/PoC Stack VI/PoC VI Stack Measurement.xlsx"
abfss_path = "abfss://local@test.dfs.core.windows.net/" + relative_path.replace("\\", "/")

file_bytes = sample_path.read_bytes()
results = convert(file_bytes, relative_path, len(file_bytes), None, abfss_file_path=abfss_path)

print("FIRST 5 TIMESERIES ROWS PER GROUP")
print("=" * 80)
for idx, result in enumerate(results, start=1):
    if result.timeseries_buffer is not None:
        ts_table = pq.read_table(io.BytesIO(result.timeseries_buffer.getvalue()))
    else:
        ts_table = result.tables["timeseries"]
    ts_df = ts_table.to_pandas()
    print(f"\n[GROUP {idx}] {result.group_name}")
    print(ts_df.head(5).to_string(index=False))
