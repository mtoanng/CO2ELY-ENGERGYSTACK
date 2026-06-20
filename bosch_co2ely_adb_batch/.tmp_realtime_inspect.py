import io
from pathlib import Path

import fastexcel
import polars as pl

from xlsx_converter import _merge_datetime_columns

sample_path = Path(r"c:\Users\got4hc\CO2ELY_ENERGYSTACK\CO2ELY_ENERGYSTACK\bosch_co2ely_adb_batch\sys_files\test_data\test\PoC Stack VI\PoC VI Stack Measurement.xlsx")
file_bytes = sample_path.read_bytes()
excel = fastexcel.read_excel(file_bytes)
print("=" * 100)
print(f"Sheet count: {len(excel.sheet_names)}")
print("Sheets:")
for i, name in enumerate(excel.sheet_names, start=1):
    print(f"  {i}. {name}")
print("=" * 100)

for sheet_name in excel.sheet_names:
    print(f"\n[SHEET] {sheet_name}")
    header_df = pl.read_excel(io.BytesIO(file_bytes), engine="calamine", sheet_name=sheet_name, has_header=False, infer_schema_length=0)
    print(f"shape={header_df.shape}")
    if header_df.shape[0] < 3:
        print("<too few rows>")
        continue

    n_cols = header_df.shape[1]
    col_names = header_df.columns
    row1_channel = [str(header_df[c][0] or "") for c in col_names]
    row2_channel_name = [str(header_df[c][1] or "") for c in col_names]
    row2_channel_name = [name if name.strip() else f"Column_{i}" for i, name in enumerate(row2_channel_name)]

    seen = {}
    unique_channels = []
    for ch in row1_channel:
        ch = ch.strip() if ch.strip() else "unnamed"
        if ch in seen:
            seen[ch] += 1
            unique_channels.append(f"{ch}_{seen[ch]}")
        else:
            seen[ch] = 0
            unique_channels.append(ch)
    row1_channel = unique_channels

    row3_values = [str(header_df[c][2] or "") for c in col_names]
    has_units = False
    from common import detect_units_row
    if header_df.shape[0] >= 3:
        has_units = detect_units_row(row3_values)
    data_start_row = 3 if has_units else 2
    df = header_df.slice(data_start_row)
    rename_map = {df.columns[i]: row1_channel[i] for i in range(min(len(df.columns), len(row1_channel)))}
    df = df.rename(rename_map)
    columns = list(rename_map.values())
    units = [v.strip() if v else "" for v in row3_values] if has_units else [""] * n_cols

    realtime_indices = [i for i, ch in enumerate(row2_channel_name) if str(ch).strip().lower() == "real time"]
    print(f"Real time header indices: {realtime_indices}")
    if not realtime_indices:
        continue

    for idx in realtime_indices:
        print(f"  row1_channel[{idx}]={row1_channel[idx]!r}")
        print(f"  row2_channel_name[{idx}]={row2_channel_name[idx]!r}")
        if idx + 1 < len(columns):
            print(f"  partner row1_channel[{idx+1}]={row1_channel[idx+1]!r}")
            print(f"  partner row2_channel_name[{idx+1}]={row2_channel_name[idx+1]!r}")

    sample_cols = []
    for idx in realtime_indices:
        sample_cols.append(columns[idx])
        if idx + 1 < len(columns):
            sample_cols.append(columns[idx + 1])
    sample_cols = [c for c in dict.fromkeys(sample_cols) if c in df.columns]
    print("  raw sample values:")
    print(df.select(sample_cols).head(8))

    merged_df, merged_cols, merged_row1, merged_row2, merged_units = _merge_datetime_columns(
        df, columns, row1_channel, row2_channel_name, units
    )
    print(f"  merged columns contains timestamp: {'timestamp' in merged_cols}")
    print(f"  merged columns first 10: {merged_cols[:10]}")
    if 'timestamp' in merged_df.columns:
        print("  merged timestamp sample:")
        print(merged_df.select(['timestamp']).head(12))
