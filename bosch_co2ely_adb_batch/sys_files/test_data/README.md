# Test Data

Integration test fixtures for the CO2 Energystack pipeline.

## Expected Files

Place small sample XLSX/CSV files here for integration testing:

- `sample_2sheet.xlsx` — 2 sheets, 10 rows each, with units row
- `sample_no_units.csv` — CSV without units row (2 header rows only)
- `sample_with_units.csv` — CSV with 3-row header (units detected)
- `sample_empty_sheet.xlsx` — XLSX with 1 empty + 1 valid sheet

## Usage

Integration test job copies these to the test UC Volume:
  /Volumes/{catalog}/converter/raw_data/_int_test/

After test, converter processes them and results are validated
by sys_validate_tables.py.
