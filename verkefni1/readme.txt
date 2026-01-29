How this works

This project downloads wage and CPI data from Statistics Iceland (PXWeb),
cleans it, loads it into SQLite, and exports a merged CSV for Excel/Jamovi.

1) Fetch raw CSVs (data/raw)
   python src\\00_fetch_to_raw_csv.py

2) Clean + load into SQLite and create SQL views
   python src\\01_load_raw_to_sql_clean.py

3) Export merged dataset for Excel/Jamovi (data/processed)
   python src\\02_export_merged_for_jamovi.py

Output files
- data/raw/wage_raw.csv
- data/raw/cpi_raw.csv
- iceland_wage_cpi.sqlite
- data/processed/wage_clean.csv
- data/processed/cpi_clean.csv
- data/processed/wage_cpi_merged_from_sql.csv

Notes
- If Excel has the merged CSV open, the export step will fail. Close the file and re-run.
- If you run the scripts from outside the project folder, it still works.

Where to run (important)
The safest way is to run from the project folder:

  cd c:\Users\OliverOrri\OneDrive\Læra\verkefni1
  python src\00_fetch_to_raw_csv.py
  python src\01_load_raw_to_sql_clean.py
  python src\02_export_merged_for_jamovi.py

You can also run with full paths from anywhere:

  python c:\Users\OliverOrri\OneDrive\Læra\verkefni1\src\00_fetch_to_raw_csv.py
  python c:\Users\OliverOrri\OneDrive\Læra\verkefni1\src\01_load_raw_to_sql_clean.py
  python c:\Users\OliverOrri\OneDrive\Læra\verkefni1\src\02_export_merged_for_jamovi.py
