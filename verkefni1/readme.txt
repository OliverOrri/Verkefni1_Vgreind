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

Postgres option (uses the same SQL files)
Set PG_DSN or DATABASE_URL, then run:

  python src\\01_load_raw_to_postgres_clean.py
  python src\\02_export_merged_for_jamovi_postgres.py

Default DSN (if env var missing):
  postgresql://postgres:postgres@localhost:5432/wage_cpi

Requires one of:
  pip install psycopg
  pip install psycopg2-binary

SQL Server option
Set SQLSERVER_CONN_STR, then run:

  python src\\01_load_raw_to_sqlserver_clean.py
  python src\\02_export_merged_for_jamovi_sqlserver.py

Default SQLSERVER_CONN_STR (if env var missing):
  Auto-detect ODBC driver (18/17/SQL Server), then:
  Server=localhost,1433;Database=wage_cpi;UID=sa;PWD=YourStrong!Passw0rd

Requires:
  pip install pyodbc

One-shot SQL Server pipeline:
  .\run_sqlserver_pipeline.ps1

Notes
- If Excel has the merged CSV open, the export step will fail. Close the file and re-run.
- If you run the scripts from outside the project folder, it still works.

Where to run (important)
The safest way is to run from the project folder:

  cd c:\verkefni1
  python src\00_fetch_to_raw_csv.py
  python src\01_load_raw_to_sql_clean.py
  python src\02_export_merged_for_jamovi.py

You can also run with full paths from anywhere:

  python c:..\verkefni1\src\00_fetch_to_raw_csv.py
  python c:..\verkefni1\src\01_load_raw_to_sql_clean.py
  python c:..\verkefni1\src\02_export_merged_for_jamovi.py
