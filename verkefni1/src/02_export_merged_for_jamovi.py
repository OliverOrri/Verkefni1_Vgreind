import sqlite3
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "iceland_wage_cpi.sqlite"
OUT_CSV = BASE_DIR / "data" / "processed" / "wage_cpi_merged_from_sql.csv"
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

def main():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM wage_cpi_merged_v ORDER BY month", conn)
    finally:
        conn.close()

    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print("Saved:", OUT_CSV)
    print("Rows exported:", len(df))

if __name__ == "__main__":
    main()
