from pathlib import Path
import os
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
OUT_CSV = BASE_DIR / "data" / "processed" / "wage_cpi_merged_from_sql.csv"
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)


def get_pg_dsn() -> str:
    return os.environ.get("PG_DSN") or os.environ.get("DATABASE_URL") or "postgresql://postgres:postgres@localhost:5432/wage_cpi"


def connect_postgres(dsn: str):
    try:
        import psycopg
        return psycopg.connect(dsn)
    except ImportError:
        import psycopg2
        return psycopg2.connect(dsn)


def main():
    dsn = get_pg_dsn()
    conn = connect_postgres(dsn)
    try:
        df = pd.read_sql_query("SELECT * FROM wage_cpi_merged_v ORDER BY month", conn)
    finally:
        conn.close()

    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print("Saved:", OUT_CSV)
    print("Rows exported:", len(df))


if __name__ == "__main__":
    main()
