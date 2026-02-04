import os
import re
import unicodedata
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"

WAGE_RAW_CSV = DATA_RAW / "wage_raw.csv"
CPI_RAW_CSV = DATA_RAW / "cpi_raw.csv"
WAGE_CLEAN_CSV = DATA_PROCESSED / "wage_clean.csv"
CPI_CLEAN_CSV = DATA_PROCESSED / "cpi_clean.csv"

MIN_MONTH = "2000-01"
MAX_MONTH = "2025-12"


def parse_px_month(px_month: str) -> str:
    m = re.match(r"^(\d{4})M(\d{2})$", str(px_month).strip())
    if not m:
        raise ValueError(f"Unexpected month format: {px_month}")
    return f"{m.group(1)}-{m.group(2)}"


def clean_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.replace({None: pd.NA, "..": pd.NA, "â€¦": pd.NA}), errors="coerce")


def assert_required_columns(df: pd.DataFrame, required: set, label: str):
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{label}: missing columns: {missing}")


def _normalize_col_name(name: str) -> str:
    text = str(name)
    text = text.replace("ð", "d").replace("đ", "d")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.lower().strip()


def find_month_column(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        if _normalize_col_name(col) == "manudur":
            return col
    return None


def filter_month_window(df: pd.DataFrame, month_col: str) -> pd.DataFrame:
    return df[(df[month_col] >= MIN_MONTH) & (df[month_col] <= MAX_MONTH)].reset_index(drop=True)


def get_sqlserver_conn_str() -> str:
    env_conn = os.environ.get("SQLSERVER_CONN_STR")
    if env_conn:
        return env_conn

    import pyodbc

    drivers = set(pyodbc.drivers())
    if "ODBC Driver 18 for SQL Server" in drivers:
        driver = "ODBC Driver 18 for SQL Server"
        trust = "TrustServerCertificate=yes;"
    elif "ODBC Driver 17 for SQL Server" in drivers:
        driver = "ODBC Driver 17 for SQL Server"
        trust = "TrustServerCertificate=yes;"
    elif "SQL Server" in drivers:
        driver = "SQL Server"
        trust = ""
    else:
        raise RuntimeError("No SQL Server ODBC driver found. Install ODBC Driver 18 for SQL Server.")

    return (
        f"Driver={{{driver}}};"
        "Server=localhost,1433;"
        "Database=wage_cpi;"
        "UID=sa;"
        "PWD=YourStrong!Passw0rd;"
        f"{trust}"
    )


def connect_sqlserver(conn_str: str):
    import pyodbc

    return pyodbc.connect(conn_str)


def build_schema(cursor):
    statements = [
        "IF OBJECT_ID('dbo.wage_index_raw','U') IS NOT NULL DROP TABLE dbo.wage_index_raw",
        "CREATE TABLE dbo.wage_index_raw (month_code NVARCHAR(7) NOT NULL, value_text NVARCHAR(64) NULL, source NVARCHAR(512) NULL, fetched_at NVARCHAR(64) NULL)",
        "IF OBJECT_ID('dbo.cpi_raw','U') IS NOT NULL DROP TABLE dbo.cpi_raw",
        "CREATE TABLE dbo.cpi_raw (month_code NVARCHAR(7) NOT NULL, value_text NVARCHAR(64) NULL, source NVARCHAR(512) NULL, fetched_at NVARCHAR(64) NULL)",
        "IF OBJECT_ID('dbo.wage_index_clean','U') IS NOT NULL DROP TABLE dbo.wage_index_clean",
        "CREATE TABLE dbo.wage_index_clean (month CHAR(7) NOT NULL PRIMARY KEY, wage_index FLOAT NOT NULL)",
        "IF OBJECT_ID('dbo.cpi_clean','U') IS NOT NULL DROP TABLE dbo.cpi_clean",
        "CREATE TABLE dbo.cpi_clean (month CHAR(7) NOT NULL PRIMARY KEY, cpi FLOAT NOT NULL)",
        "IF OBJECT_ID('dbo.wage_cpi_merged_v','V') IS NOT NULL DROP VIEW dbo.wage_cpi_merged_v",
        """
        CREATE VIEW dbo.wage_cpi_merged_v AS
        SELECT
            w.month AS month,
            CAST(SUBSTRING(w.month, 1, 4) AS INT) AS year,
            CAST(SUBSTRING(w.month, 6, 2) AS INT) AS month_num,
            CASE
                WHEN CAST(SUBSTRING(w.month, 6, 2) AS INT) IN (12, 1, 2) THEN 'Winter'
                WHEN CAST(SUBSTRING(w.month, 6, 2) AS INT) IN (3, 4, 5) THEN 'Spring'
                WHEN CAST(SUBSTRING(w.month, 6, 2) AS INT) IN (6, 7, 8) THEN 'Summer'
                ELSE 'Autumn'
            END AS season,
            w.wage_index,
            c.cpi,
            (w.wage_index / c.cpi) AS wage_to_cpi_ratio
        FROM dbo.wage_index_clean w
        INNER JOIN dbo.cpi_clean c
            ON c.month = w.month
        """
    ]
    for stmt in statements:
        cursor.execute(stmt)


def quality_checks_sql(conn):
    cur = conn.cursor()
    for t in ["wage_index_raw", "cpi_raw", "wage_index_clean", "cpi_clean"]:
        cur.execute(f"SELECT COUNT(*) FROM dbo.{t}")
        print(f"SQL rows {t}:", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM dbo.wage_cpi_merged_v")
    print("SQL rows wage_cpi_merged_v:", cur.fetchone()[0])

    cur.execute("SELECT AVG(wage_index), MIN(wage_index), MAX(wage_index) FROM dbo.wage_index_clean")
    print("SQL wage_index_clean avg/min/max:", cur.fetchone())

    cur.execute("SELECT AVG(cpi), MIN(cpi), MAX(cpi) FROM dbo.cpi_clean")
    print("SQL cpi_clean avg/min/max:", cur.fetchone())


def main():
    if not WAGE_RAW_CSV.exists() or not CPI_RAW_CSV.exists():
        raise FileNotFoundError("Missing raw CSVs. Run src/00_fetch_to_raw_csv.py first.")

    wage_raw = pd.read_csv(WAGE_RAW_CSV)
    cpi_raw = pd.read_csv(CPI_RAW_CSV)

    if "month_code" not in wage_raw.columns:
        month_col = find_month_column(wage_raw)
        if month_col:
            wage_raw = wage_raw.rename(columns={month_col: "month_code"})
    if "value_text" not in wage_raw.columns and "value" in wage_raw.columns:
        wage_raw = wage_raw.rename(columns={"value": "value_text"})

    if "month_code" not in cpi_raw.columns:
        month_col = find_month_column(cpi_raw)
        if month_col:
            cpi_raw = cpi_raw.rename(columns={month_col: "month_code"})
    if "value_text" not in cpi_raw.columns and "value" in cpi_raw.columns:
        cpi_raw = cpi_raw.rename(columns={"value": "value_text"})

    assert_required_columns(wage_raw, {"month_code", "value_text"}, "WAGE RAW")
    assert_required_columns(cpi_raw, {"month_code", "value_text"}, "CPI RAW")

    wage_clean = pd.DataFrame(
        {
            "Month": wage_raw["month_code"].map(parse_px_month),
            "WageIndex": clean_numeric(wage_raw["value_text"]),
        }
    ).sort_values("Month").reset_index(drop=True)
    wage_clean = filter_month_window(wage_clean, "Month")
    wage_clean = wage_clean.dropna(subset=["WageIndex"]).reset_index(drop=True)

    cpi_clean = pd.DataFrame(
        {
            "Month": cpi_raw["month_code"].map(parse_px_month),
            "CPI": clean_numeric(cpi_raw["value_text"]),
        }
    ).sort_values("Month").reset_index(drop=True)
    cpi_clean = filter_month_window(cpi_clean, "Month")
    cpi_clean = cpi_clean.dropna(subset=["CPI"]).reset_index(drop=True)

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    wage_clean.to_csv(WAGE_CLEAN_CSV, index=False, encoding="utf-8")
    cpi_clean.to_csv(CPI_CLEAN_CSV, index=False, encoding="utf-8")
    print("Saved:", WAGE_CLEAN_CSV)
    print("Saved:", CPI_CLEAN_CSV)

    conn_str = get_sqlserver_conn_str()
    conn = connect_sqlserver(conn_str)
    try:
        cur = conn.cursor()
        if hasattr(cur, "fast_executemany"):
            cur.fast_executemany = True

        build_schema(cur)

        wage_raw_df = wage_raw[["month_code", "value_text", "source", "fetched_at"]].fillna("").astype(str)
        cpi_raw_df = cpi_raw[["month_code", "value_text", "source", "fetched_at"]].fillna("").astype(str)
        wage_raw_rows = list(wage_raw_df.itertuples(index=False, name=None))
        cpi_raw_rows = list(cpi_raw_df.itertuples(index=False, name=None))
        cur.executemany(
            "INSERT INTO dbo.wage_index_raw (month_code, value_text, source, fetched_at) VALUES (?, ?, ?, ?)",
            wage_raw_rows,
        )
        cur.executemany(
            "INSERT INTO dbo.cpi_raw (month_code, value_text, source, fetched_at) VALUES (?, ?, ?, ?)",
            cpi_raw_rows,
        )

        wage_clean_df = wage_clean.rename(columns={"Month": "month", "WageIndex": "wage_index"}).copy()
        cpi_clean_df = cpi_clean.rename(columns={"Month": "month", "CPI": "cpi"}).copy()
        wage_clean_df["month"] = wage_clean_df["month"].astype(str)
        cpi_clean_df["month"] = cpi_clean_df["month"].astype(str)
        wage_clean_df["wage_index"] = wage_clean_df["wage_index"].astype(float)
        cpi_clean_df["cpi"] = cpi_clean_df["cpi"].astype(float)
        wage_clean_rows = list(wage_clean_df.itertuples(index=False, name=None))
        cpi_clean_rows = list(cpi_clean_df.itertuples(index=False, name=None))
        cur.executemany(
            "INSERT INTO dbo.wage_index_clean (month, wage_index) VALUES (?, ?)",
            wage_clean_rows,
        )
        cur.executemany(
            "INSERT INTO dbo.cpi_clean (month, cpi) VALUES (?, ?)",
            cpi_clean_rows,
        )

        conn.commit()
        quality_checks_sql(conn)
        print("Done. Loaded data into SQL Server.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
