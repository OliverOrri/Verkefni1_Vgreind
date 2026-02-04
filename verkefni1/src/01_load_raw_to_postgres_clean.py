import re
from pathlib import Path
import os
import unicodedata
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
SQL_DIR = BASE_DIR / "sql"

WAGE_RAW_CSV = DATA_RAW / "wage_raw.csv"
CPI_RAW_CSV = DATA_RAW / "cpi_raw.csv"
WAGE_CLEAN_CSV = DATA_PROCESSED / "wage_clean.csv"
CPI_CLEAN_CSV = DATA_PROCESSED / "cpi_clean.csv"

SCHEMA_SQL = SQL_DIR / "00_schema.sql"
VIEWS_SQL = SQL_DIR / "01_views.sql"
MIN_MONTH = "2000-01"
MAX_MONTH = "2025-12"


def parse_px_month(px_month: str) -> str:
    """
    Converts '1989M01' -> '1989-01'
    """
    m = re.match(r"^(\d{4})M(\d{2})$", str(px_month).strip())
    if not m:
        raise ValueError(f"Unexpected month format: {px_month}")
    return f"{m.group(1)}-{m.group(2)}"


def clean_numeric(series: pd.Series) -> pd.Series:
    """
    Converts value_text to numeric. Handles '..' and missing.
    """
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


def load_sql_file(cur, path: Path):
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    sql = "\n".join(lines)
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)


def get_pg_dsn() -> str:
    return os.environ.get("PG_DSN") or os.environ.get("DATABASE_URL") or "postgresql://postgres:postgres@localhost:5432/wage_cpi"


def connect_postgres(dsn: str):
    try:
        import psycopg
        return psycopg.connect(dsn)
    except ImportError:
        import psycopg2
        return psycopg2.connect(dsn)


def quality_checks_python(wage_clean: pd.DataFrame, cpi_clean: pd.DataFrame):
    if wage_clean["Month"].duplicated().any():
        raise ValueError("WAGE CLEAN: duplicate Month values after cleaning.")
    if cpi_clean["Month"].duplicated().any():
        raise ValueError("CPI CLEAN: duplicate Month values after cleaning.")

    print("Python QC: wage NA =", int(wage_clean["WageIndex"].isna().sum()))
    print("Python QC: cpi  NA =", int(cpi_clean["CPI"].isna().sum()))

    print("Python QC: wage mean =", float(wage_clean["WageIndex"].mean()))
    print("Python QC: cpi  mean =", float(cpi_clean["CPI"].mean()))


def quality_checks_sql(conn):
    cur = conn.cursor()

    for t in ["wage_index_raw", "cpi_raw", "wage_index_clean", "cpi_clean"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"SQL rows {t}:", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM wage_cpi_merged_v")
    print("SQL rows wage_cpi_merged_v:", cur.fetchone()[0])

    cur.execute("SELECT AVG(wage_index), MIN(wage_index), MAX(wage_index) FROM wage_index_clean")
    print("SQL wage_index_clean avg/min/max:", cur.fetchone())

    cur.execute("SELECT AVG(cpi), MIN(cpi), MAX(cpi) FROM cpi_clean")
    print("SQL cpi_clean avg/min/max:", cur.fetchone())


def filter_month_window(df: pd.DataFrame, month_col: str) -> pd.DataFrame:
    return df[(df[month_col] >= MIN_MONTH) & (df[month_col] <= MAX_MONTH)].reset_index(drop=True)


def main():
    if not WAGE_RAW_CSV.exists():
        raise FileNotFoundError(f"Missing {WAGE_RAW_CSV}. Run src/00_fetch_to_raw_csv.py first.")
    if not CPI_RAW_CSV.exists():
        raise FileNotFoundError(f"Missing {CPI_RAW_CSV}. Run src/00_fetch_to_raw_csv.py first.")
    if not SCHEMA_SQL.exists() or not VIEWS_SQL.exists():
        raise FileNotFoundError("Missing sql/00_schema.sql or sql/01_views.sql. Create those files first.")

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

    wage_clean = pd.DataFrame({
        "Month": wage_raw["month_code"].map(parse_px_month),
        "WageIndex": clean_numeric(wage_raw["value_text"])
    }).sort_values("Month").reset_index(drop=True)
    wage_clean = filter_month_window(wage_clean, "Month")

    cpi_clean = pd.DataFrame({
        "Month": cpi_raw["month_code"].map(parse_px_month),
        "CPI": clean_numeric(cpi_raw["value_text"])
    }).sort_values("Month").reset_index(drop=True)
    cpi_clean = filter_month_window(cpi_clean, "Month")

    cpi_clean = cpi_clean.dropna(subset=["CPI"]).reset_index(drop=True)

    quality_checks_python(wage_clean, cpi_clean)

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    wage_clean.to_csv(WAGE_CLEAN_CSV, index=False, encoding="utf-8")
    cpi_clean.to_csv(CPI_CLEAN_CSV, index=False, encoding="utf-8")
    print("Saved:", WAGE_CLEAN_CSV)
    print("Saved:", CPI_CLEAN_CSV)

    dsn = get_pg_dsn()
    conn = connect_postgres(dsn)
    try:
        conn.autocommit = False
        cur = conn.cursor()

        load_sql_file(cur, SCHEMA_SQL)

        wage_raw_rows = list(wage_raw[["month_code", "value_text", "source", "fetched_at"]].itertuples(index=False, name=None))
        cpi_raw_rows = list(cpi_raw[["month_code", "value_text", "source", "fetched_at"]].itertuples(index=False, name=None))

        cur.executemany(
            "INSERT INTO wage_index_raw (month_code, value_text, source, fetched_at) VALUES (%s, %s, %s, %s)",
            wage_raw_rows
        )
        cur.executemany(
            "INSERT INTO cpi_raw (month_code, value_text, source, fetched_at) VALUES (%s, %s, %s, %s)",
            cpi_raw_rows
        )

        wage_clean_rows = list(wage_clean.rename(columns={"Month": "month", "WageIndex": "wage_index"}).itertuples(index=False, name=None))
        cpi_clean_rows = list(cpi_clean.rename(columns={"Month": "month", "CPI": "cpi"}).itertuples(index=False, name=None))

        cur.executemany(
            "INSERT INTO wage_index_clean (month, wage_index) VALUES (%s, %s)",
            wage_clean_rows
        )
        cur.executemany(
            "INSERT INTO cpi_clean (month, cpi) VALUES (%s, %s)",
            cpi_clean_rows
        )

        load_sql_file(cur, VIEWS_SQL)

        conn.commit()
        quality_checks_sql(conn)

        print("Done. Loaded data into Postgres.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
