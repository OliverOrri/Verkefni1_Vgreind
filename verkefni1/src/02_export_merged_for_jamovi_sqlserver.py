import os
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
OUT_CSV = BASE_DIR / "data" / "processed" / "wage_cpi_merged_from_sql.csv"
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)


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


def main():
    conn = connect_sqlserver(get_sqlserver_conn_str())
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT month, year, month_num, season, wage_index, cpi, wage_to_cpi_ratio
            FROM dbo.wage_cpi_merged_v
            ORDER BY month
            """
        )
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description]
    finally:
        conn.close()

    df = pd.DataFrame.from_records(rows, columns=columns)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print("Saved:", OUT_CSV)
    print("Rows exported:", len(df))


if __name__ == "__main__":
    main()
