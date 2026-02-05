import re, json, requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)

WAGE_CSV = DATA_RAW / "wage_raw.csv"
CPI_CSV  = DATA_RAW / "cpi_raw.csv"
WAGE_YOY_CSV = DATA_RAW / "wage_yoy_raw.csv"
CPI_YOY_CSV = DATA_RAW / "cpi_yoy_raw.csv"

USER_AGENT = "PXFetcher"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
}
TIMEOUT = 60

WAGE_PX = "https://px.hagstofa.is/pxis/pxweb/is/Samfelag__launogtekjur__2_lvt__1_manadartolur/LAU04000.px"
CPI_PX  = "https://px.hagstofa.is/pxis/pxweb/is/Efnahagur__visitolur__1_vnv__1_vnv/VIS01002.px/"
CPI_INFLATION_PX = "https://px.hagstofa.is/pxis/pxweb/is/Efnahagur__visitolur__1_vnv__1_vnv/VIS01000.px"

def pxweb_to_api(url):
    api = url.replace("/pxweb/", "/api/v1/")
    api = api.replace("__", "/")
    return api.rstrip("/")

def fetch_px(px_url, query):
    api = pxweb_to_api(px_url)
    r = requests.post(
        api,
        json={"query": query, "response": {"format": "json-stat2"}},
        headers=HEADERS,
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    return r.json()

def fetch_meta(px_url):
    api = pxweb_to_api(px_url)
    r = requests.get(api, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def jsonstat_to_df(js):
    dims = js["id"]
    sizes = js["size"]
    values = js["value"]
    labels = {
        d: list(js["dimension"][d]["category"]["label"].values())
        for d in dims
    }

    rows = []
    idx = 0
    for i, m in enumerate(labels[dims[0]]):
        row = {dims[0]: m, "value": values[idx]}
        rows.append(row)
        idx += 1

    return pd.DataFrame(rows)

def fetch_wage():
    meta = fetch_meta(WAGE_PX)
    months = meta["variables"][0]["values"]
    unit = meta["variables"][1]["values"][0]
    month_code = meta["variables"][0]["code"]
    unit_code = meta["variables"][1]["code"]

    js = fetch_px(WAGE_PX, [
        {"code": month_code, "selection": {"filter": "item", "values": months}},
        {"code": unit_code, "selection": {"filter": "item", "values": [unit]}}
    ])

    df = jsonstat_to_df(js)
    df["source"] = WAGE_PX
    df["fetched_at"] = datetime.now(timezone.utc).isoformat()
    df.to_csv(WAGE_CSV, index=False)
    print("Saved", WAGE_CSV)

def fetch_cpi():
    meta = fetch_meta(CPI_PX)
    months = meta["variables"][0]["values"]
    index = meta["variables"][1]["values"][0]
    base = meta["variables"][2]["values"][0]
    month_code = meta["variables"][0]["code"]
    index_code = meta["variables"][1]["code"]
    base_code = meta["variables"][2]["code"]

    js = fetch_px(CPI_PX, [
        {"code": month_code, "selection": {"filter": "item", "values": months}},
        {"code": index_code, "selection": {"filter": "item", "values": [index]}},
        {"code": base_code, "selection": {"filter": "item", "values": [base]}}
    ])

    df = jsonstat_to_df(js)
    df["source"] = CPI_PX
    df["fetched_at"] = datetime.now(timezone.utc).isoformat()
    df.to_csv(CPI_CSV, index=False)
    print("Saved", CPI_CSV)

def fetch_wage_yoy():
    meta = fetch_meta(WAGE_PX)
    months = meta["variables"][0]["values"]
    unit_change_a = "change_A"
    month_code = meta["variables"][0]["code"]
    unit_code = meta["variables"][1]["code"]

    js = fetch_px(WAGE_PX, [
        {"code": month_code, "selection": {"filter": "item", "values": months}},
        {"code": unit_code, "selection": {"filter": "item", "values": [unit_change_a]}}
    ])

    df = jsonstat_to_df(js)
    df["source"] = WAGE_PX
    df["fetched_at"] = datetime.now(timezone.utc).isoformat()
    df.to_csv(WAGE_YOY_CSV, index=False)
    print("Saved", WAGE_YOY_CSV)

def fetch_cpi_yoy():
    meta = fetch_meta(CPI_INFLATION_PX)
    months = meta["variables"][0]["values"]
    cpi_index = "CPI"
    item_change_a = "change_A"
    month_code = meta["variables"][0]["code"]
    index_code = meta["variables"][1]["code"]
    item_code = meta["variables"][2]["code"]

    js = fetch_px(CPI_INFLATION_PX, [
        {"code": month_code, "selection": {"filter": "item", "values": months}},
        {"code": index_code, "selection": {"filter": "item", "values": [cpi_index]}},
        {"code": item_code, "selection": {"filter": "item", "values": [item_change_a]}}
    ])

    df = jsonstat_to_df(js)
    df["source"] = CPI_INFLATION_PX
    df["fetched_at"] = datetime.now(timezone.utc).isoformat()
    df.to_csv(CPI_YOY_CSV, index=False)
    print("Saved", CPI_YOY_CSV)

if __name__ == "__main__":
    fetch_wage()
    fetch_cpi()
    fetch_wage_yoy()
    fetch_cpi_yoy()
