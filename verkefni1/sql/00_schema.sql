-- 00_schema.sql
-- Raw + Clean tables (SQLite)

DROP TABLE IF EXISTS wage_index_raw;
CREATE TABLE wage_index_raw (
  month_code TEXT NOT NULL,    -- "1989M01"
  value_text TEXT,             -- raw value as text (may be "..")
  source     TEXT,
  fetched_at TEXT
);

DROP TABLE IF EXISTS cpi_raw;
CREATE TABLE cpi_raw (
  month_code TEXT NOT NULL,    -- "1989M01"
  value_text TEXT,             -- raw value as text
  source     TEXT,
  fetched_at TEXT
);

DROP TABLE IF EXISTS wage_index_clean;
CREATE TABLE wage_index_clean (
  month      TEXT NOT NULL PRIMARY KEY, -- "YYYY-MM"
  wage_index REAL NOT NULL
);

DROP TABLE IF EXISTS cpi_clean;
CREATE TABLE cpi_clean (
  month TEXT NOT NULL PRIMARY KEY, -- "YYYY-MM"
  cpi   REAL NOT NULL
);
