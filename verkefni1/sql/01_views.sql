-- 01_views.sql
-- Merged view with Jamovi-friendly categories

DROP VIEW IF EXISTS wage_cpi_merged_v;

CREATE VIEW wage_cpi_merged_v AS
SELECT
  w.month AS month,
  CAST(substr(w.month, 1, 4) AS INTEGER) AS year,
  CAST(substr(w.month, 6, 2) AS INTEGER) AS month_num,

  CASE
    WHEN CAST(substr(w.month, 6, 2) AS INTEGER) IN (12, 1, 2) THEN 'Winter'
    WHEN CAST(substr(w.month, 6, 2) AS INTEGER) IN (3, 4, 5) THEN 'Spring'
    WHEN CAST(substr(w.month, 6, 2) AS INTEGER) IN (6, 7, 8) THEN 'Summer'
    ELSE 'Autumn'
  END AS season,

  w.wage_index,
  c.cpi,
  (w.wage_index / c.cpi) AS wage_to_cpi_ratio

FROM wage_index_clean w
JOIN cpi_clean c
  ON c.month = w.month;

DROP VIEW IF EXISTS wage_cpi_annual_change_v;

CREATE VIEW wage_cpi_annual_change_v AS
SELECT
  month,
  CAST(substr(month, 1, 4) AS INTEGER) AS year,
  CAST(substr(month, 6, 2) AS INTEGER) AS month_num,
  wage_annual_pct,
  cpi_annual_pct
FROM wage_cpi_annual_change_clean;
