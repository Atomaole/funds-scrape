CREATE DATABASE IF NOT EXISTS thai_funds_pre;
USE thai_funds_pre;

CREATE OR REPLACE VIEW clean_funds_master AS
SELECT 
    m.fund_code AS fund_uid,
    'TH' AS source_system,
    m.fund_code,
    m.isin AS isin_code,
    m.amc AS amc_name,
    latest_daily.nav_date,
    latest_daily.aum AS total_aum,
    m.currency,
    COALESCE(f.ter_actual, f.ter_max) AS expense_ratio, 
    m.risk_level
FROM thai_funds.funds_master_info m
LEFT JOIN (
    SELECT t1.fund_code, t1.nav_date, t1.aum
    FROM thai_funds.funds_daily t1
    JOIN (
        SELECT fund_code, MAX(nav_date) as max_date
        FROM thai_funds.funds_daily
        GROUP BY fund_code
    ) t2 ON t1.fund_code = t2.fund_code AND t1.nav_date = t2.max_date
) latest_daily ON m.fund_code = latest_daily.fund_code
LEFT JOIN thai_funds.funds_fee f ON m.fund_code = f.fund_code;

CREATE OR REPLACE VIEW clean_fund_holdings AS
SELECT 
    h.fund_code AS fund_uid,
    h.name AS asset_name,
    h.symbol AS asset_ticker,
    h.sector AS asset_sector,
    h.type AS asset_type,
    h.percent AS weight_pct,
    ROUND((m.total_aum * h.percent / 100), 2) AS holding_value,
    h.as_of_date AS update_date
FROM thai_funds.funds_holding h
JOIN clean_funds_master m ON h.fund_code = m.fund_uid;