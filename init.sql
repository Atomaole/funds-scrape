CREATE USER IF NOT EXISTS 'fund_master'@'%' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'fund_master'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

CREATE DATABASE IF NOT EXISTS thai_funds;
USE thai_funds;

DROP TABLE IF EXISTS funds_allocations;
DROP TABLE IF EXISTS funds_holding;
DROP TABLE IF EXISTS funds_daily;
DROP TABLE IF EXISTS funds_codes;
DROP TABLE IF EXISTS funds_fee;
DROP TABLE IF EXISTS funds_statistics;
DROP TABLE IF EXISTS funds_master_info;

CREATE TABLE funds_master_info (
    fund_code VARCHAR(50) PRIMARY KEY,
    full_name_th TEXT,
    full_name_en TEXT,
    amc VARCHAR(100),
    category VARCHAR(100),
    risk_level INT,
    is_dividend VARCHAR(100),
    inception_date DATE,
    currency VARCHAR(10) DEFAULT 'THB',
    country VARCHAR(50) DEFAULT 'Thailand',
    fund_status VARCHAR(20) DEFAULT 'active',
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE funds_statistics (
    fund_code VARCHAR(50) PRIMARY KEY,
    as_of_date DATE,
    sharpe_ratio DECIMAL(10,4),
    alpha DECIMAL(10,4),
    beta DECIMAL(10,4),
    max_drawdown DECIMAL(10,4),
    recovering_period DECIMAL(10,4),
    tracking_error DECIMAL(10,4),
    turnover_ratio DECIMAL(10,4),
    fx_hedging TEXT,
    sec_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_code) REFERENCES funds_master_info(fund_code) ON DELETE CASCADE
);
CREATE TABLE funds_fee (
    fund_code VARCHAR(50) PRIMARY KEY,
    front_end_max DECIMAL(10,4),
    front_end_actual DECIMAL(10,4),
    back_end_max DECIMAL(10,4),
    back_end_actual DECIMAL(10,4),
    management_max DECIMAL(10,4),
    management_actual DECIMAL(10,4),
    ter_max DECIMAL(10,4),
    ter_actual DECIMAL(10,4),
    switching_in_max DECIMAL(10,4),
    switching_in_actual DECIMAL(10,4),
    switching_out_max DECIMAL(10,4),
    switching_out_actual DECIMAL(10,4),
    min_initial_buy DECIMAL(20,2),
    min_next_buy DECIMAL(20,2),
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_code) REFERENCES funds_master_info(fund_code) ON DELETE CASCADE
);
CREATE TABLE funds_codes (
    fund_code VARCHAR(50),
    type VARCHAR(50),
    code VARCHAR(50),
    factsheet_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fund_code, code),
    FOREIGN KEY (fund_code) REFERENCES funds_master_info(fund_code) ON DELETE CASCADE
);
CREATE TABLE funds_daily (
    fund_code VARCHAR(50),
    nav_date DATE,
    nav_value DECIMAL(18,4),
    bid DECIMAL(18,4),
    offer DECIMAL(18,4),
    aum DECIMAL(25,2),
    source VARCHAR(20),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fund_code, nav_date),
    FOREIGN KEY (fund_code) REFERENCES funds_master_info(fund_code) ON DELETE CASCADE
);
CREATE TABLE funds_holding (
    fund_code VARCHAR(50),
    symbol VARCHAR(50),
    name VARCHAR(255),
    type VARCHAR(50),
    sector VARCHAR(50),
    percent DECIMAL(10,4),
    as_of_date DATE,
    source_url TEXT,
    holding_type VARCHAR(20),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_code) REFERENCES funds_master_info(fund_code) ON DELETE CASCADE
);
CREATE TABLE funds_allocations (
    fund_code VARCHAR(50),
    name VARCHAR(255),
    type VARCHAR(50),
    source VARCHAR(20),
    percent DECIMAL(10,4),
    as_of_date DATE,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_code) REFERENCES funds_master_info(fund_code) ON DELETE CASCADE
);