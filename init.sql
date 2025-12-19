DROP TABLE IF EXISTS portfolio_allocations;
DROP TABLE IF EXISTS portfolio_holdings;
DROP TABLE IF EXISTS daily_nav;
DROP TABLE IF EXISTS funds_codes;
DROP TABLE IF EXISTS funds_fees;
DROP TABLE IF EXISTS funds_statistics;
DROP TABLE IF EXISTS funds_master;

CREATE TABLE funds_master (
    fund_code VARCHAR(50) PRIMARY KEY,
    full_name_th TEXT,
    amc VARCHAR(100),
    category VARCHAR(100),
    risk_level INT,
    is_dividend VARCHAR(100),
    inception_date DATE,
    currency VARCHAR(10) DEFAULT 'THB',
    country VARCHAR(50) DEFAULT 'Thailand',
    fund_status VARCHAR(20) DEFAULT 'active',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_code) REFERENCES funds_master(fund_code) ON DELETE CASCADE
);

CREATE TABLE funds_fees (
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
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_code) REFERENCES funds_master(fund_code) ON DELETE CASCADE
);

CREATE TABLE funds_codes (
    fund_code VARCHAR(50),
    type VARCHAR(50),
    code VARCHAR(50),
    factsheet_url TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fund_code, code),
    FOREIGN KEY (fund_code) REFERENCES funds_master(fund_code) ON DELETE CASCADE
);

CREATE TABLE daily_nav (
    fund_code VARCHAR(50),
    nav_date DATE,
    nav_value DECIMAL(18,4),
    bid_price_per_unit DECIMAL(18,4),
    offer_price_per_unit DECIMAL(18,4),
    aum DECIMAL(25,2),
    data_source VARCHAR(20),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fund_code, nav_date),
    FOREIGN KEY (fund_code) REFERENCES funds_master(fund_code) ON DELETE CASCADE
);
CREATE TABLE portfolio_holdings (
    fund_code VARCHAR(50),
    name VARCHAR(255),
    data_source VARCHAR(20),
    type VARCHAR(50),
    percent DECIMAL(10,4),
    as_of_date DATE,
    source_url TEXT,
    holding_type VARCHAR(20),
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fund_code, name, data_source),
    FOREIGN KEY (fund_code) REFERENCES funds_master(fund_code) ON DELETE CASCADE
);
CREATE TABLE portfolio_allocations (
    fund_code VARCHAR(50),
    name VARCHAR(255),
    type VARCHAR(50),
    data_source VARCHAR(20),
    percent DECIMAL(10,4),
    as_of_date DATE,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fund_code, name, type, data_source),
    FOREIGN KEY (fund_code) REFERENCES funds_master(fund_code) ON DELETE CASCADE
);