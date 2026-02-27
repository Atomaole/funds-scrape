DROP TABLE IF EXISTS fund_sector_breakdown;
DROP TABLE IF EXISTS fund_country_breakdown;
DROP TABLE IF EXISTS stock_aggregates;
DROP TABLE IF EXISTS master_fund_holdings;
DROP TABLE IF EXISTS fund_master_holdings;
DROP TABLE IF EXISTS fund_direct_holdings;
DROP TABLE IF EXISTS master_funds;
DROP TABLE IF EXISTS funds;
DROP TABLE IF EXISTS stocks;

-- TIER 1: Core Entities (Underlying Assets and Thai Funds)
-- 1. Stocks/Assets Table (Contains both Local and Global assets)
CREATE TABLE stocks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL UNIQUE,          -- e.g., 'AAPL', 'NVDA', 'PTT'
    full_name VARCHAR(255),                      -- Full company name
    sector VARCHAR(100),                         -- Business sector classification
    stock_type ENUM('TH', 'FOREIGN', 'GOLD') DEFAULT 'FOREIGN', 
    percent_change DECIMAL(5, 2) DEFAULT 0.00,
    country VARCHAR(100) DEFAULT 'USA'
);
-- 2. Thai Funds Table
CREATE TABLE funds (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name_th VARCHAR(255) NOT NULL,               -- Fund name in Thai
    name_en VARCHAR(255),                        -- Fund name in English
    amc VARCHAR(100),                            -- Asset Management Company (e.g., KAsset, SCBAM)
    category VARCHAR(100),                       -- Fund category classification
    code VARCHAR(50) UNIQUE NOT NULL,            -- Fund abbreviation code (e.g., 'K-USXNDQ-A')
    risk_level INT,                              -- Risk level rating (1-8)
    return_1y DECIMAL(5, 2) DEFAULT 0.00         -- 1-year historical return percentage
);
-- TIER 2: Intermediary Entities (Global Master Funds)
-- 3. Global Master/Feeder Funds Table
CREATE TABLE master_funds (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name_en VARCHAR(255) NOT NULL UNIQUE,        -- e.g., 'Invesco NASDAQ 100 ETF'
    amc VARCHAR(100),                            -- Global Asset Management Company (if applicable)
    category VARCHAR(100)                        -- Master fund category classification
);
-- TIER 3: Relationship and Mapping Tables
-- 4. Direct Investment: Thai Fund -> Stock (For primary dashboard view)
-- Utilized for funds that directly invest in underlying stocks without intermediaries
CREATE TABLE fund_direct_holdings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fund_id INT NOT NULL,
    stock_id INT NOT NULL,
    ranking INT,                                 -- Portfolio holding rank
    holding_value_thb DECIMAL(20, 2),            -- Total holding value in THB
    nav_thb DECIMAL(20, 2),                      -- Net Asset Value (NAV) of the Thai fund
    percent_nav DECIMAL(5, 2),                   -- Percentage of total NAV
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);
-- 5. Master Fund Investment: Thai Fund -> Master Fund (For secondary dashboard table)
-- Indicates the global master fund allocations of the respective Thai fund
CREATE TABLE fund_master_holdings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fund_id INT NOT NULL,
    master_fund_id INT NOT NULL,
    holding_value_thb DECIMAL(20, 2),            -- Total investment value allocated to Master Fund (THB)
    percent_nav DECIMAL(5, 2),                   -- Percentage of Thai fund's NAV
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE,
    FOREIGN KEY (master_fund_id) REFERENCES master_funds(id) ON DELETE CASCADE
);
-- 6. Deep Look-through: Master Fund -> Stock (For analytical charts)
-- Details the underlying global stock exposure held by the respective master fund
CREATE TABLE master_fund_holdings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    master_fund_id INT NOT NULL,
    stock_id INT NOT NULL,
    percent_weight DECIMAL(5, 2),                -- Master fund's holding weight (e.g., NVDA at 15.9%)
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (master_fund_id) REFERENCES master_funds(id) ON DELETE CASCADE,
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);
-- Additional Analytics Tables
CREATE TABLE stock_aggregates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_id INT NOT NULL,
    total_exposure_value DECIMAL(20, 2),
    portfolio_weight DECIMAL(5, 2),
    exposure_type VARCHAR(100),
    total_funds_holding INT,
    total_thai_fund_value DECIMAL(20, 2),
    global_fund_value DECIMAL(20, 2),
    FOREIGN KEY (stock_id) REFERENCES stocks(id) ON DELETE CASCADE
);
CREATE TABLE fund_sector_breakdown (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fund_id INT NOT NULL,
    sector_name VARCHAR(100) NOT NULL,
    percentage DECIMAL(5, 2) DEFAULT 0.00,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE
);
CREATE TABLE fund_country_breakdown (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fund_id INT NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    percentage DECIMAL(5, 2) DEFAULT 0.00,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE
);