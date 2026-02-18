CREATE USER IF NOT EXISTS 'fund_master'@'%' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'fund_master'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

CREATE DATABASE IF NOT EXISTS funds_API;
USE funds_API;

CREATE TABLE stocks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL UNIQUE,        -- Stock ticker symbol (e.g., 'AAPL', 'PTT')
    full_name VARCHAR(255),                    -- Full legal name of the entity
    sector VARCHAR(100),                       -- Industry or business sector classification
    stock_type ENUM('TH', 'FOREIGN', 'GOLD') DEFAULT 'FOREIGN', -- Asset classification for visualization markers
    percent_change DECIMAL(5, 2) DEFAULT 0.00,  -- Percentage price change
    country VARCHAR(100) DEFAULT 'USA'
);
CREATE TABLE funds (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name_th VARCHAR(255) NOT NULL,             -- Fund name in Thai
    name_en VARCHAR(255),                      -- Fund name in English
    amc VARCHAR(100),                          -- Asset Management Company
    category VARCHAR(100),                     -- Fund investment category
    code VARCHAR(50),                          -- Unique fund identifier/code
    risk_level INT,                            -- Risk level of the fund
    return_1y DECIMAL(5, 2) DEFAULT 0.00       -- 1-Year historical return percentage (For Avg Return)
);
CREATE TABLE stock_aggregates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_id INT NOT NULL,                     -- Reference to stocks.id
    total_exposure_value DECIMAL(20, 2),       -- Total aggregate market exposure value
    portfolio_weight DECIMAL(5, 2),            -- Aggregate percentage weight in portfolio
    exposure_type VARCHAR(100),                -- Investment exposure methodology (e.g., Global Index)
    total_funds_holding INT,                   -- Count of unique funds holding this asset
    total_thai_fund_value DECIMAL(20, 2),      -- Aggregate value in Thai funds (USD)
    global_fund_value DECIMAL(20, 2),          -- Aggregate value in global funds (USD)
    FOREIGN KEY (stock_id) REFERENCES stocks(id)
);
CREATE TABLE fund_holdings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    fund_id INT NOT NULL,                      -- Reference to funds.id
    stock_id INT NOT NULL,                     -- Reference to stocks.id
    ranking INT,                               -- Holding rank by weight/value
    investment_method ENUM('Direct', 'Feeder Fund', 'Other'), -- Methodology of asset acquisition
    holding_value_thb DECIMAL(20, 2),          -- Market value of holding in THB
    nav_thb DECIMAL(20, 2),                    -- Total Net Asset Value in THB
    percent_nav DECIMAL(5, 2),                 -- Weight as a percentage of total NAV
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Last synchronization/update timestamp
    FOREIGN KEY (fund_id) REFERENCES funds(id),
    FOREIGN KEY (stock_id) REFERENCES stocks(id)
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
CREATE INDEX idx_stock_symbol ON stocks(symbol);
CREATE INDEX idx_fund_code ON funds(code);