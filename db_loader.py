import pandas as pd
from pathlib import Path
import time
import urllib.parse
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect
from prefect import task

# CONFIGURATION
DB_USER = "root"
DB_PASS = "1234"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "thai_funds"
script_dir = Path(__file__).resolve().parent
MERGED_DIR = script_dir/"merged_output"
NAV_DIR = MERGED_DIR/"merged_nav_all"
INIT_SQL_PATH = script_dir/"init.sql"
PRE_FUNDS_SQL_PATH = script_dir/"pre-funds.sql"
LOOKBACK_DAYS = 7

encoded_password = urllib.parse.quote_plus(DB_PASS)
ROOT_URL = f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}"
DB_URL = f"mysql+pymysql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
LOG_BUFFER = []
HAS_ERROR = False

def log(msg):
    global HAS_ERROR
    if "error" in msg.lower() or "failed" in msg.lower():
        HAS_ERROR = True
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}")
    LOG_BUFFER.append(f"[{timestamp}] {msg}")

def save_log_if_error():
    if not HAS_ERROR: return
    try:
        log_dir = script_dir/"Logs"
        if not log_dir.exists():log_dir.mkdir(parents=True, exist_ok=True)
        filename = f"db_loader_{datetime.now().strftime('%Y-%m-%d')}.log"
        with open(log_dir/filename, "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_BUFFER))
    except: pass

def clean_date_val(val):
    if pd.isna(val) or val is None or str(val).strip() == "" or str(val).strip() == "N/A":
        return "NULL"
    val_str = str(val).strip()
    formats = ["%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"]
    for fmt in formats:
        try:
            dt = datetime.strptime(val_str, fmt)
            return f"'{dt.strftime('%Y-%m-%d')}'"
        except ValueError:
            continue
    return "NULL"

def get_sql_val(val, is_date=False):
    if is_date:
        return clean_date_val(val)
    if pd.isna(val) or val is None or str(val).strip() == "":
        return "NULL"
    val_str = str(val).strip()
    try:
        clean_num = val_str.replace(',', '')
        is_number = False
        if clean_num.isdigit(): 
            is_number = True
        elif clean_num.replace('.', '', 1).isdigit():
            is_number = True
        elif clean_num.startswith('-') and clean_num[1:].replace('.', '', 1).isdigit():
            is_number = True
        if is_number:
             return clean_num
    except: 
        pass
    safe_str = val_str.replace("'", "''").replace("\\", "\\\\")
    if "%" in safe_str:
        safe_str = safe_str.replace("%", " percent") 
    return f"'{safe_str}'"

def check_and_init_db():
    log("Checking Database existence")
    root_engine = create_engine(ROOT_URL)
    try:
        with root_engine.connect() as conn:
            existing_dbs = conn.execute(text("SHOW DATABASES LIKE 'thai_funds'")).fetchall()
            
            if not existing_dbs:
                log("Database 'thai_funds' not found. Initializing from init.sql")
                if not INIT_SQL_PATH.exists():
                    log(f"Critical Error: init.sql not found at {INIT_SQL_PATH}")
                    return False
                with open(INIT_SQL_PATH, 'r', encoding='utf-8') as f:
                    sql_script = f.read()
                for statement in sql_script.split(';'):
                    if statement.strip():
                        try:
                            conn.execute(text(statement))
                        except Exception as e:
                            log(f"Warning executing statement: {e}")
                log("Database & User Initialized Successfully")
            else:
                log("Database 'thai_funds' exists. Checking tables")
                db_engine = create_engine(DB_URL)
                insp = inspect(db_engine)
                if 'funds_master_info' not in insp.get_table_names():
                    with open(INIT_SQL_PATH, 'r', encoding='utf-8') as f:
                        sql_script = f.read()
                    with db_engine.connect() as conn:
                        for statement in sql_script.split(';'):
                             if statement.strip():
                                try:
                                    conn.execute(text(statement))
                                except: pass
                    log("Tables created.")

            log("Checking 'thai_funds_pre' for Views")
            existing_pre_dbs = conn.execute(text("SHOW DATABASES LIKE 'thai_funds_pre'")).fetchall()
            if not existing_pre_dbs:
                log("Database Initializing Views")
                if PRE_FUNDS_SQL_PATH.exists():
                    with open(PRE_FUNDS_SQL_PATH, 'r', encoding='utf-8') as f:
                        sql_script = f.read()
                    for statement in sql_script.split(';'):
                        if statement.strip():
                            try:
                                conn.execute(text(statement))
                            except Exception as e:
                                log(f"Warning executing View SQL: {e}")
                    log("Successfully")
                else:
                    log(f"Warning: {PRE_FUNDS_SQL_PATH} not found")
            else:
                log("Database 'thai_funds_pre' already exists")
    except Exception as e:
        log(f"Initialization Failed: {e}")
        return False
    return True

def sync_master_info(engine):
    filepath = MERGED_DIR/"merged_info.csv"
    if not filepath.exists():
        log(f"Skipping Master Info: {filepath} not found")
        return

    log("Syncing Funds Master Info")
    df = pd.read_csv(filepath)
    if df.empty: return
    date_cols = ['inception_date']
    with engine.connect() as conn:
        try:
            existing = pd.read_sql("SELECT fund_code FROM funds_master_info", conn)
            existing_codes = set(existing['fund_code']) if not existing.empty else set()
            current_codes = set(df['fund_code'].unique())
            for _, row in df.iterrows():
                cols = list(row.index)
                vals = [get_sql_val(row[c], is_date=(c in date_cols)) for c in cols]
                col_str = ", ".join(cols)
                val_str = ", ".join(vals)
                update_assignments = [f"{c} = VALUES({c})" for c in cols if c != 'fund_code']
                update_assignments.append("fund_status = 'active'")
                sql = f"""
                    INSERT INTO funds_master_info ({col_str}, fund_status) 
                    VALUES ({val_str}, 'active')
                    ON DUPLICATE KEY UPDATE 
                    {", ".join(update_assignments)};
                """
                conn.execute(text(sql))
                conn.commit()
            inactive_codes = existing_codes - current_codes
            if inactive_codes:
                codes_str = ", ".join([f"'{c}'" for c in inactive_codes])
                sql_inactive = f"UPDATE funds_master_info SET fund_status = 'inactive' WHERE fund_code IN ({codes_str})"
                conn.execute(text(sql_inactive))
                conn.commit()
                log(f"Set {len(inactive_codes)} funds to INACTIVE")
            log("Master Info Synced")
        except Exception as e:
            log(f"Error syncing Master Info: {e}")

def sync_daily_nav(engine):
    log("Syncing Daily NAVs")
    nav_files = list(NAV_DIR.glob("merged_nav_*.csv"))
    if not nav_files:
        log("No NAV files found")
        return
    count = 0
    for filepath in nav_files:
        fund_code = ""
        try:
            df = pd.read_csv(filepath)
            if df.empty: continue
            if 'fund_code' in df.columns:
                fund_code = str(df.iloc[0]['fund_code']).strip()
            else:
                fund_code = filepath.stem.removeprefix("merged_nav_")
            with engine.connect() as conn:
                res = conn.execute(text(f"SELECT MAX(nav_date) FROM funds_daily WHERE fund_code = '{fund_code}'"))
                max_date = res.scalar()
                if max_date:
                    last_date_obj = pd.to_datetime(max_date)
                    cutoff_date = last_date_obj - timedelta(days=LOOKBACK_DAYS)
                    date_col_name = 'nav_date' if 'nav_date' in df.columns else 'date'
                    df['date_obj'] = pd.to_datetime(df[date_col_name], format="%d-%m-%Y", errors='coerce')
                    df = df[df['date_obj'] > cutoff_date]
                    df = df.drop(columns=['date_obj'])
                if df.empty: continue
                values_list = []
                for _, row in df.iterrows():
                    nav_date = get_sql_val(row.get('nav_date') or row.get('date'), is_date=True)
                    nav_val = get_sql_val(row.get('nav_value') or row.get('value'))
                    aum = get_sql_val(row.get('aum') or row.get('amount'))
                    bid = get_sql_val(row.get('bid_price_per_unit') or row.get('bid') or row.get('bid_price'))
                    offer = get_sql_val(row.get('offer_price_per_unit') or row.get('offer') or row.get('offer_price'))
                    source = get_sql_val(row.get('data_source') or row.get('source_nav') or 'merged')
                    values_list.append(f"('{fund_code}', {nav_date}, {nav_val}, {aum}, {bid}, {offer}, {source})")
                if values_list:
                    sql = f"""
                        INSERT INTO funds_daily (fund_code, nav_date, nav_value, aum, bid, offer, source)
                        VALUES {", ".join(values_list)}
                        ON DUPLICATE KEY UPDATE 
                            nav_value = VALUES(nav_value),
                            aum = VALUES(aum),
                            bid = VALUES(bid),
                            offer = VALUES(offer),
                            source = VALUES(source);
                    """
                    conn.execute(text(sql))
                    conn.commit()
                    count += 1
        except Exception as e:
            log(f"Error processing NAV {fund_code}: {e}")
    log(f"Updated NAVs for {count} funds")

def sync_generic_table(engine, csv_name, table_name, pk_col):
    filepath = MERGED_DIR/csv_name
    if not filepath.exists(): return
    log(f"Syncing {table_name}")
    df = pd.read_csv(filepath)
    if df.empty: return
    date_cols_map = {
        "funds_statistics": ["as_of_date"],
        "funds_fee": [],
        "funds_codes": []
    }
    target_date_cols = date_cols_map.get(table_name, [])
    with engine.connect() as conn:
        try:
            for _, row in df.iterrows():
                cols = list(row.index)
                vals = [get_sql_val(row[c], is_date=(c in target_date_cols)) for c in cols]
                col_str = ", ".join(cols)
                val_str = ", ".join(vals)
                update_clause = ""
                update_sets = [f"{c} = VALUES({c})" for c in cols if c != pk_col and c not in pk_col.split(', ')]
                if update_sets:
                    update_clause = f"ON DUPLICATE KEY UPDATE {', '.join(update_sets)}"
                else:
                    update_clause = "ON DUPLICATE KEY UPDATE scraped_at = VALUES(scraped_at)"

                sql = f"""
                    INSERT INTO {table_name} ({col_str}) VALUES ({val_str})
                    {update_clause};
                """
                conn.execute(text(sql))
            conn.commit()
            log(f"Synced {table_name}")
        except Exception as e:
            log(f"Error syncing {table_name}: {e}")

def sync_portfolio_table(engine, csv_name, table_name):
    filepath = MERGED_DIR/csv_name
    if not filepath.exists(): return
    log(f"Syncing {table_name}")
    df = pd.read_csv(filepath)
    if df.empty: return
    use_holding_type = (table_name == "funds_holding")
    with engine.connect() as conn:
        try:
            active_funds_in_file = df['fund_code'].unique()
            for fund in active_funds_in_file:
                conn.execute(text(f"DELETE FROM {table_name} WHERE fund_code = :fund_code"), {"fund_code": fund})
                fund_rows = df[df['fund_code'] == fund]
                if fund_rows.empty: continue
                if use_holding_type:
                    cols = "fund_code, symbol, name, type, sector, percent, as_of_date, source_url, holding_type"
                else:
                    cols = "fund_code, type, name, percent, as_of_date, source_url"
                insert_sql = text(f"INSERT INTO {table_name} ({cols}) VALUES ({', '.join([':'+c.strip() for c in cols.split(',')])})")
                params = []
                for _, row in fund_rows.iterrows():
                    def norm(v, is_date=False):
                        if pd.isna(v): return None
                        if is_date and str(v).strip():
                            try: return pd.to_datetime(v, dayfirst=True).strftime("%Y-%m-%d")
                            except: return None
                        return v
                    raw_name = str(row.get("name")) if not pd.isna(row.get("name")) else ""
                    clean_name = raw_name.replace("%", " percent")
                    row_data = {
                        "fund_code": str(row.get("fund_code")).strip(),
                        "type": row.get("type"),
                        "name": clean_name,
                        "percent": None if pd.isna(row.get("percent")) else float(str(row.get("percent")).replace(',', '')),
                        "as_of_date": norm(row.get("as_of_date"), is_date=True),
                        "source_url": row.get("source_url"),
                    }
                    if use_holding_type:
                        row_data["symbol"] = norm(row.get("symbol"))
                        row_data["sector"] = norm(row.get("sector"))
                        row_data["holding_type"] = norm(row.get("holding_type"))
                    params.append(row_data)
                if params:
                    conn.execute(insert_sql, params)
            conn.commit()
            log(f"Synced {table_name}")
        except Exception as e:
            log(f"Error syncing {table_name}: {e}")

# MAIN
@task(name="database_loader", log_prints=True)
def db_loader():
    log("Starting DB Loader Process")
    if not check_and_init_db():
        log("Aborting process due to DB initialization failure")
        return
    try:
        engine = create_engine(DB_URL)
        log("Connected to Database 'thai_funds'")
    except Exception as e:
        log(f"Database Connection Failed: {e}")
        return
    sync_master_info(engine)
    sync_generic_table(engine, "all_sec_fund_info.csv", "funds_statistics", "fund_code")
    sync_generic_table(engine, "merged_fee.csv", "funds_fee", "fund_code")
    sync_generic_table(engine, "merged_codes.csv", "funds_codes", "fund_code")
    sync_portfolio_table(engine, "merged_holding.csv", "funds_holding")
    sync_portfolio_table(engine, "merged_allocations.csv", "funds_allocations")
    sync_generic_table(engine, "merged_performance.csv", "funds_performance", "fund_code")
    sync_daily_nav(engine)
    save_log_if_error()
    log("DB Load Completed")

if __name__ == "__main__":
    db_loader.fn()