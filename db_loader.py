import pandas as pd
import os
import glob
from datetime import datetime
import time
from sqlalchemy import create_engine, text
import urllib.parse
import numpy as np

# CONFIG
DB_USER = "admin"
DB_PASS = "password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "funds_db"
DATA_DIR = "final_db_data"
encoded_password = urllib.parse.quote_plus(DB_PASS)
DB_URL = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
LOG_BUFFER = []
HAS_ERROR = False
script_dir = os.path.dirname(os.path.abspath(__file__))

# MAPPING
FILE_TABLE_MAP = [
    ("final_funds_info.csv", "funds_master", ["fund_code"]),
    ("final_all_sec_fund_info.csv", "funds_statistics", ["fund_code"]),
    ("final_funds_fees.csv", "funds_fees", ["fund_code"]),
    ("final_funds_codes.csv", "funds_codes", ["fund_code", "code"]),
    ("final_funds_daily_nav.csv", "daily_nav", ["fund_code", "nav_date"]),
    ("final_funds_holdings.csv", "portfolio_holdings", ["fund_code", "name", "data_source"]), 
    ("final_funds_allocations.csv", "portfolio_allocations", ["fund_code", "name", "type", "data_source"])
]

def get_sql_type(val):
    if pd.isna(val) or val is None: return "NULL"
    if isinstance(val, (int, float, np.integer, np.floating)):
        if isinstance(val, float) or isinstance(val, np.floating):
            if val.is_integer(): return str(int(val))
        return str(val)
    val_str = str(val).strip()
    if val_str.endswith(".0"):
        try: return str(int(float(val_str)))
        except: pass
    safe_str = val_str.replace("'", "''").replace("%", "%%").replace(":", "\\:")
    return f"'{safe_str}'"

def log(msg):
    global HAS_ERROR
    if "error" in msg.lower() or "failed" in msg.lower():
        HAS_ERROR = True
    timestamp = time.strftime('%H:%M:%S')
    formatted_msg = f"[{timestamp}] {msg}"
    print(formatted_msg)
    LOG_BUFFER.append(formatted_msg)

def save_log_if_error():
    if not HAS_ERROR:
        return
    try:
        log_dir = os.path.join(script_dir, "Logs")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        script_name = os.path.basename(__file__).replace(".py", "")
        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"{script_name}_{date_str}.log"
        file_path = os.path.join(log_dir, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_BUFFER))
        print(f"Error detected. Log saved at: {file_path}")
    except Exception as e:
        print(f"Cannot save log file: {e}")

def get_existing_fund_codes(engine):
    try:
        query = "SELECT fund_code FROM funds_master"
        df = pd.read_sql(query, engine)
        return set(df['fund_code'].tolist())
    except Exception as e:
        log(f"Warning: Could not fetch existing funds: {e}")
        return set()

def clean_dataframe(df, table_name, pk_cols):
    if 'risk_level' in df.columns:
        df['risk_level'] = pd.to_numeric(df['risk_level'], errors='coerce')
    
    original_len = len(df)
    df = df.dropna(subset=pk_cols)
    if len(df) < original_len:
        log(f"Dropped {original_len - len(df)} rows with missing Primary Keys")
    return df

def cleanup_raw_daily_navs():
    raw_dirs = ["finnomena", "wealthmagik"]
    log("\nCleaning up raw daily NAV files in source folders")
    
    deleted_count = 0
    for folder in raw_dirs:
        if not os.path.exists(folder):
            continue
        pattern = os.path.join(folder, "*_daily_nav_*.csv")
        files = glob.glob(pattern)
        
        for f in files:
            try:
                os.remove(f)
                deleted_count += 1
            except Exception as e:
                log(f"Could not delete {f}: {e}")

    if deleted_count > 0:
        log(f"Removed {deleted_count} raw files from {', '.join(raw_dirs)}")
    else:
        log(f"No raw files found to clean up")

def process_file(engine, filename, table_name, pk_cols, valid_funds):
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath): return False

    df = pd.read_csv(filepath)
    if df.empty:
        try: os.remove(filepath)
        except: pass
        return True

    log(f"Processing {filename} -> Table: {table_name}")
    df = clean_dataframe(df, table_name, pk_cols)

    if table_name != "funds_master" and "fund_code" in df.columns:
        original_count = len(df)
        df = df[df['fund_code'].isin(valid_funds)].copy()
        if len(df) < original_count:
            log(f"Filtered out {original_count - len(df)} orphan rows")

    if df.empty:
        try: os.remove(filepath)
        except: pass
        return True

    df_add = df[df['sync_action'] == 'ADD'].copy()
    df_delete = df[df['sync_action'] == 'DELETE'].copy()

    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # 1.DELETE
            if not df_delete.empty:
                log(f"-> Processing DELETE actions ({len(df_delete)} rows)")
                if table_name == "funds_master":
                    for _, row in df_delete.iterrows():
                        fund_code = get_sql_type(row['fund_code'])
                        conn.execute(text(f"UPDATE funds_master SET fund_status = 'inactive' WHERE fund_code = {fund_code};"))
                else:
                    for _, row in df_delete.iterrows():
                        conditions = [f"{col} = {get_sql_type(row[col])}" for col in pk_cols]
                        conn.execute(text(f"DELETE FROM {table_name} WHERE {' AND '.join(conditions)};"))

            # 2.UPSERT
            if not df_add.empty:
                log(f"-> Upserting {len(df_add)} rows")
                df_to_insert = df_add.drop(columns=['sync_action'])
                if table_name == "funds_master":
                    df_to_insert['fund_status'] = 'active'

                columns = list(df_to_insert.columns)
                col_str = ", ".join(columns)
                pk_str = ", ".join(pk_cols)
                update_sets = [f"{col} = EXCLUDED.{col}" for col in columns if col not in pk_cols]
                update_clause = f"DO UPDATE SET {', '.join(update_sets)}" if update_sets else "DO NOTHING"
                        
                for _, row in df_to_insert.iterrows():
                    values = [get_sql_type(row[c]) for c in columns]
                    val_str = ", ".join(values)
                    sql = f"INSERT INTO {table_name} ({col_str}) VALUES ({val_str}) ON CONFLICT ({pk_str}) {update_clause};"
                    conn.execute(text(sql))
            
            trans.commit()
            log("Success")
            try: os.remove(filepath)
            except: pass
            log(f"Deleted processed file: {filename}")
            return True

        except Exception as e:
            trans.rollback()
            log(f"Error processing {filename}: {e}")
            log("File kept for debugging")
            return False

def main():
    log("Starting database Loader")
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn: pass
    except Exception as e:
        log(f"Connection Failed: {e}")
        return
    process_file(engine, "final_funds_info.csv", "funds_master", ["fund_code"], set())
    valid_funds = get_existing_fund_codes(engine)
    log(f"Found {len(valid_funds)} valid funds in Master DB")

    remaining_files = FILE_TABLE_MAP[1:]
    for filename, table_name, pk_cols in remaining_files:
        is_success = process_file(engine, filename, table_name, pk_cols, valid_funds)
        if is_success and table_name == "daily_nav":
            cleanup_raw_daily_navs()

    log("\n All Done final_db_data is clean")

if __name__ == "__main__":
    main()