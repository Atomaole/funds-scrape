import pandas as pd
from pathlib import Path
import glob
import time
import re
from datetime import datetime
from prefect import task

# CONFIG
script_dir = Path(__file__).resolve().parent

# Input Folders
FN_RAW_DIR = script_dir/"finnomena/raw_data"
FN_NAV_DIR = script_dir/"finnomena/all_nav"
WM_RAW_DIR = script_dir/"wealthmagik/raw_data"

# Output Folders
MERGED_OUTPUT_DIR = script_dir/"merged_output"
MERGED_NAV_DIR = MERGED_OUTPUT_DIR/"merged_nav_all"

for d in [MERGED_OUTPUT_DIR, MERGED_NAV_DIR]:
    if not d.exists(): d.mkdir(parents=True, exist_ok=True)

def log(msg):
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}")

def safe_read_csv(path):
    if path.exists() and path.stat().st_size > 0:
        try: return pd.read_csv(path)
        except: return pd.DataFrame()
    return pd.DataFrame()

def sanitize_filename(name):
    if not name: return "unknown"
    return re.sub(r'[<>:"/\\|?*]', '_', str(name)).strip()

def get_valid_fund_codes():
    path = FN_RAW_DIR/"finnomena_fund_list.csv"
    df = safe_read_csv(path)
    if not df.empty and 'fund_code' in df.columns:
        return set(df['fund_code'].astype(str).str.strip().tolist())
    return set()

def merge_fee():
    log("Merging Fee")
    df = safe_read_csv(FN_RAW_DIR/"finnomena_fees.csv")
    if not df.empty:
        output_path = MERGED_OUTPUT_DIR/"merged_fee.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        log(f"Saved merged_fee.csv ({len(df)} records)")

def merge_allocations(valid_codes):
    log("Merging Allocations")
    fn_df = safe_read_csv(FN_RAW_DIR/"finnomena_allocations.csv")
    if not fn_df.empty: fn_df['source'] = 'finnomena'
    wm_df = safe_read_csv(WM_RAW_DIR/"wealthmagik_allocations.csv")
    if not wm_df.empty: wm_df['source'] = 'wealthmagik'
    merged_df = pd.concat([fn_df, wm_df], ignore_index=True)
    if not merged_df.empty:
        before_count = len(merged_df)
        merged_df = merged_df[merged_df['fund_code'].astype(str).str.strip().isin(valid_codes)]
        after_count = len(merged_df)
        if before_count > after_count:
            log(f"Filtered out {before_count - after_count} rows (funds not in finnomena list)")
        output_path = MERGED_OUTPUT_DIR/"merged_allocations.csv"
        merged_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        log(f"Saved merged_allocations.csv ({len(merged_df)} records)")

def merge_codes():
    log("Merging Codes/ISIN")
    df = safe_read_csv(FN_RAW_DIR/"finnomena_codes.csv")
    if not df.empty:
        output_path = MERGED_OUTPUT_DIR/"merged_codes.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

def merge_nav():
    log("Merging NAVs")
    wm_bid_offer_file = WM_RAW_DIR/"wealthmagik_bid_offer.csv"
    wm_data_map = {}
    wm_df = safe_read_csv(wm_bid_offer_file)
    if not wm_df.empty:
        log(f"Loaded WealthMagik Bid/Offer data ({len(wm_df)} rows)")
        for _, row in wm_df.iterrows():
            code = str(row.get('fund_code', '')).strip()
            date_str = str(row.get('nav_date', '')).strip()
            bid = row.get('bid_price', '')
            offer = row.get('offer_price', '')
            if code and date_str:
                wm_data_map[(code, date_str)] = {'bid': bid, 'offer': offer}
    fin_files = list(FN_NAV_DIR.glob("*.csv"))
    total_files = len(fin_files)
    log(f"Found {total_files} historical NAV files from Finnomena")
    count = 0
    for f_path in fin_files:
        count += 1
        nav_df = safe_read_csv(f_path)
        if nav_df.empty: continue
        if 'fund_code' in nav_df.columns and not nav_df.empty:
            fund_code = str(nav_df.iloc[0]['fund_code']).strip()
        else:
            fund_code = f_path.stem
        nav_df['date_str'] = nav_df['date'].astype(str).str.strip()
        def get_bid(date_val):
            return wm_data_map.get((fund_code, date_val), {}).get('bid', '')
        def get_offer(date_val):
            return wm_data_map.get((fund_code, date_val), {}).get('offer', '')
        nav_df['bid'] = nav_df['date_str'].map(get_bid)
        nav_df['offer'] = nav_df['date_str'].map(get_offer)
        nav_df['source_nav'] = "finnomena"
        if 'fund_code' not in nav_df.columns:
            nav_df['fund_code'] = fund_code
        if 'date_str' in nav_df.columns: del nav_df['date_str']
        safe_name = sanitize_filename(fund_code)
        output_path = MERGED_NAV_DIR/f"merged_nav_{safe_name}.csv"
        nav_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        if count % 300 == 0:
            print(f"Processed NAVs: {count}/{total_files}")
    log("NAV Merge Completed")

@task(name="merged_funds_file", log_prints=True)
def merged_file():
    log("Starting Merge Process")
    valid_codes = get_valid_fund_codes()
    log(f"Master Fund List loaded: {len(valid_codes)} funds")
    merge_fee()
    merge_codes()
    merge_allocations(valid_codes)
    merge_nav()
    log("All Merge Tasks Done")

if __name__ == "__main__":
    merged_file.fn()