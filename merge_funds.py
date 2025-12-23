import pandas as pd
import numpy as np
import glob
import os
import time
import shutil
from datetime import datetime, timedelta

# CONFIGURATION
FIN_DIR = 'finnomena'
WM_DIR = 'wealthmagik'
MERGED_DIR = 'merged_output'
FINAL_DB_DIR = 'final_db_data'
HOLDING_AGE_THRESHOLD = 60
LOG_BUFFER = []
HAS_ERROR = False

GLOBAL_DELETED_FUNDS = set()
script_dir = os.path.dirname(os.path.abspath(__file__))
if not os.path.exists(MERGED_DIR): os.makedirs(MERGED_DIR)
if not os.path.exists(FINAL_DB_DIR): os.makedirs(FINAL_DB_DIR)

# Clean Final DB Data
for f in glob.glob(os.path.join(FINAL_DB_DIR, "*")):
    try: os.remove(f)
    except Exception: pass

# HELPER FUNCTIONS
def load_csv(filepath):
    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        try: return pd.read_csv(filepath)
        except Exception: return pd.DataFrame()
    return pd.DataFrame()

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

def clean_float(df, cols):
    for col in cols:
        if col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '', regex=False).str.replace('%', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').round(4)
    return df

def clean_date(df, col_name):
    if col_name in df.columns:
        df[col_name] = pd.to_datetime(df[col_name], errors='coerce', dayfirst=True)
    return df

def reorder_source_col(df, source_col_name='data_source'):
    if 'source_url' in df.columns: df = df.drop(columns=['source_url'])
    if source_col_name in df.columns:
        cols = [c for c in df.columns if c != source_col_name] + [source_col_name]
        df = df[cols]
    return df
def save_incremental(df_new, filename_base, unique_keys):
    if df_new.empty and not GLOBAL_DELETED_FUNDS:
        return
    if 'fund_code' in df_new.columns and GLOBAL_DELETED_FUNDS:
        original_count = len(df_new)
        df_new = df_new[~df_new['fund_code'].isin(GLOBAL_DELETED_FUNDS)].copy()
        if len(df_new) < original_count:
            log(f"Filtered out {original_count - len(df_new)} deleted funds from {filename_base}.")

    current_month = datetime.now().month
    monthly_filename = f"{filename_base}_{current_month}.csv"
    monthly_path = os.path.join(MERGED_DIR, monthly_filename)
    
    final_filename = f"final_{filename_base.replace('master_', '')}.csv"
    final_path = os.path.join(FINAL_DB_DIR, final_filename)
    def prepare_lookup_keys(keys_set):
        k_list = list(keys_set)
        if len(unique_keys) == 1: return [k[0] for k in k_list]
        return k_list

    if not os.path.exists(monthly_path):
        log(f"New month/file detected: Creating {monthly_filename}")
        for old_file in glob.glob(os.path.join(MERGED_DIR, f"{filename_base}_*.csv")):
            if old_file != monthly_path:
                try: os.remove(old_file)
                except: pass
        
        df_new['sync_action'] = 'ADD'
        df_new.drop(columns=['sync_action']).to_csv(monthly_path, index=False)
        df_new.to_csv(final_path, index=False)
        
    else:
        log(f"Existing month file found. Calculating Diff")
        df_old = pd.read_csv(monthly_path)
        
        def get_key_set(df):
            if not unique_keys: return set()
            return set(df[unique_keys].itertuples(index=False, name=None))

        keys_new = get_key_set(df_new)
        keys_old = get_key_set(df_old)
        
        added_keys = keys_new - keys_old
        deleted_keys = keys_old - keys_new
        
        diff_rows = []
        
        if added_keys:
            temp_new = df_new.set_index(unique_keys)
            lookup_keys = prepare_lookup_keys(added_keys)
            added_df = temp_new.loc[lookup_keys].reset_index()
            added_df['sync_action'] = 'ADD'
            diff_rows.append(added_df)
            log(f"Found {len(added_keys)} new records")
        
        if deleted_keys:
            temp_old = df_old.set_index(unique_keys)
            lookup_keys = prepare_lookup_keys(deleted_keys)
            deleted_df = temp_old.loc[lookup_keys].reset_index()
            deleted_df['sync_action'] = 'DELETE'
            diff_rows.append(deleted_df)
            log(f"Found {len(deleted_keys)} deleted records")

            if filename_base == 'master_funds_info' and unique_keys == ['fund_code']:
                for k in lookup_keys:
                    GLOBAL_DELETED_FUNDS.add(k)
                    log(f"Marked {k} for Global Cascade Delete")

        if filename_base != 'master_funds_info' and 'fund_code' in unique_keys and GLOBAL_DELETED_FUNDS:
            cascade_deletes = []
            for deleted_fund in GLOBAL_DELETED_FUNDS:
                cascade_deletes.append({'fund_code': deleted_fund, 'sync_action': 'DELETE'})
            
            if cascade_deletes:
                cascade_df = pd.DataFrame(cascade_deletes)
                for col in df_new.columns:
                    if col not in cascade_df.columns:
                        cascade_df[col] = np.nan
                
                diff_rows.append(cascade_df)
                log(f"Added {len(cascade_df)} Cascade Deletes from Master")

        df_new.to_csv(monthly_path, index=False)
        
        if diff_rows:
            final_df = pd.concat(diff_rows, ignore_index=True)
            if 'fund_code' in final_df.columns:
                final_df = final_df.drop_duplicates(subset=unique_keys + ['sync_action'])
            final_df.to_csv(final_path, index=False)
            log(f"Diff saved to {final_filename}")
        else:
            log("No changes detected")


log("Starting Merge Process (Strict Master Clean Mode)")

# 1. MASTER INFO (First Priority)
log("Processing Master Info")
fin_master = load_csv(os.path.join(FIN_DIR, 'finnomena_master_info.csv'))
wm_master = load_csv(os.path.join(WM_DIR, 'wealthmagik_master_info.csv'))
master_merged = pd.merge(fin_master, wm_master, on='fund_code', how='outer', suffixes=('_fin', '_wm'))

def combine_col_master(row, col_base):
    val_wm = row.get(f'{col_base}_wm')
    val_fin = row.get(f'{col_base}_fin')
    if pd.isna(val_wm) or val_wm == '': return val_fin
    return val_wm

cols_to_merge = ['full_name_th', 'category', 'risk_level', 'is_dividend', 'inception_date']
for col in cols_to_merge:
    master_merged[col] = master_merged.apply(lambda x: combine_col_master(x, col), axis=1)

master_merged['amc'] = master_merged['amc'] if 'amc' in master_merged.columns else master_merged['amc_fin']
master_merged['amc'] = master_merged['amc'].fillna('Unknown')
master_merged['currency'] = 'THB'
master_merged['country'] = 'Thailand'

final_master_cols = ['fund_code', 'full_name_th', 'amc', 'category', 'risk_level', 'is_dividend', 
                     'inception_date', 'currency', 'country']
final_master_cols = [c for c in final_master_cols if c in master_merged.columns]
final_master = master_merged[final_master_cols].drop_duplicates(subset=['fund_code'])
final_master = clean_date(final_master, 'inception_date')

save_incremental(final_master, 'master_funds_info', unique_keys=['fund_code'])

# 2. ALL SEC FUND INFO
log("Processing All SEC Fund Info")
sec_file_path = os.path.join(MERGED_DIR, 'all_sec_fund_info.csv')
sec_info = load_csv(sec_file_path)
if not sec_info.empty:
    sec_info = clean_date(sec_info, 'as_of_date')
    sec_info = clean_float(sec_info, ['sharpe_ratio', 'alpha', 'beta', 'max_drawdown', 'recovering_period', 'tracking_error', 'turnover_ratio'])
    save_incremental(sec_info, 'all_sec_fund_info', unique_keys=['fund_code'])
else:
    log(f"Warning: '{sec_file_path}' not found. Skipping")

# 3. FEES
log("Processing Fees")
fin_fees = load_csv(os.path.join(FIN_DIR, 'finnomena_fees.csv'))
wm_fees = load_csv(os.path.join(WM_DIR, 'wealthmagik_fees.csv'))
wm_fees = wm_fees.rename(columns={'initial_purchase': 'min_initial_buy', 'additional_purchase': 'min_next_buy'})
all_fees = pd.concat([fin_fees, wm_fees], ignore_index=True)
if 'source_url' in all_fees.columns: all_fees = all_fees.drop(columns=['source_url'])
fee_cols = ['front_end_max', 'front_end_actual', 'back_end_max', 'back_end_actual', 
            'management_max', 'management_actual', 'ter_max', 'ter_actual', 
            'switching_in_max', 'switching_in_actual', 'switching_out_max', 'switching_out_actual',
            'min_initial_buy', 'min_next_buy']
all_fees = clean_float(all_fees, fee_cols)
final_fees = all_fees.groupby('fund_code', as_index=False).first()

save_incremental(final_fees, 'master_funds_fees', unique_keys=['fund_code'])

# 4. CODES
log("Processing Codes")
fin_codes = load_csv(os.path.join(FIN_DIR, 'finnomena_codes.csv'))
wm_codes = load_csv(os.path.join(WM_DIR, 'wealthmagik_codes.csv'))
all_codes = pd.concat([fin_codes, wm_codes], ignore_index=True)
if not all_codes.empty:
    cols_to_keep = [c for c in ['fund_code', 'type', 'code', 'factsheet_url'] if c in all_codes.columns]
    final_codes = all_codes[cols_to_keep].drop_duplicates(subset=['fund_code', 'code'])
    save_incremental(final_codes, 'master_funds_codes', unique_keys=['fund_code', 'code'])

# 5. HOLDINGS & ALLOCATIONS (UPDATED LOGIC)
log("Processing Holdings & Allocations")
df_list = []
f_h = load_csv(os.path.join(FIN_DIR, 'finnomena_holdings.csv'))
if not f_h.empty:
    f_h['data_source'] = 'finnomena'
    df_list.append(f_h)
w_h = load_csv(os.path.join(WM_DIR, 'wealthmagik_holdings.csv'))
if not w_h.empty:
    w_h['data_source'] = 'wealthmagik'
    df_list.append(w_h)
w_a = load_csv(os.path.join(WM_DIR, 'wealthmagik_allocations.csv'))
if not w_a.empty:
    w_a['data_source'] = 'wealthmagik'
    df_list.append(w_a)

if df_list:
    full_portfolio = pd.concat(df_list, ignore_index=True)
    full_portfolio = clean_date(full_portfolio, 'as_of_date')
    full_portfolio = clean_float(full_portfolio, ['percent'])
    alloc_types = ['asset_alloc', 'country_alloc', 'sector_alloc']
    mask_alloc = full_portfolio['type'].isin(alloc_types)
    df_holdings = full_portfolio[~mask_alloc].copy()
    df_holdings['holding_type'] = df_holdings['data_source'].apply(lambda x: 'top5' if x == 'finnomena' else 'full')
    save_incremental(df_holdings, 'master_funds_holdings', unique_keys=['fund_code', 'name', 'data_source'])
    df_all_allocs = full_portfolio[mask_alloc].copy()
    mask_keep_all = df_all_allocs['type'].isin(['country_alloc', 'sector_alloc'])
    df_alloc_keep = df_all_allocs[mask_keep_all].copy()
    mask_asset = df_all_allocs['type'] == 'asset_alloc'
    df_asset = df_all_allocs[mask_asset].copy()
    
    if not df_asset.empty:
        date_check = df_asset.groupby(['fund_code', 'data_source'])['as_of_date'].max().unstack()
        selected_source_map = {}
        all_funds_asset = df_asset['fund_code'].unique()
        
        for fund in all_funds_asset:
            f_date = date_check.loc[fund, 'finnomena'] if 'finnomena' in date_check.columns and fund in date_check.index else pd.NaT
            w_date = date_check.loc[fund, 'wealthmagik'] if 'wealthmagik' in date_check.columns and fund in date_check.index else pd.NaT
            
            if pd.isna(w_date): selected_source_map[fund] = 'finnomena'
            elif pd.isna(f_date): selected_source_map[fund] = 'wealthmagik'
            else:
                if f_date > w_date + timedelta(days=HOLDING_AGE_THRESHOLD):
                    selected_source_map[fund] = 'finnomena'
                else:
                    selected_source_map[fund] = 'wealthmagik'
        
        df_asset['target_source'] = df_asset['fund_code'].map(selected_source_map)
        df_asset_final = df_asset[df_asset['data_source'] == df_asset['target_source']].copy()
        df_asset_final = df_asset_final.drop(columns=['target_source'])
    else:
        df_asset_final = pd.DataFrame()
    final_allocations = pd.concat([df_alloc_keep, df_asset_final], ignore_index=True)
    final_allocations = reorder_source_col(final_allocations, 'data_source')
    save_incremental(final_allocations, 'master_funds_allocations', unique_keys=['fund_code', 'name', 'type', 'data_source'])
    
else:
    log("No portfolio data found")

# 6. DAILY NAV
log("Processing Daily NAV")
nav_files = glob.glob(os.path.join(FIN_DIR, 'finnomena_daily_nav_*.csv')) + \
            glob.glob(os.path.join(WM_DIR, 'wealthmagik_daily_nav_*.csv'))

nav_dfs = []
for f in nav_files:
    df = load_csv(f)
    if not df.empty:
        if 'finnomena' in f: df['data_source'] = 'finnomena'
        elif 'wealthmagik' in f: df['data_source'] = 'wealthmagik'
        else: df['data_source'] = 'unknown'
        for col in ['bid_price_per_unit', 'offer_price_per_unit', 'scraped_at', 'aum']:
            if col not in df.columns: df[col] = np.nan
        nav_dfs.append(df)

if nav_dfs:
    all_nav = pd.concat(nav_dfs, ignore_index=True)
else:
    all_nav = pd.DataFrame()

# 1. ADD
final_nav_add = pd.DataFrame()
if not all_nav.empty:
    if GLOBAL_DELETED_FUNDS:
        all_nav = all_nav[~all_nav['fund_code'].isin(GLOBAL_DELETED_FUNDS)]
        
    all_nav = clean_date(all_nav, 'nav_date')
    all_nav = clean_float(all_nav, ['nav_value', 'aum', 'bid_price_per_unit', 'offer_price_per_unit'])
    if 'scraped_at' in all_nav.columns: all_nav['scraped_at'] = pd.to_datetime(all_nav['scraped_at'], errors='coerce')

    fin_nav = all_nav[all_nav['data_source'] == 'finnomena'].set_index(['fund_code', 'nav_date'])
    wm_nav = all_nav[all_nav['data_source'] == 'wealthmagik'].set_index(['fund_code', 'nav_date'])
    common_indices = fin_nav.index.intersection(wm_nav.index)
    only_fin_indices = fin_nav.index.difference(wm_nav.index)
    only_wm_indices = wm_nav.index.difference(fin_nav.index)
    
    final_rows = []
    if not only_fin_indices.empty: final_rows.append(fin_nav.loc[only_fin_indices].reset_index())
    if not only_wm_indices.empty: final_rows.append(wm_nav.loc[only_wm_indices].reset_index())
    if not common_indices.empty:
        f_part = fin_nav.loc[common_indices]
        w_part = wm_nav.loc[common_indices]
        merged = f_part.join(w_part, lsuffix='_fin', rsuffix='_wm')
        def smart_mix_row(row):
            nav_f = row['nav_value_fin']
            nav_w = row['nav_value_wm']
            if abs(nav_f - nav_w) > 0.001:
                t_f = row['scraped_at_fin']
                t_w = row['scraped_at_wm']
                base = '_fin' if pd.isna(t_w) or (not pd.isna(t_f) and t_f >= t_w) else '_wm'
                return pd.Series({'nav_value': row[f'nav_value{base}'], 'bid_price_per_unit': row[f'bid_price_per_unit{base}'], 'offer_price_per_unit': row[f'offer_price_per_unit{base}'], 'aum': row[f'aum{base}'], 'data_source': row[f'data_source{base}']})
            else:
                nav = nav_f
                bid = row['bid_price_per_unit_fin'] if pd.notna(row['bid_price_per_unit_fin']) else row['bid_price_per_unit_wm']
                offer = row['offer_price_per_unit_fin'] if pd.notna(row['offer_price_per_unit_fin']) else row['offer_price_per_unit_wm']
                aum = row['aum_fin'] if pd.notna(row['aum_fin']) else row['aum_wm']
                return pd.Series({'nav_value': nav, 'bid_price_per_unit': bid, 'offer_price_per_unit': offer, 'aum': aum, 'data_source': 'fin/wealth'})
        mixed_result = merged.apply(smart_mix_row, axis=1)
        final_rows.append(mixed_result.reset_index())
        
    final_nav_add = pd.concat(final_rows, ignore_index=True)
    if not final_nav_add.empty:
        final_nav_add = final_nav_add.sort_values(by=['fund_code', 'nav_date'])
        final_nav_add = final_nav_add[['fund_code', 'nav_date', 'nav_value', 'bid_price_per_unit', 'offer_price_per_unit', 'aum', 'data_source']]
        final_nav_add['sync_action'] = 'ADD'

# 2. DELETE
final_nav_delete = pd.DataFrame()
if GLOBAL_DELETED_FUNDS:
    del_list = list(GLOBAL_DELETED_FUNDS)
    final_nav_delete = pd.DataFrame({'fund_code': del_list})
    final_nav_delete['sync_action'] = 'DELETE'
    for c in ['nav_date', 'nav_value', 'bid_price_per_unit', 'offer_price_per_unit', 'aum', 'data_source']:
        final_nav_delete[c] = np.nan

# 3. Combine
final_nav_combined = pd.concat([final_nav_add, final_nav_delete], ignore_index=True)

if not final_nav_combined.empty:
    final_nav_path = os.path.join(FINAL_DB_DIR, "final_funds_daily_nav.csv")
    final_nav_combined.to_csv(final_nav_path, index=False)
    log(f"Daily NAV processed: {len(final_nav_combined)} records saved.")
else:
    log("No Daily NAV data.")

log(f"\nAll Done! Diff files are in '{FINAL_DB_DIR}'")