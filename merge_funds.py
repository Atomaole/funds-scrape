import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime, timedelta

FIN_DIR = 'finnomena'
WM_DIR = 'wealthmagik'
OUTPUT_DIR = 'merged_output'
HOLDING_AGE_THRESHOLD = 60

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# HELPER FUNCTIONS
def load_csv(filepath):
    if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
        try:
            return pd.read_csv(filepath)
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

def clean_float(df, cols):
    for col in cols:
        if col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
    return df

def clean_date(df, col_name):
    if col_name in df.columns:
        df[col_name] = pd.to_datetime(df[col_name], errors='coerce', dayfirst=True)
    return df

def reorder_source_col(df, source_col_name='data_source'):
    if 'source_url' in df.columns:
        df = df.drop(columns=['source_url'])
    if source_col_name in df.columns:
        cols = [c for c in df.columns if c != source_col_name] + [source_col_name]
        df = df[cols]
    return df

print("Starting Merge Process")
# 1. MASTER INFO & FEES & CODES
print("Processing Master Info, Fees, Codes...")

# --- Master ---
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
final_master.to_csv(os.path.join(OUTPUT_DIR, 'master_funds_info.csv'), index=False)

# --- Fees ---
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
final_fees.to_csv(os.path.join(OUTPUT_DIR, 'master_funds_fees.csv'), index=False)

# --- Codes ---
fin_codes = load_csv(os.path.join(FIN_DIR, 'finnomena_codes.csv'))
wm_codes = load_csv(os.path.join(WM_DIR, 'wealthmagik_codes.csv'))
all_codes = pd.concat([fin_codes, wm_codes], ignore_index=True)
if not all_codes.empty:
    cols_to_keep = [c for c in ['fund_code', 'type', 'code', 'factsheet_url'] if c in all_codes.columns]
    final_codes = all_codes[cols_to_keep].drop_duplicates(subset=['fund_code', 'code'])
    final_codes.to_csv(os.path.join(OUTPUT_DIR, 'master_funds_codes.csv'), index=False)

# 2. HOLDINGS & ALLOCATIONS (Smart Split)
print("Processing Holdings & Allocations")
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
    
    date_check = full_portfolio.groupby(['fund_code', 'data_source'])['as_of_date'].max().unstack()
    
    selected_source_map = {}
    all_funds_port = full_portfolio['fund_code'].unique()
    
    for fund in all_funds_port:
        f_date = date_check.loc[fund, 'finnomena'] if 'finnomena' in date_check.columns and fund in date_check.index else pd.NaT
        w_date = date_check.loc[fund, 'wealthmagik'] if 'wealthmagik' in date_check.columns and fund in date_check.index else pd.NaT
        if pd.isna(w_date):
            selected_source_map[fund] = 'finnomena'
        elif pd.isna(f_date):
            selected_source_map[fund] = 'wealthmagik'
        else:
            # Check 60 days
            if f_date > w_date + timedelta(days=HOLDING_AGE_THRESHOLD):
                selected_source_map[fund] = 'finnomena'
            else:
                selected_source_map[fund] = 'wealthmagik'

    # Filter rows based on selected source
    full_portfolio['target_source'] = full_portfolio['fund_code'].map(selected_source_map)
    final_portfolio = full_portfolio[full_portfolio['data_source'] == full_portfolio['target_source']].copy()
    alloc_types = ['asset_alloc', 'country_alloc', 'sector_alloc']
    mask_alloc = final_portfolio['type'].isin(alloc_types)
    df_allocs = final_portfolio[mask_alloc].copy()
    df_allocs = reorder_source_col(df_allocs, 'data_source') # Clean columns
    if 'target_source' in df_allocs.columns: df_allocs = df_allocs.drop(columns=['target_source'])
    df_allocs.to_csv(os.path.join(OUTPUT_DIR, 'master_funds_allocations.csv'), index=False)
    df_holdings = final_portfolio[~mask_alloc].copy()
    df_holdings = reorder_source_col(df_holdings, 'data_source') # Clean columns
    if 'target_source' in df_holdings.columns: df_holdings = df_holdings.drop(columns=['target_source'])
    df_holdings.to_csv(os.path.join(OUTPUT_DIR, 'master_funds_holdings.csv'), index=False)
    
    print(f"Allocations saved: {len(df_allocs)} records")
    print(f"Holdings saved: {len(df_holdings)} records")
else:
    print("No portfolio data found")

# 3. DAILY NAV
print("Processing Daily NAV")

# Load Files
nav_files = glob.glob(os.path.join(FIN_DIR, 'finnomena_daily_nav_*.csv')) + \
            glob.glob(os.path.join(WM_DIR, 'wealthmagik_daily_nav_*.csv'))

nav_dfs = []
for f in nav_files:
    df = load_csv(f)
    if not df.empty:
        if 'finnomena' in f:
            df['data_source'] = 'finnomena'
        elif 'wealthmagik' in f:
            df['data_source'] = 'wealthmagik'
        else:
            df['data_source'] = 'unknown'
        for col in ['bid_price_per_unit', 'offer_price_per_unit', 'scraped_at', 'aum']:
            if col not in df.columns: df[col] = np.nan
        nav_dfs.append(df)

if nav_dfs:
    all_nav = pd.concat(nav_dfs, ignore_index=True)
    all_nav = clean_date(all_nav, 'nav_date')
    all_nav = clean_float(all_nav, ['nav_value', 'aum', 'bid_price_per_unit', 'offer_price_per_unit'])
    
    if 'scraped_at' in all_nav.columns:
        all_nav['scraped_at'] = pd.to_datetime(all_nav['scraped_at'], errors='coerce')
    fin_nav = all_nav[all_nav['data_source'] == 'finnomena'].set_index(['fund_code', 'nav_date'])
    wm_nav = all_nav[all_nav['data_source'] == 'wealthmagik'].set_index(['fund_code', 'nav_date'])
    common_indices = fin_nav.index.intersection(wm_nav.index)
    only_fin_indices = fin_nav.index.difference(wm_nav.index)
    only_wm_indices = wm_nav.index.difference(fin_nav.index)
    
    final_rows = []
    if not only_fin_indices.empty:
        final_rows.append(fin_nav.loc[only_fin_indices].reset_index())
    if not only_wm_indices.empty:
        final_rows.append(wm_nav.loc[only_wm_indices].reset_index())
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
                if pd.isna(t_w) or (not pd.isna(t_f) and t_f >= t_w):
                    base = '_fin'
                else:
                    base = '_wm'
                return pd.Series({
                    'nav_value': row[f'nav_value{base}'],
                    'bid_price_per_unit': row[f'bid_price_per_unit{base}'],
                    'offer_price_per_unit': row[f'offer_price_per_unit{base}'],
                    'aum': row[f'aum{base}'],
                    'data_source': row[f'data_source{base}']
                })
                
            else:
                nav = nav_f
                bid = row['bid_price_per_unit_fin'] if pd.notna(row['bid_price_per_unit_fin']) else row['bid_price_per_unit_wm']
                offer = row['offer_price_per_unit_fin'] if pd.notna(row['offer_price_per_unit_fin']) else row['offer_price_per_unit_wm']
                aum = row['aum_fin'] if pd.notna(row['aum_fin']) else row['aum_wm']
                
                return pd.Series({
                    'nav_value': nav,
                    'bid_price_per_unit': bid,
                    'offer_price_per_unit': offer,
                    'aum': aum,
                    'data_source': 'mixed'
                })

        mixed_result = merged.apply(smart_mix_row, axis=1)
        final_rows.append(mixed_result.reset_index())
    final_nav_df = pd.concat(final_rows, ignore_index=True)
    final_nav_df = final_nav_df.sort_values(by=['fund_code', 'nav_date'])
    cols = ['fund_code', 'nav_date', 'nav_value', 'bid_price_per_unit', 'offer_price_per_unit', 'aum', 'data_source']
    final_nav_df = final_nav_df[cols]
    
    final_nav_df.to_csv(os.path.join(OUTPUT_DIR, 'master_funds_daily_nav.csv'), index=False)
    print(f"Daily NAV saved: {len(final_nav_df)} records")

else:
    print("No NAV files found")

print(f"\n Done Files in '{OUTPUT_DIR}'")