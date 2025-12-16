import pandas as pd
import numpy as np
import os

# CONFIG
FIN_DIR = 'finnomena'
WM_DIR = 'wealthmagik'
OUTPUT_DIR = 'merged_output'

files = {
    'fin_info': os.path.join(FIN_DIR, 'finnomena_info.csv'),
    'fin_fees': os.path.join(FIN_DIR, 'finnomena_fees.csv'),
    'fin_holdings': os.path.join(FIN_DIR, 'finnomena_holdings.csv'),
    'fin_codes': os.path.join(FIN_DIR, 'finnomena_codes.csv'),
    'wm_info': os.path.join(WM_DIR, 'wealthmagik_info.csv'),
    'wm_fees': os.path.join(WM_DIR, 'wealthmagik_fees.csv'),
    'wm_holdings': os.path.join(WM_DIR, 'wealthmagik_holdings.csv'),
    'wm_codes': os.path.join(WM_DIR, 'wealthmagik_codes.csv')
}

# HELPER FUNCTIONS
def load_csv(path):
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame()

def clean_date(df, col):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
    return df

if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
dfs = {k: load_csv(v) for k, v in files.items()}
print("(Clean Version)")

# INFO
print("Info")
fin_info = dfs['fin_info'].copy()
wm_info = dfs['wm_info'].copy()
wm_info = wm_info.rename(columns={'bid_price_per_unit': 'bid', 'offer_price_per_unit': 'offer'})
fin_info['temp_source'] = 'finnomena'
wm_info['temp_source'] = 'wealthmagik'
fin_info = clean_date(fin_info, 'nav_date')
wm_info = clean_date(wm_info, 'nav_date')
fin_info = clean_date(fin_info, 'inception_date')
wm_info = clean_date(wm_info, 'inception_date')

fin_info.set_index('fund_code', inplace=True)
wm_info.set_index('fund_code', inplace=True)

all_funds = list(set(fin_info.index) | set(wm_info.index))
merged_rows = []
best_source_map = {}

for fund in all_funds:
    fin_row = fin_info.loc[fund] if fund in fin_info.index else None
    wm_row = wm_info.loc[fund] if fund in wm_info.index else None
    if fin_row is None:
        merged_rows.append(wm_row)
        best_source_map[fund] = 'wealthmagik'
        continue
    if wm_row is None:
        merged_rows.append(fin_row)
        best_source_map[fund] = 'finnomena'
        continue
    fin_date = fin_row['nav_date']
    wm_date = wm_row['nav_date']
    if pd.isna(fin_date): fin_date = pd.Timestamp.min
    if pd.isna(wm_date): wm_date = pd.Timestamp.min
    if wm_date > fin_date:
        base = wm_row
        patch = fin_row
        best_source_map[fund] = 'wealthmagik'
    else:
        base = fin_row
        patch = wm_row
        best_source_map[fund] = 'finnomena'
    final_row = base.fillna(patch)
    merged_rows.append(final_row)

final_info = pd.DataFrame(merged_rows).reset_index().rename(columns={'index': 'fund_code'})
cols_to_drop = [c for c in final_info.columns if 'source' in c]
final_info = final_info.drop(columns=cols_to_drop)

print(f"Info{len(final_info)} done")

# FEES
if not dfs['fin_fees'].empty: dfs['fin_fees']['source'] = 'finnomena'
if not dfs['wm_fees'].empty: dfs['wm_fees']['source'] = 'wealthmagik'
dfs['fin_fees'] = dfs['fin_fees'].rename(columns={'min_initial_buy': 'min_initial', 'min_next_buy': 'min_next'})
dfs['wm_fees'] = dfs['wm_fees'].rename(columns={'initial_purchase': 'min_initial', 'additional_purchase': 'min_next'})

df_fees = pd.concat([dfs['fin_fees'], dfs['wm_fees']], ignore_index=True)
df_fees['is_best'] = df_fees.apply(lambda x: x['source'] == best_source_map.get(x['fund_code']), axis=1)
final_fees = df_fees[df_fees['is_best']].drop_duplicates(subset=['fund_code']).drop(columns=['is_best', 'source']) 

print(f"Fees {len(final_fees)} done")

# CODES
code_cols = ['fund_code', 'type', 'code']
final_codes = pd.concat([
    dfs['fin_codes'][code_cols] if not dfs['fin_codes'].empty else pd.DataFrame(), 
    dfs['wm_codes'][code_cols] if not dfs['wm_codes'].empty else pd.DataFrame()
]).drop_duplicates()
print(f"Codes{len(final_codes)} done")

# HOLDINGS
if not dfs['fin_holdings'].empty: dfs['fin_holdings']['source'] = 'finnomena'
if not dfs['wm_holdings'].empty: dfs['wm_holdings']['source'] = 'wealthmagik'

df_holdings = pd.concat([dfs['fin_holdings'], dfs['wm_holdings']], ignore_index=True)
df_holdings = clean_date(df_holdings, 'as_of_date')

latest_dates = df_holdings.groupby(['fund_code', 'source'])['as_of_date'].max().unstack()

def select_holding_source(row):
    fin_date = row.get('finnomena')
    wm_date = row.get('wealthmagik')
    if pd.isna(fin_date): return 'wealthmagik'
    if pd.isna(wm_date): return 'finnomena'
    return 'finnomena' if (fin_date - wm_date).days > 60 else 'wealthmagik'

latest_dates['selected_source'] = latest_dates.apply(select_holding_source, axis=1)
source_map = latest_dates['selected_source'].to_dict()

df_holdings['target_source'] = df_holdings['fund_code'].map(source_map)
final_holdings = df_holdings[df_holdings['source'] == df_holdings['target_source']].drop(columns=['target_source', 'source'])

print(f"Holdings{len(final_holdings)} done")

# SAVE
final_info.to_csv(os.path.join(OUTPUT_DIR, 'master_funds_info.csv'), index=False)
final_fees.to_csv(os.path.join(OUTPUT_DIR, 'master_funds_fees.csv'), index=False)
final_codes.to_csv(os.path.join(OUTPUT_DIR, 'master_funds_codes.csv'), index=False)
final_holdings.to_csv(os.path.join(OUTPUT_DIR, 'master_funds_holdings.csv'), index=False)

print(f"\n finish '{OUTPUT_DIR}'")