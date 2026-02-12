import pandas as pd
from thefuzz import process, fuzz
from pathlib import Path
from prefect import task
from bs4 import BeautifulSoup
import requests

script_dir = Path(__file__).resolve().parent
isin_file_path = script_dir / 'latest_isin_ref.xlsx'
input_csv_path = script_dir / 'finnomena/raw_data/finnomena_info.csv'
output_csv_path = script_dir / 'merged_output/merged_info.csv'
SET_ISIN_URL = "https://www.set.or.th/data/tsd/isin/isin.json"
BASE_SET_URL = "https://www.set.or.th"

def download_latest_isin():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
        'Referer': 'https://www.set.or.th/'
    }
    try:
        response = requests.get(SET_ISIN_URL, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        relative_url = data.get('enUrl')
        if not relative_url:
            return False
        if relative_url.startswith('http'):
             file_url = relative_url
        else:
             if not relative_url.startswith('/'):
                 relative_url = '/' + relative_url
             file_url = f"{BASE_SET_URL}{relative_url}"
        file_resp = requests.get(file_url, headers=headers, timeout=30)
        file_resp.raise_for_status()
        with open(isin_file_path, 'wb') as f:
            f.write(file_resp.content)
        return True
        
    except Exception as e:
        return False

def find_header_row(file_path):
    try:
        df_temp = pd.read_excel(file_path, header=None, nrows=20)
        for i, row in df_temp.iterrows():
            row_str = row.astype(str).str.lower().tolist()
            if any('securities symbol' in s for s in row_str) and any('isin code' in s for s in row_str):
                return i
        return 9
    except Exception as e:
        return 9

@task(name="set_isin_mapping", log_prints=True)
def set_isin_process():
    print("starting map isin")

    if not download_latest_isin():
        print("can't get new file isin")
        if not isin_file_path.exists():
            print("not have isin file")
            return
    if not isin_file_path.exists():
        print(f"can't find ISIN {isin_file_path}")
        exit()
    header_idx = find_header_row(isin_file_path)
    if not input_csv_path.exists():
        print(f"can't find info funds raw {input_csv_path}")
        exit()
    print(f"open file ISIN {isin_file_path.name}")
    try:
        df_isin = pd.read_excel(isin_file_path, sheet_name=0, header=header_idx)
    except Exception as e:
        print(f"error can't open excel file {e}")
        exit()

    isin_map_symbol = dict(zip(df_isin['Securities Symbol'].astype(str).str.strip(), df_isin['ISIN Code']))
    isin_map_name = dict(zip(df_isin['Company Name'].astype(str).str.strip(), df_isin['ISIN Code']))
    isin_company_names = list(isin_map_name.keys())

    print(f"open file funds info {input_csv_path.name}")
    df_fund = pd.read_csv(input_csv_path)

    def get_isin(row):
        f_code = str(row['fund_code']).strip() if pd.notna(row['fund_code']) else ""
        f_name_en = str(row['full_name_en']).strip() if pd.notna(row['full_name_en']) else ""
        if f_code in isin_map_symbol:
            return isin_map_symbol[f_code]
        if f_name_en in isin_map_name:
            return isin_map_name[f_name_en]
        if f_name_en:
            match = process.extractOne(f_name_en, isin_company_names, scorer=fuzz.token_sort_ratio, score_cutoff=85)
            if match:
                best_match_name = match[0]
                return isin_map_name[best_match_name]
        return None

    print("mapping")
    df_fund['isin'] = df_fund.apply(get_isin, axis=1)
    target_columns = [
        'fund_code', 
        'full_name_th', 
        'full_name_en', 
        'amc', 
        'category', 
        'risk_level', 
        'is_dividend', 
        'inception_date', 
        'isin',
        'source_url'
    ]
    available_columns = [col for col in target_columns if col in df_fund.columns]
    df_final = df_fund[available_columns]
    print(f"saving {output_csv_path.name}")
    df_final.to_csv(output_csv_path, index=False, encoding='utf-8-sig')

    print(f"done (set_isin)")

if __name__ == "__main__":
    set_isin_process.fn()