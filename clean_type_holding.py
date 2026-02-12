import pandas as pd
import re
import requests
import time
import csv
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from prefect import task
from difflib import SequenceMatcher

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / 'wealthmagik/raw_data/wealthmagik_holdings.csv' 
OUTPUT_FILE = BASE_DIR / 'merged_output/merged_holding.csv'
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
DB_FILE = Path('stock_type_holding.csv') 
OTHER_DB_FILE = Path('other_type_holding.csv')
RESUME_FILE = BASE_DIR / "clean_type_resume.log"
LOG_DIR = BASE_DIR / "Logs"
LOG_DIR.mkdir(exist_ok=True)
SEARCH_API_URL = "https://www.finnomena.com/market-info/api/public/search/_search"
QUOTE_API_URL = "https://www.finnomena.com/market-info/api/public/stock/quote"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
current_date_str = datetime.now().strftime("%Y-%m-%d")
stock_db_cache = {}
other_db_cache = set()
LOG_BUFFER = []
NUM_WORKERS = 3
_G_STORAGE = {}

def get_obj(name):
    if name not in _G_STORAGE:
        if name == "STOP_EVENT":
            _G_STORAGE[name] = threading.Event()
        else:
            _G_STORAGE[name] = threading.Lock()
    return _G_STORAGE[name]

def similarity(a, b):
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()

def log(msg):
    with get_obj("LOG_LOCK"):
        timestamp = datetime.now().strftime('%H:%M:%S')
        full_msg = f"[{timestamp}] {msg}"
        print(full_msg)
        LOG_BUFFER.append(full_msg)

def is_ticker_related(api_ticker, my_code):
    t1 = str(api_ticker).upper().replace(" ", "")
    t2 = str(my_code).upper().replace(" ", "")
    return (t1 in t2) or (t2 in t1)

def polite_sleep():
    time.sleep(random.uniform(0.8, 1.5))

def save_daily_log():
    log_filename = LOG_DIR / f"process_{current_date_str}.log"
    with open(log_filename, "a", encoding="utf-8") as f:
        f.write("\n".join(LOG_BUFFER) + "\n")
    LOG_BUFFER.clear()

def get_resume_state():
    if not RESUME_FILE.exists(): return set()
    finished = set()
    try:
        with open(RESUME_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        if not lines: return set()
        first_line_parts = lines[0].strip().split('|')
        if len(first_line_parts) < 2 or first_line_parts[1] != current_date_str:
            RESUME_FILE.unlink()
            return set()
        for line in lines:
            parts = line.strip().split('|')
            if len(parts) >= 1: finished.add(parts[0])
        return finished
    except: return set()

def append_resume_state(unique_key):
    try:
        with open(RESUME_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{unique_key}|{current_date_str}\n")
    except: pass

def load_databases():
    if DB_FILE.exists():
        with open(DB_FILE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                saved_symbol = row.get('symbol', row['holding_code']) 
                stock_db_cache[row['holding_code']] = (row['type'], row['sector'], saved_symbol)
        log(f"load 'stock' from file: {len(stock_db_cache)}")
    if OTHER_DB_FILE.exists():
        with open(OTHER_DB_FILE, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                other_db_cache.add(row['holding_code'])
        log(f"load type 'other' from file: {len(other_db_cache)}")

def save_to_other_db(code):
    if code in other_db_cache: return
    with get_obj("DB_LOCK"):
        file_exists = OTHER_DB_FILE.exists()
        with open(OTHER_DB_FILE, mode='a', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['holding_code'])
            if not file_exists: writer.writeheader()
            writer.writerow({'holding_code': code})
        other_db_cache.add(code)

def save_to_stock_db(code, s_type, sector, real_symbol):
    file_exists = DB_FILE.exists()
    mode = 'a'
    with open(DB_FILE, mode=mode, encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['holding_code', 'type', 'sector', 'symbol'])
        if not file_exists: 
            writer.writeheader()
        writer.writerow({
            'holding_code': code, 
            'type': s_type, 
            'sector': sector,
            'symbol': real_symbol
        })
    stock_db_cache[code] = (s_type, sector, real_symbol)

def process_row_task(row, writer, finished_keys):
    unique_key = f"{row['fund_code']}_{row['name']}"
    if unique_key in finished_keys: return None
    h_code = extract_code(row['name'])
    res_type, res_sector, res_symbol = 'Other', '', ''
    if h_code in other_db_cache:
        res_type, res_sector, res_symbol = 'Other', '', ''
    elif h_code in stock_db_cache:
        res_type, res_sector, res_symbol = stock_db_cache[h_code]
        if not str(res_type).startswith('Stock'):
            res_symbol = ''
    else:
        temp_type, temp_sector = classify_initial(row['name'], h_code)
        if temp_type == 'Check_System':
            res_type, res_sector, res_symbol = check_stock_api(h_code, row['name'])
        elif temp_type == 'Other':
            save_to_other_db(h_code)
            res_type, res_sector, res_symbol = 'Other', '', ''
        else:
            res_type, res_sector, res_symbol = temp_type, temp_sector, ''
    with get_obj("CSV_LOCK"):
        writer.writerow({
            'fund_code': row['fund_code'],
            'symbol': res_symbol,
            'type': res_type,
            'sector': res_sector,
            'name': row['name'],
            'percent': row['percent'],
            'as_of_date': row['as_of_date'],
            'source_url': row['source_url']
        })
        append_resume_state(unique_key)
    return f"{h_code} -> {res_symbol} ({res_type})"

def extract_code(name):
    name = str(name).strip()
    match = re.search(r'\(([^)]+)\)$', name)
    return match.group(1).strip() if match else name

def classify_initial(name_full, code):
    name_up = name_full.upper()
    code_up = code.upper()

    other_keywords = [
        'OTHER', 'อื่นๆ', 'CASH', 'DEPOSIT', 'SAVING', 'เงินฝาก', 
        'INTEREST', 'ACCRUED', 'REPO', 'REVERSE REPO', 'MARGIN'
    ]
    if any(k in name_up for k in other_keywords): return 'Other', ''
    if code_up in ['THB', 'USD', 'EUR', 'JPY', 'CNY', 'SGD']: return 'Other', ''

    bond_keywords = [
        'BANK OF THAILAND', 'BOT BOND', 'TREASURY', 'GOV BOND', 'GOVERNMENT BOND', 'DEBENTURE',
        'BILL OF EXCHANGE', 'T-BILL', 'พันธบัตร', 'หุ้นกู้', 'LOAN STOCK', 'DEB', 'NOTES', 'STRIPS', 'FIXED INCOME', 'SOVEREIGN'
    ]
    if any(k in name_up for k in bond_keywords): return 'Bond', ''
    
    if len(code_up) >= 6 and any(c.isdigit() for c in code_up) and not any(k in name_up for k in ['FUND', 'REIT', 'ETF']):
        if sum(c.isdigit() for c in code_up) >= 2: return 'Bond', ''

    fund_keywords = [
        'FUND', 'REIT', 'PROPERTY FUND', 'INFRASTRUCTURE', 'กองทุน',
        'ETF', 'UNIT TRUST', 'MUTUAL FUND', 'SICAV', 'UCITS', 'TRUST',
        'REAL ESTATE INVESTMENT'
    ]
    if any(k in name_up for k in fund_keywords): return 'Fund', ''
    
    if any(code_up.endswith(suffix) for suffix in ['-A', '-D', '-E', '-P', '-R', '-SSF', '-RMF']):
        return 'Fund', ''
    
    return 'Check_System', ''

def check_stock_api(code, hint_name):
    code_up = code.upper()
    if code_up in stock_db_cache: return stock_db_cache[code_up]
    if code_up in other_db_cache: return ('Other', '', '')
    clean_hint = hint_name.split('(')[0].strip()
    found_match = None
    best_score = 0
    def evaluate_candidates(candidates):
        nonlocal found_match, best_score
        for item in candidates:
            api_ticker = item.get('title', '').upper()
            api_desc = item.get('description', '')
            name_score = similarity(clean_hint, api_desc)
            ticker_related = is_ticker_related(api_ticker, code_up)
            ticker_score = 1.0 if ticker_related else 0.0
            final_score = name_score + (ticker_score * 0.5)
            if (ticker_related and name_score > 0.3) or (name_score > 0.8):
                if final_score > best_score:
                    best_score = final_score
                    found_match = item
    try:
        polite_sleep()
        resp = requests.get(SEARCH_API_URL, headers=HEADERS, params={'q': code_up, 'size': 5}, timeout=5).json()
        if 'data' in resp and resp['data']['result']:
            evaluate_candidates(resp['data']['result'])
        if not found_match and len(clean_hint) > 1:
            polite_sleep()
            resp_name = requests.get(SEARCH_API_URL, headers=HEADERS, params={'q': clean_hint, 'size': 10}, timeout=5).json()
            if 'data' in resp_name and resp_name['data']['result']:
                evaluate_candidates(resp_name['data']['result'])
        if not found_match:
            save_to_other_db(code_up)
            return ('Other', '', '')
        match = found_match
        real_symbol = match.get('title', code_up).upper()
        if match.get('type_en', '').lower() == 'fund': 
            return ('Fund', '', '')
        country = match.get('meta', {}).get('country_iso', 'TH')
        final_type = f"Stock ({country})"
        ex = 'US' if country == 'US' else ('HK' if country == 'HK' else None)
        q_res = requests.get(f"{QUOTE_API_URL}/{real_symbol}", headers=HEADERS, params={'exchange': ex} if ex else {}, timeout=5).json()
        sector = q_res.get('data', {}).get('sector', '') if q_res.get('status') else ''
        if sector == '-': sector = ''
        save_to_stock_db(code_up, final_type, sector, real_symbol) 
        return (final_type, sector, real_symbol)
    except Exception as e:
        return ('Other', '', '')

@task(name="clean_type_holding", log_prints=True)
def clean_holding():
    log("clean type of holding start")
    if not INPUT_FILE.exists():
        log(f"error not found: {INPUT_FILE}")
        return
    load_databases()
    finished_keys = get_resume_state()
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    total_rows = len(df)
    fieldnames = ['fund_code', 'symbol', 'type', 'sector', 'name', 'percent', 'as_of_date', 'source_url']
    file_mode = 'a' if OUTPUT_FILE.exists() and len(finished_keys) > 0 else 'w'
    try:
        with open(OUTPUT_FILE, mode=file_mode, encoding='utf-8-sig', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            if file_mode == 'w': writer.writeheader()
            with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
                rows = df.to_dict('records')
                futures = [executor.submit(process_row_task, row, writer, finished_keys) for row in rows]
                for i, future in enumerate(as_completed(futures), 1):
                    if get_obj("STOP_EVENT").is_set():
                        break
                    try:
                        result = future.result()
                        if result:
                            log(f"[{i}/{total_rows}] {result}")
                    except Exception as e:
                        log(f"error line {i} because: {e}")
                    if i % 20 == 0:
                        with get_obj("CSV_LOCK"):
                            outfile.flush()
                        save_daily_log()

    except KeyboardInterrupt:
        log("\nstop now")
    finally:
        save_daily_log()
        log(f"done (clean type holding)")

if __name__ == "__main__":
    clean_holding.fn()