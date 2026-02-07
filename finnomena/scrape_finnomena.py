import requests
import csv
import time
import re
import random
import pdfplumber
import logging
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from datetime import datetime
from prefect import task

# CONFIG
logging.getLogger("pdfminer").setLevel(logging.CRITICAL)
script_dir = Path(__file__).resolve().parent
current_date_str = datetime.now().strftime("%Y-%m-%d")
root = script_dir.parent
FN_RAW_DATA_DIR = script_dir/"raw_data"
NAV_ALL_DIR = script_dir/"all_nav"
WM_DIR = root/"wealthmagik"
WM_RAW_DATA_DIR = WM_DIR/"raw_data"
for d in [FN_RAW_DATA_DIR, NAV_ALL_DIR]:
    d.mkdir(parents=True, exist_ok=True)
OUTPUT_FUND_LIST = FN_RAW_DATA_DIR/"finnomena_fund_list.csv"
OUTPUT_MASTER    = FN_RAW_DATA_DIR/"finnomena_info.csv"
OUTPUT_ALLOCATIONS = FN_RAW_DATA_DIR/"finnomena_allocations.csv"
OUTPUT_FEES      = FN_RAW_DATA_DIR/"finnomena_fees.csv"
OUTPUT_CODES     = FN_RAW_DATA_DIR/"finnomena_codes.csv"
WM_LIST_FILE = WM_RAW_DATA_DIR/"wealthmagik_fund_list.csv"
RESUME_FILE = script_dir/"scrape_finnomena_resume.log"
PDF_LOG_FILE = script_dir/"last_pdf_run.log"
LOG_BUFFER = []
HAS_ERROR = False
_G_STORAGE = {}
NUM_WORKERS = 3  # Number of threads. Don't set more than 3 to avoid ban

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.finnomena.com/"
}

def get_obj(name):
    if name not in _G_STORAGE:
        if name == "STOP_EVENT":
            _G_STORAGE[name] = threading.Event()
        else:
            _G_STORAGE[name] = threading.Lock()
    return _G_STORAGE[name]

def log(msg):
    global HAS_ERROR
    if "error" in msg.lower() or "failed" in msg.lower():
        HAS_ERROR = True
    timestamp = time.strftime('%H:%M:%S')
    with get_obj("LOG_LOCK"):
        print(f"[{timestamp}] {msg}")
        LOG_BUFFER.append(f"[{timestamp}] {msg}")

def save_log_if_error():
    if not HAS_ERROR: return
    try:
        log_dir = root/"Logs"
        if not log_dir.exists(): log_dir.mkdir(parents=True, exist_ok=True)
        filename = f"scrape_finnomena_{datetime.now().strftime('%Y-%m-%d')}.log"
        with open(log_dir/filename, "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_BUFFER))
        with get_obj("LOG_LOCK"):
            print(f"Log saved at: {filename}")
    except: pass

def get_resume_state():
    if not RESUME_FILE.exists(): return set()
    finished = set()
    try:
        with open(RESUME_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        if not lines: return set()
        first_line_parts = lines[0].strip().split('|')
        if len(first_line_parts) < 2 or first_line_parts[1] != current_date_str:
            log(f"Resume file date mismatch Deleting and starting new")
            try: RESUME_FILE.unlink()
            except: pass
            return set()
        for line in lines:
            parts = line.strip().split('|')
            if len(parts) >= 1: finished.add(parts[0])
        log(f"Resuming Found {len(finished)} funds done")
        return finished
    except Exception as e: 
        log(f"Error reading resume file: {e}")
        return set()

def append_resume_state(code):
    with get_obj("CSV_LOCK"):
        try:
            current_time = datetime.now().strftime("%H:%M:%S")
            with open(RESUME_FILE, 'a', encoding='utf-8') as f:
                f.write(f"{code}|{current_date_str}|{current_time}\n")
        except: pass

def cleanup_resume_file():
    if RESUME_FILE.exists():
        try:
            RESUME_FILE.unlink()
            log("Resume file deleted")
        except: pass

def sanitize_filename(name):
    if not name: return "unknown_fund"
    return re.sub(r'[<>:"/\\|?*]', '_', name).strip()

def format_date(iso_date):
    if not iso_date: return ""
    try:
        return datetime.fromisoformat(iso_date.replace("Z", "+00:00")).strftime("%d-%m-%Y")
    except: return iso_date

def safe_api_get(url, params=None):
    MAX_RETRIES = 3
    RETRY_DELAY = 3
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 404:
                return None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                pass
    return None

def get_all_fund_list():
    url = "https://www.finnomena.com/fn3/api/fund/v2/public/funds"
    try:
        data = safe_api_get(url)
        return data.get("data", []) if data and data.get("status") else []
    except Exception as e:
        log(f"Error getting fund list: {e}")
        return []
    
def load_existing_codes():
    if not OUTPUT_CODES.exists(): return {}
    codes_map = {}
    try:
        with open(OUTPUT_CODES, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                c = row.get('fund_code')
                if c:
                    if c not in codes_map: codes_map[c] = []
                    codes_map[c].append(row)
    except: pass
    return codes_map

def extract_codes_from_pdf(pdf_url, fund_code):
    codes = []
    if not pdf_url: return codes
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(pdf_url, headers=HEADERS, timeout=25)
            if r.status_code == 200:
                with pdfplumber.open(BytesIO(r.content)) as pdf:
                    full_text = ""
                    for page in pdf.pages:
                        full_text += (page.extract_text(x_tolerance=3, y_tolerance=3) or "") + "\n"
                    isin_matches = re.findall(r"\b([A-Z]{2}[A-Z0-9]{9}[0-9])\b", full_text)
                    for isin in set(isin_matches):
                        codes.append({"fund_code": fund_code, "type": "ISIN", "code": isin, "factsheet_url": pdf_url})
                break
        except Exception as e:
            if attempt < MAX_RETRIES - 1: time.sleep(2)
            else: log(f"PDF Error {fund_code}: {e}")
    return codes

def check_is_monthly_run():
    if not PDF_LOG_FILE.exists(): return True
    try:
        with open(PDF_LOG_FILE, 'r') as f:
            last_date_str = f.read().strip()
        if not last_date_str: return True
        last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
        current_date = datetime.now()
        if last_date.month != current_date.month or last_date.year != current_date.year:
            return True
        return False
    except: return True

def update_pdf_run_log():
    try:
        with open(PDF_LOG_FILE, 'w') as f:
            f.write(datetime.now().strftime("%Y-%m-%d"))
    except: pass

def sync_and_clean_wealthmagik_list(valid_fn_codes):
    if not WM_LIST_FILE.exists():
        log("WealthMagik list file not found Skipping")
        return
    log("Cleaning WealthMagik list")
    cleaned_rows = []
    removed_count = 0
    try:
        with open(WM_LIST_FILE, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            if not headers: return
            for row in reader:
                wm_code = row.get('fund_code', '').strip()
                if wm_code in valid_fn_codes:
                    cleaned_rows.append(row)
                else:
                    removed_count += 1
        with open(WM_LIST_FILE, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(cleaned_rows)
        log(f"WealthMagik List: Removed {removed_count} inactive funds")
            
    except Exception as e:
        log(f"Error cleaning WM list: {e}")

def parse_fee_value(fees_list, keywords):
    for fee in fees_list:
        desc = fee.get("description", "").lower()
        if all(k in desc for k in keywords):
            return fee.get("rate", ""), fee.get("actual_value", "")
    return "", ""

def process_fund_task(fund, writers, existing_codes_map, is_monthly_run, stop_event):
    if stop_event.is_set(): return None
    fund_id = fund.get("fund_id")
    code = fund.get("short_code")
    time.sleep(random.uniform(0.5, 2.0))
    if stop_event.is_set(): return None
    info_json = {}
    is_success = False
    factsheet_url = ""

    # 1. Info
    try:
        if stop_event.is_set(): return None
        res = safe_api_get(f"https://www.finnomena.com/fn3/api/fund/v2/public/funds/{fund_id}")
        if res is None:
            log(f"Error cannot fetch info for {code}")
            return None 
        info_json = res.get("data", {}) if res else {}
        factsheet_url = info_json.get("fund_fact_sheet", "")
        row_data = {
            "fund_code": code,
            "full_name_th": info_json.get("name_th", ""),
            "full_name_en": info_json.get("name_en", ""),
            "amc": info_json.get("amc_name_en", ""),
            "category": info_json.get("aimc_category_name_en", ""),
            "risk_level": info_json.get("risk_level", ""),
            "is_dividend": "จ่าย" if info_json.get("dividend_policy") != "ไม่จ่าย" else "ไม่จ่าย",
            "inception_date": format_date(info_json.get("inception_date")),
            "source_url": f"https://www.finnomena.com/fund/{fund_id}"
        }
        with get_obj("CSV_LOCK"):
            writers['master'].writerow(row_data)
        is_success = True 

    except Exception as e: 
        log(f"Error Info {code}: {e}")
        is_success = False

    # 2. NAV
    try:
        if stop_event.is_set(): return None
        res = safe_api_get(f"https://www.finnomena.com/fn3/api/fund/v2/public/funds/{fund_id}/nav/q?range=MAX")
        nav_data = res.get("data", {}).get("navs", []) if res else []
        if nav_data:
            safe_code_filename = sanitize_filename(code)
            with open(NAV_ALL_DIR/f"{safe_code_filename}.csv", "w", newline="", encoding="utf-8") as f_nav:
                w_nav = csv.writer(f_nav)
                w_nav.writerow(["fund_code", "date", "value", "amount"]) 
                for n in nav_data:
                    if stop_event.is_set(): return None 
                    w_nav.writerow([code, format_date(n.get("date")), n.get("value"), n.get("amount")])
    except Exception as e: log(f"Error NAV {code}: {e}")

    # 3. Fee
    try:
        if stop_event.is_set(): return None
        res = safe_api_get(f"https://www.finnomena.com/fn3/api/fund/v2/public/funds/{fund_id}/fee")
        fees_list = res.get("data", {}).get("fees", []) if res else []
        front_max, front_act = parse_fee_value(fees_list, ["front-end"])
        back_max, back_act = parse_fee_value(fees_list, ["back-end"])
        mngt_max, mngt_act = parse_fee_value(fees_list, ["การจัดการ"])
        switch_in_max, switch_in_act = parse_fee_value(fees_list, ["switching", "in"])
        switch_out_max, switch_out_act = parse_fee_value(fees_list, ["switching", "out"])
        ter_max, ter_act = parse_fee_value(fees_list, ["ค่าใช้จ่ายรวมทั้งหมด"])
        
        fee_row = {
            "fund_code": code, 
            "source_url": f"https://www.finnomena.com/fund/{fund_id}", 
            "front_end_max": front_max, "front_end_actual": front_act,
            "back_end_max": back_max, "back_end_actual": back_act,
            "management_max": mngt_max, "management_actual": mngt_act,
            "ter_max": ter_max, "ter_actual": ter_act,
            "switching_in_max": switch_in_max, "switching_in_actual": switch_in_act,
            "switching_out_max": switch_out_max, "switching_out_actual": switch_out_act,
            "min_initial_buy": info_json.get("minimum_initial", ""), 
            "min_next_buy": info_json.get("minimum_subsequent", "")
        }
        with get_obj("CSV_LOCK"):
            writers['fees'].writerow(fee_row)
    except Exception as e: log(f"Error Fee {code}: {e}")

    # 4. Allocations
    try:
        if stop_event.is_set(): return None
        res = safe_api_get(f"https://www.finnomena.com/fn3/api/fund/v2/public/funds/{fund_id}/portfolio")
        port_data = res.get("data") if res else None
        if port_data:
            alloc_rows = []
            asset_alloc = port_data.get("asset_allocation") or {}
            for item in (asset_alloc.get("elements") or []):
                alloc_rows.append({
                    "fund_code": code, "type": "asset_alloc", "name": item.get("name"),
                    "percent": item.get("percent"), "as_of_date": format_date(asset_alloc.get("data_date")),
                    "source_url": f"https://www.finnomena.com/fund/{fund_id}"
                })
            sector_alloc = port_data.get("global_stock_sector") or port_data.get("sector_allocation") or {}
            for item in (sector_alloc.get("elements") or []):
                alloc_rows.append({
                    "fund_code": code, "type": "sector_alloc", "name": item.get("name"),
                    "percent": item.get("percent"), "as_of_date": format_date(sector_alloc.get("data_date")),
                    "source_url": f"https://www.finnomena.com/fund/{fund_id}"
                })
            
            with get_obj("CSV_LOCK"):
                if alloc_rows: writers['allocations'].writerows(alloc_rows)
    except Exception as e: log(f"Error Holding {code}: {e}")

    # 5. codes (PDF)
    try:
        if stop_event.is_set(): return None
        cached_rows = existing_codes_map.get(code, [])
        need_scrape = False
        if not cached_rows: need_scrape = True
        elif is_monthly_run: need_scrape = True
        elif cached_rows and cached_rows[0].get('factsheet_url') != factsheet_url: need_scrape = True 
        
        if need_scrape:
             if factsheet_url and factsheet_url.endswith(".pdf"):
                 codes_found = extract_codes_from_pdf(factsheet_url, code)
                 if codes_found: 
                    with get_obj("CSV_LOCK"): writers['codes'].writerows(codes_found)
        else:
            with get_obj("CSV_LOCK"): writers['codes'].writerows(cached_rows)
    except Exception as e: log(f"Error Codes {code}: {e}")
    with get_obj("CSV_LOCK"):
        for w in writers.values():
            pass
    if is_success:
        append_resume_state(code)
        return code
    else:
        return None

@task(name="Finnomena scraper", log_prints=True)
def finnomena_scraper():
    global HAS_ERROR
    log("Starting Finnomena Scraper")
    IS_MONTHLY_RUN = check_is_monthly_run()
    if IS_MONTHLY_RUN:
        log("Status: NEW MONTH PDF scraping ENABLED")
    else:
        log("Status: SAME MONTH PDF scraping SKIPPED")
    finished_funds = get_resume_state()
    raw_funds = get_all_fund_list()
    log(f"Fetched {len(raw_funds)} funds from API")
    active_funds = [
        f for f in raw_funds 
        if f.get("sec_is_active") is True 
        and f.get("short_code") 
        and f.get("short_code").strip()
    ]
    active_funds.sort(key=lambda x: x.get("short_code", "").strip())
    active_fund_codes = {f.get("short_code").strip() for f in active_funds}
    log(f"Found {len(active_fund_codes)} ACTIVE funds")
    existing_codes_map = load_existing_codes()
    log(f"Loaded {len(existing_codes_map)} existing funds ISIN")
    sync_and_clean_wealthmagik_list(active_fund_codes)
    with open(OUTPUT_FUND_LIST, 'w', newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["fund_code", "url"])
        writer.writeheader()
        for fund in active_funds:
            writer.writerow({
                "fund_code": fund.get("short_code"), 
                "url": f"https://www.finnomena.com/fund/{fund.get('fund_id')}"
            })
    log(f"Saved Finnomena Fund List to {OUTPUT_FUND_LIST}")
    mode = 'a'
    write_header = not OUTPUT_MASTER.exists()
    if not finished_funds:
        mode = 'w'
        write_header = True
    f_master = open(OUTPUT_MASTER, mode, newline="", encoding="utf-8-sig")
    f_fees = open(OUTPUT_FEES, mode, newline="", encoding="utf-8-sig")
    f_allocations = open(OUTPUT_ALLOCATIONS, mode, newline="", encoding="utf-8-sig")
    f_codes = open(OUTPUT_CODES, mode, newline="", encoding="utf-8-sig")
    writers = {
        'master': csv.DictWriter(f_master, fieldnames=["fund_code", "full_name_th", "full_name_en", "amc", "category", "risk_level", "is_dividend", "inception_date", "source_url"]),
        'fees': csv.DictWriter(f_fees, fieldnames=["fund_code", "source_url", "front_end_max", "front_end_actual", "back_end_max", "back_end_actual", "management_max", "management_actual", "ter_max", "ter_actual", "switching_in_max", "switching_in_actual", "switching_out_max", "switching_out_actual", "min_initial_buy", "min_next_buy"]),
        'allocations': csv.DictWriter(f_allocations, fieldnames=["fund_code", "type", "name", "percent", "as_of_date", "source_url"]),
        'codes': csv.DictWriter(f_codes, fieldnames=["fund_code", "type", "code", "factsheet_url"])
    }
    if write_header:
        for w in writers.values(): w.writeheader()
    try:
        total = len(active_funds)
        active_fund_codes_set = {f.get('short_code').strip() for f in active_funds}
        finished_funds = finished_funds.intersection(active_fund_codes_set)
        pending_funds = [f for f in active_funds if f.get('short_code').strip() not in finished_funds]
        log(f"Processing {len(pending_funds)} funds (Skipped {len(finished_funds)})")
        executor = ThreadPoolExecutor(max_workers=NUM_WORKERS)
        futures = []
        for fund in pending_funds:
            if get_obj("STOP_EVENT").is_set(): break
            futures.append(executor.submit(process_fund_task, fund, writers, existing_codes_map, IS_MONTHLY_RUN))
        count = 0
        for future in as_completed(futures):
            if get_obj("STOP_EVENT").is_set(): break
            try:
                result_code = future.result()
                if result_code:
                    count += 1
                    completed = len(finished_funds) + count
                    log(f"[{completed}/{total}] {result_code} (finnomena)")
                    if count % 10 == 0:
                        with get_obj("CSV_LOCK"):
                            f_master.flush(); f_fees.flush(); f_allocations.flush(); f_codes.flush()
            except Exception as e:
                log(f"Task Failed: {e}")

    except KeyboardInterrupt: 
        log("Stopping Scraper")
        get_obj("STOP_EVENT").set()
        executor.shutdown(wait=False, cancel_futures=True)
        HAS_ERROR = True
    except Exception as e:
        log(f"Critical Error: {e}")
    finally:
        f_master.close(); f_fees.close(); f_allocations.close(); f_codes.close()
        try: executor.shutdown(wait=True) 
        except: pass
        if not HAS_ERROR: 
            if IS_MONTHLY_RUN:
                update_pdf_run_log()
                log("Monthly PDF completed. Updated log")
        save_log_if_error()
        log("Done All tasks completed (finnomena)")

if __name__ == "__main__":
    finnomena_scraper()