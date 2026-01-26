import csv
import time
import re
from pathlib import Path
import random
import requests
import math
import threading
from urllib.parse import quote, unquote
from datetime import datetime

# CONFIG
script_dir = Path(__file__).resolve().parent
current_date_str = datetime.now().strftime("%Y-%m-%d")
INPUT_FILE = script_dir/"finnomena/raw_data/finnomena_fund_list.csv"
OUTPUT_DIR = script_dir/"merged_output"
if not OUTPUT_DIR.exists():OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILENAME = OUTPUT_DIR/"all_sec_fund_info.csv"
RESUME_FILE = script_dir/"scrape_sec_resume.log"
API_URL = "https://web-fct-api.sec.or.th/api/funds"
BATCH_SIZE = 2
MAX_RETRIES = 3
RETRY_DELAY = 2
LOG_BUFFER = []
HAS_ERROR = False
CSV_LOCK = threading.Lock()
LOG_LOCK = threading.Lock()
COUNT_LOCK = threading.Lock()
STOP_EVENT = threading.Event()
PROCESSED_COUNT = 0

def polite_sleep():
    time.sleep(random.uniform(1, 3))

def log(msg):
    global HAS_ERROR
    if "error" in msg.lower() or "failed" in msg.lower():
        HAS_ERROR = True
    timestamp = time.strftime('%H:%M:%S')
    with LOG_LOCK:
        print(f"[{timestamp}] {msg}")
        LOG_BUFFER.append(f"[{timestamp}] {msg}")

def save_log_if_error():
    if not HAS_ERROR: return
    try:
        log_dir = script_dir/"Logs"
        if not log_dir.exists():log_dir.mkdir(parents=True, exist_ok=True)
        filename = f"scrape_sec_{datetime.now().strftime('%Y-%m-%d')}.log"
        with open(log_dir/filename, "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_BUFFER))
    except Exception as e:
        print(f"Cannot save log file: {e}")

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
    try:
        current_time = datetime.now().strftime("%H:%M:%S")
        with open(RESUME_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{code}|{current_date_str}|{current_time}\n")
    except: pass

def clean_number(text):
    if text is None: return ""
    text = str(text)
    text = re.sub(r'[%,]', '', text)
    text = re.sub(r'\s+', '', text)
    if text in ["-", "N/A", "null", "None"]: return ""
    return text

def convert_thai_date(date_str):
    if not date_str or date_str == "null": return "N/A"
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            day, month, year_be = parts
            year_ce = int(year_be) - 543
            return f"{int(day):02d}-{int(month):02d}-{year_ce}"
    except: return date_str
    return date_str

def calculate_recovering_days(rp_data):
    if not rp_data or not isinstance(rp_data, dict): return ""
    total_days = 0
    y = rp_data.get("year", 0)
    m = rp_data.get("month", 0)
    d = rp_data.get("day", 0)
    if y is None and m is None and d is None: return ""
    if y: total_days += int(y) * 365
    if m: total_days += int(m) * 30
    if d: total_days += int(d)
    return str(total_days) if total_days > 0 else ""

def create_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Origin": "https://fundcheck.sec.or.th",
        "Referer": "https://fundcheck.sec.or.th/",
        "Accept": "application/json, text/plain, */*"
    })
    return s

def fetch_batch_data(session, fund_codes_batch):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(API_URL, json=fund_codes_batch, timeout=20)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 400:
                return [] 
            elif response.status_code == 429:
                time.sleep(5)
            else:
                log(f"API Error {response.status_code} (Batch size: {len(fund_codes_batch)})")
        except Exception as e:
            log(f"Connection Error (Attempt {attempt}): {e}")
            time.sleep(RETRY_DELAY)
            
    return []

def main():
    finished_funds = get_resume_state()
    all_funds = []
    if not INPUT_FILE.exists():
        log(f"Error Input file not found {INPUT_FILE}")
        return
    log(f"Reading funds from {INPUT_FILE.name}")
    with open(INPUT_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw = row.get("fund_code", "")
            if raw:
                code = unquote(raw).strip()
                if code: all_funds.append(code)
    all_funds = sorted(list(set(all_funds)))
    pending_funds = [c for c in all_funds if c not in finished_funds]
    total_all = len(all_funds)
    finished_start = total_all - len(pending_funds)
    log(f"Total: {total_all}, Finished: {finished_start}, Remaining: {len(pending_funds)}")
    if not pending_funds:
        log("All done (SEC)")
        return

    headers = [
        "fund_code", "as_of_date", 
        "sharpe_ratio", "alpha", "beta", 
        "max_drawdown", "recovering_period", 
        "tracking_error", "turnover_ratio", "fx_hedging",
        "sec_url"
    ]
    if not OUTPUT_FILENAME.exists() or OUTPUT_FILENAME.stat().st_size == 0:
        with open(OUTPUT_FILENAME, 'w', newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
    session = create_session()
    chunks = [pending_funds[i:i + BATCH_SIZE] for i in range(0, len(pending_funds), BATCH_SIZE)]
    log(f"Starting processing {len(chunks)} batches")
    global PROCESSED_COUNT
    PROCESSED_COUNT = 0
    try:
        for batch in chunks:
            if STOP_EVENT.is_set(): break
            api_data_list = fetch_batch_data(session, batch)
            api_data_map = {item.get("abbrName"): item for item in api_data_list if item.get("abbrName")}
            batch_rows = []
            for fund_code in batch:
                match_data = api_data_map.get(fund_code)
                safe_code = quote(fund_code, safe='')
                sec_page_url = f"https://fundcheck.sec.or.th/fund-detail;funds={safe_code}"
                row_data = {
                    "fund_code": fund_code,
                    "sec_url": sec_page_url,
                    "as_of_date": "N/A",
                    "sharpe_ratio": "", "alpha": "", "beta": "",
                    "max_drawdown": "", "recovering_period": "",
                    "tracking_error": "", "turnover_ratio": "", "fx_hedging": ""
                }
                if match_data:
                    row_data["as_of_date"] = convert_thai_date(match_data.get("representDate"))
                    row_data["sharpe_ratio"] = clean_number(match_data.get("sharpRatio"))
                    row_data["alpha"] = clean_number(match_data.get("alpha"))
                    row_data["beta"] = clean_number(match_data.get("beta"))
                    row_data["max_drawdown"] = clean_number(match_data.get("maximumDrawdown"))
                    row_data["tracking_error"] = clean_number(match_data.get("trackingError"))
                    row_data["turnover_ratio"] = clean_number(match_data.get("turnoverRatio"))
                    row_data["fx_hedging"] = clean_number(match_data.get("fxHedging"))
                    row_data["recovering_period"] = calculate_recovering_days(match_data.get("recoveringPeriod"))
                batch_rows.append(row_data)
                with COUNT_LOCK:
                    PROCESSED_COUNT += 1
                    current_total = finished_start + PROCESSED_COUNT
                status_msg = "(SEC)" if match_data else "(Not Found SEC)"
                log(f"[{current_total}/{total_all}] {fund_code} {status_msg}")
                append_resume_state(fund_code)
            with CSV_LOCK:
                with open(OUTPUT_FILENAME, 'a', newline="", encoding="utf-8-sig") as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writerows(batch_rows)
            polite_sleep()

    except KeyboardInterrupt:
        log("\nStopping Scraper")
        STOP_EVENT.set()
        HAS_ERROR = True
    except Exception as e:
        log(f"Critical Error: {e}")
        HAS_ERROR = True
    finally:
        save_log_if_error()
        log("Done (SEC)")

if __name__ == "__main__":
    main()