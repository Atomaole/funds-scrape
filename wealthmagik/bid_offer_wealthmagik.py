import csv
import time
from pathlib import Path
import random
import threading
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from prefect import task

# CONFIG
script_dir = Path(__file__).resolve().parent
root = script_dir.parent
current_date_str = datetime.now().strftime("%Y-%m-%d")
RAW_DATA_DIR = script_dir/"raw_data"
if not RAW_DATA_DIR.exists():RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
INPUT_FILENAME = RAW_DATA_DIR/"wealthmagik_fund_list.csv"
OUTPUT_FILENAME = RAW_DATA_DIR/"wealthmagik_bid_offer.csv"
RESUME_FILE = script_dir/"bid_offer_resume.log"
MAX_RETRIES = 3
RETRY_DELAY = 2
NUM_WORKERS = 3
LOG_BUFFER = []
HAS_ERROR = False
_G_STORAGE = {}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

def get_obj(name):
    if name not in _G_STORAGE:
        if name == "STOP_EVENT":
            _G_STORAGE[name] = threading.Event()
        else:
            _G_STORAGE[name] = threading.Lock()
    return _G_STORAGE[name]
PROCESSED_COUNT = 0 

def polite_sleep():
    time.sleep(random.uniform(1, 2))

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
        filename = f"bid_offer_wm_{datetime.now().strftime('%Y-%m-%d')}.log"
        with open(log_dir/filename, "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_BUFFER))
        with get_obj("LOG_LOCK"):
            print(f"Log saved at: {filename}")
    except: pass

def load_finished_funds():
    finished = set()
    if OUTPUT_FILENAME.exists():
        try:
            with open(OUTPUT_FILENAME, 'r', encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("fund_code"):
                        finished.add(row["fund_code"])
        except Exception as e:
            log(f"Error reading output file: {e}")
    if RESUME_FILE.exists():
        try:
            with open(RESUME_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.strip().split('|')
                    if len(parts) >= 1: finished.add(parts[0])
        except Exception as e:
            log(f"Error reading resume file: {e}")
    return finished

def append_resume_state(code):
    with get_obj("RESUME_LOCK"):
        try:
            current_time = datetime.now().strftime("%H:%M:%S")
            with open(RESUME_FILE, 'a', encoding='utf-8') as f:
                f.write(f"{code}|{current_date_str}|{current_time}\n")
        except: pass

def format_date(date_str):
    if not date_str: return ""
    try:
        return datetime.strptime(str(date_str), "%Y%m%d").strftime("%d-%m-%Y")
    except:
        return date_str

def fetch_fund_data(fund_code, fund_url):
    url = fund_url 
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Referer": "https://www.wealthmagik.com/",
        "Connection": "keep-alive"
    }
    for attempt in range(MAX_RETRIES):
        if get_obj("STOP_EVENT").is_set(): return None
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                script_tag = soup.find("script", {"id": "serverApp-state"})
                if script_tag:
                    raw_json = script_tag.string.replace('&q;', '"')
                    data = json.loads(raw_json)
                    fund_detail = data.get('fund-detail', {})
                    return {
                        "fund_code": fund_detail.get('fundCode'),
                        "nav_date": format_date(fund_detail.get('tnaclassDate')),
                        "bid_price": fund_detail.get('bidPrice'),
                        "offer_price": fund_detail.get('offerPrice')
                    }
                else:
                    return "Script Not Found"
            elif response.status_code == 404:
                return "Not Found"
        except Exception as e:
            pass
        time.sleep(RETRY_DELAY * (attempt + 1))
    return None

def process_batch(worker_id, funds, fieldnames, total_all_funds, finished_count_start):
    global PROCESSED_COUNT
    with open(OUTPUT_FILENAME, 'a', newline="", encoding="utf-8-sig") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        for fund_item in funds:
            if get_obj("STOP_EVENT").is_set(): break
            fund_code = fund_item['fund_code']
            fund_url = fund_item['url']
            result = fetch_fund_data(fund_code, fund_url)
            with get_obj("COUNT_LOCK"):
                PROCESSED_COUNT += 1
                current_progress = finished_count_start + PROCESSED_COUNT
            if isinstance(result, dict):
                with get_obj("CSV_LOCK"):
                    writer.writerow(result)
                    f_out.flush()
                append_resume_state(fund_code)
                log(f"[{current_progress}/{total_all_funds}] {fund_code} (bid_offer/wealthmagik)")
            elif result == "Not Found":
                log(f"[{current_progress}/{total_all_funds}] {fund_code} (Not Found)")
                append_resume_state(fund_code)
            else:
                log(f"[{current_progress}/{total_all_funds}] {fund_code} (No Data)")
            polite_sleep()

@task(name="bid_offer_wm_request", log_prints=True)
def bid_offer_wm_req():
    log("Starting Bid/Offer Scraper")
    if not INPUT_FILENAME.exists():
        log(f"Error: Input file not found: {INPUT_FILENAME}")
        return
    all_funds = []
    with open(INPUT_FILENAME, 'r', encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if 'fund_code' in row and row['fund_code']:
                all_funds.append({
                    'fund_code': row['fund_code'].strip(),
                    'url': row['url'].strip()
                })
    total_all_funds = len(all_funds)
    finished_funds = load_finished_funds()
    pending_funds = [f for f in all_funds if f['fund_code'] not in finished_funds]
    finished_count_start = len(finished_funds)
    remaining = len(pending_funds)
    log(f"Total: {total_all_funds}, Finished: {finished_count_start}, Remaining: {remaining}")
    if remaining == 0:
        log("All done")
        return
    fieldnames = ["fund_code", "nav_date", "bid_price", "offer_price"]
    if not OUTPUT_FILENAME.exists() or OUTPUT_FILENAME.stat().st_size == 0:
         with open(OUTPUT_FILENAME, 'w', newline="", encoding="utf-8-sig") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
    chunk_size = (len(pending_funds) // NUM_WORKERS) + 1
    batches = [pending_funds[i:i + chunk_size] for i in range(0, len(pending_funds), chunk_size)]
    log(f"Starting {len(batches)} workers")
    global PROCESSED_COUNT
    PROCESSED_COUNT = 0 
    executor = ThreadPoolExecutor(max_workers=NUM_WORKERS)
    futures = []
    try:
        for i, batch in enumerate(batches):
            futures.append(executor.submit(process_batch, i+1, batch, fieldnames, total_all_funds, finished_count_start))
        for future in as_completed(futures):
            try: future.result()
            except Exception as e: pass 
    except KeyboardInterrupt:
        log("Stopping Scraper")
        get_obj("STOP_EVENT").set()
        global HAS_ERROR
        HAS_ERROR = True
    finally:
        save_log_if_error()
        log("Done (bid_offer/WM)")

if __name__ == "__main__":
    bid_offer_wm_req()