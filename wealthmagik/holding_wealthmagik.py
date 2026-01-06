import csv
import time
import re
import os
import random
import requests
import threading
from bs4 import BeautifulSoup
from urllib.parse import unquote
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# CONFIG
script_dir = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(script_dir)
current_date_str = datetime.now().strftime("%Y-%m-%d")
RAW_DATA_DIR = os.path.join(script_dir, "raw_data")
if not os.path.exists(RAW_DATA_DIR): os.makedirs(RAW_DATA_DIR)
INPUT_FILENAME = os.path.join(RAW_DATA_DIR, "wealthmagik_fund_list.csv")
OUTPUT_FILENAME = os.path.join(RAW_DATA_DIR, "wealthmagik_holdings.csv")
RESUME_FILE = os.path.join(script_dir, "holding_resume.log")
MAX_RETRIES = 3
RETRY_DELAY = 2
LOG_BUFFER = []
HAS_ERROR = False
CSV_LOCK = threading.Lock()
LOG_LOCK = threading.Lock()
STOP_EVENT = threading.Event()
NUM_WORKERS = 1  # Number of threads. Don't set more than 3 to avoid ban

THAI_MONTH_MAP = {
    "ม.ค.": 1, "มกราคม": 1, "JAN": 1, "ก.พ.": 2, "กุมภาพันธ์": 2, "FEB": 2,
    "มี.ค.": 3, "มีนาคม": 3, "MAR": 3, "เม.ย.": 4, "เมษายน": 4, "APR": 4,
    "พ.ค.": 5, "พฤษภาคม": 5, "MAY": 5, "มิ.ย.": 6, "มิถุนายน": 6, "JUN": 6,
    "ก.ค.": 7, "กรกฎาคม": 7, "JUL": 7, "ส.ค.": 8, "สิงหาคม": 8, "AUG": 8,
    "ก.ย.": 9, "กันยายน": 9, "SEP": 9, "ต.ค.": 10, "ตุลาคม": 10, "OCT": 10,
    "พ.ย.": 11, "พฤศจิกายน": 11, "NOV": 11, "ธ.ค.": 12, "ธันวาคม": 12, "DEC": 12,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,th;q=0.8"
}

def polite_sleep():
    time.sleep(random.uniform(0.7, 1.3))

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
        log_dir = os.path.join(root, "Logs")
        if not os.path.exists(log_dir): os.makedirs(log_dir)
        filename = f"holding_wm_{datetime.now().strftime('%Y-%m-%d')}.log"
        with open(os.path.join(log_dir, filename), "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_BUFFER))
    except: pass

def get_resume_state():
    if not os.path.exists(RESUME_FILE): return set()
    finished = set()
    try:
        with open(RESUME_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        if not lines: return set()
        first_line_parts = lines[0].strip().split('|')
        if len(first_line_parts) < 2 or first_line_parts[-1] != current_date_str:
            log(f"Resume file date mismatch Deleting and starting new")
            try: os.remove(RESUME_FILE)
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
    with CSV_LOCK:
        try:
            with open(RESUME_FILE, 'a', encoding='utf-8') as f:
                f.write(f"{code}|{current_date_str}\n")
        except: pass

def cleanup_resume_file():
    if os.path.exists(RESUME_FILE):
        try: os.remove(RESUME_FILE)
        except: pass

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip() if text else ""

def parse_thai_date(text):
    if not text: return ""
    text = re.sub(r"(ข้อมูล\s*ณ\s*วันที่|ณ\s*วันที่|as of)", "", text, flags=re.IGNORECASE).strip()
    match = re.search(r"(\d{1,2})\s+([^\s\d]+)\s+(\d{2,4})", text)
    if match:
        d_str, m_str, y_str = match.groups()
        month_num = THAI_MONTH_MAP.get(m_str.strip(), 0)
        if month_num == 0: return text 
        try:
            day, year = int(d_str), int(y_str)
            if year < 100: year += 1957
            elif year > 2400: year -= 543
            return datetime(year, month_num, day).strftime("%d-%m-%Y")
        except: pass
    return text

def scrape_holdings(fund_code, profile_url):
    port_url = re.sub(r"/profile/?$", "/port", profile_url)
    time.sleep(random.uniform(0.5, 1.5))
    for attempt in range(1, MAX_RETRIES + 1):
        if STOP_EVENT.is_set(): return None
        try:
            current_headers = HEADERS.copy()
            if attempt > 1:
                current_headers.update({
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                })
            response = requests.get(port_url, headers=current_headers, timeout=20)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                results = []
                as_of_date = ""
                date_el = soup.select_one(".date-detail-text")
                if date_el:
                    as_of_date = parse_thai_date(clean_text(date_el.get_text()))
                rows = soup.select(".portallocation-list")
                for row in rows:
                    try:
                        name_el = row.select_one(".name-text")
                        weight_el = row.select_one(".ratio-text")
                        if name_el and weight_el:
                            name = clean_text(name_el.get_text())
                            weight = clean_text(weight_el.get_text()).replace("%", "")
                            if name and weight:
                                results.append({
                                    "fund_code": fund_code, "type": "holding", "name": name,
                                    "percent": weight, "as_of_date": as_of_date, "source_url": port_url
                                })
                    except: continue
                if results: return results
                if soup.select(".emptyData"): return []
            elif response.status_code == 404:
                return []
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
        except Exception as e:
            log(f"Error {fund_code}: {e}")
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
            
    return None

def process_fund_task(fund, writer):
    if STOP_EVENT.is_set(): return None
    code = unquote(fund.get("fund_code", "")).strip()
    url = fund.get("url", "")
    if not code or not url: return None
    try:
        data = scrape_holdings(code, url)
        if STOP_EVENT.is_set(): return None
        if data:
            with CSV_LOCK:
                writer.writerows(data)
            append_resume_state(code)
            return f"{code} (holding/wealthmagik)"
        elif data == []:
             append_resume_state(code)
             return f"{code} - No Data"
        else:
             raise Exception("Failed to fetch (Max retries exceeded)")

    except Exception as e:
        raise e

def main():
    finished_funds = get_resume_state()
    funds = []
    try:
        with open(INPUT_FILENAME, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f): funds.append(row)
    except: 
        log(f"Input file not found: {INPUT_FILENAME}")
        return
    mode = 'a' if finished_funds else 'w'
    f_out = open(OUTPUT_FILENAME, mode, newline="", encoding="utf-8-sig")
    keys = ["fund_code", "type", "name", "percent", "as_of_date", "source_url"]
    writer = csv.DictWriter(f_out, fieldnames=keys)
    if mode == 'w': writer.writeheader()
    pending_funds = [f for f in funds if unquote(f.get("fund_code", "")).strip() not in finished_funds]
    total = len(funds)
    current_fund_codes = {unquote(f.get("fund_code", "")).strip() for f in funds}
    finished_count_start = len(finished_funds.intersection(current_fund_codes))
    remaining = len(pending_funds)
    log(f"Total: {total}, Finished: {finished_count_start}, Remaining: {remaining}")
    if remaining == 0:
        log("All done")
        f_out.close()
        return
    log(f"Starting Scraper (holding wealthmagik)")
    executor = ThreadPoolExecutor(max_workers=NUM_WORKERS)
    futures = []
    try:
        count = 0
        for fund in pending_funds:
            if STOP_EVENT.is_set(): break
            futures.append(executor.submit(process_fund_task, fund, writer))
        for future in as_completed(futures):
            if STOP_EVENT.is_set(): break
            try:
                result_msg = future.result()
                if result_msg:
                    count += 1
                    current_total = finished_count_start + count
                    if "No Data" in result_msg:
                         log(f"[{current_total}/{total}] {result_msg}")
                    else:
                         log(f"[{current_total}/{total}] {result_msg}")
                    if count % 10 == 0:
                        with CSV_LOCK: f_out.flush()
            except Exception as e:
                pass

    except KeyboardInterrupt: 
        log("Stopping Scraper")
        STOP_EVENT.set()
        executor.shutdown(wait=False, cancel_futures=True)
        global HAS_ERROR
        HAS_ERROR = True
    finally:
        f_out.close()
        save_log_if_error()
        log("Done (holding/WM)")

if __name__ == "__main__":
    main()