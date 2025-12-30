import csv
import time
import re
import os
import random
import threading
from urllib.parse import quote, unquote
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options

# CONFIG
script_dir = os.path.dirname(os.path.abspath(__file__))
current_date_str = datetime.now().strftime("%Y-%m-%d")
INPUT_FILE = os.path.join(script_dir, "finnomena", "raw_data", "finnomena_fund_list.csv")
OUTPUT_DIR = os.path.join(script_dir, "merged_output")
if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
OUTPUT_FILENAME = os.path.join(OUTPUT_DIR, "all_sec_fund_info.csv")
RESUME_FILE = os.path.join(script_dir, "scrape_sec_resume.log")
HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 2
LOG_BUFFER = []
HAS_ERROR = False
CSV_LOCK = threading.Lock()
LOG_LOCK = threading.Lock()
COUNT_LOCK = threading.Lock()
STOP_EVENT = threading.Event()
NUM_WORKERS = 3  # Number of threads. Don't set more than 3 to avoid ban
PROCESSED_COUNT = 0

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
        log_dir = os.path.join(script_dir, "Logs")
        if not os.path.exists(log_dir): os.makedirs(log_dir)
        filename = f"scrape_sec_{datetime.now().strftime('%Y-%m-%d')}.log"
        with open(os.path.join(log_dir, filename), "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_BUFFER))
    except Exception as e:
        print(f"Cannot save log file: {e}")

def get_resume_state():
    if not os.path.exists(RESUME_FILE): return set()
    finished = set()
    try:
        with open(RESUME_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        if not lines: return set()
        first_line_parts = lines[0].strip().split('|')
        if len(first_line_parts) >= 2:
            saved_date = first_line_parts[1]
            if saved_date != current_date_str:
                log(f"Resume file date ({saved_date}) mismatch. Starting new")
                try: 
                    os.remove(RESUME_FILE) 
                except: pass
                return set()
        for line in lines:
            parts = line.strip().split('|')
            if len(parts) >= 1: finished.add(parts[0])
        log(f"Resuming Found {len(finished)} funds already done")
        return finished
    except Exception as e:
        log(f"Error reading resume file: {e}. Starting fresh")
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

def polite_sleep():
    time.sleep(random.uniform(0.3, 0.7))

def clean_text(text):
    if not text: return ""
    cleaned = re.sub(r'\s+', ' ', text).strip()
    if cleaned == "-": return ""
    return cleaned

def clean_number(text):
    if not text: return ""
    text = re.sub(r'[%,]', '', text)
    text = re.sub(r'\s+', '', text)
    if text == "-" or text == "N/A": return ""
    return text

def parse_recovering_period(text):
    if not text or text == "-" or text == "N/A": return ""
    text_clean = text.replace(" ", "")
    total_days = 0
    found_match = False
    
    match_year = re.search(r'(\d+)ปี', text_clean)
    if match_year:
        total_days += int(match_year.group(1)) * 365
        found_match = True
        
    match_month = re.search(r'(\d+)เดือน', text_clean)
    if match_month:
        total_days += int(match_month.group(1)) * 30
        found_match = True
        
    match_day = re.search(r'(\d+)วัน', text_clean)
    if match_day:
        total_days += int(match_day.group(1))
        found_match = True
        
    if found_match: return str(total_days)
    if text.strip() == "-": return ""
    return text

def convert_thai_date(date_str):
    if not date_str or date_str.startswith("N/A"): return date_str
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            day, month, year_be = parts
            year_ce = int(year_be) - 543
            return f"{int(day):02d}-{int(month):02d}-{year_ce}"
    except: return date_str
    return date_str

def make_driver():
    options = Options()
    if HEADLESS: options.add_argument("-headless")
    options.page_load_strategy = 'eager'
    options.set_preference("permissions.default.image", 2)
    options.set_preference("permissions.default.stylesheet", 2)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    driver_path = os.path.join(current_script_dir, "geckodriver")
    if not os.path.exists(driver_path):
         driver_path = os.path.join(os.path.dirname(current_script_dir), "geckodriver")
    return webdriver.Firefox(service=Service(driver_path), options=options)

def scrape_sec_info(driver, fund_code):
    safe_code = quote(fund_code, safe='') 
    url = f"https://fundcheck.sec.or.th/fund-detail;funds={safe_code}"
    empty_data = {
        "fund_code": fund_code, "sec_url": url, "as_of_date": "N/A",
        "sharpe_ratio": "", "alpha": "", "beta": "",
        "max_drawdown": "", "recovering_period": "",
        "tracking_error": "", "turnover_ratio": "", "fx_hedging": ""
    }
    for attempt in range(1, MAX_RETRIES + 1):
        if STOP_EVENT.is_set(): return empty_data
        try:
            if attempt > 1:
                try:
                    driver.delete_all_cookies()
                    driver.get("about:blank")
                    time.sleep(2)
                except: pass
            driver.get(url)
            wait = WebDriverWait(driver, 10)
            try: wait.until(EC.presence_of_element_located((By.CLASS_NAME, "card-body")))
            except:
                if attempt < MAX_RETRIES: 
                    continue
                else: return empty_data
            data = empty_data.copy()
            whole_page_text = driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r"ข้อมูล ณ วันที่.*?(\d{1,2}/\d{1,2}/\d{4})", whole_page_text)
            if match: data["as_of_date"] = convert_thai_date(match.group(1))
            else: data["as_of_date"] = "N/A"
            id_map = {
                "sharpe_ratio": "sharpe-ratio", "alpha": "alpha", "beta": "beta",
                "tracking_error": "tracking-error", "max_drawdown": "max-drawdown",
                "recovering_period": "recovering-period", "turnover_ratio": "turnover-ratio"
            }
            for field, html_id in id_map.items():
                try:
                    val = driver.execute_script(
                        f"return document.getElementById('{html_id}')?.nextElementSibling?.textContent"
                    )
                    if field == "recovering_period": 
                        data[field] = parse_recovering_period(clean_text(val))
                    else: 
                        data[field] = clean_number(val)
                except: data[field] = "" 
            try:
                fx_xpath = "//div[contains(text(), 'FX Hedging')]/following-sibling::div//div[contains(@class, 'progress-bar')]"
                fx_el = driver.find_element(By.XPATH, fx_xpath)
                data["fx_hedging"] = clean_number(fx_el.get_attribute("textContent"))
            except: data["fx_hedging"] = ""
            return data
            
        except Exception as e:
            log(f"Error {fund_code} (Attempt {attempt}): {e}")
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
    return empty_data

def get_finnomena_funds(filepath):
    unique_codes = set()
    if not os.path.exists(filepath):
        log(f"Warning File not found {filepath}")
        return []
    log(f"Reading MASTER list from {os.path.basename(filepath)}")
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_code = row.get("fund_code", "")
            if not raw_code: continue
            clean_code = unquote(raw_code).strip()
            if clean_code: unique_codes.add(clean_code)
    sorted_list = sorted(list(unique_codes))
    log(f"Total funds found in list: {len(sorted_list)}")
    return sorted_list

def process_batch(thread_id, fund_list, fieldnames, total_all_funds, finished_count_start):
    global PROCESSED_COUNT
    driver = None
    try:
        driver = make_driver()
        for i, code in enumerate(fund_list, 1):
            if STOP_EVENT.is_set(): break
            try:
                info = scrape_sec_info(driver, code)
                if STOP_EVENT.is_set(): break
                current_total_done = 0
                with COUNT_LOCK:
                    PROCESSED_COUNT += 1
                    current_total_done = finished_count_start + PROCESSED_COUNT
                with CSV_LOCK:
                    with open(OUTPUT_FILENAME, 'a', newline="", encoding="utf-8-sig") as f:
                         writer = csv.DictWriter(f, fieldnames=fieldnames)
                         writer.writerow(info)
                log(f"[{current_total_done}/{total_all_funds}] {code} (sec info)")
                append_resume_state(code)
                polite_sleep()
                
            except Exception as e:
                current_total_done = 0
                with COUNT_LOCK:
                    if 'current_total_done' not in locals() or current_total_done == 0:
                        PROCESSED_COUNT += 1
                        current_total_done = finished_count_start + PROCESSED_COUNT
                log(f"[{current_total_done}/{total_all_funds}] ERROR {code}: {e}")

    except Exception as e:
        if not STOP_EVENT.is_set():
             log(f"Thread-{thread_id} Crashed: {e}")
    finally:
        if driver:
            driver.quit()

def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n)]

def main():
    finished_funds = get_resume_state()
    all_funds = get_finnomena_funds(INPUT_FILE)
    if not all_funds:
        log("No funds found in input file")
        return

    headers = [
        "fund_code", "as_of_date", 
        "sharpe_ratio", "alpha", "beta", 
        "max_drawdown", "recovering_period", 
        "tracking_error", "turnover_ratio", "fx_hedging",
        "sec_url"
    ]
    if not os.path.exists(OUTPUT_FILENAME) or os.path.getsize(OUTPUT_FILENAME) == 0:
        with open(OUTPUT_FILENAME, 'w', newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
    pending_funds = [code for code in all_funds if code not in finished_funds]
    total_all_funds = len(all_funds)
    finished_count_start = len(finished_funds)
    remaining = len(pending_funds)
    log(f"Total: {total_all_funds}, Finished: {finished_count_start}, Remaining: {remaining}")
    if remaining == 0:
        log("All done")
        return
    batches = split_list(pending_funds, NUM_WORKERS)
    batches = [b for b in batches if len(b) > 0]
    log(f"Starting Scraper {len(batches)}")
    global PROCESSED_COUNT
    PROCESSED_COUNT = 0 
    executor = ThreadPoolExecutor(max_workers=len(batches))
    futures = []
    try:
        for i, batch in enumerate(batches):
            futures.append(executor.submit(process_batch, i+1, batch, headers, total_all_funds, finished_count_start))
        for future in as_completed(futures):
            try: future.result()
            except Exception: pass
            
    except KeyboardInterrupt:
        log("\nStopping Scraper")
        STOP_EVENT.set()
        executor.shutdown(wait=False, cancel_futures=True)
        global HAS_ERROR
        HAS_ERROR = True
    finally:
        save_log_if_error()
        log("Done")

if __name__ == "__main__":
    main()