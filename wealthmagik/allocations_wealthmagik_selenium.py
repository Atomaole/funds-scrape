import csv
import time
import re
import os
import random
import threading
from urllib.parse import unquote
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
root = os.path.dirname(script_dir)
current_date_str = datetime.now().strftime("%Y-%m-%d")
RAW_DATA_DIR = os.path.join(script_dir, "raw_data")
if not os.path.exists(RAW_DATA_DIR): os.makedirs(RAW_DATA_DIR)
INPUT_FILENAME = os.path.join(RAW_DATA_DIR, "wealthmagik_fund_list.csv")
OUTPUT_FILENAME = os.path.join(RAW_DATA_DIR, "wealthmagik_allocations.csv")
RESUME_FILE = os.path.join(script_dir, "allocations_resume.log")
HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 2
LOG_BUFFER = []
HAS_ERROR = False
CSV_LOCK = threading.Lock()
LOG_LOCK = threading.Lock()
COUNT_LOCK = threading.Lock()
STOP_EVENT = threading.Event()
NUM_WORKERS = 3
PROCESSED_COUNT = 0

THAI_MONTH_MAP = {
    "ม.ค.": 1, "มกราคม": 1, "JAN": 1, "ก.พ.": 2, "กุมภาพันธ์": 2, "FEB": 2,
    "มี.ค.": 3, "มีนาคม": 3, "MAR": 3, "เม.ย.": 4, "เมษายน": 4, "APR": 4,
    "พ.ค.": 5, "พฤษภาคม": 5, "MAY": 5, "มิ.ย.": 6, "มิถุนายน": 6, "JUN": 6,
    "ก.ค.": 7, "กรกฎาคม": 7, "JUL": 7, "ส.ค.": 8, "สิงหาคม": 8, "AUG": 8,
    "ก.ย.": 9, "กันยายน": 9, "SEP": 9, "ต.ค.": 10, "ตุลาคม": 10, "OCT": 10,
    "พ.ย.": 11, "พฤศจิกายน": 11, "NOV": 11, "ธ.ค.": 12, "ธันวาคม": 12, "DEC": 12,
}

def polite_sleep():
    time.sleep(random.uniform(1.0, 2.0))

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
        filename = f"alloc_wm_selenium_{datetime.now().strftime('%Y-%m-%d')}.log"
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
        if len(first_line_parts) < 2 or first_line_parts[1] != current_date_str:
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
            current_time = datetime.now().strftime("%H:%M:%S")
            with open(RESUME_FILE, 'a', encoding='utf-8') as f:
                f.write(f"{code}|{current_date_str}|{current_time}\n")
        except: pass

def make_driver():
    options = Options()
    if HEADLESS: options.add_argument("-headless")
    options.page_load_strategy = 'eager'
    options.set_preference("permissions.default.image", 2)
    options.set_preference("permissions.default.stylesheet", 2)
    options.set_preference("dom.webnotifications.enabled", False)
    driver_path = os.path.join(root, "geckodriver")
    if not os.path.exists(driver_path):
         driver_path = os.path.join(script_dir, "geckodriver")
    return webdriver.Firefox(service=Service(driver_path), options=options)

def close_ad_if_present(driver):
    try: 
        WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.ID, "popupAdsClose"))).click()
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

def scrape_section_selenium(driver, container_class, data_type, fund_code, url):
    results = []
    try:
        containers = driver.find_elements(By.CLASS_NAME, container_class)
        if not containers: return []
        container = containers[0]
        as_of_date = ""
        try:
            date_el = container.find_element(By.CLASS_NAME, "asofdate")
            as_of_date = parse_thai_date(clean_text(date_el.text))
        except: pass
        rows = container.find_elements(By.CSS_SELECTOR, "tr.mat-row")
        for row in rows:
            try:
                name_el = row.find_element(By.CSS_SELECTOR, ".cdk-column-name")
                percent_el = row.find_element(By.CSS_SELECTOR, ".cdk-column-ratio")
                name = clean_text(name_el.text)
                percent = clean_text(percent_el.text).replace("%", "")
                if name and percent:
                    results.append({
                        "fund_code": fund_code, 
                        "type": data_type, 
                        "name": name, 
                        "percent": percent, 
                        "as_of_date": as_of_date, 
                        "source_url": url
                    })
            except: continue
    except: pass
    return results

def scrape_allocations(driver, fund_code, profile_url):
    alloc_url = re.sub(r"/profile/?$", "/allocation", profile_url)
    all_data = []
    for attempt in range(1, MAX_RETRIES + 1):
        if STOP_EVENT.is_set(): return None
        try:
            driver.get(alloc_url)
            close_ad_if_present(driver)
            try:
                WebDriverWait(driver, 10).until(
                    EC.any_of(
                        EC.presence_of_element_located((By.CLASS_NAME, "investmentAllocationByAsset")),
                        EC.presence_of_element_located((By.CLASS_NAME, "investmentAllocationByCountry")),
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".emptyData")),
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".fundName"))
                    )
                )
            except:
                if attempt < MAX_RETRIES: continue
                else: return None
            data_asset = scrape_section_selenium(driver, "investmentAllocationByAsset", "asset_alloc", fund_code, alloc_url)
            all_data.extend(data_asset)
            data_country = scrape_section_selenium(driver, "investmentAllocationByCountry", "country_alloc", fund_code, alloc_url)
            all_data.extend(data_country)
            if all_data:
                return all_data
            else:
                if driver.find_elements(By.CSS_SELECTOR, ".emptyData") or driver.find_elements(By.CSS_SELECTOR, ".fundName"):
                    return []
                raise Exception("No data tables found")
        except Exception as e:
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
    return None

def process_batch(thread_id, fund_list, fieldnames, total_all_funds, finished_count_start):
    global PROCESSED_COUNT
    driver = None
    try:
        driver = make_driver()
        for i, fund in enumerate(fund_list, 1):
            if STOP_EVENT.is_set(): break
            code = unquote(fund.get("fund_code", "")).strip()
            url = fund.get("url", "")
            try:
                data_list = scrape_allocations(driver, code, url)
                if STOP_EVENT.is_set(): break
                current_total_done = 0
                with COUNT_LOCK:
                    PROCESSED_COUNT += 1
                    current_total_done = finished_count_start + PROCESSED_COUNT
                if data_list is not None:
                    if data_list:
                        with CSV_LOCK:
                            with open(OUTPUT_FILENAME, 'a', newline="", encoding="utf-8-sig") as f_out:
                                writer = csv.DictWriter(f_out, fieldnames=fieldnames)
                                writer.writerows(data_list)
                        log(f"[{current_total_done}/{total_all_funds}] {code} (alloc/wm-selenium)")
                    else:
                        log(f"[{current_total_done}/{total_all_funds}] {code} - No Data")
                    append_resume_state(code)
                else:
                    log(f"[{current_total_done}/{total_all_funds}] FAILED {code} (Max retries)")
                polite_sleep()
            except Exception as e:
                log(f"ERROR processing {code}: {e}")
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
    log("Starting Wealthmagik Allocations (SELENIUM)")
    finished_funds = get_resume_state()
    funds = []
    try:
        with open(INPUT_FILENAME, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f): funds.append(row)
    except: 
        log(f"Input file not found: {INPUT_FILENAME}")
        return
    pending_funds = [f for f in funds if unquote(f.get("fund_code", "")).strip() not in finished_funds]
    total_all_funds = len(funds)
    current_fund_codes = {unquote(f.get("fund_code", "")).strip() for f in funds}
    finished_count_start = len(finished_funds.intersection(current_fund_codes))
    remaining = len(pending_funds)
    log(f"Total Funds: {total_all_funds}")
    log(f"Finished (from Logs): {finished_count_start}")
    log(f"Remaining for Selenium: {remaining}")
    if remaining == 0:
        log("All done Nothing to scrape")
        return
    fieldnames = ["fund_code", "type", "name", "percent", "as_of_date", "source_url"]
    if not os.path.exists(OUTPUT_FILENAME) or os.path.getsize(OUTPUT_FILENAME) == 0:
         with open(OUTPUT_FILENAME, 'w', newline="", encoding="utf-8-sig") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
    batches = split_list(pending_funds, NUM_WORKERS)
    batches = [b for b in batches if len(b) > 0]
    log(f"Launching {len(batches)} browser threads (allocations/WM)")
    global PROCESSED_COUNT
    PROCESSED_COUNT = 0 
    executor = ThreadPoolExecutor(max_workers=len(batches))
    futures = []
    try:
        for i, batch in enumerate(batches):
            futures.append(executor.submit(process_batch, i+1, batch, fieldnames, total_all_funds, finished_count_start))
        for future in as_completed(futures):
            try: future.result()
            except Exception as e: pass 
    except KeyboardInterrupt:
        log("Stopping Scraper")
        STOP_EVENT.set()
        executor.shutdown(wait=False, cancel_futures=True)
        global HAS_ERROR
        HAS_ERROR = True
    finally:
        save_log_if_error()
        log("Selenium Scraper Finished (allocations/WM)")

if __name__ == "__main__":
    main()