import csv
import time
import re
import os
import random
from urllib.parse import unquote
from datetime import datetime
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
OUTPUT_FILENAME = os.path.join(RAW_DATA_DIR, "wealthmagik_bid_offer.csv")
RESUME_FILE = os.path.join(script_dir, "bid_offer_resume.log")
HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 2
LOG_BUFFER = []
HAS_ERROR = False

def polite_sleep():
    time.sleep(random.uniform(0.3, 1))

def log(msg):
    global HAS_ERROR
    if "error" in msg.lower() or "failed" in msg.lower():
        HAS_ERROR = True
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}")
    LOG_BUFFER.append(f"[{timestamp}] {msg}")

def save_log_if_error():
    if not HAS_ERROR: return
    try:
        log_dir = os.path.join(root, "Logs")
        if not os.path.exists(log_dir): os.makedirs(log_dir)
        filename = f"bidoffer_wm_{datetime.now().strftime('%Y-%m-%d')}.log"
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
        if lines[0].strip().split('|')[-1] != current_date_str:
            log("Date mismatch. Starting new run.")
            try: os.remove(RESUME_FILE)
            except: pass
            return set()
        for line in lines:
            parts = line.strip().split('|')
            if len(parts) >= 1: finished.add(parts[0])
        log(f"Resuming Found {len(finished)} funds done")
        return finished
    except: return set()

def append_resume_state(code):
    try:
        with open(RESUME_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{code}|{current_date_str}\n")
    except: pass

def cleanup_resume_file():
    if os.path.exists(RESUME_FILE):
        try: os.remove(RESUME_FILE)
        except: pass

def make_driver():
    options = Options()
    if HEADLESS: options.add_argument("-headless")
    options.page_load_strategy = 'eager'
    options.set_preference("permissions.default.image", 2)
    options.set_preference("permissions.default.stylesheet", 2)
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    driver_path = os.path.join(root, "geckodriver")
    return webdriver.Firefox(service=Service(driver_path), options=options)

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip() if text else ""

def clean_number(text):
    if not text: return ""
    text = re.sub(r'[%,]', '', text)
    return text.strip()

def parse_wm_date(text):
    if not text: return ""
    text = clean_text(text)
    if re.match(r"^\d{8}$", text):
        try: return datetime.strptime(text, "%Y%m%d").strftime("%d-%m-%Y")
        except: pass
    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if match:
        d, m, y = map(int, match.groups())
        if y > 2400: y -= 543
        try: return datetime(y, m, d).strftime("%d-%m-%Y")
        except: pass
    return text

def get_value_from_id_attribute(driver, prefix):
    try:
        el = driver.find_element(By.CSS_SELECTOR, f"[id^='{prefix}']")
        full_id = el.get_attribute("id")
        return full_id.replace(prefix, "")
    except: return ""

def close_ad_if_present(driver):
    try: 
        WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.ID, "popupAdsClose"))).click()
    except: pass

def scrape_bid_offer(driver, fund_code, url):
    data = {
        "fund_code": fund_code,
        "nav_date": "",
        "bid_price": "",
        "offer_price": ""
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                log(f"Retry {attempt}: Resetting driver state (bid_offer)")
                try:
                    driver.delete_all_cookies()
                    driver.get("about:blank")
                    time.sleep(2)
                except: pass
            driver.get(url)
            close_ad_if_present(driver)
            try: WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".fundName h1")))
            except: 
                if attempt < MAX_RETRIES: 
                    continue 
                else: return None
            raw_nav_date = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.tnaclassDate.")
            data["nav_date"] = parse_wm_date(raw_nav_date)
            raw_bid = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.bidPrice.")
            data["bid_price"] = clean_number(raw_bid)
            raw_offer = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.offerPrice.")
            data["offer_price"] = clean_number(raw_offer)
            return data

        except Exception as e:
            log(f"Error {fund_code}: {e}")
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
            
    return None

def main():
    finished_funds = get_resume_state()
    driver = make_driver()
    funds = []
    try:
        with open(INPUT_FILENAME, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f): funds.append(row)
    except: 
        log(f"Input file not found: {INPUT_FILENAME}")
        return
    mode = 'a' if finished_funds else 'w'
    try:
        with open(OUTPUT_FILENAME, mode, newline="", encoding="utf-8-sig") as f_out:
            keys = ["fund_code", "nav_date", "bid_price", "offer_price"]
            writer = csv.DictWriter(f_out, fieldnames=keys)
            if mode == 'w': writer.writeheader()
            total = len(funds)
            log(f"Start Scraping Bid/Offer (Total: {total})")
            for i, fund in enumerate(funds, 1):
                code = unquote(fund.get("fund_code", "")).strip()
                url = fund.get("url", "")
                if not code or not url: continue
                if code in finished_funds: continue
                log(f"[{i}/{total}] {code} (bid_offer/wealthmagik)")
                data = scrape_bid_offer(driver, code, url)
                if data:
                    writer.writerow(data)
                    f_out.flush()
                append_resume_state(code)
                polite_sleep()

    except KeyboardInterrupt: 
        log("Stopped by user")
        global HAS_ERROR
        HAS_ERROR = True
    finally:
        if driver: driver.quit()
        # if not HAS_ERROR: cleanup_resume_file()
        save_log_if_error()
        log("Done")

if __name__ == "__main__":
    main()