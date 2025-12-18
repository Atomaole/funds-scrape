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
from webdriver_manager.firefox import GeckoDriverManager

script_dir = os.path.dirname(os.path.abspath(__file__))
INPUT_FILENAME = os.path.join(script_dir, "wealthmagik_fund_list.csv")
OUTPUT_FILENAME = os.path.join(script_dir, "wealthmagik_holdings.csv")
HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 3
LOG_BUFFER = []
HAS_ERROR = False

THAI_MONTH_MAP = {
    "ม.ค.": 1, "มกราคม": 1, "JAN": 1, "ก.พ.": 2, "กุมภาพันธ์": 2, "FEB": 2,
    "มี.ค.": 3, "มีนาคม": 3, "MAR": 3, "เม.ย.": 4, "เมษายน": 4, "APR": 4,
    "พ.ค.": 5, "พฤษภาคม": 5, "MAY": 5, "มิ.ย.": 6, "มิถุนายน": 6, "JUN": 6,
    "ก.ค.": 7, "กรกฎาคม": 7, "JUL": 7, "ส.ค.": 8, "สิงหาคม": 8, "AUG": 8,
    "ก.ย.": 9, "กันยายน": 9, "SEP": 9, "ต.ค.": 10, "ตุลาคม": 10, "OCT": 10,
    "พ.ย.": 11, "พฤศจิกายน": 11, "NOV": 11, "ธ.ค.": 12, "ธันวาคม": 12, "DEC": 12,
}

def polite_sleep():
    time.sleep(random.uniform(0.5, 1))

def log(msg):
    global HAS_ERROR
    if "error" in msg.lower() or "failed" in msg.lower():
        HAS_ERROR = True
    timestamp = time.strftime('%H:%M:%S')
    formatted_msg = f"[{timestamp}] {msg}"
    print(formatted_msg)
    LOG_BUFFER.append(formatted_msg)

def save_log_if_error():
    if not HAS_ERROR:
        return
    try:
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_script_dir)
        log_dir = os.path.join(project_root, "Logs")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        script_name = os.path.basename(__file__).replace(".py", "")
        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"{script_name}_{date_str}.log"
        file_path = os.path.join(log_dir, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_BUFFER))
        print(f"Error detected. Log saved at: {file_path}")
    except Exception as e:
        print(f"Cannot save log file: {e}")

def make_driver():
    options = webdriver.FirefoxOptions()
    if HEADLESS: options.add_argument("-headless")
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_script_dir)
    driver_path = os.path.join(project_root, "geckodriver")
    return webdriver.Firefox(service=Service(driver_path), options=options)

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def parse_thai_date(text):
    if not text: return ""
    text = re.sub(r"(ข้อมูล\s*ณ\s*วันที่|ณ\s*วันที่|as of)", "", text, flags=re.IGNORECASE).strip()
    match = re.search(r"(\d{1,2})\s+([^\s\d]+)\s+(\d{2,4})", text)
    if match:
        d_str, m_str, y_str = match.groups()
        month_num = THAI_MONTH_MAP.get(m_str.strip(), 0)
        if month_num == 0: return text 
        try:
            day = int(d_str)
            year = int(y_str)
            if year < 100: year += 1957
            elif year > 2400: year -= 543
            return datetime(year, month_num, day).strftime("%d-%m-%Y")
        except: pass
    return text

def clean_deleted_funds(output_filename, valid_fund_codes):
    if not os.path.exists(output_filename): return
    rows_to_keep = []
    deleted_count = 0
    try:
        with open(output_filename, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            if not fieldnames: return
            for row in reader:
                if row.get("fund_code", "").strip() in valid_fund_codes:
                    rows_to_keep.append(row)
                else: deleted_count += 1
        if deleted_count > 0:
            log(f"Cleaning {os.path.basename(output_filename)}: Removed {deleted_count}")
            with open(output_filename, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows_to_keep)
    except: pass

def close_ad(driver):
    try:
        WebDriverWait(driver, 1).until(EC.element_to_be_clickable((By.ID, "popupAdsClose"))).click()
    except: pass

def scrape_holdings(driver, fund_code, profile_url):
    port_url = re.sub(r"/profile/?$", "/port", profile_url)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(port_url)
            close_ad(driver)
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "fund-port-info")))
            except: pass 

            results = []
            as_of_date = ""
            try:
                date_el = driver.find_element(By.CSS_SELECTOR, ".date-detail-text")
                as_of_date = parse_thai_date(clean_text(date_el.text))
            except: pass
            rows = driver.find_elements(By.CSS_SELECTOR, ".portallocation-list")
            for row in rows:
                try:
                    name = clean_text(row.find_element(By.CSS_SELECTOR, ".name-text").text)
                    weight = clean_text(row.find_element(By.CSS_SELECTOR, ".ratio-text").text).replace("%", "")
                    if name and weight:
                        results.append({
                            "fund_code": fund_code,
                            "type": "holding",
                            "name": name,
                            "percent": weight,
                            "as_of_date": as_of_date,
                            "source_url": port_url
                        })
                except: continue
            if results: return results
            if driver.find_elements(By.CLASS_NAME, "emptyData"): return []
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
        except Exception as e:
            log(f"Error {fund_code}: {e}")
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
    return []

def main():
    driver = make_driver()
    funds = []
    valid_codes = set()
    try:
        with open(INPUT_FILENAME, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                funds.append(row)
                valid_codes.add(unquote(row.get("fund_code", "")).strip())
    except: return

    clean_deleted_funds(OUTPUT_FILENAME, valid_codes)

    existing = set()
    if os.path.exists(OUTPUT_FILENAME):
        try:
            with open(OUTPUT_FILENAME, "r", encoding="utf-8-sig") as f:
                for row in csv.DictReader(f): existing.add(row.get("fund_code", "").strip())
        except: pass

    mode = 'a' if os.path.exists(OUTPUT_FILENAME) else 'w'
    f_out = open(OUTPUT_FILENAME, mode, newline="", encoding="utf-8-sig")
    
    try:
        keys = ["fund_code", "type", "name", "percent", "as_of_date", "source_url"]
        writer = csv.DictWriter(f_out, fieldnames=keys)
        if mode == 'w': writer.writeheader()
        
        total = len(funds)
        log(f"Start Scraping Holdings: {total}")
        
        for i, fund in enumerate(funds, 1):
            code = unquote(fund.get("fund_code", "")).strip()
            url = fund.get("url", "")
            if not code or not url: continue
            if code in existing: continue
            
            log(f"[{i}/{total}] {code} (holding/port)")
            data = scrape_holdings(driver, code, url)
            if data:
                writer.writerows(data)
                f_out.flush()
            polite_sleep()

    except KeyboardInterrupt: log("Stop")
    finally:
        f_out.close()
        if driver: driver.quit()
        save_log_if_error()
        log("Done")

if __name__ == "__main__":
    main()