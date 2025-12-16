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
from selenium.common.exceptions import TimeoutException

script_dir = os.path.dirname(os.path.abspath(__file__))
INPUT_FILENAME = os.path.join(script_dir, "wealthmagik_fund_list.csv")
OUTPUT_FILENAME = os.path.join(script_dir, "wealthmagik_holdings.csv")
HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 3

THAI_MONTH_MAP = {
    "ม.ค.": 1, "มกราคม": 1, "JAN": 1,
    "ก.พ.": 2, "กุมภาพันธ์": 2, "FEB": 2,
    "มี.ค.": 3, "มีนาคม": 3, "MAR": 3,
    "เม.ย.": 4, "เมษายน": 4, "APR": 4,
    "พ.ค.": 5, "พฤษภาคม": 5, "MAY": 5,
    "มิ.ย.": 6, "มิถุนายน": 6, "JUN": 6,
    "ก.ค.": 7, "กรกฎาคม": 7, "JUL": 7,
    "ส.ค.": 8, "สิงหาคม": 8, "AUG": 8,
    "ก.ย.": 9, "กันยายน": 9, "SEP": 9,
    "ต.ค.": 10, "ตุลาคม": 10, "OCT": 10,
    "พ.ย.": 11, "พฤศจิกายน": 11, "NOV": 11,
    "ธ.ค.": 12, "ธันวาคม": 12, "DEC": 12,
}

def polite_sleep():
    t = random.uniform(0.5, 1) 
    time.sleep(t)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def make_driver():
    options = webdriver.FirefoxOptions()
    if HEADLESS:
        options.add_argument("-headless")
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
        except ValueError: pass
    match2 = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", text)
    if match2:
        try:
            day, month, year = map(int, match2.groups())
            if year > 2400: year -= 543
            return datetime(year, month, day).strftime("%d-%m-%Y")
        except ValueError: pass
    return text

def close_ad_if_present(driver):
    try:
        WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.ID, "popupAdsClose"))
        ).click()
        time.sleep(0.5)
    except: pass

def scrape_holdings(driver, fund_code, profile_url):
    port_url = re.sub(r"/profile/?$", "/port", profile_url)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(port_url)
            close_ad_if_present(driver)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".portallocation-list, .emptyData"))
                )
            except:
                if attempt < MAX_RETRIES:
                    raise Exception("Element not found (Timeout)")
                else:
                    return []
            if driver.find_elements(By.CSS_SELECTOR, ".emptyData"):
                return []
            holdings_data = []
            as_of_date = ""
            try:
                date_el = driver.find_element(By.CSS_SELECTOR, ".date-detail-text")
                raw_date = clean_text(date_el.text)
                as_of_date = parse_thai_date(raw_date)
            except Exception: 
                pass
            rows = driver.find_elements(By.CSS_SELECTOR, ".portallocation-list")
            for row in rows:
                try:
                    name_el = row.find_element(By.CSS_SELECTOR, ".name-text")
                    name = clean_text(name_el.text)
                    weight_el = row.find_element(By.CSS_SELECTOR, ".ratio-text")
                    weight = clean_text(weight_el.text).replace("%", "")
                    if name and weight:
                        holdings_data.append({
                            "fund_code": fund_code,
                            "holding_name": name,
                            "percent": weight,
                            "as_of_date": as_of_date,
                            "source_url": port_url
                        })
                except:
                    continue
            return holdings_data
        except Exception as e:
            log(f"Error {fund_code} (Attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                log(f"Failed {fund_code}")
    return []

def main():
    driver = make_driver()
    all_holdings = []
    
    try:
        funds_to_scrape = []
        try:
            with open(INPUT_FILENAME, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    funds_to_scrape.append(row)
        except FileNotFoundError:
            log(f"not found {INPUT_FILENAME}")
            return
        total_funds = len(funds_to_scrape)
        log(f"start scrape ({total_funds})")
        for i, fund in enumerate(funds_to_scrape, 1):
            code = unquote(fund.get("fund_code", "")).strip()
            url = fund.get("url", "")
            if not code or not url: continue
            log(f"[{i}/{total_funds}]{code} (holding)")
            data = scrape_holdings(driver, code, url)
            if data:
                all_holdings.extend(data)
            polite_sleep() 

    except KeyboardInterrupt:
        log("Stop")
    except Exception as e:
        log(f"Error: {e}")
    finally:
        if all_holdings:
            log(f"saving {len(all_holdings)} to {OUTPUT_FILENAME}")
            keys = ["fund_code", "holding_name", "percent", "as_of_date", "source_url"]
            with open(OUTPUT_FILENAME, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                writer.writerows(all_holdings)
            log("done")
        else:
            log("Error or No Data")
        if driver:
            try:
                driver.quit()
                log("Closing Browser")
            except Exception:
                pass
if __name__ == "__main__":
    main()