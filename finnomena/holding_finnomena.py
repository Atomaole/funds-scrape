import os
import csv
import time
import re
import random
from selenium import webdriver
from datetime import datetime
from urllib.parse import unquote
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

script_dir = os.path.dirname(os.path.abspath(__file__))
INPUT_FILENAME = os.path.join(script_dir, "finnomena_fund_list.csv") 
OUTPUT_FILENAME = os.path.join(script_dir, "finnomena_holdings.csv")
HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 3

def polite_sleep():
    t = random.uniform(0.5, 1) 
    time.sleep(t)

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
        except ValueError:
            pass
    return text

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

def clean_deleted_funds(output_filename, valid_fund_codes):
    if not os.path.exists(output_filename):
        return
    rows_to_keep = []
    fieldnames = []
    deleted_count = 0
    try:
        with open(output_filename, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            if not fieldnames: return
            for row in reader:
                code = row.get("fund_code", "").strip()
                if code in valid_fund_codes:
                    rows_to_keep.append(row)
                else:
                    deleted_count += 1
        if deleted_count > 0:
            log(f"Cleaning {os.path.basename(output_filename)}: Removed {deleted_count}")
            with open(output_filename, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows_to_keep)
    except Exception as e:
        log(f"Error cleaning file {output_filename}: {e}")

def scrape_pie_chart(driver, section_id, data_type, fund_code, url):
    results = []
    try:
        try:
            section = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.ID, section_id))
            )
        except:
            return []
        as_of_date = ""
        try:
            date_el = section.find_element(By.CSS_SELECTOR, ".data-date")
            raw_date = clean_text(date_el.text)
            as_of_date = parse_thai_date(raw_date)
        except: pass
        items = section.find_elements(By.CSS_SELECTOR, ".top-holding-item")
        
        for item in items:
            try:
                name_el = item.find_element(By.CSS_SELECTOR, ".title")
                name = clean_text(name_el.text)
                percent_el = item.find_element(By.CSS_SELECTOR, ".percent")
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
            except:
                continue
    except Exception as e:
        pass
        
    return results

def scrape_holdings(driver, fund_code, base_url):
    port_url = base_url.rstrip("/") + "/portfolio"
    all_data = []

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(port_url)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "fund-portfolio"))
                )
            except:
                pass
            try:
                section_top5 = driver.find_element(By.ID, "section-top-5-holding")
                as_of_date_top5 = ""
                try:
                    date_el = section_top5.find_element(By.CSS_SELECTOR, ".data-date")
                    as_of_date_top5 = parse_thai_date(clean_text(date_el.text))
                except: pass
                
                items = section_top5.find_elements(By.CSS_SELECTOR, ".top-holding-item")
                for item in items:
                    try:
                        name = clean_text(item.find_element(By.CSS_SELECTOR, ".title").text)
                        percent = clean_text(item.find_element(By.CSS_SELECTOR, ".percent").text).replace("%", "")
                        if name and percent:
                            all_data.append({
                                "fund_code": fund_code,
                                "type": "holding",
                                "name": name,
                                "percent": percent,
                                "as_of_date": as_of_date_top5,
                                "source_url": port_url
                            })
                    except: continue
            except: 
                pass

            assets = scrape_pie_chart(driver, "section-allocation-chart", "asset_alloc", fund_code, port_url)
            all_data.extend(assets)
            sectors = scrape_pie_chart(driver, "section-stock-allocation", "sector_alloc", fund_code, port_url)
            all_data.extend(sectors)
            if all_data:
                return all_data
            if attempt < MAX_RETRIES:
                 raise Exception("No data found, retrying...")

        except Exception as e:
            log(f"Error {fund_code} (Attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                log(f"Failed {fund_code} after {MAX_RETRIES} attempts.")
    
    return []

def main():
    driver = make_driver()
    funds_to_scrape = []
    valid_codes = set()
    try:
        with open(INPUT_FILENAME, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader: 
                funds_to_scrape.append(row)
                c = unquote(row.get("fund_code", "")).strip()
                if c: valid_codes.add(c)
    except FileNotFoundError:
        log("not found file list funds")
        return
    clean_deleted_funds(OUTPUT_FILENAME, valid_codes)
    existing_codes = set()
    if os.path.exists(OUTPUT_FILENAME):
        try:
            with open(OUTPUT_FILENAME, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_codes.add(row.get("fund_code", "").strip())
            log(f"Found {len(existing_codes)} existing funds")
        except Exception as e:
            log(f"Error reading existing file: {e}")
    file_exists = os.path.exists(OUTPUT_FILENAME)
    mode = 'a' if file_exists else 'w'
    f_out = open(OUTPUT_FILENAME, mode, newline="", encoding="utf-8-sig")
    
    try:
        keys = ["fund_code", "type", "name", "percent", "as_of_date", "source_url"]
        
        writer = csv.DictWriter(f_out, fieldnames=keys)
        if not file_exists:
            writer.writeheader()

        total_funds = len(funds_to_scrape)
        log(f"scrape holding + allocations {total_funds}")
        
        for i, fund in enumerate(funds_to_scrape, 1):
            code = unquote(fund.get("fund_code", "")).strip()
            url = fund.get("url", "")
            if not code or not url: continue
            if code in existing_codes:
                continue
            log(f"[{i}/{total_funds}] {code} (holding/fin)")
            data = scrape_holdings(driver, code, url)
            if data:
                writer.writerows(data)
                f_out.flush()
            
            polite_sleep() 
            
    except KeyboardInterrupt:
        log("Stop")
    except Exception as e:
        log(f"Error: {e}")
    finally:
        f_out.close()
        if driver:
            try: driver.quit()
            except: pass
        log("Done")

if __name__ == "__main__":
    main()