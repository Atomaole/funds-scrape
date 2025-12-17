import csv
import time
import re
import os
import random
from selenium import webdriver
from urllib.parse import unquote
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

script_dir = os.path.dirname(os.path.abspath(__file__))
INPUT_FILENAME = os.path.join(script_dir, "finnomena_fund_list.csv")
OUTPUT_FILENAME = os.path.join(script_dir, "finnomena_fees.csv")
HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 3

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

def parse_percent(text):
    if not text or "N/A" in text or "-" == text:
        return ""
    text = text.replace("%", "").replace(",", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if match:
        return match.group(0)
    return ""

def extract_number_only(text):
    if not text: return ""
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text.replace(",", ""))
    return match.group(0) if match else ""

def extract_fee_pair(driver, keywords):
    try:
        xpath_query = "//div[contains(@class,'fin-row') and (" + " or ".join([f"contains(., '{k}')" for k in keywords]) + ")]"
        row = driver.find_element(By.XPATH, xpath_query)
        fee_elements = row.find_elements(By.CSS_SELECTOR, ".fee-text")
        values = [parse_percent(el.text) for el in fee_elements if parse_percent(el.text)]
        
        max_fee = ""
        actual_fee = ""
        
        if len(values) >= 2:
            max_fee = values[0]
            actual_fee = values[1]
        elif len(values) == 1:
            max_fee = values[0]
            
        return max_fee, actual_fee

    except Exception:
        return "", ""

def extract_buying_min(driver):
    initial_buy = ""
    next_buy = ""
    try:
        elements = driver.find_elements(By.CSS_SELECTOR, ".p-buying.buying-total")
        
        if len(elements) >= 2:
            initial_buy = extract_number_only(elements[0].text)
            next_buy = extract_number_only(elements[1].text)
        elif len(elements) == 1:
            initial_buy = extract_number_only(elements[0].text)
            
    except Exception:
        pass
    return initial_buy, next_buy

def scrape_fees(driver, fund_code, base_url):
    fee_url = base_url.rstrip("/") + "/fee"
    empty_data = {
        "fund_code": fund_code,
        "source_url": fee_url,
        "front_end_max": "", "front_end_actual": "",
        "back_end_max": "", "back_end_actual": "",
        "management_max": "", "management_actual": "",
        "ter_max": "", "ter_actual": "", 
        "switching_in_max": "", "switching_in_actual": "",
        "switching_out_max": "", "switching_out_actual": "",
        "min_initial_buy": "", "min_next_buy": ""
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(fee_url)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".fee-text, .p-buying"))
                )
            except:
                if attempt < MAX_RETRIES:
                    raise Exception("Fees/Buying info not loaded (Timeout)")
                else:
                    return empty_data
            data = empty_data.copy()
            data["front_end_max"], data["front_end_actual"] = extract_fee_pair(driver, ["Front-end Fee", "ค่าธรรมเนียมการขาย"])
            data["back_end_max"], data["back_end_actual"] = extract_fee_pair(driver, ["Back-end Fee", "ค่าธรรมเนียมการรับซื้อคืน"])
            data["management_max"], data["management_actual"] = extract_fee_pair(driver, ["Management Fee", "ค่าธรรมเนียมการจัดการ"])
            data["ter_max"], data["ter_actual"] = extract_fee_pair(driver, ["Total Expense Ratio", "ค่าธรรมเนียมและค่าใช้จ่ายรวม"])
            data["switching_in_max"], data["switching_in_actual"] = extract_fee_pair(driver, ["Switching-in Fee", "สับเปลี่ยนหน่วยลงทุนเข้า"])
            data["switching_out_max"], data["switching_out_actual"] = extract_fee_pair(driver, ["Switching-out Fee", "สับเปลี่ยนหน่วยลงทุนออก"])
            data["min_initial_buy"], data["min_next_buy"] = extract_buying_min(driver)
            return data

        except Exception as e:
            log(f"Error {fund_code} (Attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                log(f"Failed {fund_code}")
    return empty_data

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
            log(f"Found {len(existing_codes)} existing records")
        except Exception as e:
            log(f"Error reading existing file: {e}")
    file_exists = os.path.exists(OUTPUT_FILENAME)
    mode = 'a' if file_exists else 'w'
    f_out = open(OUTPUT_FILENAME, mode, newline="", encoding="utf-8-sig")
    try:
        headers = [
            "fund_code", "source_url",
            "front_end_max", "front_end_actual",
            "back_end_max", "back_end_actual",
            "management_max", "management_actual",
            "ter_max", "ter_actual", 
            "switching_in_max", "switching_in_actual",
            "switching_out_max", "switching_out_actual",
            "min_initial_buy", "min_next_buy"
        ]
        writer = csv.DictWriter(f_out, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        funds_to_scrape = []
        try:
            with open(INPUT_FILENAME, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader: funds_to_scrape.append(row)
        except FileNotFoundError:
            log("not found file list funds")
            return

        total_funds = len(funds_to_scrape)
        log(f"starting scrape fee {total_funds}")
        for i, fund in enumerate(funds_to_scrape, 1):
            code = unquote(fund.get("fund_code", "")).strip()
            url = fund.get("url", "")
            if not code or not url: continue
            if code in existing_codes:
                continue
            log(f"[{i}/{total_funds}] {code} (fee/fin)")
            fee_data = scrape_fees(driver, code, url)
            if fee_data:
                writer.writerow(fee_data)
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