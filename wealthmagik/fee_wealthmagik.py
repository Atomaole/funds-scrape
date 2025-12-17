import csv
import time
import re
import os
import random
from urllib.parse import unquote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

script_dir = os.path.dirname(os.path.abspath(__file__))
INPUT_FILENAME = os.path.join(script_dir, "wealthmagik_fund_list.csv")
OUTPUT_FILENAME = os.path.join(script_dir, "wealthmagik_fees.csv")
HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 3

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def polite_sleep():
    t = random.uniform(0.5, 1) 
    time.sleep(t)
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
    if not text or text == "-" or "N/A" in text.upper():
        return ""
    text = text.replace("%", "").replace(",", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    return match.group(0) if match else ""

def get_text_by_id(driver, element_id):
    try:
        el = driver.find_element(By.ID, element_id)
        return clean_text(el.text)
    except:
        return ""

def close_ad_if_present(driver):
    try:
        WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.ID, "popupAdsClose"))
        ).click()
        time.sleep(0.5)
    except: pass

def scrape_fees(driver, fund_code, profile_url):
    base_url = re.sub(r"/(profile|port|risk)/?$", "", profile_url)
    fee_url = base_url + "/fee"
    empty_data = {
        "fund_code": fund_code,
        "source_url": fee_url,
        "initial_purchase": "",
        "additional_purchase": "",
        "front_end_max": "", "front_end_actual": "",
        "back_end_max": "", "back_end_actual": "",
        "switching_in_max": "", "switching_in_actual": "",
        "switching_out_max": "", "switching_out_actual": "",
        "management_max": "", "management_actual": "",
        "ter_max": "", "ter_actual": "" 
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(fee_url)
            close_ad_if_present(driver)
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "wmg.funddetailfee.text.frontEndFee-ffs"))
                )
            except:
                if attempt < MAX_RETRIES:
                    raise Exception("Fee elements not found (Timeout)")
                else:
                    return empty_data
            data = empty_data.copy()
            data["initial_purchase"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.initialPurchase-ffs"))
            data["additional_purchase"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.additionalPurchase-ffs"))

            data["front_end_max"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.frontEndFee-ffs"))
            data["front_end_actual"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.frontEndFee-actual"))

            data["back_end_max"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.backEndFee-ffs"))
            data["back_end_actual"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.backEndFee-actual"))

            data["switching_in_max"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.switchingInFee-ffs"))
            data["switching_in_actual"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.switchingInFee-actual"))

            data["switching_out_max"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.switchingOutFee-ffs"))
            data["switching_out_actual"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.switchingOutFee-actual"))

            data["management_max"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.managementFee-ffs"))
            data["management_actual"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.managementFee-actual"))

            data["ter_max"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.totalExpenseRatio-ffs"))
            data["ter_actual"] = parse_percent(get_text_by_id(driver, "wmg.funddetailfee.text.totalExpenseRatioActual-ffs"))

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
        log(f"can't find {INPUT_FILENAME}")
        return
    clean_deleted_funds(OUTPUT_FILENAME, valid_codes)
    existing_codes = set()
    if os.path.exists(OUTPUT_FILENAME):
        try:
            with open(OUTPUT_FILENAME, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_codes.add(row.get("fund_code", "").strip())
            log(f"Found {len(existing_codes)} existing records. Skipping them.")
        except Exception as e:
            log(f"Error reading existing file: {e}")
    file_exists = os.path.exists(OUTPUT_FILENAME)
    mode = 'a' if file_exists else 'w'
    f_out = open(OUTPUT_FILENAME, mode, newline="", encoding="utf-8-sig")
    
    try:
        headers = [
            "fund_code", "source_url",
            "initial_purchase", "additional_purchase",
            "front_end_max", "front_end_actual",
            "back_end_max", "back_end_actual",
            "switching_in_max", "switching_in_actual",
            "switching_out_max", "switching_out_actual",
            "management_max", "management_actual",
            "ter_max", "ter_actual"
        ]
        writer = csv.DictWriter(f_out, fieldnames=headers)
        if not file_exists:
            writer.writeheader()
        funds_to_scrape = []
        try:
            with open(INPUT_FILENAME, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    funds_to_scrape.append(row)
        except FileNotFoundError:
            log(f"can't find {INPUT_FILENAME}")
            return
            
        total_funds = len(funds_to_scrape)
        log(f"starting ({total_funds})")
        
        for i, fund in enumerate(funds_to_scrape, 1):
            code = unquote(fund.get("fund_code", "")).strip()
            url = fund.get("url", "")
            if not code or not url: continue
            if code in existing_codes:
                continue
            log(f"[{i}/{total_funds}] {code} (fee/magik)")
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
            try:
                driver.quit()
                log("Closing Browser")
            except Exception:
                pass

if __name__ == "__main__":
    main()