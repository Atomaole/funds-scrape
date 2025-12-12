import csv
import time
import re
import os
import random
from urllib.parse import quote, unquote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

script_dir = os.path.dirname(os.path.abspath(__file__))

INPUT_FILES = [
    os.path.join(script_dir, "finnomena", "finnomena_fund_list.csv"),
    os.path.join(script_dir, "wealthmagik", "wealthmagik_fund_list.csv")
]

OUTPUT_FILENAME = os.path.join(script_dir, "all_sec_fund_info.csv")

HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 3

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def polite_sleep():
    time.sleep(random.uniform(1.0, 2.0))

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def parse_recovering_period(text):
    if not text or text == "-" or text == "N/A":
        return text
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
        
    if found_match:
        return str(total_days)
    return text

def convert_thai_date(date_str):
    if not date_str or date_str.startswith("N/A"): return date_str
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            day, month, year_be = parts
            year_ce = int(year_be) - 543
            return f"{int(day):02d}-{int(month):02d}-{year_ce}"
    except:
        return date_str
    return date_str

def make_driver():
    options = webdriver.FirefoxOptions()
    if HEADLESS: options.add_argument("-headless")
    options.set_preference("dom.webnotifications.enabled", False)
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    return webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)

def scrape_sec_info(driver, fund_code):
    safe_code = quote(fund_code)
    url = f"https://fundcheck.sec.or.th/fund-detail;funds={safe_code}"
    empty_data = {
        "fund_code": fund_code,
        "sec_url": url,
        "as_of_date": "N/A",
        "sharpe_ratio": "",
        "alpha": "",
        "beta": "",
        "max_drawdown": "",
        "recovering_period": "",
        "tracking_error": "",
        "turnover_ratio": "",
        "fx_hedging": ""
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(url)
            wait = WebDriverWait(driver, 15)
            try:
                wait.until(EC.presence_of_element_located((By.CLASS_NAME, "card-body")))
            except:
                if attempt < MAX_RETRIES:
                    raise Exception("Page not loaded (Card Body missing)")
                else:
                    return empty_data
            try:
                def page_has_date(d):
                    body_text = d.find_element(By.TAG_NAME, "body").text
                    return re.search(r"ข้อมูล ณ วันที่.*?\d{1,2}/\d{1,2}/\d{4}", body_text, re.DOTALL)
                wait.until(page_has_date)
            except:
                if attempt < MAX_RETRIES:
                     raise Exception("Data not populated (Date missing)")
            data = empty_data.copy()
            whole_page_text = driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r"ข้อมูล ณ วันที่.*?(\d{1,2}/\d{1,2}/\d{4})", whole_page_text, re.DOTALL)
            if match:
                data["as_of_date"] = convert_thai_date(match.group(1))
            else:
                data["as_of_date"] = "N/A (Not Found)"
            id_map = {
                "sharpe_ratio": "sharpe-ratio", "alpha": "alpha", "beta": "beta",
                "tracking_error": "tracking-error", "max_drawdown": "max-drawdown",
                "recovering_period": "recovering-period", "turnover_ratio": "turnover-ratio"
            }

            for field, html_id in id_map.items():
                try:
                    xpath = f"//div[@id='{html_id}']/following-sibling::div"
                    val_el = driver.find_element(By.XPATH, xpath)
                    raw_val = driver.execute_script("return arguments[0].textContent;", val_el)
                    cleaned_val = clean_text(raw_val)
                    if field == "recovering_period":
                        data[field] = parse_recovering_period(cleaned_val)
                    else:
                        data[field] = cleaned_val
                except:
                    data[field] = "" 
            try:
                fx_xpath = "//div[contains(text(), 'FX Hedging')]/following-sibling::div//div[contains(@class, 'progress-bar')]"
                fx_el = driver.find_element(By.XPATH, fx_xpath)
                raw_fx = driver.execute_script("return arguments[0].textContent;", fx_el)
                data["fx_hedging"] = clean_text(raw_fx)
            except:
                data["fx_hedging"] = ""
            return data

        except Exception as e:
            log(f"Error {fund_code} (Attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                log(f"Failed {fund_code}")
    return empty_data

def get_unique_fund_codes(file_list):
    unique_codes = set()
    for filepath in file_list:
        if not os.path.exists(filepath):
            log(f"Warning File not found {filepath}")
            continue
        log(f"Reading list from {os.path.basename(filepath)}")
        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_code = row.get("fund_code", "")
                if not raw_code: continue
                clean_code = unquote(raw_code).strip()
                if clean_code:
                    unique_codes.add(clean_code)
    sorted_list = sorted(list(unique_codes))
    log(f"Total funds {len(sorted_list)}")
    return sorted_list

def main():
    all_funds = get_unique_fund_codes(INPUT_FILES)
    if not all_funds:
        log("No funds founds")
        return
    driver = make_driver()
    results = []
    try:
        total = len(all_funds)
        for i, code in enumerate(all_funds, 1):
            log(f"[{i}/{total}]{code}")
            info = scrape_sec_info(driver, code)
            results.append(info)
            polite_sleep()
            
    except KeyboardInterrupt:
        log("Stopped")
    except Exception as e:
        log(f"Critical Error {e}")
    finally:
        if results:
            log(f"Saving to {OUTPUT_FILENAME}")
            headers = [
                "fund_code", "as_of_date", 
                "sharpe_ratio", "alpha", "beta", 
                "max_drawdown", "recovering_period", 
                "tracking_error", "turnover_ratio", "fx_hedging",
                "sec_url"
            ]
            with open(OUTPUT_FILENAME, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(results)
            log("Done")
        if driver:
            driver.quit()

if __name__ == "__main__":
    main()