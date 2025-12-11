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
parent_dir = os.path.dirname(script_dir)

EXISTING_DATA_FILE = os.path.join(parent_dir, "finnomena", "main_sec_fund_info.csv")
WEALTHMAGIK_LIST_FILE = os.path.join(script_dir, "wealthmagik_fund_list.csv")
OUTPUT_FILENAME = os.path.join(script_dir, "merge_sec_fund_info.csv")

HEADLESS = True

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def polite_sleep():
    time.sleep(random.uniform(1.0, 2.0))

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

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
    return webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)

def scrape_sec_info(driver, fund_code):
    safe_code = quote(fund_code)
    url = f"https://fundcheck.sec.or.th/fund-detail;funds={safe_code}"
    data = {
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

    try:
        driver.get(url)
        wait = WebDriverWait(driver, 10)
        try:
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "card-body")))
        except:
            log(f"something wrong: {fund_code}")
            return data
        try:
            def page_has_date(d):
                body_text = d.find_element(By.TAG_NAME, "body").text
                return re.search(r"ข้อมูล ณ วันที่.*?\d{1,2}/\d{1,2}/\d{4}", body_text, re.DOTALL)

            wait.until(page_has_date)
            whole_page_text = driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r"ข้อมูล ณ วันที่.*?(\d{1,2}/\d{1,2}/\d{4})", whole_page_text, re.DOTALL)
            
            if match:
                data["as_of_date"] = convert_thai_date(match.group(1))
            else:
                data["as_of_date"] = "N/A (Not Found)"
        except:
            data["as_of_date"] = "N/A (Error)"
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
                data[field] = clean_text(raw_val)
            except:
                data[field] = "" 
        try:
            fx_xpath = "//div[contains(text(), 'FX Hedging')]/following-sibling::div//div[contains(@class, 'progress-bar')]"
            fx_el = driver.find_element(By.XPATH, fx_xpath)
            raw_fx = driver.execute_script("return arguments[0].textContent;", fx_el)
            data["fx_hedging"] = clean_text(raw_fx)
        except:
            data["fx_hedging"] = ""

    except Exception as e:
        log(f"Error scraping {fund_code}: {e}")

    return data

def main():
    existing_db = {}
    if os.path.exists(EXISTING_DATA_FILE):
        log(f"loding: {EXISTING_DATA_FILE}")
        with open(EXISTING_DATA_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get("fund_code", "").strip()
                if code:
                    existing_db[code] = row
        log(f"loding done: {len(existing_db)}")
    else:
        log(f"not found {EXISTING_DATA_FILE}")
    if not os.path.exists(WEALTHMAGIK_LIST_FILE):
        log(f"not found {WEALTHMAGIK_LIST_FILE}")
        return

    funds_to_process = []
    with open(WEALTHMAGIK_LIST_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_code = row.get("fund_code", "")
            clean_code = unquote(raw_code).strip()
            if clean_code:
                funds_to_process.append(clean_code)
    funds_to_process = list(set(funds_to_process))
    funds_to_process.sort()

    log(f"list funds from wealthmagik: {len(funds_to_process)}")
    final_results = []
    funds_to_scrape = []
    for code in funds_to_process:
        if code in existing_db:
            final_results.append(existing_db[code])
        else:
            funds_to_scrape.append(code)

    log(f"{EXISTING_DATA_FILE}: {len(final_results)} funds (to not scrape again)")
    log(f"more scrape: {len(funds_to_scrape)}")

    if funds_to_scrape:
        driver = make_driver()
        try:
            for i, code in enumerate(funds_to_scrape, 1):
                log(f"[{i}/{len(funds_to_scrape)}] {code}")
                
                new_data = scrape_sec_info(driver, code)
                final_results.append(new_data)
                
                polite_sleep()
        except KeyboardInterrupt:
            log("stop")
        finally:
            driver.quit()
    else:
        log("no need to scrape more")

    if final_results:
        log(f"saving: {OUTPUT_FILENAME}")
        headers = [
            "fund_code", "as_of_date", 
            "sharpe_ratio", "alpha", "beta", 
            "max_drawdown", "recovering_period", 
            "tracking_error", "turnover_ratio", "fx_hedging",
            "sec_url"
        ]
        with open(OUTPUT_FILENAME, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(final_results)
            
        log(f"done get {len(final_results)} funds")

if __name__ == "__main__":
    main()