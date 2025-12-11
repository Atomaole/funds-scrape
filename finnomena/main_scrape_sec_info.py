import csv
import time
import re
import os
import random
from urllib.parse import quote, unquote
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager


script_dir = os.path.dirname(os.path.abspath(__file__))
INPUT_FILENAME = os.path.join(script_dir, "finnomena_fund_list.csv")
OUTPUT_FILENAME = os.path.join(script_dir, "main_sec_fund_info.csv")
HEADLESS = True

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def polite_sleep():
    t = random.uniform(1.0, 2.0) 
    time.sleep(t)

def make_driver():
    options = webdriver.FirefoxOptions()
    if HEADLESS:
        options.add_argument("-headless")
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    options.set_preference("dom.webnotifications.enabled", False)
    return webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)

def convert_thai_date(date_str):
    if not date_str or date_str.startswith("N/A"):
        return date_str
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            day, month, year_be = parts
            year_ce = int(year_be) - 543
            return f"{int(day):02d}-{int(month):02d}-{year_ce}"
    except Exception as e:
        return date_str
    return date_str

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
            log(f"something wrong {fund_code}")
            return data
        try:
            def page_has_date(d):
                body_text = d.find_element(By.TAG_NAME, "body").text
                return re.search(r"ข้อมูล ณ วันที่.*?\d{1,2}/\d{1,2}/\d{4}", body_text, re.DOTALL)
            wait.until(page_has_date)
            whole_page_text = driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r"ข้อมูล ณ วันที่.*?(\d{1,2}/\d{1,2}/\d{4})", whole_page_text, re.DOTALL)
            
            if match:
                raw_date = match.group(1)
                data["as_of_date"] = convert_thai_date(raw_date)
            else:
                script_finder = """
                var elements = document.querySelectorAll('span.sub-topic');
                for (var i = 0; i < elements.length; i++) {
                    if (elements[i].textContent.includes('ข้อมูล ณ วันที่')) {
                        return elements[i].textContent;
                    }
                }
                return '';
                """
                fallback_text = driver.execute_script(script_finder)
                match_fallback = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", fallback_text)
                if match_fallback:
                    data["as_of_date"] = match_fallback.group(1)
                else:
                    data["as_of_date"] = "N/A (Not Found)"

        except Exception as e:
            data["as_of_date"] = "N/A (Error)"
        id_map = {
            "sharpe_ratio": "sharpe-ratio",
            "alpha": "alpha",
            "beta": "beta",
            "tracking_error": "tracking-error",
            "max_drawdown": "max-drawdown",
            "recovering_period": "recovering-period",
            "turnover_ratio": "turnover-ratio"
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
    except Exception as e:
        log(f"Error {fund_code}: {e}")
    return data

def main():
    driver = make_driver()
    all_data = []
    
    try:
        funds_to_scrape = []
        
        if not os.path.exists(INPUT_FILENAME):
            log(f"can't find {INPUT_FILENAME}")
            return
        with open(INPUT_FILENAME, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                funds_to_scrape.append(row)
        total = len(funds_to_scrape)
        log(f"starting {total}")
        for i, fund in enumerate(funds_to_scrape, 1):
            code = unquote(fund.get("fund_code", "")).strip()
            if not code: continue
            log(f"[{i}/{total}]{code}")
            info = scrape_sec_info(driver, code)
            all_data.append(info)
            polite_sleep()

    except KeyboardInterrupt:
        log("stop")
    except Exception as e:
        log(f"Error: {e}")
    finally:
        if all_data:
            log(f"saving {OUTPUT_FILENAME}")
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
                writer.writerows(all_data)
            log("done")
        
        if driver:
            driver.quit()
            log("close browser")

if __name__ == "__main__":
    main()