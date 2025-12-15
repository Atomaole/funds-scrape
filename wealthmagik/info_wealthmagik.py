import csv
import time
import re
import os
import requests
import pdfplumber
import random
from urllib.parse import unquote
from io import BytesIO
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

script_dir = os.path.dirname(os.path.abspath(__file__))
INPUT_FILENAME = os.path.join(script_dir, "wealthmagik_fund_list.csv")
OUTPUT_INFO_FILENAME = os.path.join(script_dir, "wealthmagik_info.csv")
OUTPUT_CODES_FILENAME = os.path.join(script_dir, "wealthmagik_codes.csv")
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
    return webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)

def clean_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', text).strip()

def parse_wm_date(text):
    if not text: return ""
    text = clean_text(text)
    if re.match(r"^\d{8}$", text):
        try:
            d = datetime.strptime(text, "%Y%m%d")
            return d.strftime("%d-%m-%Y")
        except: pass
    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if match:
        d, m, y = map(int, match.groups())
        if y > 2400: y -= 543
        try:
            return datetime(y, m, d).strftime("%d-%m-%Y")
        except: pass
        
    return text

def get_text_by_id(driver, element_id):
    try:
        el = driver.find_element(By.ID, element_id)
        return clean_text(el.text)
    except:
        return ""

def get_value_from_id_attribute(driver, prefix):
    try:
        el = driver.find_element(By.CSS_SELECTOR, f"[id^='{prefix}']")
        raw_id = el.get_attribute("id")
        return raw_id.replace(prefix, "")
    except:
        return ""

def fetch_pdf_bytes(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=25)
        if r.status_code == 200: return r.content
    except: pass
    return None

def extract_codes_from_pdf(pdf_bytes):
    codes = []
    if not pdf_bytes: return codes
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            full_text = ""
            for page in pdf.pages:
                text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
                full_text += text + "\n"
            isin_matches = re.findall(r"\b([A-Z]{2}[A-Z0-9]{9}[0-9])\b", full_text)
            for isin in set(isin_matches):
                codes.append({"type": "ISIN", "code": isin})
            found_bloomberg = set()
            label_matches = re.finditer(r"Bloomberg\s*(?:Code|Ticker|ID)?\s*[:：]?\s+([A-Z0-9-]{3,15}\s+[A-Z]{2}(?:\s+Equity)?)", full_text, re.IGNORECASE)
            for m in label_matches:
                raw = m.group(1).strip().upper()
                if len(raw) > 4: found_bloomberg.add(raw)
            equity_matches = re.finditer(r"\b([A-Z0-9-]{3,15}\s+[A-Z]{2}\s+Equity)\b", full_text, re.IGNORECASE)
            for m in equity_matches:
                found_bloomberg.add(m.group(1).strip().upper())
            tb_matches = re.finditer(r"\b([A-Z0-9-]{3,15}\s+TB)\b", full_text)
            for m in tb_matches:
                found_bloomberg.add(m.group(1).strip().upper())
            for bb_code in found_bloomberg:
                codes.append({"type": "Bloomberg", "code": bb_code})
    except: pass
    return codes

def close_ad_if_present(driver):
    try:
        WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.ID, "popupAdsClose"))
        ).click()
        time.sleep(0.5)
    except: pass

def scrape_info_and_pdf(driver, fund_code, url):
    info_data = {
        "fund_code": fund_code,
        "full_name_th": "",
        "nav_value": "",
        "nav_date": "",
        "risk_level": "",
        "category": "",
        "is_dividend": "",
        "inception_date": "",
        "aum": "",
        "bid_price_per_unit": "",
        "offer_price_per_unit": "",
        "source_url": url
    }
    found_codes = []
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(url)
            close_ad_if_present(driver)
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".fundName h1")))
            except:
                if attempt < MAX_RETRIES:
                    raise Exception("Page not loaded (Title missing)")
                else:
                    return info_data, found_codes
            try:
                info_data["full_name_th"] = get_text_by_id(driver, "wmg.funddetailinfo.text.categoryTH")
                if not info_data["full_name_th"]:
                    info_data["full_name_th"] = driver.find_element(By.XPATH, "//div[@class='fundName']/span[@class='categoryTH']").text
            except: pass
            
            try:
                info_data["nav_value"] = clean_text(driver.find_element(By.CLASS_NAME, "nav").text)
                raw_nav_date = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.tnaclassDate.")
                info_data["nav_date"] = parse_wm_date(raw_nav_date)
            except: pass
            
            info_data["aum"] = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.totalnetAsset.")
            info_data["risk_level"] = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.riskSpectrum.")
            info_data["category"] = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.aimcCategories.")
            info_data["is_dividend"] = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.isDividend.")
            info_data["bid_price_per_unit"] = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.bidPrice.")
            info_data["offer_price_per_unit"] = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.offerPrice.")
            raw_inception = get_value_from_id_attribute(driver, "wmg.funddetailinfo.text.inceptionDate.")
            info_data["inception_date"] = parse_wm_date(raw_inception)
            try:
                raw_pdf_url = get_value_from_id_attribute(driver, "wmg.funddetailinfo.button.factSheetPath.")
                pdf_url = unquote(raw_pdf_url).strip()
                if pdf_url and pdf_url.startswith("http"):
                    pdf_bytes = fetch_pdf_bytes(pdf_url)
                    if pdf_bytes:
                        extracted = extract_codes_from_pdf(pdf_bytes)
                        for item in extracted:
                            item['fund_code'] = fund_code
                            item["factsheet_url"] = pdf_url
                            found_codes.append(item)
            except Exception:
                pass

            return info_data, found_codes

        except Exception as e:
            log(f"Error {fund_code} (Attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                log(f"Failed {fund_code}")
    return info_data, found_codes

def main():
    driver = make_driver()
    all_info = []
    all_codes = []
    
    try:
        funds_to_scrape = []
        try:
            with open(INPUT_FILENAME, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader: funds_to_scrape.append(row)
        except FileNotFoundError: return

        total_funds = len(funds_to_scrape)
        log(f"starting ({total_funds}")

        for i, fund in enumerate(funds_to_scrape, 1):
            code = unquote(fund.get("fund_code", "")).strip()
            url = fund.get("url", "")
            if not code or not url: continue
            log(f"[{i}/{total_funds}]{code} (info)")
            
            info, codes = scrape_info_and_pdf(driver, code, url)
            all_info.append(info)
            if codes: all_codes.extend(codes)
            polite_sleep()

    except KeyboardInterrupt:
        log("Stop")
    except Exception as e:
        log(f"Error: {e}")
    finally:
        if all_info:
            log(f"saving {OUTPUT_INFO_FILENAME}")
            headers_info = [
                "fund_code", "full_name_th", "nav_value", "nav_date", 
                "bid_price_per_unit", "offer_price_per_unit",
                "category", "risk_level", "aum",
                "is_dividend", "inception_date", "source_url"
            ]
            with open(OUTPUT_INFO_FILENAME, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=headers_info)
                writer.writeheader()
                writer.writerows(all_info)
        if all_codes:
            log(f"saving {OUTPUT_CODES_FILENAME}")
            headers_codes = ["fund_code", "type", "code","factsheet_url"]
            with open(OUTPUT_CODES_FILENAME, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=headers_codes)
                writer.writeheader()
                writer.writerows(all_codes) 
        if driver:
            try:
                driver.quit()
                log("Closing Browser")
            except Exception:
                pass

if __name__ == "__main__":
    main()