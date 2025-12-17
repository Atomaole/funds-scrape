import csv
import time
import re
import os
import random
import requests
import pdfplumber
from io import BytesIO
from datetime import datetime
from selenium import webdriver
from urllib.parse import unquote
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager

script_dir = os.path.dirname(os.path.abspath(__file__))
current_date_str = datetime.now().strftime("%Y-%m-%d")

INPUT_FILENAME = os.path.join(script_dir, "finnomena_fund_list.csv")
OUTPUT_MASTER_FILENAME = os.path.join(script_dir, "finnomena_master_info.csv")
OUTPUT_DAILY_FILENAME = os.path.join(script_dir, f"finnomena_daily_nav_{current_date_str}.csv")
OUTPUT_CODES_FILENAME = os.path.join(script_dir, "finnomena_codes.csv")

HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 3

THAI_MONTH_MAP = {
    "ม.ค.": 1, "มกราคม": 1, "JAN": 1, "ก.พ.": 2, "กุมภาพันธ์": 2, "FEB": 2,
    "มี.ค.": 3, "มีนาคม": 3, "MAR": 3, "เม.ย.": 4, "เมษายน": 4, "APR": 4,
    "พ.ค.": 5, "พฤษภาคม": 5, "MAY": 5, "มิ.ย.": 6, "มิถุนายน": 6, "JUN": 6,
    "ก.ค.": 7, "กรกฎาคม": 7, "JUL": 7, "ส.ค.": 8, "สิงหาคม": 8, "AUG": 8,
    "ก.ย.": 9, "กันยายน": 9, "SEP": 9, "ต.ค.": 10, "ตุลาคม": 10, "OCT": 10,
    "พ.ย.": 11, "พฤศจิกายน": 11, "NOV": 11, "ธ.ค.": 12, "ธันวาคม": 12, "DEC": 12,
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

def parse_thai_date(text):
    if not text: return ""
    text = re.sub(r"(ข้อมูล\s*ณ\s*วันที่|ณ\s*วันที่|as of|วันที่จดทะเบียนกองทุน)", "", text, flags=re.IGNORECASE).strip()
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

def extract_number_only(text):
    if not text: return ""
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text.replace(",", ""))
    return match.group(0) if match else ""

def get_detail_dict(driver):
    details = {}
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, ".fund-detail .detail-row")
        for row in rows:
            try:
                left = clean_text(row.find_element(By.CSS_SELECTOR, ".left").text)
                right = clean_text(row.find_element(By.CSS_SELECTOR, ".right").text)
                if left: details[left] = right
            except: continue
    except: pass
    return details

def fetch_pdf_bytes(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=15)
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
    except Exception as e:
        print(f"PDF Error: {e}")
    return codes

def scrape_info(driver, fund_code, url, need_master=True):
    # need_master = True is mean never scrape this funds before
    # need_master = False is mean need to scrape all details
    
    master_data = None
    daily_data = {
        "fund_code": fund_code,
        "nav_date": "",
        "nav_value": "",
        "aum": "", 
        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    found_codes = []
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            driver.get(url)
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
            except:
                if attempt < MAX_RETRIES: raise Exception("Page not loaded")
                else: return master_data, daily_data, found_codes
            try:
                nav_box = WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".fund-nav-percent")))
                h3_elem = nav_box.find_element(By.TAG_NAME, "h3")
                daily_data["nav_value"] = extract_number_only(h3_elem.text)
                p_elem = nav_box.find_element(By.TAG_NAME, "p")
                daily_data["nav_date"] = parse_thai_date(clean_text(p_elem.text))
            except: pass
            details = get_detail_dict(driver)
            daily_data["aum"] = extract_number_only(details.get("มูลค่าทรัพย์สินสุทธิ", ""))
            if need_master:
                master_data = {
                    "fund_code": fund_code,
                    "full_name_th": "",
                    "amc": "",
                    "category": "",
                    "risk_level": "",
                    "is_dividend": "",
                    "inception_date": "",
                    "source_url": url,
                }
                try:
                    p_elem = driver.find_element(By.XPATH, "//header[@id='fund-header']//p[1]")
                    master_data["full_name_th"] = clean_text(p_elem.text)
                except: pass
                master_data["amc"] = details.get("บลจ", "")
                master_data["category"] = details.get("ประเภทกอง", "")
                master_data["risk_level"] = extract_number_only(details.get("ค่าความเสี่ยง", ""))
                master_data["is_dividend"] = details.get("นโยบายการจ่ายปันผล", "")
                master_data["inception_date"] = parse_thai_date(details.get("วันที่จดทะเบียนกองทุน", ""))
                try:
                    pdf_link_el = driver.find_element(By.XPATH, "//a[contains(text(), 'หนังสือชี้ชวน') or contains(@href, '.pdf')]")
                    pdf_url = pdf_link_el.get_attribute("href")
                    if pdf_url:
                        pdf_bytes = fetch_pdf_bytes(pdf_url)
                        extracted = extract_codes_from_pdf(pdf_bytes)
                        for item in extracted:
                            item['fund_code'] = fund_code
                            item['factsheet_url'] = pdf_url
                            found_codes.append(item)
                except: pass
            return master_data, daily_data, found_codes
        except Exception as e:
            log(f"Error {fund_code} (Attempt {attempt}): {e}")
            if attempt < MAX_RETRIES: time.sleep(RETRY_DELAY)
            else: log(f"Failed {fund_code}")
    
    return master_data, daily_data, found_codes

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
    
    clean_deleted_funds(OUTPUT_MASTER_FILENAME, valid_codes)
    clean_deleted_funds(OUTPUT_CODES_FILENAME, valid_codes)
    existing_master_codes = set()
    if os.path.exists(OUTPUT_MASTER_FILENAME):
        try:
            with open(OUTPUT_MASTER_FILENAME, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_master_codes.add(row['fund_code'].strip())
            log(f"Found {len(existing_master_codes)} existing funds in Master file.")
        except Exception as e:
            log(f"Error reading master file: {e}")

    headers_master = ["fund_code", "full_name_th", "amc", "category", "risk_level", "is_dividend", "inception_date", "source_url"]
    headers_daily = ["fund_code", "nav_date", "nav_value", "aum", "scraped_at"]
    headers_codes = ["fund_code", "type", "code","factsheet_url"]
    exists_master = os.path.exists(OUTPUT_MASTER_FILENAME)
    exists_daily = os.path.exists(OUTPUT_DAILY_FILENAME)
    exists_codes = os.path.exists(OUTPUT_CODES_FILENAME)
    f_master = open(OUTPUT_MASTER_FILENAME, 'a', newline="", encoding="utf-8-sig")
    f_daily = open(OUTPUT_DAILY_FILENAME, 'a', newline="", encoding="utf-8-sig")
    f_codes = open(OUTPUT_CODES_FILENAME, 'a', newline="", encoding="utf-8-sig")
    try:
        writer_master = csv.DictWriter(f_master, fieldnames=headers_master)
        writer_daily = csv.DictWriter(f_daily, fieldnames=headers_daily)
        writer_codes = csv.DictWriter(f_codes, fieldnames=headers_codes)
        if not exists_master: writer_master.writeheader()
        if not exists_daily: writer_daily.writeheader()
        if not exists_codes: writer_codes.writeheader()
        total_funds = len(funds_to_scrape)
        log(f"Starting Scrape {total_funds} Funds")

        for i, fund in enumerate(funds_to_scrape, 1):
            code = unquote(fund.get("fund_code", "")).strip()
            url = fund.get("url", "")
            if not code or not url: continue
            need_master = code not in existing_master_codes
            status_msg = "FULL Update" if need_master else "Daily Update"
            log(f"[{i}/{total_funds}] {code} : {status_msg}") 
            m_data, d_data, codes = scrape_info(driver, code, url, need_master=need_master)
            if d_data: 
                writer_daily.writerow(d_data)
                f_daily.flush()

            if m_data: 
                writer_master.writerow(m_data)
                f_master.flush()

            if codes: 
                writer_codes.writerows(codes)
                f_codes.flush()
            
            polite_sleep()
            
    except KeyboardInterrupt:
        log("Stop")
    except Exception as e:
        log(f"Error: {e}")
    finally:
        f_master.close()
        f_daily.close()
        f_codes.close()
        if driver:
            try: driver.quit()
            except: pass
        log("Done")

if __name__ == "__main__":
    main()