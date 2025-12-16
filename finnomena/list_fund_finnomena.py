import os
import csv
import time
import re
import random
from typing import List, Set
from urllib.parse import unquote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import TimeoutException

script_dir = os.path.dirname(os.path.abspath(__file__))
LIST_BASE_URL = "https://www.finnomena.com/fund/filter?size=1000&page={page}"
MAX_PAGES = 5
OUTPUT_FILENAME = os.path.join(script_dir, "finnomena_fund_list.csv")
HEADLESS = True
MAX_RETRIES = 3
RETRY_DELAY = 3

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

def polite_sleep():
    time.sleep(random.uniform(1, 2))

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

def unlock_scroll(driver):
    js = """
    try {
      document.documentElement.style.overflow = 'auto';
      document.body.style.overflow = 'auto';
      document.querySelectorAll('.modal-backdrop, .overlay').forEach(el => el.remove());
    } catch(e) {}
    """
    driver.execute_script(js)

def extract_fund_code_from_url(url: str) -> str:
    if "/fund/" not in url:
        return ""
    parts = url.split("/fund/")
    if len(parts) > 1:
        raw_code = parts[-1].strip()
        return unquote(raw_code)
    return ""

def main():
    driver = make_driver()
    all_funds: Set[tuple] = set()
    try:
        log("starting scrape list")
        for page in range(1, MAX_PAGES + 1):
            log(f"--- Processing Page {page} ---")
            success = False
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    url = LIST_BASE_URL.format(page=page)
                    driver.get(url)
                    unlock_scroll(driver)
                    try:
                        WebDriverWait(driver, 20).until(
                            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/fund/')]"))
                        )
                    except TimeoutException:
                        if attempt < MAX_RETRIES:
                            raise Exception("Timeout waiting for funds")
                        else:
                            log(f"Page {page} seems empty or failed.")
                            break
                    elements = driver.find_elements(By.XPATH, "//a[contains(@href, '/fund/')]")
                    found_on_page = 0
                    for el in elements:
                        full_url = el.get_attribute("href")
                        if not full_url: continue
                        if "filter" in full_url or "search" in full_url: continue
                        clean_url = full_url.split("?")[0]
                        fund_code = extract_fund_code_from_url(clean_url)
                        if fund_code and clean_url:
                            if (fund_code, clean_url) not in all_funds:
                                all_funds.add((fund_code, clean_url))
                                found_on_page += 1
                    if found_on_page > 0:
                        log(f"Page {page}: Found {found_on_page} funds.")
                        success = True
                        break
                    else:
                        log(f"Page {page}: No funds found. Stopping pagination.")
                        success = True
                        break
                except Exception as e:
                    log(f"Error Page {page} (Attempt {attempt}/{MAX_RETRIES}): {e}")
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_DELAY)
                    else:
                        log(f"Failed Page {page}")
            if success and found_on_page == 0:
                break
            polite_sleep()

    except Exception as e:
        log(f"error: {e}")
    finally:
        driver.quit()
        log("close browser")

    if all_funds:
        sorted_funds = sorted(list(all_funds))
        log(f"saving {len(sorted_funds)} funds to {OUTPUT_FILENAME}...")
        
        with open(OUTPUT_FILENAME, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["fund_code", "url"])
            writer.writerows(sorted_funds)
        log("done")
    else:
        log("Error: No funds found or Internet issue")

if __name__ == "__main__":
    main()