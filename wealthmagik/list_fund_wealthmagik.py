import os
import csv
import time
import re
from urllib.parse import quote, unquote
from selenium import webdriver
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

script_dir = os.path.dirname(os.path.abspath(__file__))
LIST_PAGE_URL = "https://www.wealthmagik.com/funds"
RAW_DATA_DIR = os.path.join(script_dir, "raw_data")
if not os.path.exists(RAW_DATA_DIR):
    os.makedirs(RAW_DATA_DIR)
OUTPUT_FILENAME = os.path.join(RAW_DATA_DIR, "wealthmagik_fund_list.csv")
HEADLESS = True
LIST_MAX_SECONDS = 300 
MAX_SCROLL_RETRIES = 10
MAX_PAGE_LOAD_RETRIES = 3
LOG_BUFFER = []
HAS_ERROR = False

def log(msg):
    global HAS_ERROR
    if "error" in msg.lower() or "failed" in msg.lower():
        HAS_ERROR = True
    timestamp = time.strftime('%H:%M:%S')
    formatted_msg = f"[{timestamp}] {msg}"
    print(formatted_msg)
    LOG_BUFFER.append(formatted_msg)

def save_log_if_error():
    if not HAS_ERROR:
        return
    try:
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_script_dir)
        log_dir = os.path.join(project_root, "Logs")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        script_name = os.path.basename(__file__).replace(".py", "")
        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"{script_name}_{date_str}.log"
        file_path = os.path.join(log_dir, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_BUFFER))
        print(f"Error detected. Log saved at: {file_path}")
    except Exception as e:
        print(f"Cannot save log file: {e}")

def make_driver():
    options = webdriver.FirefoxOptions()
    if HEADLESS:
        options.add_argument("-headless")
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    options.set_preference("dom.webnotifications.enabled", False)
    
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(current_script_dir)
    driver_path = os.path.join(root, "geckodriver")
    return webdriver.Firefox(service=Service(driver_path), options=options)

def unlock_scroll(driver):
    js = """
    try {
      document.documentElement.style.overflow = 'auto';
      document.body.style.overflow = 'auto';
      document.querySelectorAll('.modal-backdrop,.overlay,[id*="overlay"]').forEach(el=>el.remove());
    } catch(e) {}
    """
    driver.execute_script(js)

def close_ad_if_present(driver):
    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "popupAdsClose"))
        ).click()
        time.sleep(0.5)
    except TimeoutException:
        pass
    unlock_scroll(driver)

def get_scrollable_container(driver):
    js = """
    const items = document.querySelectorAll('.fundCode');
    if (!items.length) return document.body;
    let el = items[items.length-1];
    while (el && el !== document.body) {
      el = el.parentElement;
      const cs = getComputedStyle(el);
      if ((cs.overflowY === 'auto' || cs.overflowY === 'scroll') && el.scrollHeight > el.clientHeight) {
        return el;
      }
    }
    return document.body;
    """
    return driver.execute_script(js)

def smooth_scroll_to_bottom(driver, container):
    driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", container)

def elements_count(driver) -> int:
    try:
        return len(driver.find_elements(By.CSS_SELECTOR, ".fundCode"))
    except:
        return 0

def scrape_process():
    driver = make_driver()
    fund_list = []
    try:
        log("starting")
        driver.get(LIST_PAGE_URL)
        time.sleep(2)
        
        close_ad_if_present(driver)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".fundCode")))
        container = get_scrollable_container(driver)
        try:
            ActionChains(driver).move_to_element(container).click().perform()
        except: pass
        start_time = time.time()
        last_count = elements_count(driver)
        retries = 0
        log("searching")
        while True:
            if time.time() - start_time > LIST_MAX_SECONDS:
                log("Time limit reached")
                break
            smooth_scroll_to_bottom(driver, container)
            time.sleep(0.5) 
            new_count = elements_count(driver)
            if new_count > last_count:
                last_count = new_count
                retries = 0 
            else:
                retries += 1
                if retries >= MAX_SCROLL_RETRIES:
                    log("No new elements found Stopping scroll")
                    break
        log("Extracting data")
        elems = driver.find_elements(By.CSS_SELECTOR, ".fundCode")
        unique_funds = set()
        for el in elems:
            try:
                raw_id = el.get_attribute("id") or ""
                prefix = "wmg.fundscreenerdetail.button.fundcode."
                
                raw_val = raw_id.replace(prefix, "").strip() if raw_id.startswith(prefix) else el.text.strip()
                fund_code = unquote(raw_val).strip()
                
                if fund_code and fund_code not in unique_funds:
                    unique_funds.add(fund_code)
                    safe_code = quote(fund_code.replace("&", " "), safe='')
                    url = f"https://www.wealthmagik.com/funds/{safe_code}/profile"
                    fund_list.append([fund_code, url])
            except:
                continue
        return fund_list

    except Exception as e:
        log(f"Scrape Error: {e}")
        return None
    finally:
        driver.quit()

def main():
    final_fund_list = []
    for attempt in range(1, MAX_PAGE_LOAD_RETRIES + 1):
        log(f"Attempt {attempt}/{MAX_PAGE_LOAD_RETRIES} to scrape list")
        result = scrape_process()
        if result and len(result) > 0:
            final_fund_list = result
            break
        else:
            log(f"Attempt {attempt} failed or got 0 funds Retrying")
            time.sleep(3)
    if final_fund_list:
        final_fund_list.sort(key=lambda x: x[0])
        with open(OUTPUT_FILENAME, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["fund_code", "url"])
            writer.writerows(final_fund_list)
        log(f"Done {len(final_fund_list)} funds")
    else:
        log("Failed to scrape funds after retries")
    save_log_if_error()

if __name__ == "__main__":
    main()