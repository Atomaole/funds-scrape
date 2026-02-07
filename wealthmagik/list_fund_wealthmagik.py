from pathlib import Path
import csv
import requests
from urllib.parse import quote, unquote
import time, datetime
from prefect import task

JSON_URL = "https://www.wealthmagik.com/json-search/fundSearch.json"
BASE_URL = "https://www.wealthmagik.com"
CURRENT_SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = CURRENT_SCRIPT_DIR.parent
RAW_DATA_DIR = CURRENT_SCRIPT_DIR/"raw_data"
OUTPUT_FILENAME = RAW_DATA_DIR/"wealthmagik_fund_list.csv"
LOG_BUFFER = []
HAS_ERROR = False

if not RAW_DATA_DIR.exists():
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

def log(msg):
    global HAS_ERROR
    if "error" in msg.lower() or "failed" in msg.lower():
        HAS_ERROR = True
    timestamp = time.strftime('%H:%M:%S')
    formatted_msg = f"[{timestamp}] {msg}"
    print(formatted_msg)
    LOG_BUFFER.append(formatted_msg)

def save_log_if_error():
    if not HAS_ERROR: return
    try:
        log_dir = ROOT/"Logs"
        if not log_dir.exists(): log_dir.mkdir(parents=True, exist_ok=True)
        filename = f"list_funds_wm_{datetime.now().strftime('%Y-%m-%d')}.log"
        with open (log_dir/filename, "w", encoding="utf-8") as f:
            f.write("\n".join(LOG_BUFFER))
    except: pass

def scrape_with_requests():
    log(f"Starting fetch data from: {JSON_URL}")
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,th;q=0.8"
    }
    try:
        response = requests.get(JSON_URL, headers=HEADERS)
        response.raise_for_status()
        response.encoding = "utf-8"
        json_response = response.json()
        data_list = json_response.get('data', [])
        log(f"Received raw items: {len(data_list)}")
        fund_list = []
        unique_funds = set()
        for item in data_list:
            raw_v = item.get('v', '')
            fund_code = raw_v.split('---')[0].strip() if '---' in raw_v else raw_v
            if fund_code and fund_code not in unique_funds:
                unique_funds.add(fund_code)
                raw_url = item.get('u', '')
                if raw_url:
                    raw_url = raw_url.strip()
                    if raw_url.startswith("http"):
                        base_link = raw_url
                    else:
                        if not raw_url.startswith("/"):
                            raw_url = "/" + raw_url
                        base_link = f"{BASE_URL}{raw_url}"
                    if base_link.endswith("/profile"):
                         final_url = base_link
                    else:
                         final_url = f"{base_link}/profile"
                    fund_list.append([fund_code, final_url])
        return fund_list
    except Exception as e:
        log(f"Error during scrape: {e}")
        return None

@task(name="list_wm", log_prints=True)
def list_wm():
    final_fund_list = scrape_with_requests()
    if final_fund_list:
        final_fund_list.sort(key=lambda x: x[0])
        log(f"Found {len(final_fund_list)} unique items")
        with open(OUTPUT_FILENAME, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['fund_code', 'url']) 
            writer.writerows(final_fund_list)
        log(f"Saved to: {OUTPUT_FILENAME}")
    else:
        log("Failed to fetch data")

if __name__ == "__main__":
    list_wm()