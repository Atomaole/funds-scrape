import os
os.environ["PREFECT_API_URL"] = "http://127.0.0.1:4200/api"

import time
from pathlib import Path
from datetime import datetime
from prefect import task, flow, get_run_logger
from prefect.client.schemas.schedules import CronSchedule

# File Path (Prefect)
from finnomena.scrape_finnomena import finnomena_scraper
from wealthmagik.allocations_wealthmagik import allo_wm_req
from wealthmagik.bid_offer_wealthmagik import bid_offer_wm_req
from wealthmagik.holding_wealthmagik import holding_wm_req
#from wealthmagik.allocations_wealthmagik_selenium import allo_wm_sel
#from wealthmagik.bid_offer_wealthmagik_selenium import bid_offer_wm_sel
#from wealthmagik.holding_wealthmagik_selenium import holding_wm_sel
from wealthmagik.list_fund_wealthmagik import list_wm
from db_loader import db_loader
from clean_type_holding import clean_holding
from merge_funds import merged_file
from scrape_sec_info import sec_scrape
from update_driver import update_geckodriver

#CONFIG
DAILY_START_TIME = "01:00"
HOURS_WAIT_FOR_ROUND_2 = 5
DAYS_TO_SKIP = [6, 0]   # 6=Sunday, 0=Monday
DATE_LOG_FILE = "date.log"
MODE_FOR_WEALTHMAGIK = 1
"""
MODE FOR WEALTHMAGIK
1 = work one thing at the time (recommend)
2 = work bid_offer first and then will work allocations and holding at the same time
3 = work together at the same time 
"""
ALWAYS_SELENIUM_WM = False # No longer support selenium

# FILE PATHS
script_dir = Path(__file__).resolve().parent
RESUME_WM_HOLDING    = script_dir/"wealthmagik/holding_resume.log"
RESUME_WM_ALLOC      = script_dir/"wealthmagik/allocations_resume.log"
RESUME_SEC           = script_dir/"scrape_sec_resume.log"

def is_skip_day():
    return datetime.now().weekday() in DAYS_TO_SKIP

def check_is_new_month():
    log_path = script_dir / DATE_LOG_FILE
    current_date = datetime.now()
    if not log_path.exists(): return True
    try:
        with open(log_path, 'r') as f:
            last_run_str = f.read().strip()
            if not last_run_str: return True
            last_run_date = datetime.strptime(last_run_str, "%Y-%m-%d")
            if last_run_date.month != current_date.month or last_run_date.year != current_date.year:
                return True
            return False
    except: return True

def update_date_log():
    try:
        with open(script_dir / DATE_LOG_FILE, 'w') as f:
            f.write(datetime.now().strftime("%Y-%m-%d"))
        print(f"[MASTER] Updated date.log")
    except Exception as e: 
        print(f"[MASTER] Failed to update date.log: {e}")

@flow(name="Execute Scraping Round", log_prints=True)
def perform_scraping_round(round_name, is_new_month):
    logger = get_run_logger()
    logger.info(f"STARTING: {round_name}")
    if ALWAYS_SELENIUM_WM:
        update_geckodriver()
    list_wm() 
    background_tasks = []
    task_fin = finnomena_scraper.submit()
    background_tasks.append(task_fin)
    time.sleep(6)

    if is_new_month or RESUME_SEC.exists():
        task_sec = sec_scrape.submit()
        background_tasks.append(task_sec)
    else:
        logger.info("Skipping SEC Info (Resume not found)")

    if ALWAYS_SELENIUM_WM:
        #task_holding_func = holding_wm_sel
        #task_alloc_func   = allo_wm_sel
        #task_bid_func     = bid_offer_wm_sel
        print("No longer support selenium but you can still use but maybe it not work property with prefect")
        print("Wealthmagik will not work right now change mode")
        eng = "Selenium"
    else:
        task_holding_func = holding_wm_req
        task_alloc_func   = allo_wm_req
        task_bid_func     = bid_offer_wm_req
        eng = "Requests"
    
    logger.info(f"WM Mode: {MODE_FOR_WEALTHMAGIK} | Engine: {eng}")

    if MODE_FOR_WEALTHMAGIK == 1:
        task_bid_func.submit().wait()
        if is_new_month or RESUME_WM_HOLDING.exists():
            task_holding_func.submit().wait()
            clean_holding.submit().wait()
        if is_new_month or RESUME_WM_ALLOC.exists():
            task_alloc_func.submit().wait()

    elif MODE_FOR_WEALTHMAGIK == 2:
        task_bid_func.submit().wait()
        if is_new_month or RESUME_WM_HOLDING.exists():
            h_future = task_holding_func.submit()
            c_future = clean_holding.submit(wait_for=[h_future])
            background_tasks.append(c_future)
        if is_new_month or RESUME_WM_ALLOC.exists():
            background_tasks.append(task_alloc_func.submit())

    elif MODE_FOR_WEALTHMAGIK == 3:
        background_tasks.append(task_bid_func.submit())
        if is_new_month or RESUME_WM_HOLDING.exists():
            h_future = task_holding_func.submit()
            c_future = clean_holding.submit(wait_for=[h_future])
            background_tasks.append(c_future)
        if is_new_month or RESUME_WM_ALLOC.exists():
            background_tasks.append(task_alloc_func.submit())

    if background_tasks:
        logger.info(f"Waiting for {len(background_tasks)} background tasks")
        for t in background_tasks:
            t.wait()
    logger.info("All scraping tasks finished.")

    merged_file()
    db_loader()

# MAIN
@flow(name="Daily scraper", log_prints=True)
def daily_scraper_cycle():
    print(f"Starting Daily Pipeline at {datetime.now()}")
    if is_skip_day():
        print(f"Today is Skip Day (Day {datetime.now().weekday()})")
        return

    is_new_month = check_is_new_month()
    if is_new_month: print("New Month Detected: Full Scrape Mode")

    perform_scraping_round("ROUND_1", is_new_month)
    print(f"Round 1 Done Resting for {HOURS_WAIT_FOR_ROUND_2} hours")
    time.sleep(HOURS_WAIT_FOR_ROUND_2 * 3600) 

    print(f"ROUND 2 (Cleanup/Retry)")
    perform_scraping_round("ROUND_2", is_new_month)
    print("Updating logs.")
    update_date_log()
    print("Pipeline Finished")

if __name__ == "__main__":
    print(f"Scheduled to run daily at {DAILY_START_TIME}. Waiting")
    my_schedule = CronSchedule(
        cron="0 1 * * *", 
        timezone="Asia/Bangkok"
    )
    daily_scraper_cycle.serve(
        name="funds-scraper",
        schedule=my_schedule, 
        tags=["funds_thai"]
    )