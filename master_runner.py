import subprocess
import sys
import time
import signal
from pathlib import Path
from datetime import datetime, timedelta

# CONFIG
AUTO_MODE = True    # True=loop False=one round
RUN_ON_START = False # True=Manual test immediately (NO save log)
DAILY_START_TIME = "01:00"  # Time of round 1 to start
HOURS_WAIT_FOR_ROUND_2 = 5  # Time to wating round 2
DAYS_TO_SKIP = [6, 0]   # skip [6=sunday, 0=monday]
DATE_LOG_FILE = "date.log"
MODE_FOR_WEALTHMAGIK = 1
"""
MODE FOR WEALTHMAGIK
1 = work one thing at the time (recommend)
2 = work bid_offer first and then will work allocations and holding at the same time
3 = work together at the same time 
"""
ALWAYS_SELENIUM_WM = False

# FILE PATHS
script_dir = Path(__file__).resolve().parent
SCRIPT_UPDATE_DRIVER = script_dir/"update_driver.py"
SCRIPT_LIST_WM       = script_dir/"wealthmagik/list_fund_wealthmagik.py"
SCRIPT_SCRAPE_FIN    = script_dir/"finnomena/scrape_finnomena.py"
SCRIPT_WM_BID_OFFER_REQ = script_dir/"wealthmagik/bid_offer_wealthmagik.py"
SCRIPT_WM_BID_OFFER_SEL = script_dir/"wealthmagik/bid_offer_wealthmagik_selenium.py"
SCRIPT_WM_HOLDING_REQ   = script_dir/"wealthmagik/holding_wealthmagik.py"
SCRIPT_WM_ALLOC_REQ     = script_dir/"wealthmagik/allocations_wealthmagik.py"
SCRIPT_WM_HOLDING_SEL   = script_dir/"wealthmagik/holding_wealthmagik_selenium.py"
SCRIPT_WM_ALLOC_SEL     = script_dir/"wealthmagik/allocations_wealthmagik_selenium.py"
SCRIPT_SEC           = script_dir/"scrape_sec_info.py"
SCRIPT_MERGE         = script_dir/"merge_funds.py"
SCRIPT_DB_LOADER     = script_dir/"db_loader.py"
RESUME_WM_HOLDING    = script_dir/"wealthmagik/holding_resume.log"
RESUME_WM_ALLOC      = script_dir/"wealthmagik/allocations_resume.log"
RESUME_SEC           = script_dir/"scrape_sec_resume.log"
LOG_BUFFER = []
active_processes = []

def log(msg):
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] [MASTER] {msg}")
    LOG_BUFFER.append(f"[{timestamp}] {msg}")

def kill_all_process():
    global active_processes
    if not active_processes:
        log("No active processes to stop")
        return
    log(f"Stopping {len(active_processes)} active processes")
    for p in active_processes:
        if p.poll() is None:
            try:
                if sys.platform == "win32":
                    p.send_signal(signal.CTRL_C_EVENT)
                else:
                    p.send_signal(signal.SIGINT)
            except: pass
    
    start_wait = time.time()
    while time.time() - start_wait < 10:
        if all(p.poll() is not None for p in active_processes):
            break
        time.sleep(0.5)
        
    for p in active_processes:
        if p.poll() is None:
            log(f"Process {p.pid} unresponsive KILLING it")
            try: p.kill()
            except: pass
    active_processes = []
    log("All processes stopped")

def launch_async(path, description):
    global active_processes
    if not path.exists():
        log(f"Error: Script not found {path}")
        return None
    log(f"Launching Async: {description}")
    try:
        kw = {}
        if sys.platform == "win32":
            kw['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
        p = subprocess.Popen([sys.executable, str(path)], **kw)
        active_processes.append(p)
        return p
    except Exception as e:
        log(f"Failed to launch {description}: {e}")
        return None

def run_sync(path, description):
    global active_processes
    if not path.exists():
        log(f"Error: Script not found {path}")
        return False
    log(f"Running Sync: {description}")
    try:
        kw = {}
        if sys.platform == "win32":
            kw['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
        p = subprocess.Popen([sys.executable, str(path)], **kw)
        active_processes.append(p)
        p.wait()
        exit_code = p.returncode
        if p in active_processes: active_processes.remove(p)
        if exit_code == 0:
            log(f"Finished: {description}")
            return True
        else:
            log(f"Failed: {description} (Code {exit_code})")
            return False
    except KeyboardInterrupt:
        raise
    except Exception as e:
        log(f"Exception: {e}")
        return False

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
                log(f"Month changed detected")
                return True
            return False
    except: return True

def update_date_log():
    try:
        with open(script_dir / DATE_LOG_FILE, 'w') as f:
            f.write(datetime.now().strftime("%Y-%m-%d"))
        log("Updated date.log")
    except Exception as e: log(f"Failed to update date.log: {e}")

def get_seconds_until_daily_start(start_time_str):
    now = datetime.now()
    h, m = map(int, start_time_str.split(":"))
    target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return (target - now).total_seconds(), target

def is_skip_day():
    return datetime.now().weekday() in DAYS_TO_SKIP

# MAIN PIPELINE
def run_pipeline(current_slot=None):
    log(f"STARTING PIPELINE for round: {current_slot if current_slot else 'Manual'}")
    if current_slot == "ROUND_1" and is_skip_day():
        log(f"Today is skip day (Day {datetime.now().weekday()}). Skipping pipeline")
        return False
    start_time = time.time()
    if ALWAYS_SELENIUM_WM: run_sync(SCRIPT_UPDATE_DRIVER, "Update GeckoDriver")
    is_new_month = check_is_new_month()
    if is_new_month: log("New Month (Will scrape full set)")
    else: log("Same Month (Checking Resume Logs)")

    # 1. list WealthMagik
    run_sync(SCRIPT_LIST_WM, "WealthMagik Fund List")
    bg_procs = []

    # 2. Finnomena
    p_fin = launch_async(SCRIPT_SCRAPE_FIN, "Finnomena Scraper")
    if p_fin: bg_procs.append(p_fin)
    time.sleep(5)

    # 3. SEC Info
    if is_new_month or RESUME_SEC.exists():
        p_sec = launch_async(SCRIPT_SEC, "SEC Info")
        if p_sec: bg_procs.append(p_sec)
    else:
        log("Skipping SEC Info (Resume file not found - Assuming Done)")
    time.sleep(5)

    # 4. Wealthmagik
    if ALWAYS_SELENIUM_WM:
        target_holding = SCRIPT_WM_HOLDING_SEL
        target_alloc   = SCRIPT_WM_ALLOC_SEL
        target_bid_offer = SCRIPT_WM_BID_OFFER_SEL
        engine_name    = "Selenium"
    else:
        target_holding = SCRIPT_WM_HOLDING_REQ
        target_alloc   = SCRIPT_WM_ALLOC_REQ
        target_bid_offer = SCRIPT_WM_BID_OFFER_REQ
        engine_name    = "Requests"

    if current_slot == "ROUND_1":
        log(f"ROUND 1 {engine_name} engine")
    elif current_slot == "ROUND_2":
        log(f"ROUND 2 {engine_name} engine")
    else:
        log(f"MANUAL {engine_name} engine")

    if MODE_FOR_WEALTHMAGIK == 1:
        run_sync(target_bid_offer, "WM Bid/Offer")
        if is_new_month or RESUME_WM_HOLDING.exists():
            run_sync(target_holding, f"WM Holdings ({engine_name})")
        if is_new_month or RESUME_WM_ALLOC.exists():
            run_sync(target_alloc, f"WM Allocations ({engine_name})")

    elif MODE_FOR_WEALTHMAGIK == 2:
        run_sync(target_bid_offer, "WM Bid/Offer")
        if is_new_month or RESUME_WM_HOLDING.exists():
            p_hold = launch_async(target_holding, f"WM Holdings ({engine_name})")
            if p_hold: bg_procs.append(p_hold) 
        if is_new_month or RESUME_WM_ALLOC.exists():
            p_alloc = launch_async(target_alloc, f"WM Allocations ({engine_name})")
            if p_alloc: bg_procs.append(p_alloc)

    elif MODE_FOR_WEALTHMAGIK == 3:
        p_bid = launch_async(target_bid_offer, "WM Bid/Offer")
        if p_bid: bg_procs.append(p_bid)
        if is_new_month or RESUME_WM_HOLDING.exists():
            p_hold = launch_async(target_holding, f"WM Holdings ({engine_name})")
            if p_hold: bg_procs.append(p_hold)
        if is_new_month or RESUME_WM_ALLOC.exists():
            p_alloc = launch_async(target_alloc, f"WM Allocations ({engine_name})")
            if p_alloc: bg_procs.append(p_alloc)

    if bg_procs:
        log(f"Waiting for ALL background processes ({len(bg_procs)}) to finish")
        for p in bg_procs:
            p.wait()
            if p in active_processes: active_processes.remove(p)
    
    log("All scrapers finished")
    run_sync(SCRIPT_MERGE, "Merging Data")
    run_sync(SCRIPT_DB_LOADER, "Database Loader")
    if current_slot == "ROUND_2":
        log("Updating date.log to mark COMPLETED")
        update_date_log()
    else:
        log(f"Finished {current_slot}. NOT updating date.log (Waiting for next round)")
    
    log(f"PIPELINE FINISHED in {time.time() - start_time:.2f} seconds")
    return True

# MAIN
def main():
    log(f"Config Start Time={DAILY_START_TIME}, Retry Delay={HOURS_WAIT_FOR_ROUND_2} Hours")
    try:
        if RUN_ON_START:
            run_pipeline(current_slot="MANUAL") 
        if not AUTO_MODE:
            log("AUTO_MODE is False Exiting")
            return
        while AUTO_MODE:
            seconds_wait, next_dt = get_seconds_until_daily_start(DAILY_START_TIME)
            log(f"[WAITING] Sleeping {seconds_wait/3600:.2f} hours until Daily Start at {next_dt.strftime('%H:%M:%S')}")
            time.sleep(seconds_wait)
            log("Starting ROUND 1 (Requests)")
            did_run = run_pipeline(current_slot="ROUND_1")
            if not did_run:
                log("Skip day detected")
                continue
            log(f"[WAITING] Round 1 Finished waiting {HOURS_WAIT_FOR_ROUND_2} hours before Round 2")
            time.sleep(HOURS_WAIT_FOR_ROUND_2 * 3600)
            log("Starting ROUND 2 (Cleanup)")
            run_pipeline(current_slot="ROUND_2")
            log("Daily Cycle Completed Looping back to wait for tomorrow")

    except KeyboardInterrupt:
        log("\nUSER INTERRUPT DETECTED")
        kill_all_process()
        log("Master Runner Exited Cleanly")
        sys.exit(0)

if __name__ == "__main__":
    main()