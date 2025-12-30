import subprocess
import os
import sys
import time
import signal
from datetime import datetime, timedelta

# CONFIG
AUTO_MODE = True   # True=loop, False=one round
RUN_ON_START = False    # True=do now after run False=waiting time (4.00AM)
SCHEDULE_TIME = "04:30" # time to start (can change)
DAYS_TO_SKIP = [6,0]   # skip [6=sunday, 0=monday]
DATE_LOG_FILE = "date.log"
MODE_FOR_WEALTHMAGIK = 2 # 1=scrape one by one 2= bif_offer first and follow by holding, allocations 3=scrape all in one

# FILE PATHS
script_dir = os.path.dirname(os.path.abspath(__file__))
SCRIPT_UPDATE_DRIVER = os.path.join(script_dir, "update_driver.py")
SCRIPT_LIST_WM      = os.path.join(script_dir, "wealthmagik", "list_fund_wealthmagik.py")
SCRIPT_SCRAPE_FIN   = os.path.join(script_dir, "finnomena", "scrape_finnomena.py")
SCRIPT_WM_BID_OFFER = os.path.join(script_dir, "wealthmagik", "bid_offer_wealthmagik.py")
SCRIPT_WM_HOLDING   = os.path.join(script_dir, "wealthmagik", "holding_wealthmagik.py")
SCRIPT_WM_ALLOC     = os.path.join(script_dir, "wealthmagik", "allocations_wealthmagik.py")
SCRIPT_SEC          = os.path.join(script_dir, "scrape_sec_info.py")
SCRIPT_MERGE        = os.path.join(script_dir, "merge_funds.py")
SCRIPT_DB_LOADER    = os.path.join(script_dir, "db_loader.py")
RESUME_WM_HOLDING = os.path.join(script_dir, "wealthmagik", "holding_resume.log")
RESUME_WM_ALLOC   = os.path.join(script_dir, "wealthmagik", "allocations_resume.log")
RESUME_SEC        = os.path.join(script_dir, "scrape_sec_resume.log")
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
    if not os.path.exists(path):
        log(f"Error: Script not found {path}")
        return None
    log(f"Launching Async: {description}")
    try:
        kw = {}
        if sys.platform == "win32":
            kw['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
        p = subprocess.Popen([sys.executable, path], **kw)
        active_processes.append(p)
        return p
    except Exception as e:
        log(f"Failed to launch {description}: {e}")
        return None

def run_sync(path, description):
    global active_processes
    if not os.path.exists(path):
        log(f"Error: Script not found {path}")
        return False
    log(f"Running Sync: {description}")
    try:
        kw = {}
        if sys.platform == "win32":
            kw['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
        p = subprocess.Popen([sys.executable, path], **kw)
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
    log_path = os.path.join(script_dir, DATE_LOG_FILE)
    current_date = datetime.now()
    if not os.path.exists(log_path): return True
    try:
        with open(log_path, 'r') as f:
            last_run_str = f.read().strip()
            if not last_run_str: return True
            last_run_date = datetime.strptime(last_run_str, "%Y-%m-%d")
            if last_run_date.month != current_date.month or last_run_date.year != current_date.year:
                log(f"Month changed detected.")
                return True
            return False
    except: return True

def update_date_log():
    try:
        with open(os.path.join(script_dir, DATE_LOG_FILE), 'w') as f:
            f.write(datetime.now().strftime("%Y-%m-%d"))
        log("Updated date.log")
    except Exception as e: log(f"Failed to update date.log: {e}")

def get_seconds_until_next_run(target_time_str):
    now = datetime.now()
    target_hour, target_minute = map(int, target_time_str.split(":"))
    target = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    if target <= now: target = target + timedelta(days=1)
    return (target - now).total_seconds()

def is_skip_day():
    return datetime.now().weekday() in DAYS_TO_SKIP

# MAIN PIPELINE
def run_pipeline():
    log("STARTING PIPELINE")
    if is_skip_day() and not RUN_ON_START:
        log(f"Today is skip day (Day {datetime.now().weekday()}). Skipping")
        return
    start_time = time.time()
    run_sync(SCRIPT_UPDATE_DRIVER, "Update GeckoDriver")
    is_new_month = check_is_new_month()
    if is_new_month: log("New Month: All tasks will run")
    else: log("Same Month: Checking Resume Logs")
        
    # 1. list WealthMagik
    run_sync(SCRIPT_LIST_WM, "WealthMagik Fund List")
    bg_procs = []
    
    # 2. Finnomena
    p_fin = launch_async(SCRIPT_SCRAPE_FIN, "Finnomena Scraper")
    if p_fin: bg_procs.append(p_fin)
    time.sleep(8)

    # 3. SEC Info
    if is_new_month or os.path.exists(RESUME_SEC):
        p_sec = launch_async(SCRIPT_SEC, "SEC Info")
        if p_sec: bg_procs.append(p_sec)
    else:
        log("Skipping SEC Info (Done & Same Month)")
    time.sleep(5)

    # 4. Wealthmagik
    if MODE_FOR_WEALTHMAGIK == 1:
        run_sync(SCRIPT_WM_BID_OFFER, "WealthMagik Bid/Offer")
        if is_new_month or os.path.exists(RESUME_WM_HOLDING):
            run_sync(SCRIPT_WM_HOLDING, "WM Holdings")
        else:
            log("Skipping WM Holdings (Done & Same Month)")
        if is_new_month or os.path.exists(RESUME_WM_ALLOC):
            run_sync(SCRIPT_WM_ALLOC, "WM Allocations")
        else:
            log("Skipping WM Allocations (Done & Same Month)")

    elif MODE_FOR_WEALTHMAGIK == 2:
        run_sync(SCRIPT_WM_BID_OFFER, "WealthMagik Bid/Offer")
        if is_new_month or os.path.exists(RESUME_WM_HOLDING):
            p_hold = launch_async(SCRIPT_WM_HOLDING, "WM Holdings")
            if p_hold: bg_procs.append(p_hold) 
        else:
            log("Skipping WM Holdings (Done & Same Month)")
        if is_new_month or os.path.exists(RESUME_WM_ALLOC):
            p_alloc = launch_async(SCRIPT_WM_ALLOC, "WM Allocations")
            if p_alloc: bg_procs.append(p_alloc)
        else:
            log("Skipping WM Allocations (Done & Same Month)")

    elif MODE_FOR_WEALTHMAGIK == 3:
        p_bid = launch_async(SCRIPT_WM_BID_OFFER, "WealthMagik Bid/Offer")
        if p_bid: bg_procs.append(p_bid)
        if is_new_month or os.path.exists(RESUME_WM_HOLDING):
            p_hold = launch_async(SCRIPT_WM_HOLDING, "WM Holdings")
            if p_hold: bg_procs.append(p_hold)
        else:
            log("Skipping WM Holdings (Done & Same Month)")
        if is_new_month or os.path.exists(RESUME_WM_ALLOC):
            p_alloc = launch_async(SCRIPT_WM_ALLOC, "WM Allocations")
            if p_alloc: bg_procs.append(p_alloc)
        else:
            log("Skipping WM Allocations (Done & Same Month)")

    if bg_procs:
        log(f"Waiting for ALL background processes ({len(bg_procs)}) to finish")
        for p in bg_procs:
            p.wait()
            if p in active_processes: active_processes.remove(p)
    
    log("All scrapers finished")
    run_sync(SCRIPT_MERGE, "Merging Data")
    run_sync(SCRIPT_DB_LOADER, "Database Loader")
    update_date_log()
    log(f"PIPELINE FINISHED in {time.time() - start_time:.2f} seconds")

# MAIN
def main():
    log("Master Runner Initialized")
    log(f"Config: AUTO={AUTO_MODE}, RUN_ON_START={RUN_ON_START}")
    try:
        if RUN_ON_START:
            run_pipeline()
        else:
            log("Waiting for next schedule..")
        if not AUTO_MODE:
            log("AUTO_MODE is False. Exiting")
            return
        while AUTO_MODE:
            seconds_wait = get_seconds_until_next_run(SCHEDULE_TIME)
            next_run_time = datetime.now() + timedelta(seconds=seconds_wait)
            log(f"Sleeping {seconds_wait/3600:.2f} hours. Next run at: {next_run_time.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(seconds_wait)
            if is_skip_day():
                log(f"Skip Day. back to sleep")
                continue
            run_pipeline()
            
    except KeyboardInterrupt:
        log("\nUSER INTERRUPT DETECTED")
        kill_all_process()
        log("Master Runner Exited Cleanly")
        sys.exit(0)

if __name__ == "__main__":
    main()