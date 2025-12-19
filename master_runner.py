import subprocess
import os
import sys
import time
import signal
import glob
from datetime import datetime
from update_driver import update_geckodriver

# for test
TEST_MODE_SKIP_LIST = True

DIR_FIN = "finnomena"
DIR_WM = "wealthmagik"
DIR_MERGED = "merged_output"

LOG_BUFFER = []
HAS_ERROR = False
active_processes = []

script_dir = os.path.dirname(os.path.abspath(__file__))

SCRIPT_LIST_FIN = os.path.join(DIR_FIN, "list_fund_finnomena.py")
SCRIPT_LIST_WM = os.path.join(DIR_WM, "list_fund_wealthmagik.py")

SCRIPTS_DETAILS_FIN = [
    os.path.join(DIR_FIN, "info_finnomena.py"),
    os.path.join(DIR_FIN, "fee_finnomena.py"),
    os.path.join(DIR_FIN, "holding_finnomena.py")
]

SCRIPTS_DETAILS_WM = [
    os.path.join(DIR_WM, "info_wealthmagik.py"),
    os.path.join(DIR_WM, "fee_wealthmagik.py"),
    os.path.join(DIR_WM, "holding_wealthmagik.py"),
    os.path.join(DIR_WM, "allocations_wealthmagik.py")
]

SCRIPT_SEC = "scrape_sec_info.py"
SCRIPT_MERGE = "merge_funds.py"
DATABASE = "db_loader.py"

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
        log_dir = os.path.join(script_dir, "Logs")
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

def check_and_clear_monthly_data():
    log("[Check] Checking for new month cleanup")
    current_month = datetime.now().month
    current_year = datetime.now().year
    target_dirs = [DIR_FIN, DIR_WM, DIR_MERGED]
    
    for d in target_dirs:
        if not os.path.exists(d):
            continue
        files = glob.glob(os.path.join(d, "*.csv"))
        if not files:
            continue
        try:
            first_file = files[0]
            file_timestamp = os.path.getmtime(first_file)
            file_date = datetime.fromtimestamp(file_timestamp)
            if file_date.month != current_month or file_date.year != current_year:
                log(f"New Month '{d}' ({file_date.strftime('%Y-%m')} vs {datetime.now().strftime('%Y-%m')})")
                log(f" -> Cleaning all in '{d}' to start fresh")
                
                count = 0
                for f in files:
                    try:
                        os.remove(f)
                        count += 1
                    except Exception as e:
                        log(f"Error deleting {f}: {e}")
                log(f" -> Deleted {count} files.")
            else:
                log(f"({d})Keeping existing data")
                
        except Exception as e:
            log(f"Error checking files in {d}: {e}")

def launch_batch(script_list, batch_name):
    log(f"stating {batch_name}")
    procs = []
    for script_path in script_list:
        if os.path.exists(script_path):
            log(f" -> {script_path}")
            kwargs = {}
            if sys.platform == "win32":
                kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
            p = subprocess.Popen([sys.executable, script_path], **kwargs)
            procs.append(p)
            active_processes.append(p)
        else:
            log(f"File not found: {script_path}")
    return procs

def kill_all_processes():
    log("\n[Master] interruption Stopping all processes")
    for p in active_processes:
        if p.poll() is None:
            try:
                if sys.platform == "win32":
                    p.send_signal(signal.CTRL_C_EVENT)
                else:
                    p.send_signal(signal.SIGINT)
            except Exception as e:
                p.terminate()

    start_wait = time.time()
    for p in active_processes:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()
    log("[Master] All stopped")

def main():
    try:
        update_geckodriver()
        start_total = time.time()
        check_and_clear_monthly_data()
        if not TEST_MODE_SKIP_LIST:
            log(f"\n[Phase 1/4] Scraping Fund Lists")
            p1 = subprocess.Popen([sys.executable, SCRIPT_LIST_FIN])
            p2 = subprocess.Popen([sys.executable, SCRIPT_LIST_WM])
            p1.wait()
            p2.wait()
            log("Finish lists")
        else:
            log(f"\n[Phase 1/4] SKIPPED")

        log(f"\n[Phase 2/4] Scraping Details")
        all_running_processes = []
        
        log(f"\nStarting Finnomena Set")
        procs_fin = launch_batch(SCRIPTS_DETAILS_FIN, "Finnomena")
        all_running_processes.extend(procs_fin)

        time.sleep(5)
        log(f"\nStarting WealthMagik Set")
        procs_wm = launch_batch(SCRIPTS_DETAILS_WM, "WealthMagik")
        all_running_processes.extend(procs_wm)

        time.sleep(3)
        log(f"\nStarting SEC Info")
        if os.path.exists(SCRIPT_SEC):
            p_sec = subprocess.Popen([sys.executable, SCRIPT_SEC])
            all_running_processes.append(p_sec)
        
        log(f"\nMonitoring all {len(all_running_processes)} processes until finish")
        for p in all_running_processes:
            p.wait()
        log("Finish scraping all details")

        log(f"\n[Phase 3/4] Merging")
        if os.path.exists(SCRIPT_MERGE):
            subprocess.run([sys.executable, SCRIPT_MERGE])
            log("Merge Complete")

        log(f"\n[Phase 4/4] add to database")
        if os.path.exists(DATABASE):
            subprocess.run([sys.executable, DATABASE])
            log("done")

        end_total = time.time()
        log(f"\nFinished in {end_total - start_total:.2f} seconds")

    except KeyboardInterrupt:
        kill_all_processes()
    finally:
        save_log_if_error()
        log("Master runner exited")

if __name__ == "__main__":
    main()