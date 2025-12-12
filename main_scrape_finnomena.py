import subprocess
import os
import sys
import time

FOLDER_NAME = "finnomena"
SCRIPT_LIST = "list_fund_finnomena.py" 
SCRIPTS_CONCURRENT = [
    "info_finnomena.py",
    "fee_finnomena.py",
    "holding_finnomena.py"
]

def get_script_path(script_name):
    return os.path.join(FOLDER_NAME, script_name)

def main():
    start_time = time.time()
    print("Starting")
    list_script_path = get_script_path(SCRIPT_LIST)
    print(f"\n[1/2] scrape list funds: {SCRIPT_LIST}")
    result = subprocess.run([sys.executable, list_script_path])
    
    if result.returncode != 0:
        print("error when scrape list funds")
        return

    print("finish scrape list funds")
    print(f"\n[2/2] scraping holding, fee, info, codes : {', '.join(SCRIPTS_CONCURRENT)}")
    
    processes = []
    for script_name in SCRIPTS_CONCURRENT:
        path = get_script_path(script_name)
        p = subprocess.Popen([sys.executable, path])
        processes.append(p)
        print(f"runing {script_name} (PID: {p.pid})")

    print("\n working scrape")
    for p in processes:
        p.wait()
    end_time = time.time()
    duration = end_time - start_time
    print(f"\nFinish")
    print(f"used time for {duration:.2f} seccond")

if __name__ == "__main__":
    if not os.path.exists(FOLDER_NAME):
        print(f"can't not find '{FOLDER_NAME}'")
    else:
        main()