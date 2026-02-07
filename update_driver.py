from pathlib import Path
import os
import shutil
import stat
from webdriver_manager.firefox import GeckoDriverManager
from prefect import task

@task(name="update driver", log_prints=True)
def update_geckodriver():
    current_folder = Path(__file__).resolve().parent
    temp_cache_dir = current_folder/".temp_wdm"
    print(f"Checking for geckodriver updates (Temp dir: {temp_cache_dir})")
    try:
        os.environ['WDM_CACHE_PATH'] = str(temp_cache_dir)
        downloaded_path = GeckoDriverManager().install()
        destination_path = current_folder/"geckodriver"
        print(f"Copying from: {downloaded_path}")
        shutil.copy2(downloaded_path, destination_path)
        st = destination_path.stat()
        destination_path.chmod(st.st_mode | stat.S_IEXEC)
        print(f"Updated & ready at: {destination_path}")
        return True

    except Exception as e:
        print(f"Failed to update driver: {e}")
        return False
        
    finally:
        if temp_cache_dir.exists():
            print(f"Cleaning up temp folder")
            try:
                shutil.rmtree(temp_cache_dir)
            except Exception as e:
                print(f"Warning: Could not delete temp folder: {e}")

if __name__ == "__main__":
    update_geckodriver()