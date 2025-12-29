import os
import shutil
import stat
from webdriver_manager.firefox import GeckoDriverManager

def update_geckodriver():
    current_folder = os.path.dirname(os.path.abspath(__file__))
    temp_cache_dir = os.path.join(current_folder, ".temp_wdm")
    print(f"Checking for geckodriver updates (Temp dir: {temp_cache_dir})")
    try:
        os.environ['WDM_CACHE_PATH'] = temp_cache_dir
        downloaded_path = GeckoDriverManager().install()
        destination_path = os.path.join(current_folder, "geckodriver")
        print(f"Copying from: {downloaded_path}")
        shutil.copy2(downloaded_path, destination_path)
        st = os.stat(destination_path)
        os.chmod(destination_path, st.st_mode | stat.S_IEXEC)
        print(f"Updated & ready at: {destination_path}")
        return True

    except Exception as e:
        print(f"Failed to update driver: {e}")
        return False
        
    finally:
        if os.path.exists(temp_cache_dir):
            print(f"Cleaning up temp folder")
            try:
                shutil.rmtree(temp_cache_dir)
            except Exception as e:
                print(f"Warning: Could not delete temp folder: {e}")

if __name__ == "__main__":
    update_geckodriver()