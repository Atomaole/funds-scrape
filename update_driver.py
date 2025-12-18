import os
import shutil
import stat
from webdriver_manager.firefox import GeckoDriverManager

def update_geckodriver():
    print("Checking for geckodriver updates")
    try:
        downloaded_path = GeckoDriverManager().install()
        current_folder = os.path.dirname(os.path.abspath(__file__))
        destination_path = os.path.join(current_folder, "geckodriver")
        print(f"Copying from: {downloaded_path}")
        shutil.copy2(downloaded_path, destination_path)
        st = os.stat(destination_path)
        os.chmod(destination_path, st.st_mode | stat.S_IEXEC)
        
        print(f"updated & ready at: {destination_path}")
        return True

    except Exception as e:
        print(f"Failed to update driver: {e}")
        return False

if __name__ == "__main__":
    update_geckodriver()