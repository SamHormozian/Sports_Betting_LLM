import os
import subprocess

# Directories
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
SRC_DIR = os.path.join(PROJECT_ROOT, "src")

# Paths to individual scripts
DATA_GATHERING_SCRIPT = os.path.join(SCRIPTS_DIR, "data_gathering.py")
CLEANER_SCRIPT = os.path.join(SRC_DIR, "cleaner.py")
FEATURE_ENGINEER_SCRIPT = os.path.join(SRC_DIR, "feature_engineering.py")

def run_script(script_path, description):
    """
    Run a Python script and display the status.
    """
    print(f"\n{'=' * 40}")
    print(f"Running: {description}")
    print(f"{'=' * 40}")
    if not os.path.exists(script_path):
        print(f"Error: {script_path} does not exist!")
        exit(1)

    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"{description} completed successfully.\n")
    else:
        print(f"Error while running {description}:\n{result.stderr}")
        exit(1)

if __name__ == "__main__":
    print(" Starting the Master Pipeline...")

    # Step 1: Data Gathering
    run_script(DATA_GATHERING_SCRIPT, "Data Gathering Pipeline")

    # Step 2: Data Cleaning
    run_script(CLEANER_SCRIPT, "Data Cleaning Pipeline")

    # Step 3: Feature Engineering
    run_script(FEATURE_ENGINEER_SCRIPT, "Feature Engineering Pipeline")

    print("\n All tasks completed successfully!")
