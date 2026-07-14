import BCDA_API, Coverage, Patients, EOB
from Credentials import engine_DEV_Final
from sqlalchemy import text
from pathlib import Path
import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

onedrive = next(p for p in Path(os.environ["USERPROFILE"]).iterdir()
                if p.name.startswith("OneDrive - "))

# ----------------- LOGGING SETUP -----------------
central = ZoneInfo("America/Chicago")
now_central = datetime.now(central)
current = now_central.strftime("%Y-%m-%d")
current_year = now_central.year
log_file = onedrive / "LogFile" / "BCDA" / f"{current_year}_BCDA_DownloadLog.log"


log_file.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(console_handler)

# ----------------- DATA DIRECTORY -----------------
data_dir = Path(r"C:\BCDA\Data")

def start_sql_job():
    with engine_DEV_Final.begin() as conn:
        conn.execute(text("EXEC msdb.dbo.sp_start_job @job_name = :job"),
                {"job": "BCDA_Run_ALL"})

def unlink_files():
    for file in data_dir.iterdir():
        try:
            file.unlink()
            print(f"Deleted file: {file.name}")
        except Exception as e:
            logging.error(f"Failed to delete {file.name}: {e}")


def run_module(name, func):
    try:
        func()
        logging.info(f"{name} completed successfully")
    except Exception as e:
        logging.exception(f"{name} failed with error: {e}")
        raise

def main():
    logging.info("Starting BCDA pipeline")
    run_module("BCDA API", BCDA_API.main)
    run_module("Coverage", Coverage.main)
    run_module("Patients", Patients.main)
    run_module("EOB", EOB.main)
    run_module("unlink Files",unlink_files)
    run_module("Starting SQL PROC BCDA_run_all",start_sql_job)


if __name__ == "__main__":
    main()