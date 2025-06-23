import logging
import os
import subprocess

from apscheduler.schedulers.blocking import BlockingScheduler

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s.%(msecs)03d] %(module)s:%(lineno)d %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    handlers=[
        logging.FileHandler("logs/scheduler.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


venv_python = os.path.join(".", ".venv", "Scripts", "python.exe")


def run_script_1():
    logging.info("Запуск: D_LENDING_MANUFACTURING_BVU_RK.py")
    subprocess.run([venv_python, "D_LENDING_MANUFACTURING_BVU_RK.py"])


def run_script_2():
    logging.info("Запуск: D_LENDING_TOTAL_BVU_RK.py")
    subprocess.run([venv_python, "D_LENDING_TOTAL_BVU_RK.py"])


def run_script_3():
    logging.info("Запуск: D_LENDING_APK_BVU_RK.py")
    subprocess.run([venv_python, "D_LENDING_APK_BVU_RK.py"])


scheduler = BlockingScheduler()

# Каждое 1-е число месяца в 01:00
scheduler.add_job(run_script_1, "cron", day=1, hour=1, minute=0)
scheduler.add_job(run_script_2, "cron", day=1, hour=1, minute=0)
scheduler.add_job(run_script_3, "cron", day=1, hour=1, minute=0)

logging.info("Планировщик запущен. Ожидание запуска задач...")
scheduler.start()
