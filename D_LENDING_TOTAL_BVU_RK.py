import logging
import os
import re
from calendar import monthrange
from datetime import datetime

import pandas as pd
import requests
import vertica_python
from bs4 import BeautifulSoup
from pandas.errors import EmptyDataError

from config import settings

# Конфигурация
base_url = "https://www.nationalbank.kz"
listing_urls = [
    f"{base_url}/ru/news/banking-sector-loans-to-economy-analytics/rubrics/2319",
    f"{base_url}/ru/news/banking-sector-loans-to-economy-analytics/rubrics/2204",
    f"{base_url}/ru/news/banking-sector-loans-to-economy-analytics/rubrics/1985",
    f"{base_url}/ru/news/banking-sector-loans-to-economy-analytics/rubrics/1907",
]

save_folder = "downloads"
os.makedirs(save_folder, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s.%(msecs)03d] %(module)s:%(lineno)d %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    filename="logs/lending-total.log",
    encoding="utf-8",
)

logger = logging.getLogger(__name__)


TABLE_NAME = "DWH.D_LENDING_TOTAL_BVU_RK"
# TABLE_NAME = "SANDBOX.D_LENDING_TOTAL_BVU_RK"


# --- Вспомогательные функции ---
def find_row_contains(df, keyword):
    keyword = keyword.lower().strip()
    for i, row in df.iterrows():
        if pd.notna(row.iloc[0]):
            cell = str(row.iloc[0]).lower().strip()
            if keyword in cell:
                return i
    return None


def get_value_by_keyword(df, row_keyword, col_index):
    row_idx = find_row_contains(df, row_keyword)
    if row_idx is not None:
        try:
            return df.iloc[row_idx, col_index]
        except IndexError:
            return None
    return None


def get_filename_from_cd(cd):
    if not cd:
        return None
    fname = re.findall('filename="(.+)"', cd)
    return fname[0] if fname else None


# --- Получение нового PACKAGE_ID ---
with vertica_python.connect(**settings.conn_info) as conn:
    cursor = conn.cursor()
    cursor.execute(f"SELECT COALESCE(MAX(PACKAGE_ID), 0) FROM {TABLE_NAME}")
    max_package_id = cursor.fetchone()[0]
    PACKAGE_ID = max_package_id + 1
    logger.info(f"Новый PACKAGE_ID: {PACKAGE_ID}")

# Сбор ссылок
logger.info("Шаг 1: Сбор ссылок...")
report_links = []
for listing_url in listing_urls:
    try:
        resp = requests.get(listing_url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all(
            "a", string=lambda t: t and "Кредиты банковского сектора экономике" in t
        ):
            href = tag.get("href")
            if href and href.startswith("/"):
                report_links.append((tag.text.strip(), base_url + href))
    except Exception as e:
        logger.error(f"Ошибка при загрузке {listing_url}: {e}")

if not report_links:
    logger.error("Нет подходящих ссылок.")
    raise Exception("Нет подходящих ссылок.")
logger.info(f"Найдено ссылок: {len(report_links)}")

# Извлечение данных
logger.info("Шаг 2: Извлечение данных...")
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
all_rows = []

for title, report_url in report_links:
    logger.info(f"--- Обработка: {title} ---")
    try:
        resp = requests.get(report_url, timeout=20)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "").lower()

        file_content = None
        if (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            in content_type
        ):
            file_content = resp.content
        elif "text/html" in content_type:
            soup = BeautifulSoup(resp.text, "html.parser")
            tag = soup.find("a", href=lambda h: h and ".xlsx" in h.lower())
            if tag:
                actual_file_url = base_url + tag["href"]
                file_resp = requests.get(actual_file_url, timeout=30)
                file_resp.raise_for_status()
                file_content = file_resp.content
            else:
                logger.info("XLSX-файл не найден на HTML-странице.")
                continue
        else:
            logger.warning(f"Неизвестный формат контента: {content_type}")
            continue

        xls = pd.ExcelFile(file_content, engine="openpyxl")
        sheet_issued = next((s for s in xls.sheet_names if "выдано" in s.lower()), None)
        sheet_rates = next((s for s in xls.sheet_names if "ставк" in s.lower()), None)
        if not sheet_issued or not sheet_rates:
            continue

        df_issued = xls.parse(sheet_issued)
        df_rates = xls.parse(sheet_rates)

        headers_row = df_issued.iloc[2, 1:]
        periods = []
        for idx, val in enumerate(headers_row):
            if isinstance(val, str) and "." in val:
                try:
                    #                    val = val.strip().replace("*", "")
                    #                    m, y = val.split(".")
                    #                    m, y = int(m), int("20" + y)
                    val_clean = re.sub(r"[^\d\.]", "", val)
                    m, y = val_clean.split(".")
                    m, y = int(m), int("20" + y)
                    # m, y = val.split("."); m, y = int(m), int("20" + y)
                    last_day = monthrange(y, m)[1]
                    full_date = f"{y}-{m:02d}-{last_day}"
                    col_nat = df_issued.columns[idx + 2]
                    col_for = df_issued.columns[idx + 3]
                    periods.append((val, full_date, col_nat, col_for))
                #                    periods.append((val_clean, full_date, col_nat, col_for))
                except:
                    continue

        rate_nat_col = df_rates.columns.get_loc("Unnamed: 7")
        rate_for_col = df_rates.columns.get_loc("Unnamed: 8")
        # rate_nat = get_value_by_keyword(df_rates, "по всем кредитам", rate_nat_col)
        # rate_for = get_value_by_keyword(df_rates, "по всем кредитам", rate_for_col)

        # for _, period_date, col_nat, col_for in periods:
        for short_period, period_date, col_nat, col_for in periods:
            col_nat_idx = df_issued.columns.get_loc(col_nat)
            col_for_idx = df_issued.columns.get_loc(col_for)

            month_col_idx = None
            for idx, val in enumerate(
                df_rates.iloc[2]
            ):  # строка с подписями месяцев: '12.24', '01.25', и т.д.
                # if isinstance(val, str) and short_period in val:
                #    month_col_idx = idx
                #    break
                if isinstance(val, str):
                    clean_val = val.strip().replace("*", "")
                    if short_period.strip().replace("*", "") == clean_val:
                        month_col_idx = idx
                        break

            rate_nat = rate_for = None
            if month_col_idx is not None:
                try:
                    label = str(df_rates.iloc[3, month_col_idx]).lower()
                    # next_label = str(df_rates.iloc[3, month_col_idx + 1]).lower()
                    # cur_rate = df_rates.iloc[4, month_col_idx]
                    # next_rate = df_rates.iloc[4, month_col_idx + 1]
                    if "нац" in label:
                        rate_nat = df_rates.iloc[4, month_col_idx]
                        rate_for = df_rates.iloc[4, month_col_idx + 1]
                    else:
                        rate_for = df_rates.iloc[4, month_col_idx]
                        rate_nat = df_rates.iloc[4, month_col_idx + 1]
                except Exception as e:
                    print(f"Ошибка при извлечении ставок за {short_period}: {e}")
                    rate_nat = None
                    rate_for = None
                # cell_currency = str(df_rates.iloc[3, month_col_idx]).lower()
                # if "нац" in cell_currency:
                #    rate_nat = df_rates.iloc[4, month_col_idx]
                #    rate_for = df_rates.iloc[4, month_col_idx + 1]
                # else:
                #    rate_for = df_rates.iloc[4, month_col_idx]
                #    rate_nat = df_rates.iloc[4, month_col_idx + 1]
            # rate_row_idx = find_row_contains(df_rates, short_period)

            # rate_nat = df_rates.iloc[rate_row_idx, rate_nat_col] if rate_row_idx is not None else None
            # rate_for = df_rates.iloc[rate_row_idx, rate_for_col] if rate_row_idx is not None else None

            val_nat_total = (
                get_value_by_keyword(df_issued, "всего кредиты выданные", col_nat_idx)
                or 0
            )
            val_for_total = (
                get_value_by_keyword(df_issued, "всего кредиты выданные", col_for_idx)
                or 0
            )
            mapping = {
                1: ("Всего", val_nat_total + val_for_total),
                2: ("Всего в национальной валюте", val_nat_total),
                3: ("Всего в иностранной валюте", val_for_total),
                4: (
                    "В нац. валюте, малое предпринимательство",
                    get_value_by_keyword(
                        df_issued, "малого предпринимательства", col_nat_idx
                    ),
                ),
                5: (
                    "В нац. валюте, среднее предпринимательство",
                    get_value_by_keyword(
                        df_issued, "среднего предпринимательства", col_nat_idx
                    ),
                ),
                6: (
                    "В нац. валюте, крупное предпринимательство",
                    get_value_by_keyword(
                        df_issued, "крупного предпринимательства", col_nat_idx
                    ),
                ),
                7: (
                    "В ин. валюте, малое предпринимательство",
                    get_value_by_keyword(
                        df_issued, "малого предпринимательства", col_for_idx
                    ),
                ),
                8: (
                    "В ин. валюте, среднее предпринимательство",
                    get_value_by_keyword(
                        df_issued, "среднего предпринимательства", col_for_idx
                    ),
                ),
                9: (
                    "В ин. валюте, крупное предпринимательство",
                    get_value_by_keyword(
                        df_issued, "крупного предпринимательства", col_for_idx
                    ),
                ),
            }
            for type_id, (desc, value) in mapping.items():
                if value is None or not pd.notna(value):
                    continue
                rate = None
                if type_id in [2, 4, 5, 6]:
                    rate = rate_nat
                elif type_id in [3, 7, 8, 9]:
                    rate = rate_for
                all_rows.append(
                    {
                        "LOAD_DATE": timestamp,
                        "PACKAGE_ID": PACKAGE_ID,
                        "TYPE": type_id,
                        "TYPE_DESCRIPTION": desc,
                        "ISSUED_MONTH_KZT": float(value),
                        "RATE_PERCENTAGE": (
                            float(rate) if rate is not None and pd.notna(rate) else None
                        ),
                        "PERIOD": period_date,
                    }
                )

    except Exception as e:
        logger.error(f"Ошибка при обработке '{title}': {e}")

# Шаг 3: Выгрузка в витрину
logger.info("Шаг 3: Загрузка в Vertica...")
df = pd.DataFrame(all_rows)
df.drop_duplicates(subset=["PERIOD", "TYPE"], inplace=True)

insert_query = f"""
INSERT INTO {TABLE_NAME} (
    LOAD_DATE,
    PACKAGE_ID,
    TYPE,
    TYPE_DESCRIPTION,
    ISSUED_MONTH_KZT,
    RATE_PERCENTAGE,
    PERIOD
) VALUES (:LOAD_DATE, :PACKAGE_ID, :TYPE, :TYPE_DESCRIPTION, :ISSUED_MONTH_KZT, :RATE_PERCENTAGE, :PERIOD)
"""

# Преобразуем PERIOD в строку
df["PERIOD"] = df["PERIOD"].astype(str)

# Заменяем NaN на None
df = df.where(pd.notnull(df), None)

with vertica_python.connect(**settings.conn_info) as conn:
    cursor = conn.cursor()
    successful = 0
    failed_rows = 0

    for idx, record in enumerate(df.to_dict(orient="records"), start=1):
        try:
            cursor.execute(insert_query, record)
            successful += 1
        except Exception as e:
            logger.error(f"Ошибка при вставке строки #{idx}: {record}")
            logger.error(f"Текст ошибки: {str(e)}")
            failed_rows += 1

    conn.commit()
    logger.info(f"Успешно загружено строк: {successful}")
    if failed_rows:
        logger.warning(f"Не удалось загрузить строк: {failed_rows}")
