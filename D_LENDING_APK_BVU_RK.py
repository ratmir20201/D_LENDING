import logging
import os
import re
from calendar import monthrange
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import vertica_python
from bs4 import BeautifulSoup

from config import settings

# Настройки
base_url = "https://www.nationalbank.kz"
rubric_urls = [
    "https://www.nationalbank.kz/ru/news/banking-sector-loans-to-economy-analytics/rubrics/1907",
    "https://www.nationalbank.kz/ru/news/banking-sector-loans-to-economy-analytics/rubrics/1985",
    "https://www.nationalbank.kz/ru/news/banking-sector-loans-to-economy-analytics/rubrics/2204",
    "https://www.nationalbank.kz/ru/news/banking-sector-loans-to-economy-analytics/rubrics/2319",
]
save_folder = Path("./nbkr_downloads")
save_folder.mkdir(parents=True, exist_ok=True)

include_keyword = "Кредиты банковского сектора субъектам предпринимательства"
exclude_keywords = [
    "по видам экономической деятельности",
    "по расширенной классификации",
]

TYPE_MAPPING = {
    "субъектам малого предпринимательства в национальной валюте": 2,
    "субъектам малого предпринимательства в иностранной валюте": 3,
    "субъектам среднего предпринимательства в национальной валюте": 4,
    "субъектам среднего предпринимательства в иностранной валюте": 5,
    "субъектам крупного предпринимательства в национальной валюте": 6,
    "субъектам крупного предпринимательства в иностранной валюте": 7,
}
REVERSE_MAPPING = {v: k for k, v in TYPE_MAPPING.items()}
month_map = {
    "январь": 1,
    "февраль": 2,
    "март": 3,
    "апрель": 4,
    "май": 5,
    "июнь": 6,
    "июль": 7,
    "август": 8,
    "сентябрь": 9,
    "октябрь": 10,
    "ноябрь": 11,
    "декабрь": 12,
}
LOAD_DATE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s.%(msecs)03d] %(module)s:%(lineno)d %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    filename="logs/lending-apk.log",
    encoding="utf-8",
)

logger = logging.getLogger(__name__)


target_table = "DWH.D_LENDING_APK_BVU_RK"


def extract_target_files(html):
    soup = BeautifulSoup(html, "html.parser")
    file_entries = []
    for item in soup.select("div.posts-files__item"):
        title_tag = item.select_one("div.posts-files__title a")
        if not title_tag:
            continue
        title_text = title_tag.text.strip()
        href = title_tag.get("href", "")
        if include_keyword in title_text and not any(
            bad in title_text for bad in exclude_keywords
        ):
            full_url = base_url + href
            file_id = href.split("/")[-1]
            file_name = f"{file_id}.xlsx"
            file_entries.append((title_text, full_url, file_name))
    return file_entries


all_records = []
for url in rubric_urls:
    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception:
        logger.error("Ошибка при запросе на url %s", url)
        continue

    for title, file_url, file_name in extract_target_files(response.text):
        name_part, ext = os.path.splitext(file_name)
        today_str = datetime.now().strftime("%Y%m%d")
        file_name_with_date = f"{name_part}_{today_str}{ext}"
        file_path = save_folder / file_name_with_date
        try:
            if not file_path.exists():
                file_data = requests.get(file_url)
                file_data.raise_for_status()
                with open(file_path, "wb") as f:
                    f.write(file_data.content)

            xls = pd.ExcelFile(file_path, engine="openpyxl")
            df = xls.parse("Выдано", header=None)

            dates = df.iloc[4].ffill()
            categories = df.iloc[5].ffill()
            currencies = df.iloc[6].ffill()

            full_headers = []
            for d, c, v in zip(dates, categories, currencies):
                if pd.isna(d) or pd.isna(c) or pd.isna(v):
                    full_headers.append(None)
                else:
                    full_headers.append(
                        f"{str(d).strip()} | {str(c).strip()} {str(v).strip()}"
                    )

            agri_row_idx = df[
                df[0].astype(str).str.contains("сельское", case=False, na=False)
            ].index[0]
            agri_values = df.iloc[agri_row_idx]

            period_cat_map = {}
            for i, val in enumerate(agri_values[1:], start=1):
                header = full_headers[i]
                if not header:
                    continue
                try:
                    period_raw, cat_full = header.split("|")
                    cat_full = cat_full.strip()
                    match = re.search(r"за\s(\w+)\s(\d{4})", period_raw.strip())
                    if not match:
                        continue
                    month_name, year = match.groups()
                    month = month_map.get(month_name.lower())
                    if not month:
                        continue
                    last_day = monthrange(int(year), month)[1]
                    period = f"{year}-{month:02d}-{last_day}"
                    TYPE = TYPE_MAPPING.get(cat_full)
                    if not TYPE:
                        continue
                    value = str(val).replace(" ", "").replace(",", ".")
                    value = float(value) if value and value != "nan" else 0.0
                    period_cat_map[(period, TYPE)] = {
                        "LOAD_DATE": LOAD_DATE,
                        "TYPE": TYPE,
                        "TYPE_DESCRIPTION": cat_full,
                        "AGRICULTURAL_INDUSTRY": round(value, 2),
                        "PERIOD": period,
                        "PERIOD_TYPE": "month",
                    }
                except Exception:
                    # logger.error("Неожиданная ошибка")
                    continue

            grouped = {}
            for (period, TYPE), data in period_cat_map.items():
                grouped.setdefault(period, []).append(data)

            for period, records in grouped.items():
                total = sum(r["AGRICULTURAL_INDUSTRY"] for r in records)
                records.append(
                    {
                        "LOAD_DATE": LOAD_DATE,
                        "TYPE": 1,
                        "TYPE_DESCRIPTION": "Всего",
                        "AGRICULTURAL_INDUSTRY": round(total, 2),
                        "PERIOD": period,
                        "PERIOD_TYPE": "month",
                    }
                )
                all_records.extend(records)
        except Exception:
            continue

# Обработка
if not all_records:
    logger.error("Нет данных для обработки: all_records пуст.")
    raise ValueError("Нет данных для обработки: all_records пуст.")

df_result = pd.DataFrame(all_records)
df_result["PERIOD"] = pd.to_datetime(df_result["PERIOD"])
df_result["YEAR"] = df_result["PERIOD"].dt.year

with vertica_python.connect(**settings.conn_info) as connection:
    cur = connection.cursor()
    cur.execute(f"SELECT MAX(PACKAGE_ID) FROM {target_table}")
    max_package_id = cur.fetchone()[0] or 0
    new_package_id = max_package_id + 1
    df_result["PACKAGE_ID"] = new_package_id

    df_yearly = (
        df_result.groupby(["YEAR", "TYPE", "TYPE_DESCRIPTION"])
        .agg({"AGRICULTURAL_INDUSTRY": "sum"})
        .reset_index()
    )
    df_yearly["PERIOD"] = pd.to_datetime(df_yearly["YEAR"].astype(str) + "-12-31")
    df_yearly["PERIOD_TYPE"] = "year"
    df_yearly["LOAD_DATE"] = LOAD_DATE
    df_yearly["PACKAGE_ID"] = new_package_id

    df_final = pd.concat(
        [
            df_result[
                [
                    "LOAD_DATE",
                    "PACKAGE_ID",
                    "TYPE",
                    "TYPE_DESCRIPTION",
                    "AGRICULTURAL_INDUSTRY",
                    "PERIOD",
                    "PERIOD_TYPE",
                ]
            ],
            df_yearly[
                [
                    "LOAD_DATE",
                    "PACKAGE_ID",
                    "TYPE",
                    "TYPE_DESCRIPTION",
                    "AGRICULTURAL_INDUSTRY",
                    "PERIOD",
                    "PERIOD_TYPE",
                ]
            ],
        ],
        ignore_index=True,
    )

    df_final.drop_duplicates(subset=["PERIOD", "TYPE", "PERIOD_TYPE"], inplace=True)

    insert_query = f"""
        INSERT INTO {target_table} (
            LOAD_DATE, PACKAGE_ID, TYPE, TYPE_DESCRIPTION,
            AGRICULTURAL_INDUSTRY, PERIOD, PERIOD_TYPE
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    values = [
        (
            row["LOAD_DATE"],
            row["PACKAGE_ID"],
            row["TYPE"],
            row["TYPE_DESCRIPTION"],
            row["AGRICULTURAL_INDUSTRY"],
            row["PERIOD"],
            row["PERIOD_TYPE"],
        )
        for _, row in df_final.iterrows()
    ]

    cur.executemany(insert_query, values)

logger.info(f"Успешно загружено {len(df_final)} строк.")
logger.info(f"Данные успешно загружены в Vertica с PACKAGE_ID = {new_package_id}")
