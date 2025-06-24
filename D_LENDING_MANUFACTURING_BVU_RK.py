import gc
import logging
import re
from calendar import monthrange
from collections import defaultdict
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests
import vertica_python
from bs4 import BeautifulSoup

from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s.%(msecs)03d] %(module)s:%(lineno)d %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M",
    filename="logs/lending-manufacturing.log",
    encoding="utf-8",
)

logger = logging.getLogger(__name__)


BASE_URL = "https://www.nationalbank.kz"
RUBRIC_URLS = [
    "https://www.nationalbank.kz/ru/news/banking-sector-loans-to-economy-analytics/rubrics/2204",
    "https://www.nationalbank.kz/ru/news/banking-sector-loans-to-economy-analytics/rubrics/1985",
    "https://www.nationalbank.kz/ru/news/banking-sector-loans-to-economy-analytics/rubrics/1907",
    "https://www.nationalbank.kz/ru/news/banking-sector-loans-to-economy-analytics/rubrics/2319",
]
SEARCH_PHRASE = "Кредиты банковского сектора субъектам предпринимательства по видам экономической деятельности"
TARGET_SHEET_NAME = "Выдано"
TARGET_TYPES = {
    1: "2. Обрабатывающая промышленность",
    2: "3. Прочие отрасли промышленности",
    3: "Транспорт и складирование",
    4: "Информация и связь",
}


logger.info("Инициализация парсера для листа 'Выдано'...")

TABLE_NAME = "DWH.D_LENDING_MANUFACTURING_BVU_RK"


def make_unique_columns(columns):
    seen = defaultdict(int)
    unique = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            unique.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            unique.append(col)
    return unique


def parse_sheet_custom(xls, timestamp, package_id):
    local_data = []

    if TARGET_SHEET_NAME not in xls.sheet_names:
        logger.error("   -> Лист 'Выдано' не найден.")
        return []

    logger.info("   -> Чтение листа 'Выдано'...")
    df = xls.parse(TARGET_SHEET_NAME, header=None)

    try:
        date_row = df.iloc[3].ffill()
        metric_row = df.iloc[4].ffill()
    except Exception:
        logger.error("   -> Ошибка чтения заголовков.")
        return []

    columns = []
    for i, (d, m) in enumerate(zip(date_row, metric_row)):
        if pd.isna(d) or pd.isna(m):
            columns.append(f"col_{i}")
        else:
            columns.append(f"{str(d).strip()}_{str(m).strip()}")

    df.columns = make_unique_columns(columns)
    df = df.iloc[5:].reset_index(drop=True)

    df.rename(columns={df.columns[0]: "Отрасли экономики"}, inplace=True)
    df = df[df["Отрасли экономики"].notna()]

    sum_columns = [
        col for col in df.columns if col.endswith("Сумма") and not col.startswith("за")
    ]
    logger.info(f"   -> Найдено {len(sum_columns)} колонок с суммами.")

    melted_df = df.melt(
        id_vars=["Отрасли экономики"],
        value_vars=sum_columns,
        var_name="period",
        value_name="ISSUED_LOAN_SUM",
    )

    melted_df.dropna(subset=["ISSUED_LOAN_SUM"], inplace=True)
    melted_df = melted_df[melted_df["ISSUED_LOAN_SUM"] != "-"]

    records = []

    for _, row in melted_df.iterrows():
        raw_period = row["period"].split("_")[0]
        match = re.match(r"(\d{2})\.(\d{2})", raw_period)
        if not match:
            continue
        month, year_suffix = map(int, match.groups())
        year = 2000 + year_suffix
        last_day = monthrange(year, month)[1]
        period = f"{year}-{month:02d}-{last_day}"

        desc = re.sub(r"\s+", " ", str(row["Отрасли экономики"]).strip())
        value = row["ISSUED_LOAN_SUM"]

        type_id = next(
            (
                k
                for k, v in TARGET_TYPES.items()
                if re.sub(r"\s+", " ", v.strip()) == desc
            ),
            None,
        )
        if type_id:
            records.append(
                {
                    "LOAD_DATE": timestamp,
                    "TYPE": type_id,
                    "TYPE_DESCRIPTION": desc,
                    "PERIOD": period,
                    "PERIOD_TYPE": "month",
                    "ISSUED_LOAN_SUM": float(value),
                    "PACKAGE_ID": package_id,
                }
            )

    # Добавляем годовые суммы
    df_months = pd.DataFrame(records)
    df_grouped = df_months.copy()
    df_grouped["YEAR"] = df_grouped["PERIOD"].str[:4]
    grouped = (
        df_grouped.groupby(["TYPE", "TYPE_DESCRIPTION", "YEAR", "PACKAGE_ID"])
        .agg({"ISSUED_LOAN_SUM": "sum"})
        .reset_index()
    )

    for _, row in grouped.iterrows():
        year_end_date = f"{row['YEAR']}-12-31"
        records.append(
            {
                "LOAD_DATE": timestamp,
                "TYPE": row["TYPE"],
                "TYPE_DESCRIPTION": row["TYPE_DESCRIPTION"],
                "PERIOD": year_end_date,
                "PERIOD_TYPE": "year",
                "ISSUED_LOAN_SUM": row["ISSUED_LOAN_SUM"],
                "PACKAGE_ID": row["PACKAGE_ID"],
            }
        )

    return records


# Получение нового PACKAGE_ID
with vertica_python.connect(**settings.conn_info) as conn:
    cursor = conn.cursor()
    cursor.execute(f"SELECT COALESCE(MAX(PACKAGE_ID), 0) FROM {TABLE_NAME}")
    max_package_id = cursor.fetchone()[0]
    PACKAGE_ID = max_package_id + 1
    logger.info(f"Новый PACKAGE_ID: {PACKAGE_ID}")

#  Сбор ссылок и парсинг
logger.info("\U0001f50d Сбор ссылок...")
report_links = []
for url in RUBRIC_URLS:
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup.find_all("a", string=lambda t: t and SEARCH_PHRASE in t):
        href = tag.get("href")
        if href and href.startswith("/"):
            report_links.append((BASE_URL + href, tag.string.strip()))

logger.info(f" Найдено ссылок: {len(report_links)}")
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
final_data = []

for link, title in report_links:
    logger.info(f"--- Обработка: {title} ---")
    try:
        resp = requests.get(link, timeout=30)
        resp.raise_for_status()
        xls = pd.ExcelFile(BytesIO(resp.content), engine="openpyxl")
        final_data.extend(parse_sheet_custom(xls, timestamp, PACKAGE_ID))
        del xls, resp
        gc.collect()
    except Exception as e:
        logger.error(f"   -> Ошибка: {e}")

# Загрузка в Vitрину
logger.info("Финализация...")
if final_data:
    df = pd.DataFrame(final_data)
    df = df[pd.to_numeric(df["ISSUED_LOAN_SUM"], errors="coerce").notnull()]
    df["ISSUED_LOAN_SUM"] = df["ISSUED_LOAN_SUM"].astype(float)
    df.drop_duplicates(
        subset=["TYPE", "PERIOD", "PACKAGE_ID", "PERIOD_TYPE"], inplace=True
    )
    df["TYPE_DESCRIPTION"] = (
        df["TYPE_DESCRIPTION"].str.replace(r"^\d+\.\s*", "", regex=True).str.strip()
    )

    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        LOAD_DATE,
        TYPE,
        TYPE_DESCRIPTION,
        PERIOD,
        PERIOD_TYPE,
        ISSUED_LOAN_SUM,
        PACKAGE_ID
    ) VALUES (:LOAD_DATE, :TYPE, :TYPE_DESCRIPTION, :PERIOD, :PERIOD_TYPE, :ISSUED_LOAN_SUM, :PACKAGE_ID)
    """

    with vertica_python.connect(**settings.conn_info) as conn:
        cursor = conn.cursor()
        cursor.executemany(insert_query, df.to_dict(orient="records"))
        conn.commit()
        logger.info(f"Загружено в витрину: {len(df)} строк.")
else:
    logger.error("Данные не найдены.")
