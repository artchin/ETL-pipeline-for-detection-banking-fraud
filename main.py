import os
import shutil
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import json

# функции были вынесены в py_scripts для облегчения main.py - импортируем их
import py_scripts.incr_loading
import py_scripts.loading_rep_fraud
from py_scripts.comm_funcs import (
    log_meta,
    create_tables,
    clear_stg_tables,
    update_facts,
    drop_tmp_tables
)

# загрузка данных cred.json и подключения к базе PostgresSQL
with open ('cred.json', "r") as f:
    cred = json.load(f)

conn = psycopg2.connect(**cred)
cursor = conn.cursor()

url = f"postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}"
engine = create_engine(url)

cursor.execute('set search_path to bank')


# создание таблиц из sql-скрипта
create_tables(r'sql_scripts\create_tables.sql')


# Текущий год - 2025, в файлах 2021. Чтобы сохранить историчность логов (2021 год)
# создаем глобальную переменную date_global, которая будет хранить date загруженных отчетов

date_global = None

# загрузка в sql, автоматическое создание стейджинговых таблиц,
# логирование данных в таблице META_LOADING, перемещение использованных файлов в \archive
def transactions2sql_return_date (path):
    global date_global
    df = pd.read_csv(path, sep=';')       # (decimal=',') можно использовать вместо df['amount']
    rows_processed = len(df)

    # приведение типов данных - обсуждали с Гайк на консультации
    df['amount'] = df['amount'].astype(str).str.replace(',', '.').astype(float)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    # возвращаем дату отчета в глобальную переменную
    date_global = df['transaction_date'].dt.date.iloc[0]

    df.to_sql(
        name=f"stg_transactions",
        con=engine,
        schema="bank",
        if_exists="replace",
        index=False
    )
    
    log_meta("stg_transactions", date_global, rows_processed)

    # перемещаем считанный файл в /archive
    shutil.move(path, f"archive/{os.path.basename(path)}.backup")


def passport2sql(path):
    df = pd.read_excel(path)
    rows_processed = len(df)
    df.to_sql(name="stg_passport_blacklist", con=engine, schema="bank", if_exists="replace", index=False)
    log_meta("stg_passport_blacklist", date_global, rows_processed)
    shutil.move(path, f"archive/{os.path.basename(path)}.backup")


def terminals2sql(path):
    df = pd.read_excel(path)
    rows_processed = len(df)
    df.to_sql(name="stg_terminals", con=engine, schema="bank", if_exists="replace", index=False)
    log_meta("stg_terminals", date_global, rows_processed)
    shutil.move(path, f"archive/{os.path.basename(path)}.backup")


# последовательно расскоментировать каждый день, закрывая предыдущий >

# >считывание файлов, день 1
# transactions2sql_return_date(r'data\transactions_01032021.txt')
# passport2sql (r'data\passport_blacklist_01032021.xlsx')
# terminals2sql(r'data\terminals_01032021.xlsx')

# >считывание файлов, день 2
# transactions2sql_return_date(r'data\transactions_02032021.txt')
# passport2sql (r'data\passport_blacklist_02032021.xlsx')
# terminals2sql(r'data\terminals_02032021.xlsx')

# >считывание файлов, день 3
# transactions2sql_return_date(r'data\transactions_03032021.txt')
# passport2sql (r'data\passport_blacklist_03032021.xlsx')
# terminals2sql(r'data\terminals_03032021.xlsx')

# обновить FACT-таблицы данными из стейджинга
update_facts()

# выделение инкремента и обновление DWH_DIM_TERMINALS_HIST
py_scripts.incr_loading.create_new_terminals()
py_scripts.incr_loading.create_deleted_terminals()
py_scripts.incr_loading.create_updated_terminals()
py_scripts.incr_loading.update_terminals_hist(date_global)

# удаление стейджинговых и временных таблиц
# удаление стейджинговых таблиц попадает в отчет META_LOADING
clear_stg_tables(date_global)
drop_tmp_tables()

# ежедневная загрузка данных в REP_FRAUD
py_scripts.loading_rep_fraud.loading_rep_fraud(date_global)

if cursor:
    cursor.close()
if conn:
    conn.close()
