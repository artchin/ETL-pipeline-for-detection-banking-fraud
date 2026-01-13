import psycopg2
import json

with open ('cred.json', "r") as f:
    cred = json.load(f)

conn = psycopg2.connect(**cred)
cursor = conn.cursor()

cursor.execute('set search_path to bank')

# функция логирования данных в таблицу META_LOADING
def log_meta(table_name, event_dt, rows_processed, status="SUCCESS"):
    cursor.execute(
        "INSERT INTO META_LOADING (table_name, event_dt, rows_processed, status) VALUES (%s, %s, %s, %s)",
        (table_name, event_dt, rows_processed, status)
    )
    conn.commit()

# создание пустых таблиц
def create_tables(path):
    with open (path, "r", encoding='utf-8') as f:
        sql_script = f.read()
    cursor.execute(sql_script)
    conn.commit()

# функция очистки всех STG-таблиц с занесением событий в META_LOADING
def clear_stg_tables(date_global):
    stg_tables = [
        'stg_transactions',
        'stg_terminals', 
        'stg_passport_blacklist'
    ]
    
    try:
        for table in stg_tables:
            cursor.execute(f"TRUNCATE TABLE {table}")
            log_meta(f'CLEAR_{table}', date_global, 0, 'SUCCESS')
        
        conn.commit()

    except Exception as e:
        print(f"Ошибка при очистке STG-таблиц: {e}")
        log_meta('CLEAR_STG_TABLES', date_global, 0, f'ERROR: {str(e)}')

# обновление таблиц FACT
def update_facts():
    cursor.execute( """
        INSERT INTO DWH_FACT_PASSPORT_BLACKLIST (date, passport)  
	    SELECT date, passport FROM STG_PASSPORT_BLACKLIST stg
        WHERE NOT EXISTS (
            SELECT 1 FROM DWH_FACT_PASSPORT_BLACKLIST dwh 
            WHERE stg.passport = dwh.passport
        )
    """)

    cursor.execute( """
        INSERT INTO dwh_fact_transactions (
        transaction_id,
        transaction_date,
        card_num,
        oper_type,
        amount,
        oper_result,
        terminal
	)
	    SELECT
            transaction_id,
            transaction_date,
            card_num,
            oper_type,
            amount,
            oper_result,
            terminal
        FROM STG_transactions
	""")

    conn.commit()

# удаление временных таблиц
def drop_tmp_tables():
    cursor.execute("DROP TABLE IF EXISTS stg_new_terminals")
    cursor.execute("DROP TABLE IF EXISTS stg_deleted_terminals")
    cursor.execute("DROP TABLE IF EXISTS stg_updated_terminals")
    conn.commit()