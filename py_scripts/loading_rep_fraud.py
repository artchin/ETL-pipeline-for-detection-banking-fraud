import psycopg2
import json
from py_scripts.comm_funcs import log_meta

with open ('cred.json', "r") as f:
    cred = json.load(f)

conn = psycopg2.connect(**cred)
cursor = conn.cursor()

cursor.execute('set search_path to bank')

# ежедневное создание итогового отчета - date_global
def loading_rep_fraud (date_global):
    cursor.execute("""
        INSERT INTO REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT 
            t.transaction_date as event_dt,
            cl.passport_num as passport,
            CONCAT(cl.last_name, ' ', cl.first_name, ' ', COALESCE(cl.patronymic, '')) as fio,
            cl.phone as phone,
            'blocked or expired passport' as event_type,
            %(date_global)s as report_dt
        FROM DWH_FACT_TRANSACTIONS t
        JOIN cards ca ON t.card_num = ca.card_num
        JOIN accounts ac ON ca.account = ac.account
        JOIN clients cl ON ac.client = cl.client_id
        LEFT JOIN DWH_FACT_PASSPORT_BLACKLIST b1 ON cl.passport_num = b1.passport
        WHERE (
         -- Паспорт просрочен на момент транзакции
        (cl.passport_valid_to < t.transaction_date::date) 
        OR 
        -- Паспорт в черном списке И транзакция произошла после даты включения в черный список
        (b1.passport IS NOT NULL AND t.transaction_date::date > b1.date)
        )
        AND t.oper_result = 'SUCCESS';
    """, {"date_global": date_global})

    rows_processed = cursor.rowcount

    log_meta('REP_FRAUD_passport', date_global, rows_processed, 'SUCCESS')

    cursor.execute("""
        INSERT INTO REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT
            t.transaction_date as event_dt,
            cl.passport_num as passport,
            CONCAT(cl.last_name, ' ', cl.first_name, ' ', COALESCE(cl.patronymic, '')) as fio,
            cl.phone as phone,
            'invalid contract' as event_type,
            %(date_global)s as report_dt
        FROM DWH_FACT_TRANSACTIONS t
        JOIN cards ca ON t.card_num = ca.card_num
        JOIN accounts ac ON ca.account = ac.account
        JOIN clients cl ON ac.client = cl.client_id
        WHERE t.transaction_date::date > ac.valid_to
        AND t.oper_result = 'SUCCESS';
    """, {"date_global": date_global})

    rows_processed = cursor.rowcount

    log_meta('REP_FRAUD_contract', date_global, rows_processed, 'SUCCESS')

    cursor.execute("""
        INSERT INTO REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT DISTINCT ON (t2.transaction_date, cl.passport_num)
            t2.transaction_date as event_dt,
            cl.passport_num as passport,
            CONCAT(cl.last_name, ' ', cl.first_name, ' ', COALESCE(cl.patronymic, '')) as fio,
            cl.phone as phone,
            'ops in diff cities less one hour' as event_type,
            %(date_global)s as report_dt
        FROM DWH_FACT_TRANSACTIONS t1
        JOIN DWH_FACT_TRANSACTIONS t2 ON t1.card_num = t2.card_num
            AND t1.transaction_date < t2.transaction_date
            AND EXTRACT(EPOCH FROM (t2.transaction_date - t1.transaction_date)) < 3600
        JOIN cards ca ON t2.card_num = ca.card_num
        JOIN accounts ac ON ca.account = ac.account
        JOIN clients cl ON ac.client = cl.client_id
        JOIN DWH_DIM_TERMINALS_HIST th1 ON t1.terminal = th1.terminal_id
            AND t1.transaction_date BETWEEN th1.effective_from AND th1.effective_to
        JOIN DWH_DIM_TERMINALS_HIST th2 ON t2.terminal = th2.terminal_id
            AND t2.transaction_date BETWEEN th2.effective_from AND th2.effective_to
        WHERE th1.terminal_city <> th2.terminal_city
        AND t2.oper_result = 'SUCCESS'
        AND NOT EXISTS (
            SELECT 1 FROM REP_FRAUD rf 
            WHERE rf.passport = cl.passport_num 
            AND rf.event_dt = t2.transaction_date
            AND rf.event_type = 'ops in diff cities less one hour'
        );
        """, {"date_global": date_global})
    
    rows_processed = cursor.rowcount

    log_meta('REP_FRAUD_diff_cities', date_global, rows_processed, 'SUCCESS')

    cursor.execute("""
        INSERT INTO REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)
            SELECT DISTINCT ON (t3.transaction_date, cl.passport_num)
                t3.transaction_date as event_dt,
                cl.passport_num as passport,
                CONCAT(cl.last_name, ' ', cl.first_name, ' ', COALESCE(cl.patronymic, '')) as fio,
                cl.phone as phone,
                'amount guessing' as event_type,
                %(date_global)s as report_dt
            FROM DWH_FACT_TRANSACTIONS t1
            JOIN DWH_FACT_TRANSACTIONS t2 ON t1.card_num = t2.card_num
                AND t1.transaction_date < t2.transaction_date
                AND EXTRACT(EPOCH FROM (t2.transaction_date - t1.transaction_date)) < 1200
            JOIN DWH_FACT_TRANSACTIONS t3 ON t2.card_num = t3.card_num
                AND t2.transaction_date < t3.transaction_date
                AND EXTRACT(EPOCH FROM (t3.transaction_date - t2.transaction_date)) < 1200
            JOIN cards ca ON t3.card_num = ca.card_num
            JOIN accounts ac ON ca.account = ac.account
            JOIN clients cl ON ac.client = cl.client_id
            WHERE t1.oper_result = 'REJECT'
                AND t2.oper_result = 'REJECT'
                AND t3.oper_result = 'SUCCESS'
                AND t1.amount > t2.amount
                AND t2.amount > t3.amount
                AND EXTRACT(EPOCH FROM (t3.transaction_date - t1.transaction_date)) < 1200
            AND NOT EXISTS (
                SELECT 1 FROM REP_FRAUD rf 
                WHERE rf.passport = cl.passport_num 
                AND rf.event_dt = t3.transaction_date
                AND rf.event_type = 'amount guessing'
    );
    """, {"date_global": date_global})

    rows_processed = cursor.rowcount

    log_meta('REP_FRAUD_attempt_amount', date_global, rows_processed, 'SUCCESS')

    conn.commit()