import psycopg2
import json

with open ('cred.json', "r") as f:
    cred = json.load(f)

conn = psycopg2.connect(**cred)
cursor = conn.cursor()

cursor.execute('set search_path to bank')

# выделение инкремента и обновление DWH_DIM_TERMINALS_HIST
def create_new_terminals():
    cursor.execute("""
        CREATE TABLE stg_new_terminals AS
            SELECT 
                t1.terminal_id,
                t1.terminal_type,
                t1.terminal_city,
                t1.terminal_address
            FROM STG_TERMINALS t1
            LEFT JOIN DWH_DIM_TERMINALS_HIST t2
            ON t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id IS NULL
        """)
    conn.commit()

def create_deleted_terminals():
    cursor.execute("""
        CREATE TABLE stg_deleted_terminals AS
            SELECT 
                t1.terminal_id,
                t1.terminal_type,
                t1.terminal_city,
                t1.terminal_address
            FROM DWH_DIM_TERMINALS_HIST t1
            LEFT JOIN STG_TERMINALS t2
            ON t1.terminal_id = t2.terminal_id
            WHERE t2.terminal_id IS NULL
        """)
    conn.commit()

def create_updated_terminals():
    cursor.execute("""
        CREATE TABLE stg_updated_terminals AS
            SELECT 
                t1.terminal_id,
                t1.terminal_type,
                t1.terminal_city,
                t1.terminal_address
            FROM STG_TERMINALS t1
            JOIN DWH_DIM_TERMINALS_HIST t2
            ON t1.terminal_id = t2.terminal_id
            WHERE t1.terminal_type IS DISTINCT FROM t2.terminal_type
                OR t1.terminal_city IS DISTINCT FROM t2.terminal_city
                OR t1.terminal_address IS DISTINCT FROM t2.terminal_address
        """)
    conn.commit()

def update_terminals_hist(date_global):
    cursor.execute("""
        INSERT INTO DWH_DIM_TERMINALS_HIST (
        	terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from
            )
        SELECT 
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            %(date_global)s 
        FROM stg_new_terminals
    """, {"date_global": date_global})


    cursor.execute("""
        UPDATE DWH_DIM_TERMINALS_HIST
        SET effective_to = %(date_global)s - interval '1 day'
        WHERE terminal_id in (SELECT terminal_id from stg_updated_terminals)
        AND effective_to = 'infinity'::date
    """, {"date_global": date_global})

    cursor.execute("""
        INSERT INTO DWH_DIM_TERMINALS_HIST (
        	terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from
            )
        SELECT 
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            %(date_global)s 
        FROM stg_updated_terminals
    """, {"date_global": date_global})

    cursor.execute("""
        UPDATE DWH_DIM_TERMINALS_HIST
        SET effective_to = %(date_global)s - interval '1 day'
        WHERE terminal_id in (SELECT terminal_id from stg_deleted_terminals)
        AND effective_to = 'infinity'::date
    """, {"date_global": date_global})

    cursor.execute("""
        INSERT INTO DWH_DIM_TERMINALS_HIST (
        	terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            effective_from,
            deleted_flg
            )
        SELECT 
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            %(date_global)s,
            True
        FROM stg_deleted_terminals
    """, {"date_global": date_global})

    conn.commit()