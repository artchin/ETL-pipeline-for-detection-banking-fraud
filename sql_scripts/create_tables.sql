-- создание стейджинговых таблиц:
-- pandas to_sql автоматически создаст 3 STG таблицы, но мы можем их расскоментировать - результат не измениться

-- CREATE TABLE IF NOT EXISTS STG_TRANSACTIONS (
-- 	transaction_id VARCHAR(50),
-- 	transaction_date TIMESTAMP,
-- 	amount NUMERIC(10, 2),
-- 	card_num VARCHAR(20),
-- 	oper_type VARCHAR(10),
-- 	oper_result VARCHAR(10),
-- 	terminal VARCHAR(10)
-- );

-- CREATE TABLE IF NOT EXISTS STG_TERMINALS (
-- 	terminal_id VARCHAR(10),
-- 	terminal_type VARCHAR(10),
-- 	terminal_city VARCHAR(50),
-- 	terminal_address VARCHAR(100)
-- );

-- CREATE TABLE IF NOT EXISTS STG_PASSPORT_BLACKLIST (
-- 	passport VARCHAR(20),
-- 	date DATE
-- );

-- создание фактовых таблиц
CREATE TABLE IF NOT EXISTS DWH_FACT_TRANSACTIONS (
	transaction_id varchar (50),
	transaction_date timestamp,
	card_num varchar (50),
	oper_type varchar (20),
	amount NUMERIC(10, 2),
	oper_result varchar (20),
	terminal varchar (20)
);

CREATE TABLE IF NOT EXISTS DWH_FACT_PASSPORT_BLACKLIST (
	date date,
	passport varchar (50)
);
	

-- создание таблицы измерений
CREATE TABLE IF NOT EXISTS DWH_DIM_TERMINALS_HIST (
	terminal_id varchar (10),
	terminal_type varchar (10),
	terminal_city varchar (20),
	terminal_address varchar (100),
	effective_from date,
	effective_to date default 'infinity'::date,
	deleted_flg boolean default FALSE
);

-- отчетная таблица
CREATE TABLE IF NOT EXISTS REP_FRAUD (
	event_dt timestamp,
	passport varchar (20),
	fio varchar (100),
	phone varchar (20),
	event_type VARCHAR (100),
	report_dt date
);


-- метаданные
CREATE TABLE IF NOT EXISTS META_LOADING (
	table_name varchar (50),
	event_dt date default CURRENT_DATE,
	rows_processed INT,
	status VARCHAR (50)
);