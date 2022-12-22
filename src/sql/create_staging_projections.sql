DROP PROJECTION IF EXISTS currencies_date;

CREATE PROJECTION currencies_date (id, date_update, currency_code, currency_code_with, currency_with_div, vertica_update_dttm) 
AS SELECT id, date_update, currency_code, currency_code_with, currency_with_div, vertica_update_dttm FROM currencies
ORDER BY date_update
SEGMENTED BY hash(date_update) ALL NODES KSAFE;

DROP PROJECTION IF EXISTS transactions_date;

CREATE PROJECTION transactions_date (operation_id, account_number_from, account_number_to, currency_code, country,
									 status, transaction_type, amount, transaction_dt, transaction_date, vertica_update_dttm)
AS SELECT operation_id, account_number_from, account_number_to, currency_code, country,
status, transaction_type, amount, transaction_dt, transaction_date, vertica_update_dttm
FROM transactions
ORDER BY transaction_date
SEGMENTED BY hash(transaction_date, operation_id) ALL NODES KSAFE;
