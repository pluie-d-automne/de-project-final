DROP TABLE IF EXISTS currencies;

CREATE TABLE currencies (
id IDENTITY(1,1) NOT NULL,
date_update timestamp NOT NULL,
currency_code int NOT NULL,
currency_code_with int NOT NULL,
currency_with_div numeric(14,2) NOT NULL,
vertica_update_dttm timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
)
ORDER BY date_update
SEGMENTED BY hash(date_update) all nodes;

DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions(
operation_id varchar NOT NULL,account_number_from bigint NOT NULL,account_number_to bigint NOT  NULL,currency_code int NOT NULL,country varchar NOT NULL,status varchar NOT NULL,transaction_type varchar NOT NULL,amount numeric(14, 2),transaction_dt timestamp,
transaction_date timestamp DEFAULT DATE_TRUNC('DAY', transaction_dt),
vertica_update_dttm timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
)
ORDER BY transaction_date, operation_id
SEGMENTED BY hash(transaction_date, operation_id) all nodes;

 