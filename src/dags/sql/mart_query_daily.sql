DROP TABLE IF EXISTS daily_upd;
CREATE TEMPORARY TABLE daily_upd
(
    date_update date NOT NULL,
    currency_from int NOT NULL,
    amount_total numeric(14,2) NOT NULL,
    cnt_transactions int NOT NULL,
    avg_transactions_per_account float NOT NULL,
    cnt_accounts_make_transactions int NOT NULL
);

INSERT INTO daily_upd (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
WITH t AS (
	SELECT operation_id,
	   account_number_from,
	   account_number_to,
	   currency_code,
	   country,
	   status,
	   transaction_type,
	   amount,
	   transaction_date,
	   ROW_NUMBER() OVER (PARTITION BY operation_id ORDER BY vertica_update_dttm DESC) as rn
	FROM PDKUDRYAVTSEVAYANDEXRU__STAGING.transactions t
	WHERE account_number_to >=0
		  AND account_number_from >=0
	  	  AND transaction_date = {execution_date}
), c AS (	  
	SELECT date_update,
	   currency_code,
	   currency_code_with,
	   currency_with_div,
	   ROW_NUMBER() OVER (PARTITION BY currency_code,  currency_code_with ORDER BY vertica_update_dttm DESC) as rn
	FROM PDKUDRYAVTSEVAYANDEXRU__STAGING.currencies c 
	WHERE currency_code = 420 --currency_code in USA
		  AND date_update = {execution_date}
)
SELECT t.transaction_date::date AS date_update,
	   t.currency_code AS currency_from,
	   SUM(t.amount * (CASE t.currency_code WHEN 420 THEN 1 ELSE c.currency_with_div END)) AS amount_total,
	   COUNT(DISTINCT t.operation_id) AS cnt_transactions,
	   COUNT(DISTINCT t.operation_id)/COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
	   COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
	FROM t LEFT JOIN c
		ON t.currency_code = c.currency_code_with AND c.rn = 1
		WHERE t.rn=1
	GROUP BY  t.transaction_date, t.currency_code;
	
MERGE INTO
PDKUDRYAVTSEVAYANDEXRU__DWH.global_metrics gm
USING daily_upd s
ON gm.date_update = s.date_update AND gm.currency_from=s.currency_from
WHEN MATCHED THEN UPDATE
SET amount_total = s.amount_total,
	cnt_transactions = s.cnt_transactions,
	avg_transactions_per_account = s.avg_transactions_per_account,
	cnt_accounts_make_transactions = s.cnt_accounts_make_transactions
WHEN NOT MATCHED THEN
INSERT (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
VALUES (s.date_update, s.currency_from, s.amount_total, s.cnt_transactions, s.avg_transactions_per_account, s.cnt_accounts_make_transactions);

DROP TABLE IF EXISTS daily_upd;
