DROP TABLE IF EXISTS global_metrics;

CREATE TABLE global_metrics (
date_update date NOT NULL,
currency_from int NOT NULL,
amount_total numeric(14,2) NOT NULL,
cnt_transactions int NOT NULL,
avg_transactions_per_account float NOT NULL,
cnt_accounts_make_transactions int NOT NULL)
ORDER BY date_update, currency_from
SEGMENTED BY hash(date_update, currency_from) all nodes;
