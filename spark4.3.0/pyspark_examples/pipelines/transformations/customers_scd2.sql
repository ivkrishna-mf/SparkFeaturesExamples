-- The same flow in SQL (Spark 4.3): STORED AS SCD TYPE 2 / TRACK HISTORY ON
CREATE STREAMING TABLE customers_history_sql;

CREATE FLOW customers_history_flow AS AUTO CDC INTO customers_history_sql
FROM STREAM(raw.customer_cdc_feed)
KEYS (customer_id)
APPLY AS DELETE WHEN op = 'DELETE'
SEQUENCE BY seq
COLUMNS * EXCEPT (op, seq)
STORED AS SCD TYPE 2
TRACK HISTORY ON (city, plan);
