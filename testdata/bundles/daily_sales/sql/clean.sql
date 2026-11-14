-- Cleans one day of raw orders and merges it into main.sales.orders_clean.
-- :run_date is the task parameter.
CREATE TABLE IF NOT EXISTS main.sales.orders_clean (
  order_id   BIGINT,
  order_date DATE,
  country    STRING,
  amount     DECIMAL(12, 2),
  customer   STRING
);

MERGE INTO main.sales.orders_clean AS t
USING (
  SELECT CAST(order_id AS BIGINT)          AS order_id,
         CAST(order_date AS DATE)          AS order_date,
         upper(trim(country))              AS country,
         CAST(amount AS DECIMAL(12, 2))    AS amount,
         trim(customer)                    AS customer
  FROM main.sales.orders_raw
  WHERE order_date = :run_date
    AND amount IS NOT NULL
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
