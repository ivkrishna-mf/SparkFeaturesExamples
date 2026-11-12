-- Spark Declarative Pipelines — SQL definitions live alongside the Python ones.
CREATE MATERIALIZED VIEW top_customers AS
SELECT cust_id, revenue
FROM customer_revenue
WHERE revenue > 50;
