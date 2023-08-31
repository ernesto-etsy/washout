--Create table of sample queries for Search Explain
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.top_queries_sample_last_90_days` AS
WITH cte AS (
SELECT query_raw, COUNT(DISTINCT visit_id) AS visits_that_used_query
FROM `etsy-data-warehouse-prod.search.query_sessions_all`
WHERE _date >= CURRENT_DATE() - 90
GROUP BY 1
)
SELECT
query_raw,
visits_that_used_query,
visits_that_used_query / (SELECT COUNT(DISTINCT visit_id) FROM `etsy-data-warehouse-prod.search.query_sessions_all`WHERE _date >= CURRENT_DATE() - 90) AS pct_of_visits_covered,
RANK() OVER (ORDER BY visits_that_used_query DESC) AS rank
FROM cte
ORDER BY visits_that_used_query DESC;