-- THIS QUERY IS AN ATTEMPT TO USE rstewarts's SEARCH EXPLAIN LOGIC TO POPULATE A TABLE WITH A SAMPLE OF
-- THE MOST POPULAR SEARCH QUERIES, IT THEN CALCULATES BASED ON THE OUTPUT THE "P0" METRICS
-- DESCRIBED ON THIS DOC: https://docs.google.com/spreadsheets/d/1w1kwcYAiIVqY6v5gnzu63BKPj_AT2qmtbppjBaJeAaA/edit#gid=0

-- See this spreadsheet for rstewart's search explain, user-friendly version:
-- https://docs.google.com/spreadsheets/d/1ZjMfUjEJ1xnXa3ItrQOMnbLR_En4jrS44aaY7v1-jno

DECLARE _DATE DATE;
DECLARE _PLACEMENT STRING;
DECLARE _QUERIES ARRAY<STRING>;
DECLARE _MATCHING STRING;
DECLARE _RANKING STRING;
DECLARE _MARKETOPT STRING;
DECLARE _USERID INT64;
DECLARE _SAMPLE INT64;

SET _DATE = CURRENT_DATE() - 1; -- Set date range
SET _PLACEMENT = "wsg"; -- See https://docs.etsycorp.com/searchx-docs/docs/search_placements/#search-placement-registry
SET _SAMPLE = 100; --Set sample size

-- Find current MMX Behaviors here: https://atlas.etsycorp.com/search-experiment-queue
-- Set to "ANY" to ignore.
SET _MATCHING = "ANY";
SET _RANKING = "ANY";
SET _MARKETOPT = "ANY";

-- Set to: 0 to ignore, -1 for signed-out, or >0 for signed-in.
SET _USERID = 0;

--Creates a sample table of the most common queries in the past 7 days
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.top_queries_sample` AS
WITH cte AS (
SELECT query_raw, COUNT(DISTINCT visit_id) AS visits_that_used_query
FROM `etsy-data-warehouse-prod.search.query_sessions_all`
WHERE _date >= CURRENT_DATE() - 7
GROUP BY 1
)
SELECT
query_raw AS query,
visits_that_used_query,
visits_that_used_query / (SELECT COUNT(DISTINCT visit_id) FROM `etsy-data-warehouse-prod.search.query_sessions_all`WHERE _date >= CURRENT_DATE() - 7) AS pct_of_visits_covered,
RANK() OVER (ORDER BY visits_that_used_query DESC) AS rank
FROM cte
ORDER BY visits_that_used_query DESC;

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.multi_searchexplain_sample` AS

WITH query_list AS (
  SELECT query FROM `etsy-data-warehouse-dev.ecanales.top_queries_sample` WHERE rank <= _SAMPLE
  -- Add more queries here
),

-- Original single_request CTE, but now it will work with multiple queries
single_request AS (
  SELECT 
    Q.query, 
    R.OrganicRequestMetadata.candidateSources, 
    R.RequestIdentifiers
  FROM query_list Q
  CROSS JOIN `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*` R
  WHERE DATE(R.queryTime) >= _DATE
    AND R.request.options.searchPlacement = _PLACEMENT
    AND R.request.query = Q.query
    AND (request.options.mmxBehavior.matching = _MATCHING OR _MATCHING = "ANY")
    AND (request.options.mmxBehavior.ranking = _RANKING OR _RANKING = "ANY")
    AND (request.options.mmxBehavior.marketOptimization = _MARKETOPT OR _MARKETOPT = "ANY")
    AND (request.options.personalizationOptions.userId = _USERID OR _USERID <= 0)
    AND (request.options.signedInUser = FALSE OR _USERID >= 0)
    AND request.offset = 0
    AND request.sortBy = 'relevance'
    AND request.sortOrder = 'desc'
    AND request.options.queryType = "ponycorn_seller_quality"
    AND request.options.cacheBucketId = "live|web"
    AND request.options.csrOrganic = TRUE
    LIMIT 10000
),
metadata AS (
  SELECT
    RequestIdentifiers.etsyRequestUUID AS uuid,
    query,
    C.stage,
    C.source,
    C.listingIds  
  FROM single_request
  CROSS JOIN UNNEST(candidateSources) AS C
  WHERE C.interleavedBehavior IS NULL
),
solr    AS (SELECT uuid, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "POST_FILTER" AND M.source = "SOLR" ),
nir     AS (SELECT uuid, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "POST_FILTER" AND M.source = "ANN"  ),
xwalk   AS (SELECT uuid, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "POST_FILTER" AND M.source = "XWALK"),
xml     AS (SELECT uuid, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "POST_FILTER" AND M.source = "XML"  ),
borda   AS (SELECT uuid, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "POST_BORDA"  AND M.source IS NULL  ),
ranking AS (SELECT uuid, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "RANKING"     AND M.source IS NULL  ),
mo_list AS (SELECT uuid, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "MO_LISTWISE" AND M.source IS NULL  ),
mo_last AS (SELECT uuid, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "MO_LASTPASS" AND M.source IS NULL  )

SELECT
  mo_last.uuid,
  mo_last.query,
  listing_id,
  solr.rank    AS solr,
  nir.rank     AS nir,
  xwalk.rank   AS xwalk,
  xml.rank     AS xml,
  borda.rank   AS borda,
  ranking.rank AS ranking,
  mo_list.rank AS mo_list,
  mo_last.rank AS mo_last
FROM (SELECT * FROM mo_last  WHERE rank <= 1000) AS mo_last
  LEFT JOIN (SELECT * FROM mo_list  WHERE rank <= 1000) AS mo_list USING (uuid,query,listing_id)
  LEFT JOIN (SELECT * FROM ranking  WHERE rank <= 1000) AS ranking USING (uuid,query,listing_id)
  LEFT JOIN (SELECT * FROM borda  WHERE rank <= 1000) AS borda   USING (uuid,query,listing_id)
  LEFT JOIN (SELECT * FROM solr  WHERE rank <= 1000) AS solr    USING (uuid,query,listing_id)
  LEFT JOIN (SELECT * FROM nir  WHERE rank <= 1000) AS nir     USING (uuid,query,listing_id)
  LEFT JOIN (SELECT * FROM xwalk  WHERE rank <= 1000) AS xwalk   USING (uuid,query,listing_id)
  LEFT JOIN (SELECT * FROM xml  WHERE rank <= 1000) AS xml     USING (uuid,query,listing_id)
ORDER BY
  query,
  mo_last NULLS LAST,
  mo_list NULLS LAST,
  ranking NULLS LAST,
  borda   NULLS LAST,
  solr    NULLS LAST,
  nir     NULLS LAST,
  xwalk   NULLS LAST,
  xml     NULLS LAST;


--STEP 3: Calculate metrics from table
--overall stats (first page)
SELECT
SUM(CASE WHEN (solr IS NOT NULL AND mo_last <= 48) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 48 THEN 1 ELSE 0 END) AS solr_first_page_share,
SUM(CASE WHEN (nir IS NOT NULL AND mo_last <= 48) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 48 THEN 1 ELSE 0 END) AS nir_first_page_share,
SUM(CASE WHEN (xwalk IS NOT NULL AND mo_last <= 48) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 48 THEN 1 ELSE 0 END) AS xwalk_first_page_share,
AVG(CASE WHEN mo_last <= 48 THEN borda ELSE NULL END) AS borda_avg_first_page_rank,
AVG(CASE WHEN mo_last <= 48 THEN ranking ELSE NULL END) AS ranking_avg_first_page_rank,
AVG(CASE WHEN mo_last <= 48 THEN mo_last ELSE NULL END) AS mo_avg_first_page_rank
FROM `etsy-data-warehouse-dev.ecanales.multi_searchexplain_sample`;

--query level stats (first page)
SELECT
query,
SUM(CASE WHEN (solr IS NOT NULL AND mo_last <= 48) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 48 THEN 1 ELSE 0 END) AS solr_first_page_share,
SUM(CASE WHEN (nir IS NOT NULL AND mo_last <= 48) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 48 THEN 1 ELSE 0 END) AS nir_first_page_share,
SUM(CASE WHEN (xwalk IS NOT NULL AND mo_last <= 48) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 48 THEN 1 ELSE 0 END) AS xwalk_first_page_share,
AVG(CASE WHEN mo_last <= 48 THEN borda ELSE NULL END) AS borda_avg_first_page_rank,
AVG(CASE WHEN mo_last <= 48 THEN ranking ELSE NULL END) AS ranking_avg_first_page_rank,
AVG(CASE WHEN mo_last <= 48 THEN mo_last ELSE NULL END) AS mo_avg_first_page_rank
FROM `etsy-data-warehouse-dev.ecanales.multi_searchexplain_sample`
GROUP BY 1;
