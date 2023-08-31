-- Given a single search request, for earch listing, returns the rank at each stage of the request.

-- See this spreadsheet for more user-friendly version:
-- https://docs.google.com/spreadsheets/d/1ZjMfUjEJ1xnXa3ItrQOMnbLR_En4jrS44aaY7v1-jno

DECLARE _DATE DATE;
DECLARE _PLACEMENT STRING;
DECLARE _QUERIES ARRAY<STRING>;
DECLARE _MATCHING STRING;
DECLARE _RANKING STRING;
DECLARE _MARKETOPT STRING;
DECLARE _USERID INT64;

SET _DATE = "2023-08-29";
SET _PLACEMENT = "wsg"; -- See https://docs.etsycorp.com/searchx-docs/docs/search_placements/#search-placement-registry

-- Find current MMX Behaviors here: https://atlas.etsycorp.com/search-experiment-queue
-- Set to "ANY" to ignore.
SET _MATCHING = "ANY";
SET _RANKING = "ANY";
SET _MARKETOPT = "ANY";

-- Set to: 0 to ignore, -1 for signed-out, or >0 for signed-in.
SET _USERID = 0;

WITH multiple_requests AS (
  SELECT request.query, OrganicRequestMetadata.candidateSources, RequestIdentifiers
  FROM `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*`
  WHERE DATE(queryTime) >= CURRENT_DATE() - 90
    AND request.options.searchPlacement = _PLACEMENT
    AND request.query IN (SELECT query_raw
                          FROM `etsy-data-warehouse-dev.ecanales.top_queries_sample_last_90_days`
                          WHERE rank <= 10)
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
    LIMIT 10
),
metadata AS (
  SELECT
    RequestIdentifiers.etsyRequestUUID AS uuid,
    query,
    C.stage,
    C.source,
    C.listingIds  
  FROM multiple_requests
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
  xml     NULLS LAST
