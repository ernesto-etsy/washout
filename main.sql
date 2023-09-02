--Get all uuids and queries from an experiment and start date
--IN ORDER TO RUN THIS SCRIPT:
--1. SET A CONFIG FLAG FROM A SEARCH EXPERIMENT
--2. CHOOSE A DATE THAT FALLS WITHIN THE TIMELINE OF THAT EXPERIMENT
--3. IF YOU WANT A BIGGER SAMPLE, INCREASE THE LIMIT STATEMENT OR REMOVE IT (LINE 32)
--4. REPLACE ALL INSTANCES OF "ecanales" WITH YOUR OWN DATASET NAME


-------------PART 1: GATHER A SAMPLE OF UUIDS FROM AN EXPERIMENT-------------
DECLARE d DATE;
DECLARE config STRING;


SET config = 'ranking/search.mmx.2023_q3.dir_taxo_boost_50_max_web';
SET _date = '2023-08-07'; --random date within experiment boundary to lower processing time

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.experiment_uuids_sample` AS

SELECT
  properties.value AS request_uuid,
  ab.key AS ab_flag,
  ab.value.value_tuple[OFFSET(0)] AS ab_flag_variant,
FROM
  `etsy-visit-pipe-prod.canonical.beacons_recent` b
INNER JOIN
  UNNEST(b.ab.map) ab, UNNEST(b.properties.map) properties
WHERE
  DATE(_PARTITIONTIME) = _date
  AND properties.key = "request_uuid"
  AND ab.key = config
  AND ab.value.value_tuple[OFFSET(0)] <> 'ineligible'
GROUP BY
  request_uuid, ab_flag, ab_flag_variant
ORDER BY
  RAND()
LIMIT 10000;

-------------PART 2: PULL ALL ASSOCIATED VISIT IDS AND SEARCH QUERIES FROM EXPERIMENT SAMPLE-------------

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.experiment_uuids_sample_searches` AS

with events as (
  select
    visit_id,
    DATE(_PARTITIONTIME) AS _date,
    (select value from unnest(beacon.properties.key_value) where key = 'mmx_request_uuid') mmx_request_uuid,
    (select value from unnest(beacon.properties.key_value) where key = 'request_uuid') request_uuid,
    (select value from unnest(beacon.properties.key_value) where key = 'mmx_variant') mmx_variant,
    (select value from unnest(beacon.properties.key_value) where key = 'query') query,
    beacon.timestamp
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons` 
where beacon.event_name = 'search'
and DATE(_PARTITIONTIME) =  '2023-08-07'
)
SELECT e._date,s.request_uuid,s.ab_flag,e.visit_id,e.query
FROM `etsy-data-warehouse-dev.ecanales.experiment_uuids_sample` s
  JOIN events e on e.request_uuid = s.request_uuid
WHERE e.query IS NOT NULL;

-------------PART 3: PULL FROM RPC LOGS-------------

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.multi_searchexplain_sample` AS
WITH requests AS (
SELECT 
    Q.*,
    R.OrganicRequestMetadata.candidateSources, 
    R.RequestIdentifiers
  FROM `etsy-data-warehouse-dev.ecanales.experiment_uuids_sample_searches` Q
  CROSS JOIN `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*` R
  WHERE DATE(R.queryTime) = Q._date
    AND R.request.options.searchPlacement = "wsg"
    AND RequestIdentifiers.etsyRequestUUID = Q.request_uuid
    AND request.sortBy = 'relevance'
    AND request.sortOrder = 'desc'
),
metadata AS (
  SELECT
    _date,
    request_uuid,
    ab_flag,
    visit_id,
    query,
    C.stage,
    C.source,
    C.listingIds  
  FROM requests
  CROSS JOIN UNNEST(candidateSources) AS C
  WHERE C.interleavedBehavior IS NULL
),
solr    AS (SELECT _date, request_uuid, ab_flag, visit_id, query, listing_id, ROW_NUMBER() OVER (PARTITION BY request_uuid) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "POST_FILTER" AND M.source = "SOLR" ),
nir     AS (SELECT _date, request_uuid, ab_flag, visit_id, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "POST_FILTER" AND M.source = "ANN"  ),
xwalk   AS (SELECT _date, request_uuid, ab_flag, visit_id, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "POST_FILTER" AND M.source = "XWALK"),
xml     AS (SELECT _date, request_uuid, ab_flag, visit_id, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "POST_FILTER" AND M.source = "XML"  ),
borda   AS (SELECT _date, request_uuid, ab_flag, visit_id, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "POST_BORDA"  AND M.source IS NULL  ),
ranking AS (SELECT _date, request_uuid, ab_flag, visit_id, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "RANKING"     AND M.source IS NULL  ),
mo_list AS (SELECT _date, request_uuid, ab_flag, visit_id, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "MO_LISTWISE" AND M.source IS NULL  ),
mo_last AS (SELECT _date, request_uuid, ab_flag, visit_id, query, listing_id, ROW_NUMBER() OVER (PARTITION BY query) AS rank FROM metadata M CROSS JOIN UNNEST(listingIds) AS listing_id WHERE M.stage = "MO_LASTPASS" AND M.source IS NULL  )

SELECT
  mo_last._date,
  mo_last.request_uuid,
  mo_last.ab_flag,
  mo_last.visit_id,
  mo_last.query,
  solr.rank    AS solr,
  nir.rank     AS nir,
  xwalk.rank   AS xwalk,
  xml.rank     AS xml,
  borda.rank   AS borda,
  ranking.rank AS ranking,
  mo_list.rank AS mo_list,
  mo_last.rank AS mo_last
FROM (SELECT * FROM mo_last  WHERE rank <= 1000) AS mo_last
  LEFT JOIN (SELECT * FROM mo_list  WHERE rank <= 1000) AS mo_list USING (request_uuid,listing_id)
  LEFT JOIN (SELECT * FROM ranking  WHERE rank <= 1000) AS ranking USING (request_uuid,listing_id)
  LEFT JOIN (SELECT * FROM borda  WHERE rank <= 1000) AS borda   USING (request_uuid,listing_id)
  LEFT JOIN (SELECT * FROM solr  WHERE rank <= 1000) AS solr    USING (request_uuid,listing_id)
  LEFT JOIN (SELECT * FROM nir  WHERE rank <= 1000) AS nir     USING (request_uuid,listing_id)
  LEFT JOIN (SELECT * FROM xwalk  WHERE rank <= 1000) AS xwalk   USING (request_uuid,listing_id)
  LEFT JOIN (SELECT * FROM xml  WHERE rank <= 1000) AS xml     USING (request_uuid,listing_id)
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

-------------PART 4: CALCULATE KEY RETRIEVAL WASHOUT METRICS FROM SAMPLE-------------
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
