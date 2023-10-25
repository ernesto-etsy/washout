DECLARE _dates ARRAY<DATE>;
DECLARE config STRING;
DECLARE _platform STRING;

SET config = 'ranking/search.mmx.2023_q3.axwalk_faisspp_boe';
SET _dates = [DATE('2023-10-07'), DATE('2023-10-09'), DATE('2023-10-11')];
-- 'wsg' if WEB experiment, 'allsr' if BOE experiment
SET platform = "wsg"

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.faiss_xwalk_experiment_uuids_sample` AS

SELECT
  properties.value AS request_uuid,
  ab.key AS ab_flag,
  ab.value.value_tuple[OFFSET(0)] AS ab_flag_variant,
FROM
  `etsy-visit-pipe-prod.canonical.beacons_recent` b
INNER JOIN
  UNNEST(b.ab.map) ab, UNNEST(b.properties.map) properties
WHERE
  DATE(_PARTITIONTIME) IN UNNEST(_dates)
  AND properties.key = "request_uuid"
  AND ab.key = config
  AND ab.value.value_tuple[OFFSET(0)] <> 'ineligible'
GROUP BY
  request_uuid, ab_flag, ab_flag_variant
ORDER BY
  RAND()
LIMIT 3000000;

-------------PART 2: PULL ALL ASSOCIATED VISIT IDS AND SEARCH QUERIES FROM EXPERIMENT SAMPLE-------------

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.faiss_xwalk_experiment_uuids_sample_searches` AS

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
and DATE(_PARTITIONTIME) IN UNNEST(_dates)
)
SELECT e._date,s.request_uuid,s.ab_flag,e.visit_id,e.query
FROM `etsy-data-warehouse-dev.ecanales.faiss_xwalk_experiment_uuids_sample` s
  JOIN events e on e.request_uuid = s.request_uuid
WHERE e.query IS NOT NULL;

-------------PART 3: PULL FROM RPC LOGS-------------

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.faiss_xwalk_multi_searchexplain_sample` AS
WITH requests AS (
SELECT 
    Q.*,
    R.OrganicRequestMetadata.candidateSources, 
    R.RequestIdentifiers
  FROM `etsy-data-warehouse-dev.ecanales.faiss_xwalk_experiment_uuids_sample_searches` Q
  CROSS JOIN `etsy-searchinfra-gke-prod-2.thrift_mmx_listingsv2search_search.rpc_logs_*` R
  WHERE DATE(R.queryTime) = Q._date
    AND R.request.options.searchPlacement = _platform
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
-- Create temporary table
CREATE TEMP TABLE temp_base AS
SELECT
  a.*,
  b.ab_variant,
  c.platform,
  CASE 
    WHEN c.platform = "desktop" AND mo_last <= 48 THEN 1
    WHEN c.platform = "mobile_web" AND mo_last <= 34 THEN 1
    WHEN c.platform = "boe" AND mo_last <= 28 THEN 1
  ELSE 0 END AS page_1_flag,
  CASE 
    WHEN c.platform = "desktop" AND mo_last <= 48*3 THEN 1
    WHEN c.platform = "mobile_web" AND mo_last <= 34*3 THEN 1
    WHEN c.platform = "boe" AND mo_last <= 28*3 THEN 1
  ELSE 0 END AS top_3_flag
FROM
  `etsy-data-warehouse-dev.ecanales.faiss_xwalk_multi_searchexplain_sample` a
JOIN
  `etsy-data-warehouse-prod.catapult.ab_tests` b
ON a.visit_id = b.visit_id AND b.ab_test = config AND b._date IN UNNEST(_dates)
JOIN
  `etsy-data-warehouse-prod.weblog.recent_visits` c
ON b.visit_id = c.visit_id AND c._date IN UNNEST(_dates);
--KEY METRIC: FIRST PAGE SHARE OVERALL
SELECT
ab_variant,
count(distinct visit_id) as visit_count,
count(distinct request_uuid) as request_count,
count(*) as listing_count,
--Solr
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_only_first_page_share,
--NIR
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_only_first_page_share,
--XWalk
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS xwalk_only_first_page_share,
--XML
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS xml_only_first_page_share,
--Solr and NIR
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_first_page_share,
--Solr and XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_xwalk_first_page_share,
--Solr and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_xml_first_page_share,
--NIR and XWalk
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_xwalk_first_page_share,
--NIR and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_xml_first_page_share,
--XWalk and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS xwalk_xml_first_page_share,
--Solr, NIR and XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_xwalk_first_page_share,
--Solr, NIR and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_xml_first_page_share,
--Solr, XWalk and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_xwalk_xml_first_page_share,
--NIR, XWalk and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_xwalk_xml_first_page_share,
--Solr, NIR, XWalk and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_xwalk_xml_first_page_share
FROM temp_base
GROUP BY 1
ORDER BY 1;

--KEY METRIC: FIRST TO THIRD PAGES SHARE OVERALL
SELECT
ab_variant,
count(distinct visit_id) as visit_count,
count(distinct request_uuid) as request_count,
count(*) as listing_count,
--Solr
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_only_3p_share,
--NIR
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS nir_only_3p_share,
--XWalk
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS xwalk_only_3p_share,
--XML
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS xml_only_3p_share,
--Solr and NIR
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_3p_share,
--Solr and XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_xwalk_3p_share,
--Solr and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_xml_3p_share,
--NIR and XWalk
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS nir_xwalk_3p_share,
--NIR and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS nir_xml_3p_share,
--XWalk and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS xwalk_xml_3p_share,
--Solr, NIR and XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_xwalk_3p_share,
--Solr, NIR and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_xml_3p_share,
--Solr, XWalk and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_xwalk_xml_3p_share,
--NIR, XWalk and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS nir_xwalk_xml_3p_share,
--Solr, NIR, XWalk and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_xwalk_xml_3p_share
FROM temp_base
GROUP BY 1
ORDER BY 1;

--KEY METRIC: 1000 CANDIDATES SHARE OVERALL
SELECT
ab_variant,
count(distinct visit_id) as visit_count,
count(distinct request_uuid) as request_count,
count(*) as listing_count,
--Solr
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_only_3p_share,
--NIR
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS nir_only_3p_share,
--XWalk
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS xwalk_only_3p_share,
--XML
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS xml_only_3p_share,
--Solr and NIR
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_nir_3p_share,
--Solr and XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_xwalk_3p_share,
--Solr and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_xml_3p_share,
--NIR and XWalk
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS nir_xwalk_3p_share,
--NIR and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS nir_xml_3p_share,
--XWalk and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS xwalk_xml_3p_share,
--Solr, NIR and XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_nir_xwalk_3p_share,
--Solr, NIR and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_nir_xml_3p_share,
--Solr, XWalk and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_xwalk_xml_3p_share,
--NIR, XWalk and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS nir_xwalk_xml_3p_share,
--Solr, NIR, XWalk and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_nir_xwalk_xml_3p_share
FROM temp_base
GROUP BY 1
ORDER BY 1;

--KEY METRIC: FIRST PAGE SHARE BY SAMPLE DATE
SELECT
ab_variant,
_date,
count(distinct visit_id) as visit_count,
count(distinct request_uuid) as request_count,
count(*) as listing_count,
--Solr
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_only_first_page_share,
--NIR
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_only_first_page_share,
--XWalk
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS xwalk_only_first_page_share,
--XML
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS xml_only_first_page_share,
--Solr and NIR
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_first_page_share,
--Solr and XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_xwalk_first_page_share,
--Solr and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_xml_first_page_share,
--NIR and XWalk
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_xwalk_first_page_share,
--NIR and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_xml_first_page_share,
--XWalk and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS xwalk_xml_first_page_share,
--Solr, NIR and XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_xwalk_first_page_share,
--Solr, NIR and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_xml_first_page_share,
--Solr, XWalk and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_xwalk_xml_first_page_share,
--NIR, XWalk and XML
SUM(CASE WHEN (solr IS NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_xwalk_xml_first_page_share,
--Solr, NIR, XWalk and XML
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND xml IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_xwalk_xml_first_page_share
FROM temp_base
GROUP BY 1,2
ORDER BY 2,1;
