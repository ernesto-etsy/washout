DECLARE _dates ARRAY<DATE>;
DECLARE config STRING;

SET config = 'ranking/search.mmx.2023_q3.nir_t_hqi_web';
SET _dates = [DATE('2023-08-29'), DATE('2023-09-02'), DATE('2023-09-05')]; 

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.nirt_1m_experiment_uuids_sample` AS

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
LIMIT 1000000;

-------------PART 2: PULL ALL ASSOCIATED VISIT IDS AND SEARCH QUERIES FROM EXPERIMENT SAMPLE-------------

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.nirt_1m_experiment_uuids_sample_searches` AS

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
FROM `etsy-data-warehouse-dev.ecanales.nirt_1m_experiment_uuids_sample` s
  JOIN events e on e.request_uuid = s.request_uuid
WHERE e.query IS NOT NULL;

-------------PART 3: PULL FROM RPC LOGS-------------

CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.nirt_1m_multi_searchexplain_sample` AS
WITH requests AS (
SELECT 
    Q.*,
    R.OrganicRequestMetadata.candidateSources, 
    R.RequestIdentifiers
  FROM `etsy-data-warehouse-dev.ecanales.nirt_1m_experiment_uuids_sample_searches` Q
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
with base as (
  select
    a.*,
    b.ab_variant,
    c.platform,
    case 
      when c.platform = "desktop" and mo_last <= 48 then 1
      when c.platform = "mobile_web" and mo_last <= 34 then 1
      when c.platform = "boe" and mo_last <= 28 then 1
    else 0 end as page_1_flag,
     case 
      when c.platform = "desktop" and mo_last <= 48*3 then 1
      when c.platform = "mobile_web" and mo_last <= 34*3 then 1
      when c.platform = "boe" and mo_last <= 28*3 then 1
    else 0 end as top_3_flag
  from
    `etsy-data-warehouse-dev.ecanales.nirt_1m_multi_searchexplain_sample` a join
    `etsy-data-warehouse-prod.catapult.ab_tests` b on a.visit_id = b.visit_id and b.ab_test = 'ranking/search.mmx.2023_q3.nir_t_hqi_web' and b._date IN UNNEST(_dates) join
    `etsy-data-warehouse-prod.weblog.recent_visits` c on b.visit_id = c.visit_id and c._date IN UNNEST(_dates)
)
SELECT
ab_variant,
_date,
count(distinct visit_id) as visit_count,
count(distinct request_uuid) as request_count,
count(*) as listing_count,
-- first page share
SUM(CASE WHEN (solr IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_first_page_share,
SUM(CASE WHEN (nir IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_first_page_share,
SUM(CASE WHEN (nir IS NOT NULL AND solr IS NULL AND xwalk IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_only_first_page_share,
SUM(CASE WHEN (xwalk IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS xwalk_first_page_share,
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_first_page_share,
SUM(CASE WHEN (solr IS NOT NULL AND xwalk IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS xwalk_nir_first_page_share,
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS all_algos_first_page_share,
-- first page rank
AVG(CASE WHEN solr is not null and page_1_flag = 1 then solr end) as avg_solr_pg_1_rank,
AVG(CASE WHEN nir is not null and page_1_flag = 1 then nir end) as avg_nir_pg_1_rank,
AVG(case when xwalk is not null and page_1_flag = 1 then xwalk end) as avg_xwalk_pg_1_rank,
AVG(CASE WHEN solr IS NOT NULL AND nir IS NOT NULL AND page_1_flag = 1 THEN nir END) as avg_solr_nir_pg_1_rank,
AVG(CASE WHEN nir IS NOT NULL AND xwalk IS NOT NULL AND page_1_flag = 1 THEN xwalk END) as avg_nir_xwalk_pg_1_rank,
AVG(CASE WHEN solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND page_1_flag = 1 THEN xwalk END) as avg_all_algos_pg_1_rank,
AVG(CASE WHEN page_1_flag = 1 THEN borda ELSE NULL END) AS borda_avg_first_page_rank,
AVG(CASE WHEN page_1_flag = 1 THEN ranking ELSE NULL END) AS ranking_avg_first_page_rank,
AVG(CASE WHEN page_1_flag = 1 THEN mo_last ELSE NULL END) AS mo_avg_first_page_rank,
-- top 3 share
SUM(CASE WHEN (solr IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_top_3p_share,
SUM(CASE WHEN (nir IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS nir_top_3p_share,
SUM(CASE WHEN (xwalk IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS xwalk_top_3p_share,
SUM(CASE WHEN (nir IS NOT NULL AND solr IS NULL AND xwalk IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS nir_only_3p_share,
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_top_3p_share,
SUM(CASE WHEN (solr IS NOT NULL AND xwalk IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS xwalk_nir_top_3p_share,
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS all_algos_top_3p_share,
--top 3 rank
AVG(CASE WHEN solr IS NOT NULL AND top_3_flag = 1 THEN solr END) as avg_solr_pg_3_rank,
AVG(CASE WHEN nir IS NOT NULL AND top_3_flag = 1 THEN nir END) as avg_nir_pg_3_rank,
AVG(CASE WHEN xwalk IS NOT NULL AND top_3_flag = 1 THEN xwalk END) as avg_xwalk_pg_3_rank,
AVG(CASE WHEN solr IS NOT NULL AND nir IS NOT NULL AND top_3_flag = 1 THEN nir END) as avg_solr_nir_pg_3_rank,
AVG(CASE WHEN nir IS NOT NULL AND xwalk IS NOT NULL AND top_3_flag = 1 THEN xwalk END) as avg_nir_xwalk_pg_3_rank,
AVG(CASE WHEN solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND top_3_flag = 1 THEN xwalk END) as avg_all_algos_pg_3_rank,
AVG(CASE WHEN top_3_flag = 1 THEN borda ELSE NULL END) AS borda_avg_top_3p_rank,
AVG(CASE WHEN top_3_flag = 1 THEN ranking ELSE NULL END) AS ranking_avg_top_3p_rank,
AVG(CASE WHEN top_3_flag = 1 THEN mo_last ELSE NULL END) AS mo_avg_top_3p_rank,
-- top 1000 share
SUM(CASE WHEN (solr IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_top_1k_share,
SUM(CASE WHEN (nir IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS nir_top_1k_share,
SUM(CASE WHEN (xwalk IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS xwalk_top_1k_share,
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_nir_top_1k_share,
SUM(CASE WHEN (solr IS NOT NULL AND xwalk IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS xwalk_nir_top_1k_share,
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS xwalk_top_1k_share,
SUM(CASE WHEN (nir IS NOT NULL AND solr IS NULL AND xwalk IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS nir_only_1k_share,


-- top 1000 rank
avg(case when solr is not null and mo_last <= 1000 then solr end) as avg_solr_top_1k_rank,
avg(case when nir is not null and mo_last <= 1000 then nir end) as avg_nir_top_1k_rank,
avg(case when xwalk is not null and mo_last <= 1000 then xwalk end) as avg_xwalk_top_1k_rank,
avg(CASE WHEN solr IS NOT NULL AND nir IS NOT NULL AND mo_last <= 1000 then nir end) as avg_solr_nir_top_1k_rank,
avg(CASE WHEN nir IS NOT NULL AND xwalk IS NOT NULL AND mo_last <= 1000 then xwalk end) as avg_nir_xwalk_top_1k_rank,
avg(CASE WHEN solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND mo_last <= 1000 then xwalk end) as avg_all_algos_top_1k_rank,
AVG(CASE WHEN mo_last <= 1000 THEN borda ELSE NULL END) AS borda_avg_top_1k_rank,
AVG(CASE WHEN mo_last <= 1000 THEN ranking ELSE NULL END) AS ranking_avg_top_1k_rank,
AVG(CASE WHEN mo_last <= 1000 THEN mo_last ELSE NULL END) AS mo_avg_top_1k_rank
FROM base
group by 1,2
order by 2,1;
