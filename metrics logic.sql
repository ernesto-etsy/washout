DECLARE _dates ARRAY<DATE>;
DECLARE config STRING;

SET config = 'ranking/search.mmx.2023_q3.xwalk_vi_fbv2';
SET _dates = [DATE('2023-09-08'), DATE('2023-09-11'), DATE('2023-09-14')]; 
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
  `etsy-data-warehouse-dev.ecanales.vi_fbv2_multi_searchexplain_sample` a
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
--Solr (only)
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_only_first_page_share,
--Solr <> NIR (no XWalk)
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_first_page_share,
--Solr <> XWalk (no NIR)
SUM(CASE WHEN (solr IS NOT NULL AND xwalk IS NOT NULL AND nir IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_xwalk_first_page_share,
--NIR (only)
SUM(CASE WHEN (nir IS NOT NULL AND solr IS NULL AND xwalk IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_only_first_page_share,
--NIR <> XWalk (no Solr)
SUM(CASE WHEN (nir IS NOT NULL AND xwalk IS NOT NULL AND solr IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_xwalk_first_page_share,
--XWalk (only)
SUM(CASE WHEN (xwalk IS NOT NULL AND solr IS NULL AND nir IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS xwalk_only_first_page_share,
--Solr <> NIR <> XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS all_algos_first_page_share,
FROM temp_base
GROUP BY 1
ORDER BY 1;

--KEY METRIC: FIRST TO THIRD PAGES SHARE OVERALL
SELECT
ab_variant,
count(distinct visit_id) as visit_count,
count(distinct request_uuid) as request_count,
count(*) as listing_count,
--Solr (only)
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_only_3p_share,
--Solr <> NIR (no XWalk)
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_3p_share,
--Solr <> XWalk (no NIR)
SUM(CASE WHEN (solr IS NOT NULL AND xwalk IS NOT NULL AND nir IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS solr_xwalk_3p_share,
--NIR (only)
SUM(CASE WHEN (nir IS NOT NULL AND solr IS NULL AND xwalk IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS nir_only_3p_share,
--NIR <> XWalk (no Solr)
SUM(CASE WHEN (nir IS NOT NULL AND xwalk IS NOT NULL AND solr IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS nir_xwalk_3p_share,
--XWalk (only)
SUM(CASE WHEN (xwalk IS NOT NULL AND solr IS NULL AND nir IS NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS xwalk_only_3p_share,
--Solr <> NIR <> XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND top_3_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN top_3_flag = 1 THEN 1 ELSE 0 END) AS all_algos_3p_share,
FROM temp_base
GROUP BY 1
ORDER BY 1;

--KEY METRIC: 1000 CANDIDATES SHARE OVERALL
SELECT
ab_variant,
count(distinct visit_id) as visit_count,
count(distinct request_uuid) as request_count,
count(*) as listing_count,
--Solr (only)
SUM(CASE WHEN (solr IS NOT NULL AND xwalk IS NULL AND nir IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_only_1k_share,
--Solr <> NIR (no XWalk)
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_nir_1k_share,
--Solr <> XWalk (no NIR)
SUM(CASE WHEN (solr IS NOT NULL AND xwalk IS NOT NULL AND nir IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS solr_xwalk_1k_share,
--NIR (only)
SUM(CASE WHEN (nir IS NOT NULL AND xwalk IS NULL AND solr IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS nir_only_1k_share,
--NIR <> XWalk (no Solr)
SUM(CASE WHEN (nir IS NOT NULL AND xwalk IS NOT NULL AND solr IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS nir_xwalk_1k_share,
--XWalk (only)
SUM(CASE WHEN (xwalk IS NOT NULL AND solr IS NULL AND nir IS NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS xwalk_only_1k_share,
--Solr <> NIR <> XWalk
SUM(CASE WHEN (xwalk IS NOT NULL AND solr IS NOT NULL AND nir IS NOT NULL AND mo_last <= 1000) THEN 1 ELSE 0 END) / SUM(CASE WHEN mo_last <= 1000 THEN 1 ELSE 0 END) AS all_algos_1k_share,
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
--Solr (only)
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NULL AND xwalk IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_only_first_page_share,
--Solr <> NIR (no XWalk)
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_nir_first_page_share,
--Solr <> XWalk (no NIR)
SUM(CASE WHEN (solr IS NOT NULL AND xwalk IS NOT NULL AND nir IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS solr_xwalk_first_page_share,
--NIR (only)
SUM(CASE WHEN (nir IS NOT NULL AND solr IS NULL AND xwalk IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_only_first_page_share,
--NIR <> XWalk (no Solr)
SUM(CASE WHEN (nir IS NOT NULL AND xwalk IS NOT NULL AND solr IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS nir_xwalk_first_page_share,
--XWalk (only)
SUM(CASE WHEN (xwalk IS NOT NULL AND solr IS NULL AND nir IS NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS xwalk_only_first_page_share,
--Solr <> NIR <> XWalk
SUM(CASE WHEN (solr IS NOT NULL AND nir IS NOT NULL AND xwalk IS NOT NULL AND page_1_flag = 1) THEN 1 ELSE 0 END) / SUM(CASE WHEN page_1_flag = 1 THEN 1 ELSE 0 END) AS all_algos_first_page_share,
FROM temp_base
GROUP BY 1,2
ORDER BY 2,1;
