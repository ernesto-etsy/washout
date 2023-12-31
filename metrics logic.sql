DECLARE _dates ARRAY<DATE>;
DECLARE config STRING;
DECLARE _platform STRING;
DECLARE _algos ARRAY<STRING>;

SET config = 'ranking/search.mmx.2023_q3.axwalk_faisspp_boe';
SET _dates = [DATE('2023-10-07'), DATE('2023-10-09'), DATE('2023-10-11')];
SET _algos = ["SOLR","NIR","XWALK","XML"];

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
CREATE OR REPLACE TABLE `etsy-data-warehouse-dev.ecanales.faiss_xwalk_multi_searchexplain_metrics` AS
WITH first_page AS (
--KEY METRIC: FIRST PAGE SHARE OVERALL
SELECT
"First Page" AS metric,
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
GROUP BY ab_variant
ORDER BY ab_variant),
--KEY METRIC: FIRST TO THIRD PAGES SHARE OVERALL
three_pages AS (
SELECT
"Three Pages" AS metric,
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
GROUP BY ab_variant
ORDER BY 1),
--KEY METRIC: 1000 CANDIDATES SHARE OVERALL
one_k AS (
SELECT
"1K" AS metric,
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
GROUP BY ab_variant
ORDER BY 1
)
SELECT * FROM first_page
UNION ALL
SELECT * FROM three_pages
UNION ALL
SELECT * FROM one_k;
