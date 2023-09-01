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
GROUP BY 1
