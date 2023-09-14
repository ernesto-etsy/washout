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
    `etsy-data-warehouse-dev.pdavidoff.nirt_1m_multi_searchexplain_sample` a join
    `etsy-data-warehouse-prod.catapult.ab_tests` b on a.visit_id = b.visit_id and b.ab_test = 'ranking/search.mmx.2023_q3.nir_t_hqi_web' and b._date = "2023-09-01" join
    `etsy-data-warehouse-prod.weblog.recent_visits` c on b.visit_id = c.visit_id and c._date = "2023-09-01"
)
SELECT
ab_variant,
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
group by 1
order by 1;
