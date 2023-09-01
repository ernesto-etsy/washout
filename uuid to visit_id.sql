with events as (
  select
    visit_id,
    DATE(_PARTITIONTIME) AS _date,
    (select value from unnest(beacon.properties.key_value) where key = 'request_uuid') request_uuid,
    beacon.timestamp
from
`etsy-visit-pipe-prod.canonical.visit_id_beacons`
where beacon.event_name = 'search'
and DATE(_PARTITIONTIME) >=  "2023-08-29"
)
SELECT DISTINCT se.uuid, visit_id
FROM `etsy-data-warehouse-dev.ecanales.multi_searchexplain_sample` se
  JOIN events e on e.request_uuid = se.uuid
;
