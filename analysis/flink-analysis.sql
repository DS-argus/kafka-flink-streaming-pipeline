--CREATE INDEX IF NOT EXISTS idx_events_event_time ON events     (event_time);
--CREATE INDEX IF NOT EXISTS idx_pviews_event_time ON page_views (event_time);
--CREATE INDEX IF NOT EXISTS idx_events_uuid       ON events     (uuid);
--CREATE INDEX IF NOT EXISTS idx_pviews_uuid       ON page_views (uuid);
--CREATE INDEX IF NOT EXISTS idx_events_doc        ON events     (document_id);
--CREATE INDEX IF NOT EXISTS idx_pviews_doc        ON page_views (document_id);


/* ------------------------------------------------------------------
   1. 최근 1시간 분(分)-단위 이벤트 수
   ------------------------------------------------------------------ */

WITH latest AS (SELECT max(event_time) AS max_ts FROM events),	-- 과거 데이터니까 어쩔 수 없이 현재 시간 역추적 
     minute_bucket AS (											-- 1시간 이내 데이터만 분미만은 버림해서 bucket 생성 
       SELECT date_trunc('minute', event_time) AS minute_ts
       FROM   events, latest
       WHERE  event_time >= latest.max_ts - interval '1 hour'
     )
SELECT minute_ts,												-- 동일 시간대 (분 기준)에 속한 데이터 개수 count
       count(*) AS events_per_min
FROM   minute_bucket
GROUP  BY minute_ts
ORDER  BY minute_ts;

WITH latest AS (SELECT max(event_time) AS max_ts FROM page_views),	-- 과거 데이터니까 어쩔 수 없이 현재 시간 역추적 
     minute_bucket AS (											-- 1시간 이내 데이터만 분미만은 버림해서 bucket 생성 
       SELECT date_trunc('minute', event_time) AS minute_ts
       FROM   page_views, latest
       WHERE  event_time >= latest.max_ts - interval '1 hour'
     )
SELECT minute_ts,												-- 동일 시간대 (분 기준)에 속한 데이터 개수 count
       count(*) AS page_views_per_min
FROM   minute_bucket
GROUP  BY minute_ts
ORDER  BY minute_ts;


/* ------------------------------------------------------------------
   2. 최근 6시간 내 상위 10개 문서 (6h)
   ------------------------------------------------------------------ */
WITH latest AS (
  SELECT max(event_time) AS max_ts FROM events
)
SELECT  e.document_id,
        count(*) AS impressions
FROM    events e, latest
WHERE   e.event_time >= latest.max_ts - interval '6 hours'
GROUP   BY e.document_id
ORDER   BY impressions DESC           
LIMIT   10;



/* ------------------------------------------------------------------
   3. 국가·주별 PV (24 h)
   ------------------------------------------------------------------ */
WITH latest AS (
  SELECT max(event_time) AS max_ts
  FROM   events
)
SELECT split_part(p.geo_location,'>',1) AS country,   -- 예: 'US'
       split_part(p.geo_location,'>',2) AS state,     -- 예: 'TX'
       split_part(p.geo_location,'>',3) AS dma,       -- 예: '623'
       count(*)                       AS pv
FROM   page_views p, latest
WHERE  p.event_time >= latest.max_ts - interval '24 hours'
GROUP  BY country, state, dma
ORDER  BY pv DESC;



/* ------------------------------------------------------------------
   4. Traffic Source 분포 (24 h)
   ------------------------------------------------------------------ */
WITH latest AS (
		SELECT max(event_time) AS max_ts 
		FROM events
	)
SELECT 
	CASE p.traffic_source
		WHEN 1 THEN 'Desktop'
		WHEN 2 THEN 'Mobile'
		WHEN 3 THEN 'Tablet'
		ELSE 'Other'
	END AS traffic_source_label,
	   count(*) AS clicks
FROM page_views p, latest
WHERE p.event_time >= latest.max_ts - interval '24 hours'
GROUP BY p.traffic_source
ORDER BY clicks DESC;



/* ------------------------------------------------------------------
   5. 세션 길이·페이지 수 (12 h, 30 분 Idle 기준)
   ------------------------------------------------------------------ */
with latest as (
		select max(event_time) as max_ts
		from events
	),
     ordered as (
		select uuid, event_time, lag(event_time) over (partition by uuid
order by
	event_time) as prev_time
from
	page_views p,
	latest
where
	p.event_time >= latest.max_ts - interval '12 hours'
     )
select
	uuid,
	event_time,
	case
		when prev_time is null
		or event_time - prev_time > interval '30 minutes'
                then 1
		else 0
	end as new_session
from
	ordered;

WITH latest  AS (SELECT max(event_time) AS max_ts FROM events),
     ordered AS (
       SELECT uuid,
              event_time,
              lag(event_time) OVER (PARTITION BY uuid ORDER BY event_time) AS prev_time
       FROM   page_views p, latest
       WHERE  p.event_time >= latest.max_ts - interval '12 hours'
     ),
     session_marks AS (
       SELECT uuid,
              event_time,
              CASE
                WHEN prev_time IS NULL
                  OR event_time - prev_time > interval '30 minutes'
                THEN 1 ELSE 0 END AS new_session
       FROM   ordered
     ),
     session_ids AS (
       SELECT uuid,
              event_time,
              sum(new_session) OVER (PARTITION BY uuid ORDER BY event_time) AS session_id
       FROM   session_marks
     )
SELECT count(DISTINCT (uuid, session_id))          AS sessions_12h,
       round(avg(events_per_session), 2)           AS avg_pages_per_session
FROM (
  SELECT uuid, session_id, count(*) AS events_per_session
  FROM   session_ids
  GROUP  BY uuid, session_id
) t;



/* ------------------------------------------------------------------
   6. 엔드-투-엔드 지연(ms) 모니터 (최근 5 분)
   ------------------------------------------------------------------ */
WITH latest AS (SELECT max(event_time) AS max_ts FROM events)
SELECT round(
         avg( extract(epoch FROM (latest.max_ts - event_time)) * 1000 )
       ) AS avg_ms_delay
FROM   events, latest
WHERE  event_time >= latest.max_ts - interval '5 minutes';



/* ------------------------------------------------------------------
   7. 시간대 × 요일 히트맵 (최근 7 d)
   ------------------------------------------------------------------ */
WITH latest AS (SELECT max(event_time) AS max_ts FROM events)
SELECT to_char(event_time, 'HH24') AS hour,      -- 00 ~ 23
       to_char(event_time, 'Dy')   AS weekday,   -- Mon, Tue …
       count(*)                    AS pv
FROM   page_views p, latest
WHERE  p.event_time >= latest.max_ts - interval '7 days'
GROUP  BY hour, weekday;



/* ------------------------------------------------------------------
   8. 시간별 TOP 20 문서 (최근 24 h)
   ------------------------------------------------------------------ */
WITH latest AS (
		SELECT max(event_time) AS max_ts 
		FROM events
	),
	hourly AS (
		SELECT document_id,
			   date_trunc('hour', event_time) AS hour_bucket,
			   count(*) AS pv
		FROM page_views p, latest
		WHERE p.event_time >= latest.max_ts - interval '24 hours'	
		GROUP BY hour_bucket, document_id
	),
	ranked AS (
		SELECT *,
			   row_number() OVER (PARTITION BY hour_bucket ORDER BY pv DESC) AS rn
		FROM hourly
	)
SELECT hour_bucket,
	   document_id,
	   pv
FROM ranked
WHERE rn <= 20
ORDER BY hour_bucket, rn;



/* ------------------------------------------------------------------
   9. D0 ~ D7 재방문 Retention  (변동 없음 – 전체 데이터 기준)
   ------------------------------------------------------------------ */
WITH first_seen AS (
		SELECT uuid,
			   min(event_time) AS first_time
		FROM events
		GROUP BY uuid
	),
	day_n AS (
		SELECT uuid,
			   floor(extract(epoch FROM (event_time - first_time)) / 86400)::int AS day_delta
		FROM events e
		JOIN first_seen f USING (uuid)
		WHERE event_time < first_time + interval '8 days'
	)
SELECT day_delta,
	   count(DISTINCT uuid) AS returning_users
FROM day_n
GROUP BY day_delta
ORDER BY day_delta;
