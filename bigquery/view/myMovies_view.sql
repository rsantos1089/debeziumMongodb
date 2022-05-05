CREATE OR REPLACE VIEW analytics_video.myMovies_view AS

WITH myMovies_delta AS (
  SELECT concat("[",after,"]") content_after ,
  patch,filter,updateDescription,source,op,(case when op = 'd' then ts_ms -1 else ts_ms end ) ts_ms,transaction
  from `chrome-coast-348406.analytics_video.myMovies_delta`
),

myMovies_arrange AS(
SELECT 
array( 
  SELECT AS struct
  STRUCT(JSON_EXTRACT(content_after,"$._id") AS _id, JSON_EXTRACT(content_after,"$.title") AS title, JSON_EXTRACT(content_after,"$.year") AS year, JSON_EXTRACT(content_after,"$.type") AS type ) AS payload
  from unnest(analytics_video.json2array(content_after)) AS content_after
) as after,
patch,filter,updateDescription	,source,op,ts_ms,transaction
FROM myMovies_delta
 ),

myMovies_arrange_final as(
 select payload as after,patch,filter,updateDescription	,source,op,ts_ms,transaction
  FROM myMovies_arrange r  CROSS JOIN UNNEST(r.after) as after
)

SELECT * EXCEPT(op, row_num) FROM (
 SELECT *, ROW_NUMBER() OVER (PARTITION BY after._id ORDER BY ts_ms DESC) AS row_num
from myMovies_arrange_final )
WHERE row_num = 1 	AND op <> 'd' ;
