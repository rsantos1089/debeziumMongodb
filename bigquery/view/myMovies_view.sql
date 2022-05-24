CREATE OR REPLACE VIEW analytics_video.myMovies_view AS

WITH myMovies_delta AS (
 
 SELECT STRUCT(JSON_EXTRACT(after,"$._id") AS _id,
               JSON_EXTRACT(after,"$.title") AS title, 
  	           JSON_EXTRACT(after,"$.year") AS year, 
  	           JSON_EXTRACT(after,"$.type") AS type ) AS after ,
  patch, STRUCT(JSON_EXTRACT(filter,"$._id")  AS _id) AS filter,updateDescription,
  source,op,(case when op = 'd' then ts_ms -1 else ts_ms end ) ts_ms,transaction
  from `chrome-coast-348406.analytics_video.myMovies_delta`

),

generate_pk as(
 select * , (CASE WHEN after._id is null then filter._id else after._id end) as PK_ID 
 from  myMovies_delta
)

SELECT * EXCEPT(op, row_num,PK_ID) FROM (
select *,ROW_NUMBER() OVER (PARTITION BY PK_ID ORDER BY ts_ms DESC) AS row_num
from generate_pk )
WHERE row_num = 1 	AND op <> 'd' ;

