WITH costs_decorated AS (
SELECT
    date AS effective_start_date,
    DATEADD(DAY, -1, LEAD(date, 1, current_date) OVER (PARTITION BY location_id, item_id ORDER BY date)) AS effective_end_date,
    location_id,
    item_id,
    cost
FROM {{source('raw', 'costs')}}
) 

select *
from costs_decorated
