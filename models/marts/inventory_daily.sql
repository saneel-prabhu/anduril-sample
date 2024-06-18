with temp_spine as (
select * 
-- from int_date_spine
from {{ ref('int_date_spine')}}
), 

temp_transactions_summarized as (
select * 
-- from int_transactions_summarized
from {{ ref('int_transactions_summarized')}}
), 

first_transactions as (
select 
    location_id, 
    bin_id, 
    inventory_status_id, 
    item_id,
    min(transaction_date) as first_date
from temp_transactions_summarized 
group by 1,2,3,4
), 

date_item_loc_bin_spine as (
SELECT * 
FROM temp_spine as ds 
CROSS JOIN 
    (SELECT distinct location_id, bin_id, inventory_status_id, item_id, first_date from first_transactions) as t
WHERE ds.date >= t.first_date
), 

joined_data as (
select 
    ds.date, 
    ds.location_id, 
    ds.bin_id, 
    ds.inventory_status_id, 
    ds.item_id,
    COALESCE(t.ENDING_QUANTITY, 0) AS ending_quantity,
    COALESCE(t.ENDING_VALUE, 0) AS ending_value
from date_item_loc_bin_spine as ds 
left join temp_transactions_summarized as t 
    on ds.date = t.transaction_date 
    and ds.location_id = t.location_id
    and ds.bin_id = t.bin_id
    and ds.inventory_status_id = t.inventory_status_id
    and ds.item_id = t.item_id
),  

inventory_daily as (
SELECT 
    date,
    location_id,
    bin_id,
    inventory_status_id,
    item_id,
    SUM(ending_quantity) OVER (PARTITION BY location_id, bin_id, inventory_status_id, item_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS inventory,
    ROUND(SUM(ending_value) OVER (PARTITION BY location_id, bin_id, inventory_status_id, item_id ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),2) AS inventory_value
FROM joined_data
) 

SELECT * 
FROM inventory_daily