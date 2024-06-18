with temp_costs as (
select * 
-- from stg_costs
from {{ ref('stg_costs') }}
), 

transaction_line_items as (
select * 
-- from stg_transaction_line
from {{ ref('stg_transaction_line') }}
), 

transactions_grouped as (
SELECT 
    t.location_id,
    t.bin_id,
    t.inventory_status_id,
    t.item_id,
    t.transaction_date,
    sum(t.quantity) as ending_quantity
from transaction_line_items as t 
group by 1,2,3,4,5
),

transactions_summarized as (
select 
    t.location_id,
    t.bin_id,
    t.inventory_status_id,
    t.item_id,
    t.transaction_date,
    t.ending_quantity,
    (t.ending_quantity * c.cost) as ending_value 
from transactions_grouped as t 
inner join temp_costs as c 
    ON t.item_id = c.item_id 
    AND t.location_id = c.location_id 
    AND t.transaction_date between c.effective_start_date and c.effective_end_date 
) 

select * 
from transactions_summarized

