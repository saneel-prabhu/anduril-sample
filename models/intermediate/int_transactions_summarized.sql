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

transactions_summarized as (
SELECT 
    t.location_id,
    t.bin_id,
    t.inventory_status_id,
    t.item_id,
    t.transaction_date,
    sum(t.quantity) as quantity,
    sum(c.cost) as cost 
from transaction_line_items as t 
left join temp_costs as c 
    ON t.item_id = c.item_id 
    AND t.location_id = c.location_id 
    AND t.transaction_date between c.effective_start_date and c.effective_end_date 
group by 1,2,3,4,5
)

select * 
from transactions_summarized
