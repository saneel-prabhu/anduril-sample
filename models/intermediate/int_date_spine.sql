with spine as (
select (SELECT min(to_date(transaction_date)) from {{source('raw', 'transaction_line')}}) as date 

union all

select dateadd('day', 1, date) 
from spine 
where date <= (SELECT max(to_date(transaction_date)) from {{source('raw', 'transaction_line')}})
)

select * 
from spine 
