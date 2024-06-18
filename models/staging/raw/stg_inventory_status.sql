SELECT 
    id, 
    name, 
FROM {{source('raw', 'inventory_status')}}
