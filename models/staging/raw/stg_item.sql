SELECT 
    id, 
    name, 
FROM {{source('raw', 'item')}}
