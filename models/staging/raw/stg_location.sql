SELECT 
    id, 
    name, 
FROM {{source('raw', 'location')}}
