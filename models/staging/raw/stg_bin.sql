SELECT 
    id, 
    name, 
FROM {{source('raw', 'bin')}}
