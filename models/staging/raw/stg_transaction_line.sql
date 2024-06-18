WITH transaction_line_items AS (
    SELECT 
        *, ROW_NUMBER() OVER (PARTITION BY transaction_date, transaction_id, transaction_line_id, item_id, bin_id, inventory_status_id, location_id, TRANSACTION_TYPE, quantity, TYPE_BASED_DOCUMENT_NUMBER, TYPE_BASED_DOCUMENT_STATUS ORDER BY transaction_date, transaction_id, transaction_line_id) AS row_num 
    from {{source('raw', 'transaction_line')}}
)

SELECT 
    TO_DATE(TRANSACTION_DATE) AS TRANSACTION_DATE, 
    TRANSACTION_ID, 
    TRANSACTION_LINE_ID, 
    TRANSACTION_TYPE, 
    TYPE_BASED_DOCUMENT_NUMBER, 
    TYPE_BASED_DOCUMENT_STATUS, 
    ITEM_ID, 
    BIN_ID, 
    INVENTORY_STATUS_ID, 
    LOCATION_ID, 
    QUANTITY
from transaction_line_items 
where row_num = 1 