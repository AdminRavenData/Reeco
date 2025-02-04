select 
    INVENTORY_count_id,
    CREATE_DATETIME,
    UPDATED_ATETIME,
    BUYER_ID,
    OUTLET_ID,
    user_id,
    user_name,
    STARTED_DATETIME,
    FINISH_DATETIME,
    STATUS,
    locations_count,
    sum(INVENTORY_ITEM_TOTAL_VALUE) as INVENTORY_ITEM_TOTAL_VALUE

from     {{ref("base_inventory_unified")}}


group by 

    INVENTORY_count_id,
    CREATE_DATETIME,
    UPDATED_ATETIME,
    BUYER_ID,
    OUTLET_ID,
    user_id,
    user_name,
    STARTED_DATETIME,
    FINISH_DATETIME,
    STATUS,
    locations_count