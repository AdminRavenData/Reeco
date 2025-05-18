-- CREATE OR REPLACE VIEW stg_OrderService_Orders AS
with inventory_temp as(
    select *,
    ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC ,__ts_ms  DESC ) AS rn
    from 
    REECO.MONGO.inventoryservice_inventorycounts
    WHERE ISDELETED = FALSE 
    And BUYERID not in (select demo_id from  {{ref("stg_demo_ids")}})
    ),

stg_buyer_timezone as(
    select 
        BUYER_ID,
        timezone 
    from
        {{ref("stg_buyer_timezone")}} 
)

select 
    _id as INVENTORY_count_id,
    CONVERT_TIMEZONE('UTC', tm.timezone, CREATEDATETIME) as CREATE_DATETIME,
    CONVERT_TIMEZONE('UTC', tm.timezone, UPDATEDATETIME) as UPDATED_ATETIME,
    BUYERID as BUYER_ID,
    OUTLETID as OUTLET_ID,
    CREATEDBYUSERID as user_id,
    CREATEDBYUSERNAME as user_name,
    CONVERT_TIMEZONE('UTC', tm.timezone, STARTEDDATETIME) as STARTED_DATETIME,
    CONVERT_TIMEZONE('UTC', tm.timezone, FINISHDATETIME) as FINISH_DATETIME,
    STATUS,
     ARRAY_SIZE(OBJECT_KEYS(locations)) AS locations_count

from inventory_temp

left join 
    stg_buyer_timezone  tm
on
    inventory_temp.BUYERID = tm.BUYER_ID

where rn=1