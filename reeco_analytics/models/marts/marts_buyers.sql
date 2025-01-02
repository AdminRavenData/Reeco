with order_view as(
    SELECT
        date(ORDER_CREATED_DATE) AS date,
        BUYER_ID,
        OUTLET_ID,
        CHAINID,
        COUNT(DISTINCT order_id) AS orders_count_distinct,
        SUM(ITEM_PRICE_ORDERED) AS total_value_ordered,
        SUM(ITEM_PRICE_RECEIVED) AS total_value_recieved,
        NULL::INT AS DOCUMENT_count_distinct,
        NULL::FLOAT AS total_value_document
    from
    {{ref("marts_orders_documents_unified")}}

    where IS_REMOVED_FROM_ORDER = False
    and IS_REPORTED_MISSING = False
    and BUYER_ID is not null

    group by 1,2,3,4
),

document_view as(
    SELECT
        date(DOCUMENT_CREATED_DATE) AS date,
        BUYER_ID,
        OUTLET_ID,
        CHAINID,
        NULL::INT AS orders_count_distinct,
        NULL::FLOAT AS total_value_ordered,
        NULL::FLOAT AS total_value_recieved,
        COUNT(DISTINCT DOCUMENT_ID) AS DOCUMENT_count_distinct,
        SUM(ITEM_PRICE_DOCUMENT) AS total_value_document

    from
    {{ref("marts_orders_documents_unified")}}

    where BUYER_ID is not null

    group by 1,2,3,4
),

order_document_view_temp as(
    SELECT * FROM order_view
    UNION ALL
    SELECT * FROM document_view
),

order_document_view as(
    select
    date,
    BUYER_ID,
    OUTLET_ID,
    CHAINID,
    SUM(orders_count_distinct) AS orders_count_distinct,
    SUM(total_value_ordered) AS total_value_ordered,
    SUM(total_value_recieved) AS total_value_recieved,
    SUM(DOCUMENT_count_distinct) AS DOCUMENT_count_distinct,
    SUM(total_value_document) AS total_value_document
FROM order_document_view_temp 
GROUP BY 1, 2, 3, 4
),

buyers_view as( 
select distinct
BUYER_NAME,
BUYER_ID

from 
   {{ref("stg_BuyerService_Buyers")}}
),

outlet_view as(
select distinct
OUTLET_NAME,
OUTLET_ID

from 
    {{ref("stg_BuyerService_Buyers")}}
),

inventory_view as (
select
date(UPDATEDATETIME) as inventory_date,
BUYERID,
OUTLETID,
sum(CASE_COUNT_VALUE) as items_case_invenory_counted,
sum(CASE_TOTAL_VALUE) as items_case_invenory_TOTAL_VALUE,
sum(UNIT_COUNT_VALUE) as items_unit_invenory_counted,
sum(UNIT_TOTAL_VALUE) as items_unit_invenory_TOTAL_VALUE,
sum(EACH_COUNT_VALUE) as items_each_invenory_counted,
sum(EACH_TOTAL_VALUE) as items_each_invenory_TOTAL_VALUE

from
    {{ref("stg_InventoryService_InventoryCounts")}}
where (BUYERNAME is not null or BUYERID is not null)
group by 1,2,3
),

buyers_unified_info as(
select 
    coalesce(order_document_view.date, inventory_view.inventory_date ) as date,
    coalesce(order_document_view.BUYER_ID, inventory_view.BUYERID ) as BUYER_ID,
    coalesce(order_document_view.OUTLET_ID, inventory_view.OUTLETID ) as OUTLET_ID,

    order_document_view.* exclude( date,BUYER_ID,OUTLET_ID ),
    inventory_view.* exclude(inventory_date,BUYERID,OUTLETID)

from 
order_document_view
full outer join inventory_view
on 
order_document_view.BUYER_ID = inventory_view.BUYERID
and
order_document_view.OUTLET_ID = inventory_view.OUTLETID
and
order_document_view.date = inventory_view.inventory_date
)

select 
buyers_view.BUYER_NAME,
outlet_view.OUTLET_NAME ,
buyers_unified_info.* 
from 
buyers_unified_info
left join 
buyers_view
on
buyers_unified_info.BUYER_ID  =  buyers_view.BUYER_ID
left join
outlet_view
on
buyers_unified_info.OUTLET_ID  =  outlet_view.OUTLET_ID
