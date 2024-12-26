with order_document_view as(
select
coalesce(date(ORDER_UPDATED_DATE),date(DOCUMENT_UPDATED_DATE)) as order_doc_date,
BUYER_NAME,
OUTLET_NAME,
CHAINID,
count(distinct order_id) as orders_count_distinct,
count(distinct DOCUMENT_ID) as DOCUMENT_count_distinct,
sum(ITEM_PRICE_ORDERED) as total_value_ordered,
sum(ITEM_PRICE_RECEIVED) as total_value_recieved,
sum(ITEM_PRICE_DOCUMENT) as total_value_document

from
 {{ref("marts_orders_documents_unified")}}

where IS_REMOVED_FROM_ORDER = False
and IS_REPORTED_MISSING = False

group by 1,2,3,4
),

-- buyers_view as(
-- select distinct
-- CHAINID,
-- BUYER_ID
-- from 
--     ref{{"base_buyers"}}

-- ),

inventory_view as (
select
date(UPDATEDATETIME) as inventory_date,
BUYERNAME,
OUTLETNAME,
sum(COUNTVALUE) as items_invenory_counted,
sum(TOTALVALUE) as TOTAL_VALUE_invenory
from
    {{ref("stg_InventoryService_InventoryCounts")}}

group by 1,2,3
)

select 
order_document_view.*,
inventory_view.items_invenory_counted,
inventory_view.TOTAL_VALUE_invenory


from 
order_document_view
left join inventory_view
on 
order_document_view.BUYER_NAME = inventory_view.BUYERNAME
and
order_document_view.OUTLET_NAME = inventory_view.OUTLETNAME
and
order_document_view.order_doc_date = inventory_view.inventory_date
