with order_document_view as(
select
coalesce(date(ORDER_UPDATED_DATE),date(DOCUMENT_UPDATED_DATE)) as order_doc_date,
supplier_name,
count(distinct order_id) as orders_count_distinct,
-- count(distinct DOCUMENT_ID) as DOCUMENT_count_distinct,
sum(ITEM_PRICE_ORDERED) as total_value_ordered,
sum(ITEM_PRICE_RECEIVED) as total_value_recieved,
-- sum(ITEM_PRICE_DOCUMENT) as total_value_document

from
 {{ref("marts_orders_documents_unified")}}

where IS_REMOVED_FROM_ORDER = False
and IS_REPORTED_MISSING = False

group by 1,2
),

stg_supplier_view as(
SELECT
SUPPLIER_NAME,
SUPPLIER_CITY,
SUPPLIER_COUNTRY
FROM
 {{ref("stg_SupplierService_Suppliers")}}

)

select order_document_view.*,
stg_supplier_view.* EXCLUDE(SUPPLIER_NAME)
from order_document_view
left join
stg_supplier_view
on 
order_document_view.supplier_name = stg_supplier_view.SUPPLIER_NAME