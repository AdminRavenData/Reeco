with documents_view as(
select 
ORDERID,
document_Sku,
max(DOCUMENT_ID) DOCUMENT_ID,
max(document_item_name) as document_item_name,
max(document_Order_Quantity) as document_Order_Quantity,
max(document_Item_Price) as document_Item_Price,
max(TOTALPRODUCTSPRICE) AS order_price_document
from 
 {{ref("stg_DocumentService_Documents")}}

group by 1,2
)

-- ,

-- orders_total_price_view as(
-- select 
-- ORDER_ID,  
-- sum(ITEM_PRICE_ORDERED) as order_price_ordered, 
-- sum(ITEM_PRICE_RECEIVED) as order_price_recieved  
-- from
--  {{ref("stg_OrderService_Orders")}}
-- group by ORDER_ID 
-- )

select 
o.*,
d.document_Order_Quantity,
d.document_Item_Price,
d.document_item_name,
d.order_price_document,
d.DOCUMENT_ID
-- ,
-- ov.order_price_ordered, 
-- ov.order_price_recieved

from 
 {{ref("stg_OrderService_Orders")}} o
left join documents_view d
on o.ORDER_ID = d.ORDERID and  o.ORDER_CATALOG_ITEM_SKU = d.document_Sku
-- left join orders_total_price_view ov
-- on 
-- o.ORDER_ID = ov.ORDER_ID