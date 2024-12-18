with documents_view as(
select 
max(TOTALPRODUCTSPRICE) AS order_price_document,
ORDERID

from 
 {{ref("stg_DocumentService_Documents")}}
group by ORDERID
),

orders_total_price_view as(
select 
ORDER_ID,  
sum(ITEM_PRICE_ORDERED) as order_price_ordered, 
sum(ITEM_PRICE_RECEIVED) as order_price_recieved  
from
 {{ref("stg_OrderService_Orders")}}
group by ORDER_ID 
)

select 
o.* ,
ov.order_price_ordered, 
ov.order_price_recieved,
d.order_price_document

from 
 {{ref("stg_OrderService_Orders")}} o
left join documents_view d
on o.ORDER_ID = d.ORDERID
left join orders_total_price_view ov
on 
o.ORDER_ID = ov.ORDER_ID