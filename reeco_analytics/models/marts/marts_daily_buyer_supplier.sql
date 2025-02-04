with sup_group_id as(
    select distinct 
        SUPPLIER_ID,
        SUPPLIERS_GROUP_ID
    from
    {{ref("base_supplier_unified")}} 

),

buyers_view as(
    select
        CHAIN_ID,
        BUYER_ID

    from
        {{ref("base_buyer_outlet")}} 
),


sup_order as(
    select
        BUYER_SUPPLIER_ID,
        BUYER_ID,
        SUPPLIER_ID,
        date(RECIEVED_DATE_ITEM) as ORDER_DELIVERY_DATETIME,

        count(distinct order_id) as orders_placed,
        sum(TOTAL_PRICE_RECIEVED_ITEM) as orders_placed_TOTAL_PRICE

    from
        {{ref("base_order_unified")}} 

    group by
        BUYER_SUPPLIER_ID,
        BUYER_ID,
        SUPPLIER_ID,
        date(RECIEVED_DATE_ITEM)
)

select 
distinct
    sup_order.BUYER_SUPPLIER_ID,
    sup_order.BUYER_ID,
    buyers_view.CHAIN_ID,
    sup_order.SUPPLIER_ID,
    sup_group_id.SUPPLIERS_GROUP_ID,
    ORDER_DELIVERY_DATETIME,
    orders_placed,
    orders_placed_TOTAL_PRICE

from 
    sup_order
left join 
    sup_group_id
on
sup_order.SUPPLIER_ID = sup_group_id.SUPPLIER_ID

left join 
    buyers_view
on
sup_order.BUYER_ID = buyers_view.BUYER_ID
order by 1,6