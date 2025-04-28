with sup_group_id as(
    select distinct 
        SUPPLIER_ID,
        SUPPLIERS_GROUP_ID
    from
    {{ref("base_supplier_unified")}} 

),

sup_order as(
    select
        SUPPLIER_ID,
        date(RECIEVED_DATE_ITEM) as ORDER_DELIVERY_DATETIME,

        count(distinct order_id) as orders_placed,
        sum(TOTAL_PRICE_RECIEVED_ITEM) as orders_placed_TOTAL_PRICE

    from
        {{ref("base_order_unified")}} 

    group by
        SUPPLIER_ID,
        date(RECIEVED_DATE_ITEM)
)

select 
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
-- where orders_placed_TOTAL_PRICE is not null
order by 1,2,3
