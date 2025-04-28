with stg_order_header as(
    select * 
    from
    {{ref("stg_order_header")}}
),

stg_orders_lines as (
    select * 
    from
    {{ref("stg_order_line")}}
)

select 
o.*,
l.* exclude (order_id,ORDER_CREATED_DATE)
 from stg_order_header o
left join
stg_orders_lines l
on 
o.order_id = l.order_id