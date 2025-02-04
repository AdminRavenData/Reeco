with stg_inventory_header as(
    select * 
    from
    {{ref("stg_inventory_header")}}
),

stg_inventory_line as (
    select * 
    from
    {{ref("stg_inventory_line")}}
)

select 
h.*,
l.* exclude (INVENTORY_COUNT_ID)
 from stg_inventory_header h
left join
stg_inventory_line l
on 
h.INVENTORY_COUNT_ID = l.INVENTORY_COUNT_ID