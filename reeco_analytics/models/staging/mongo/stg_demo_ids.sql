with demo_buyers as (
    select 
        _ID as demo_id,

        FROM 
            {{ source('reeco_mongo', 'BUYERSERVICE_BUYERS') }}
        where ISDEMOACCOUNT = True

),

demo_suppliers as(
    select 
        _ID as demo_id
        
        FROM 
            {{ source('reeco_mongo', 'SUPPLIERSERVICE_SUPPLIERS') }}
        where ISDEMOACCOUNT = True

),

unified_demo_ids as (

select * from demo_buyers

Union all

select * from demo_suppliers
)

select * from unified_demo_ids
group by 1
