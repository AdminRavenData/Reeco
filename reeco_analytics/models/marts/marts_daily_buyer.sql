with buyers_view as(
select
    CHAIN_ID,
    BUYER_ID,
    OUTLET_ID,
    CHAIN_NAME,
    BUYER_NAME,
    OUTLET_NAME
from
    {{ref("base_buyer_outlet")}} 
),

orders_view as(
    select
        BUYER_ID,
        OUTLET_ID,
        date(ORDER_CREATED_DATE) as CREATE_DATETIME,

        -- max(date(ORDER_DELIVERY_DATETIME)) as ORDER_DELIVERY_DATETIME,
        sum(QUANTITY_PACKING_ORDERED_ITEM) as ORDERED_QUANTITY,
        sum(QUANTITY_PACKING_RECIEVED_ITEM) as RECIEVED_QUANTITY,
        sum(TOTAL_PRICE_ORDERED_ITEM) as ORDERED_TOTAL_PRICE,
        sum(TOTAL_PRICE_RECIEVED_ITEM) as RECIEVED_TOTAL_PRICE

    from
        {{ref("base_order_unified")}} 

    group by

        BUYER_ID,
        OUTLET_ID,
        date(ORDER_CREATED_DATE)
),

documents_view as(
    select 
        BUYER_ID,
        OUTLET_ID,
        date(CREATE_DATETIME) as CREATE_DATETIME,

        max(TOTAL_PRODUCT_PRICE) as TOTAL_PRODUCT_PRICE_documents,
        max(TOTAL_AMOUNT) as TOTAL_AMOUNT_documents,
        sum(Total_Price_item_document) as Total_Price_document


    from
        {{ref("base_document_unified")}} 

    group by
        BUYER_ID,
        OUTLET_ID,
        date(CREATE_DATETIME)
),

inventory_view as(
    select 
        BUYER_ID,
        OUTLET_ID,
        date(CREATE_DATETIME) as CREATE_DATETIME,
        count(distinct INVENTORY_COUNT_ID) as inventor_daily_count,
        sum(INVENTORY_ITEM_TOTAL_VALUE) as INVENTORY_ITEM_TOTAL_VALUE

    from
    {{ref("base_inventory_unified")}} 

    group by
        BUYER_ID,
        OUTLET_ID,
        date(CREATE_DATETIME) 
),

orders_documents_unified as (

    select 
        coalesce(orders_view.BUYER_ID, documents_view.BUYER_ID) as BUYER_ID,
        coalesce(orders_view.OUTLET_ID, documents_view.OUTLET_ID) as OUTLET_ID,
        coalesce(orders_view.CREATE_DATETIME, documents_view.CREATE_DATETIME) as CREATE_DATETIME,

        orders_view.* exclude(BUYER_ID, OUTLET_ID, CREATE_DATETIME),
        documents_view.* exclude(BUYER_ID, OUTLET_ID, CREATE_DATETIME)


    from orders_view
    full outer join documents_view 
    on (orders_view.BUYER_ID = documents_view.BUYER_ID and 
    orders_view.OUTLET_ID = documents_view.OUTLET_ID and
    orders_view.CREATE_DATETIME = documents_view.CREATE_DATETIME) or 
    (orders_view.BUYER_ID = documents_view.BUYER_ID and 
    orders_view.CREATE_DATETIME = documents_view.CREATE_DATETIME and 
    orders_view.OUTLET_ID is null and  documents_view.OUTLET_ID is null)

),

orders_documents_inventory_unified as (
    select 
        coalesce(orders_documents_unified.BUYER_ID, inventory_view.BUYER_ID) as BUYER_ID,
        coalesce(orders_documents_unified.OUTLET_ID, inventory_view.OUTLET_ID) as OUTLET_ID,
        coalesce(orders_documents_unified.CREATE_DATETIME, inventory_view.CREATE_DATETIME) as CREATE_DATETIME,
        orders_documents_unified.* exclude(BUYER_ID, OUTLET_ID, CREATE_DATETIME),
        inventory_view.* exclude(BUYER_ID, OUTLET_ID, CREATE_DATETIME)

    from
        orders_documents_unified
    full outer join inventory_view 
    on (orders_documents_unified.BUYER_ID = inventory_view.BUYER_ID and 
    orders_documents_unified.OUTLET_ID = inventory_view.OUTLET_ID and
    orders_documents_unified.CREATE_DATETIME = inventory_view.CREATE_DATETIME) or
    (orders_documents_unified.BUYER_ID = inventory_view.BUYER_ID and 
    orders_documents_unified.CREATE_DATETIME = inventory_view.CREATE_DATETIME and 
    orders_documents_unified.OUTLET_ID is null and inventory_view.OUTLET_ID is null)
)

select 
    buyers_view.chain_id,
    orders_documents_inventory_unified.*,
    buyers_view.* exclude (chain_id, BUYER_ID, OUTLET_ID),

from
    orders_documents_inventory_unified
left join 
    buyers_view

on orders_documents_inventory_unified.BUYER_ID = buyers_view.BUYER_ID 
and orders_documents_inventory_unified.OUTLET_ID = buyers_view.OUTLET_ID 


order by chain_id, orders_documents_inventory_unified.BUYER_ID, orders_documents_inventory_unified.OUTLET_ID, orders_documents_inventory_unified.CREATE_DATETIME