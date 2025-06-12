with buyers_view as(
select
    CHAIN_ID,
    BUYER_ID,
    Buyer_created_at,
    buyer_deleted_at,
    OUTLET_ID,
    CHAIN_NAME,
    BUYER_NAME,
    OUTLET_NAME
from
    {{ref("base_buyer_outlet")}} 

union all

select
    CHAIN_ID,
    BUYER_ID,
    min(Buyer_created_at) as Buyer_created_at,
    min(buyer_deleted_at) as buyer_deleted_at,
    null OUTLET_ID,
    CHAIN_NAME,
    BUYER_NAME,
    null OUTLET_NAME
from
    {{ref("base_buyer_outlet")}} 
group by
    CHAIN_ID,
    BUYER_ID,
    CHAIN_NAME,
    BUYER_NAME
),

buyers_roles as(
select
    BUYER_ID,
    PURCHASING as role_PURCHASING, 
    ACCOUNTS_PAYABLE as role_ACCOUNTS_PAYABLE,
    INVENTORY as role_INVENTORY,
    ISDISABLED
from
    {{ref("base_buyer")}} 
),


orders_view as(
    select
        BUYER_ID,
        OUTLET_ID,
        date(ORDER_CREATED_DATE) as CREATE_DATETIME,
        sum(QUANTITY_PACKING_ORDERED_ITEM) as ORDERED_QUANTITY_ITEM,
        COUNT(DISTINCT Checkout_ID) as Checkout_QUANTITY,       
        COUNT(DISTINCT ORDER_ID) as ORDERED_QUANTITY,
        sum(TOTAL_PRICE_ORDERED_ITEM) as ORDERED_TOTAL_PRICE,
        sum(TOTAL_PRICE_RECIEVED_ITEM) as RECIEVED_TOTAL_PRICE
    from
        {{ref("base_order_unified")}} 
    group by
        BUYER_ID,
        OUTLET_ID,
        date(ORDER_CREATED_DATE)
    
    -- to include yesterday in the data frame in order to update all the metrics according to yesterday
    UNION ALL
    
    select
        distinct bo.BUYER_ID,
        bo.OUTLET_ID,
        DATEADD(day, -1, CURRENT_DATE()) as CREATE_DATETIME,
        null as ORDERED_QUANTITY_ITEM,
        null as Checkout_QUANTITY,
        null as ORDERED_QUANTITY,
        null as ORDERED_TOTAL_PRICE,
        null as RECIEVED_TOTAL_PRICE
    from
        {{ref("base_order_unified")}} bo
    where not exists (
        select 1
        from {{ref("base_order_unified")}} check_orders
        where check_orders.BUYER_ID = bo.BUYER_ID
        and check_orders.OUTLET_ID IS NOT DISTINCT FROM bo.OUTLET_ID
        and date(check_orders.ORDER_CREATED_DATE) = DATEADD(day, -1, CURRENT_DATE())
    )
),

documents_view as(
    select 
        BUYER_ID,
        OUTLET_ID,
        date(CREATE_DATETIME) as CREATE_DATETIME,
        count(distinct DOCUMENT_ID) as document_quantity,
        sum(Total_Price_item_document) as Total_Price_document
    from
        {{ref("base_document_unified")}} 
    group by
        BUYER_ID,
        OUTLET_ID,
        date(CREATE_DATETIME)
        
    -- to include yesterday in the data frame in order to update all the metrics according to yesterday
    UNION ALL
    
    select
        distinct bd.BUYER_ID,
        bd.OUTLET_ID,
        DATEADD(day, -1, CURRENT_DATE()) as CREATE_DATETIME,
        null as document_quantity,
        null as Total_Price_document
    from
        {{ref("base_document_unified")}} bd
    where not exists (
        select 1
        from {{ref("base_document_unified")}} check_docs
        where check_docs.BUYER_ID = bd.BUYER_ID
        and check_docs.OUTLET_ID IS NOT DISTINCT FROM bd.OUTLET_ID
        and date(check_docs.CREATE_DATETIME) = DATEADD(day, -1, CURRENT_DATE())
    )
),

inventory_view as(
    select 
        BUYER_ID,
        OUTLET_ID,
        date(CREATE_DATETIME) as CREATE_DATETIME,
        count(distinct INVENTORY_COUNT_ID) as inventory_daily_count,
        sum(INVENTORY_ITEM_TOTAL_VALUE) as INVENTORY_ITEM_TOTAL_VALUE
    from
      {{ref("base_inventory_unified")}} 
    group by
        BUYER_ID,
        OUTLET_ID,
        date(CREATE_DATETIME)
        
    -- to include yesterday in the data frame in order to update all the metrics according to yesterday
    UNION ALL
    
    select
        distinct bi.BUYER_ID,
        bi.OUTLET_ID,
        DATEADD(day, -1, CURRENT_DATE()) as CREATE_DATETIME,
        null as inventory_daily_count,
        null as INVENTORY_ITEM_TOTAL_VALUE
    from
        {{ref("base_inventory_unified")}} bi
    where not exists (
        select 1
        from {{ref("base_inventory_unified")}} check_inv
        where check_inv.BUYER_ID = bi.BUYER_ID
        and check_inv.OUTLET_ID IS NOT DISTINCT FROM bi.OUTLET_ID
        and date(check_inv.CREATE_DATETIME) = DATEADD(day, -1, CURRENT_DATE())
    )
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
    on orders_view.BUYER_ID = documents_view.BUYER_ID  
      AND orders_view.OUTLET_ID IS NOT DISTINCT FROM documents_view.OUTLET_ID
      AND orders_view.CREATE_DATETIME = documents_view.CREATE_DATETIME
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
    on orders_documents_unified.BUYER_ID = inventory_view.BUYER_ID  
      AND orders_documents_unified.OUTLET_ID IS NOT DISTINCT FROM inventory_view.OUTLET_ID
      AND orders_documents_unified.CREATE_DATETIME = inventory_view.CREATE_DATETIME
)

select 
    buyers_view.CHAIN_NAME,
    buyers_view.BUYER_NAME,
    buyers_view.OUTLET_NAME,
    buyers_view.BUYER_CREATED_AT,
    buyers_view.BUYER_DELETED_AT,
    orders_documents_inventory_unified.* exclude (BUYER_ID, OUTLET_ID),
    buyers_view.CHAIN_ID,
    buyers_view.BUYER_ID,
    buyers_view.OUTLET_ID,
    role_PURCHASING, 
    role_ACCOUNTS_PAYABLE, 
    role_INVENTORY,
    ISDISABLED
from
    orders_documents_inventory_unified
left join 
    buyers_view
on 
    orders_documents_inventory_unified.BUYER_ID = buyers_view.BUYER_ID 
    AND orders_documents_inventory_unified.OUTLET_ID IS NOT DISTINCT FROM buyers_view.OUTLET_ID
left join 
    buyers_roles
on
    orders_documents_inventory_unified.BUYER_ID = buyers_roles.BUYER_ID
where ISDISABLED != TRUE
