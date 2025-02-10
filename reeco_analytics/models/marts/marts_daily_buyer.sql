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

union all
select
    CHAIN_ID,
    BUYER_ID,
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



orders_view as(
    select
        BUYER_ID,
        OUTLET_ID,
        date(ORDER_CREATED_DATE) as CREATE_DATETIME,

        -- max(date(ORDER_DELIVERY_DATETIME)) as ORDER_DELIVERY_DATETIME,
        sum(QUANTITY_PACKING_ORDERED_ITEM) as ORDERED_QUANTITY_ITEM,
        -- sum(QUANTITY_PACKING_RECIEVED_ITEM) as RECIEVED_QUANTITY_ITEM,
        COUNT(DISTINCT ORDER_ID) as ORDERED_QUANTITY,
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

        count(distinct DOCUMENT_ID) as document_quantity,
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
),

final as (
  -- Your original query goes here. For example:
  select 
      buyers_view.chain_id,
      orders_documents_inventory_unified.*,
      buyers_view.* exclude (chain_id, BUYER_ID, OUTLET_ID)
  from
      orders_documents_inventory_unified
  left join 
      buyers_view
    on 
      (orders_documents_inventory_unified.BUYER_ID = buyers_view.BUYER_ID 
       and orders_documents_inventory_unified.OUTLET_ID = buyers_view.OUTLET_ID
       and orders_documents_inventory_unified.OUTLET_ID is not null 
       and buyers_view.OUTLET_ID is not null)
    or 
      (orders_documents_inventory_unified.BUYER_ID = buyers_view.BUYER_ID 
       and orders_documents_inventory_unified.OUTLET_ID is null 
       and buyers_view.OUTLET_ID is null)
  order by 
      chain_id, 
      orders_documents_inventory_unified.BUYER_ID, 
      orders_documents_inventory_unified.OUTLET_ID, 
      orders_documents_inventory_unified.CREATE_DATETIME
),

deltas as (
  select
    f.*,
    -- Compute the previous CREATE_DATETIME per group.
    lag(CREATE_DATETIME) over (partition by BUYER_ID, OUTLET_ID order by CREATE_DATETIME) as prev_create,
    
    -- When there is an order, compute the day difference between current and previous CREATE_DATETIME.
    case 
      when ORDERED_QUANTITY_ITEM is not null then 
        DATEDIFF('day',
          lag(CREATE_DATETIME) over (partition by BUYER_ID, OUTLET_ID order by CREATE_DATETIME),
          CREATE_DATETIME)
    end as order_delta,
    
    -- When there is a document, compute the day difference.
    case 
      when document_quantity is not null then 
        DATEDIFF('day',
          lag(CREATE_DATETIME) over (partition by BUYER_ID, OUTLET_ID order by CREATE_DATETIME),
          CREATE_DATETIME)
    end as document_delta,
    
    -- Previous order date (ignoring rows without an order)
    max(case when ORDERED_QUANTITY_ITEM is not null then CREATE_DATETIME end)
      over (partition by BUYER_ID, OUTLET_ID order by CREATE_DATETIME rows between unbounded preceding and 1 preceding) as previous_order_date,
    
    -- Previous document date (ignoring rows without a document)
    max(case when document_quantity is not null then CREATE_DATETIME end)
      over (partition by BUYER_ID, OUTLET_ID order by CREATE_DATETIME rows between unbounded preceding and 1 preceding) as previous_document_date
  from final f
),

final_enriched as (
  select
    d.*,
    -- Rolling median of order day differences over a 14-day window.
      MEDIAN(order_delta) over (partition by BUYER_ID, OUTLET_ID) as MEDIAN_order_interval,
    
    -- Rolling median of document day differences over a 14-day window.
      MEDIAN(document_delta) over (partition by BUYER_ID, OUTLET_ID)  as MEDIAN_document_interval,
    
    -- Rolling median of ORDERED_QUANTITY_ITEM over a 14-day window.
      MEDIAN(ORDERED_QUANTITY_ITEM) over (partition by BUYER_ID, OUTLET_ID) as MEDIAN_ordered_qty_last2w,
    
    -- Rolling median of document_quantity over a 14-day window.
      MEDIAN(document_quantity) over (partition by BUYER_ID, OUTLET_ID) as MEDIAN_document_qty_last2w
  from deltas d
),

final_with_days as (
  select
    fe.*,
    -- For each group, if no previous order date exists (only one row), use the group's minimum CREATE_DATETIME.
    max(previous_order_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_order_date,
    max(previous_document_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_document_date,
    max(MEDIAN_order_interval) over (partition by BUYER_ID, OUTLET_ID) as group_MEDIAN_order_interval,
    max(MEDIAN_document_interval) over (partition by BUYER_ID, OUTLET_ID) as group_median_document_interval
  from final_enriched fe
)

select 
  *,
  DATEADD(day, group_MEDIAN_order_interval, group_prev_order_date) + 1 as predicted_next_order_date,
  DATEDIFF('day', current_date(), DATEADD(day, group_MEDIAN_order_interval, group_prev_order_date)) + 1 as days_till_next_order,
  DATEADD(day, group_median_document_interval, group_prev_document_date) + 1 as predicted_next_document_date,
  DATEDIFF('day', current_date(), DATEADD(day, group_median_document_interval, group_prev_document_date)) + 1 as days_till_next_document
from final_with_days
