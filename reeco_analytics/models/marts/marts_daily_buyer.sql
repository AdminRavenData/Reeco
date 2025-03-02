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
        count(distinct INVENTORY_COUNT_ID) as inventory_daily_count,
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
  select 
      buyers_view.CHAIN_NAME,
      buyers_view.BUYER_NAME,
      buyers_view.OUTLET_NAME,
      orders_documents_inventory_unified.* exclude ( BUYER_ID, OUTLET_ID),
      buyers_view.CHAIN_ID,
      buyers_view.BUYER_ID,
      buyers_view.OUTLET_ID
      
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

-------------------------------------------
-- Adding some metrics to identify churn --
-------------------------------------------

deltas as (
  select
    f.*,
    -- Compute the previous CREATE_DATETIME per group for orders.
    CASE 
      WHEN ORDERED_QUANTITY_ITEM IS NOT NULL 
      THEN lag(CREATE_DATETIME) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME)
    END AS prev_create_order,    

    -- Compute the previous CREATE_DATETIME per group for documents.
    CASE 
      WHEN DOCUMENT_QUANTITY IS NOT NULL 
      THEN lag(CREATE_DATETIME) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME)
    END AS prev_create_document, 

    -- Compute the previous CREATE_DATETIME per group for inventories.
    CASE 
      WHEN inventory_daily_count IS NOT NULL 
      THEN lag(CREATE_DATETIME) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME)
    END AS prev_create_inventory, 

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
    
    -- When there is an inventory, compute the day difference.
    case 
      when inventory_daily_count is not null then 
        DATEDIFF('day',
          lag(CREATE_DATETIME) over (partition by BUYER_ID, OUTLET_ID order by CREATE_DATETIME),
          CREATE_DATETIME)
    end as inventory_delta,

    -- Previous order date (ignoring rows without an order)
    max(case when ORDERED_QUANTITY_ITEM is not null then CREATE_DATETIME end)
      over (partition by BUYER_ID, OUTLET_ID order by CREATE_DATETIME rows between unbounded preceding and 1 preceding) as previous_order_date,
    
    -- Previous document date (ignoring rows without a document)
    max(case when document_quantity is not null then CREATE_DATETIME end)
      over (partition by BUYER_ID, OUTLET_ID order by CREATE_DATETIME rows between unbounded preceding and 1 preceding) as previous_document_date,

    -- Previous document date (ignoring rows without a document)
    max(case when inventory_daily_count is not null then CREATE_DATETIME end)
      over (partition by BUYER_ID, OUTLET_ID order by CREATE_DATETIME rows between unbounded preceding and 1 preceding) as previous_invenory_date

  from final f
),

final_enriched as (
  select
    t1.*,
    -- Rolling median of order day differences over a 3-months window.
      (
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t2.order_delta)
        FROM deltas t2
        WHERE t2.BUYER_ID = t1.BUYER_ID
          AND t2.OUTLET_ID IS NOT DISTINCT FROM t1.OUTLET_ID
          AND t2.CREATE_DATETIME BETWEEN DATEADD(day, -90, t1.CREATE_DATETIME) AND t1.CREATE_DATETIME
      ) AS MEDIAN_order_interval_per_row,

    -- -- Rolling median of document day differences over a 3-months window.
      (
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t2.document_delta)
        FROM deltas t2
        WHERE t2.BUYER_ID = t1.BUYER_ID
          AND t2.OUTLET_ID IS NOT DISTINCT FROM t1.OUTLET_ID
          AND t2.CREATE_DATETIME BETWEEN DATEADD(day, -90, t1.CREATE_DATETIME) AND t1.CREATE_DATETIME
      ) AS MEDIAN_document_interval_per_row,

    -- -- Rolling median of inventory day differences over a 3-months window.
      (
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t2.inventory_delta)
        FROM deltas t2
        WHERE t2.BUYER_ID = t1.BUYER_ID
          AND t2.OUTLET_ID IS NOT DISTINCT FROM t1.OUTLET_ID
          AND t2.CREATE_DATETIME BETWEEN DATEADD(day, -90, t1.CREATE_DATETIME) AND t1.CREATE_DATETIME
      ) AS MEDIAN_inventory_interval_per_row
      
  from deltas t1
  order by 2,3,4 desc
),

final_with_days as (
  select
    fe.*,
    -- For each group, if no previous order date exists (only one row), use the group's minimum CREATE_DATETIME.
    max(previous_order_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_order_date,
    max(previous_document_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_document_date,
    max(previous_invenory_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_inventory_date,

    -- returns the latest median for each BUYER_ID-OUTLET_ID
    FIRST_VALUE(MEDIAN_order_interval_per_row IGNORE NULLS) OVER (
      PARTITION BY BUYER_ID, OUTLET_ID
      ORDER BY CREATE_DATETIME DESC
    ) AS latest_median_order_interval,
    FIRST_VALUE(MEDIAN_document_interval_per_row IGNORE NULLS) OVER (
      PARTITION BY BUYER_ID, OUTLET_ID
      ORDER BY CREATE_DATETIME DESC
    ) AS latest_median_document_interval,
    FIRST_VALUE(MEDIAN_inventory_interval_per_row IGNORE NULLS) OVER (
      PARTITION BY BUYER_ID, OUTLET_ID
      ORDER BY CREATE_DATETIME DESC
    ) AS latest_median_inventory_interval

  from
    final_enriched fe
)

select 
  *,
  DATEADD(day, latest_median_order_interval, group_prev_order_date) + 1 as predicted_next_order_date,
  DATEADD(day, latest_median_document_interval, group_prev_document_date) + 1 as predicted_next_document_date,
  DATEADD(day, latest_median_inventory_interval, group_prev_inventory_date) + 1 as predicted_next_inventory_date,

  DATEDIFF('day', current_date(), DATEADD(day, latest_median_order_interval, group_prev_order_date)) + 1 as days_till_next_order,
  DATEDIFF('day', current_date(), DATEADD(day, latest_median_document_interval, group_prev_document_date)) + 1 as days_till_next_document,
  DATEDIFF('day', current_date(), DATEADD(day, latest_median_inventory_interval, group_prev_inventory_date)) + 1 as days_till_next_inventory,

  DATEDIFF('day', current_date(), DATEADD(day, latest_median_order_interval*2, group_prev_order_date)) + 1 as days_till_order_desertion,
  DATEDIFF('day', current_date(), DATEADD(day, latest_median_document_interval*2, group_prev_document_date)) + 1 as days_till_document_desertion,
  DATEDIFF('day', current_date(), DATEADD(day, latest_median_inventory_interval*2, group_prev_inventory_date)) + 1 as days_till_inventory_desertion
from final_with_days
