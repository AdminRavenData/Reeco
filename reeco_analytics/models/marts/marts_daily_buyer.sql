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

-- -- Generate a list of missing dates (yesterday and today)
-- date_generator AS (
--     SELECT DATEADD(DAY, -1, CURRENT_DATE) AS CREATE_DATETIME
-- ),

-- distinct_buyers_outlets AS (
--     SELECT DISTINCT 
--         CHAIN_ID,
--         CHAIN_NAME,
--         BUYER_ID,
--         BUYER_NAME,
--         OUTLET_ID,
--         OUTLET_NAME,
--         CREATE_DATETIME
--     FROM buyers_view
--     CROSS JOIN date_generator
-- ),

-- missing_dates AS (
--     SELECT 
--         distinct_buyers_outlets.CHAIN_ID,
--         distinct_buyers_outlets.CHAIN_NAME,
--         distinct_buyers_outlets.BUYER_ID,
--         distinct_buyers_outlets.BUYER_NAME,
--         distinct_buyers_outlets.OUTLET_ID,
--         distinct_buyers_outlets.OUTLET_NAME,
--         distinct_buyers_outlets.CREATE_DATETIME
--     FROM distinct_buyers_outlets
--     LEFT JOIN orders_documents_inventory_unified AS existing_data
--     ON 
--       (distinct_buyers_outlets.BUYER_ID = existing_data.BUYER_ID and 
--       distinct_buyers_outlets.OUTLET_ID = existing_data.OUTLET_ID and
--       distinct_buyers_outlets.CREATE_DATETIME = existing_data.CREATE_DATETIME) or
--       (distinct_buyers_outlets.BUYER_ID = existing_data.BUYER_ID and 
--       distinct_buyers_outlets.CREATE_DATETIME = existing_data.CREATE_DATETIME and 
--       distinct_buyers_outlets.OUTLET_ID is null and existing_data.OUTLET_ID is null)

--     WHERE existing_data.BUYER_ID IS NULL
-- ),

final as (
  select 
      buyers_view.CHAIN_NAME,
      buyers_view.BUYER_NAME,
      buyers_view.OUTLET_NAME,
      orders_documents_inventory_unified.* exclude ( BUYER_ID, OUTLET_ID),
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
      (orders_documents_inventory_unified.BUYER_ID = buyers_view.BUYER_ID 
       and orders_documents_inventory_unified.OUTLET_ID = buyers_view.OUTLET_ID
       and orders_documents_inventory_unified.OUTLET_ID is not null 
       and buyers_view.OUTLET_ID is not null)
    or 
      (orders_documents_inventory_unified.BUYER_ID = buyers_view.BUYER_ID 
       and orders_documents_inventory_unified.OUTLET_ID is null 
       and buyers_view.OUTLET_ID is null)

  left join 
      buyers_roles
    on
      orders_documents_inventory_unified.BUYER_ID = buyers_roles.BUYER_ID
  where ISDISABLED != TRUE
),

-------------------------------------------
-- Adding some metrics to identify churn --
-------------------------------------------

previous_action as (
  select
    f.*,
      -- Previous order date
      MAX(CASE WHEN ORDERED_QUANTITY_ITEM IS NOT NULL THEN CREATE_DATETIME END)
        OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_order_date,

      -- Previous document date
      MAX(CASE WHEN document_quantity IS NOT NULL THEN CREATE_DATETIME END)
        OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_document_date,

      -- Previous inventory date (corrected spelling)
      MAX(CASE WHEN inventory_daily_count IS NOT NULL THEN CREATE_DATETIME END)
        OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_inventory_date
        
  from final f
),

deltas as(
      -- When there is an order, compute the day difference between current and previous CREATE_DATETIME.
    SELECT
      previous_action.*,
      -- Order delta calculation
      DATEDIFF('day', previous_order_date, CREATE_DATETIME) AS order_delta,

      -- Document delta calculation
      DATEDIFF('day', previous_document_date, CREATE_DATETIME)  AS document_delta,

      -- Inventory delta calculation
      DATEDIFF('day', previous_inventory_date, CREATE_DATETIME)  inventory_delta

    FROM 
      previous_action
),

buyer_outlet_periods AS (
  -- for each outlet filltering the dates the median will be computed on according to the outlet's lifetime in reeco
  SELECT
    BUYER_ID,
    OUTLET_ID,
    DATEDIFF('day', MIN(CREATE_DATETIME), MAX(CREATE_DATETIME)) AS period_days,
    CASE 
      WHEN DATEDIFF('day', MIN(CREATE_DATETIME), MAX(CREATE_DATETIME)) < 30 THEN 14
      WHEN DATEDIFF('day', MIN(CREATE_DATETIME), MAX(CREATE_DATETIME)) < 90 THEN 30
      WHEN DATEDIFF('day', MIN(CREATE_DATETIME), MAX(CREATE_DATETIME)) < 365 THEN 90
      ELSE 180
    END AS window_days
  FROM deltas
  GROUP BY BUYER_ID, OUTLET_ID
),

final_enriched as (
  -- Computing the medians over time for each outlet
  SELECT
      d.*,
      bop.window_days,
      (CASE 
          WHEN order_delta IS NOT NULL THEN 
            (
              SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t2.order_delta)
              FROM deltas t2
              WHERE t2.BUYER_ID = d.BUYER_ID
                AND t2.OUTLET_ID IS NOT DISTINCT FROM d.OUTLET_ID
                AND t2.CREATE_DATETIME BETWEEN DATEADD(day, -bop.window_days, d.CREATE_DATETIME) AND d.CREATE_DATETIME
            )
          ELSE NULL 
        END
      ) AS MEDIAN_order_interval_per_row,

      ( case when document_delta is not null then (
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t2.document_delta)
        FROM deltas t2
        WHERE t2.BUYER_ID = d.BUYER_ID
          AND t2.OUTLET_ID IS NOT DISTINCT FROM d.OUTLET_ID
          AND t2.CREATE_DATETIME BETWEEN DATEADD(day, -bop.window_days, d.CREATE_DATETIME) AND d.CREATE_DATETIME
        )  else null end
      ) AS MEDIAN_document_interval_per_row,

      (case when inventory_delta is not null then (
          SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t2.inventory_delta)
          FROM deltas t2
          WHERE t2.BUYER_ID = d.BUYER_ID
            AND t2.OUTLET_ID IS NOT DISTINCT FROM d.OUTLET_ID
            AND t2.CREATE_DATETIME BETWEEN DATEADD(day, -bop.window_days, d.CREATE_DATETIME) AND d.CREATE_DATETIME
        )  else null end 
      ) AS MEDIAN_inventory_interval_per_row

    FROM deltas d
    LEFT JOIN buyer_outlet_periods bop
      ON 
        (d.BUYER_ID = bop.BUYER_ID 
        and d.OUTLET_ID = bop.OUTLET_ID
        and d.OUTLET_ID is not null 
        and bop.OUTLET_ID is not null)
      or 
        (d.BUYER_ID = bop.BUYER_ID 
        and d.OUTLET_ID is null 
        and bop.OUTLET_ID is null)
  )
,

final_with_days as (
  -- computing the latest median per outlet- flow (orders, documents, inventory)
  select
    fe.*,
    -- For each group, if no previous order date exists (only one row), use the group's minimum CREATE_DATETIME.
    max(previous_order_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_order_date,
    max(previous_document_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_document_date,
    max(previous_inventory_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_inventory_date,

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
    ,
    AVG(
      CASE 
        WHEN CREATE_DATETIME BETWEEN DATEADD(day, -window_days, current_date()) AND current_date()
        THEN MEDIAN_order_interval_per_row 
        ELSE NULL 
      END
    ) OVER (PARTITION BY BUYER_ID, OUTLET_ID) AS avg_recent_order_median,

    AVG(
      CASE 
        WHEN CREATE_DATETIME BETWEEN DATEADD(day, -window_days, current_date()) AND current_date()
        THEN MEDIAN_DOCUMENT_INTERVAL_PER_ROW 
        ELSE NULL 
      END
    ) OVER (PARTITION BY BUYER_ID, OUTLET_ID) AS avg_recent_document_median,

    AVG(
      CASE 
        WHEN CREATE_DATETIME BETWEEN DATEADD(day, -window_days, current_date()) AND current_date()
        THEN MEDIAN_INVENTORY_INTERVAL_PER_ROW 
        ELSE NULL 
      END
    ) OVER (PARTITION BY BUYER_ID, OUTLET_ID) AS avg_recent_inventory_median

  from final_enriched fe
),

final_with_grades as(

  select 
    *,
    DATEADD(day, latest_median_order_interval, group_prev_order_date)  as predicted_next_order_date,
    DATEADD(day, latest_median_document_interval, group_prev_document_date)  as predicted_next_document_date,
    DATEADD(day, latest_median_inventory_interval, group_prev_inventory_date) as predicted_next_inventory_date,

    DATEDIFF('day', current_date(), DATEADD(day, latest_median_order_interval, group_prev_order_date))  as days_till_next_order,
    DATEDIFF('day', current_date(), DATEADD(day, latest_median_document_interval, group_prev_document_date))  as days_till_next_document,
    DATEDIFF('day', current_date(), DATEADD(day, latest_median_inventory_interval, group_prev_inventory_date))  as days_till_next_inventory,

    DATEDIFF('day', current_date(), DATEADD(day, latest_median_order_interval*2, group_prev_order_date)) + 1 as days_till_order_desertion,
    DATEDIFF('day', current_date(), DATEADD(day, latest_median_document_interval*2, group_prev_document_date)) + 1 as days_till_document_desertion,
    DATEDIFF('day', current_date(), DATEADD(day, latest_median_inventory_interval*2, group_prev_inventory_date)) + 1 as days_till_inventory_desertion,

    -- Compute grade_order_outlet: only if ROLE_PURCHASING is true and TOTAL_PRICE_DOCUMENT is not null
    CASE 
      WHEN role_PURCHASING AND DATEDIFF(DAY, GROUP_PREV_ORDER_DATE, CURRENT_DATE) < 90 THEN  
        LEAST(
          GREATEST(
            80 
            -  GREATEST( LEAST(100 * ((latest_median_order_interval - avg_recent_order_median) / avg_recent_order_median),20),-20)
            - 20 * (
                CASE 
                  WHEN DAYS_TILL_NEXT_ORDER > -1 THEN 0 
                  WHEN DAYS_TILL_NEXT_ORDER < 0 THEN (-1 * DAYS_TILL_NEXT_ORDER / latest_median_order_interval)
                END
            ),
            0
          ),
          100
        )
      ELSE NULL
    END AS grade_order_outlet,

    CASE 
      WHEN ROLE_ACCOUNTS_PAYABLE AND DATEDIFF(DAY, GROUP_PREV_DOCUMENT_DATE, CURRENT_DATE) < 90 THEN 
        LEAST(
          GREATEST(
            80 
            -  GREATEST( LEAST(100 * ((LATEST_MEDIAN_DOCUMENT_INTERVAL - avg_recent_document_median) / avg_recent_document_median),20),-20)
            - 20 * (
                CASE 
                  WHEN DAYS_TILL_NEXT_DOCUMENT > -1 THEN 0 
                  ELSE (-1 * DAYS_TILL_NEXT_DOCUMENT / LATEST_MEDIAN_DOCUMENT_INTERVAL)
                END
            ),
            0
          ),
          100
        )
      ELSE NULL
    END AS grade_document_outlet,

    CASE 
      WHEN ROLE_INVENTORY AND DATEDIFF(DAY, GROUP_PREV_INVENTORY_DATE, CURRENT_DATE) < 90 THEN 
        LEAST(
          GREATEST(
            80 
            -  GREATEST( LEAST(100 * ((LATEST_MEDIAN_INVENTORY_INTERVAL - avg_recent_inventory_median) / avg_recent_inventory_median),20),-20)
            - 20 * (
                CASE 
                  WHEN DAYS_TILL_NEXT_INVENTORY > -1 THEN 0 
                  ELSE (-1 * DAYS_TILL_NEXT_INVENTORY / LATEST_MEDIAN_INVENTORY_INTERVAL)
                END
            ),
            0
          ),
          100
        )
      ELSE NULL
    END AS grade_inventory_outlet,

    
    -- vars to compute the outlets weights
    CASE 
      WHEN role_PURCHASING AND DATEDIFF(DAY, GROUP_PREV_ORDER_DATE, CURRENT_DATE) < 90 THEN
      1/latest_median_order_interval else null end as pre_outlet_order_weighet,
    CASE 
      WHEN ROLE_ACCOUNTS_PAYABLE AND DATEDIFF(DAY, GROUP_PREV_DOCUMENT_DATE, CURRENT_DATE) < 90 THEN 
    1/LATEST_MEDIAN_DOCUMENT_INTERVAL else null end as pre_outlet_document_weighet,
    CASE 
      WHEN ROLE_INVENTORY AND DATEDIFF(DAY, GROUP_PREV_INVENTORY_DATE, CURRENT_DATE) < 90 THEN 
    1/LATEST_MEDIAN_INVENTORY_INTERVAL else null end as pre_outlet_inventory_weighet
  from final_with_days
  ),

final_with_days_with_buyer_temp_weights as(
  select 
    distinct buyer_id, 
    outlet_id, 
    pre_outlet_order_weighet as latest_median_order_interval, 
    pre_outlet_document_weighet as LATEST_MEDIAN_DOCUMENT_INTERVAL, 
    pre_outlet_inventory_weighet as LATEST_MEDIAN_INVENTORY_INTERVAL
    
  from 
    final_with_grades
),

final_with_days_with_buyer_weights AS (
  select
    buyer_id, 
    sum(latest_median_order_interval) as pre_buyer_order_weighet,
    sum(LATEST_MEDIAN_DOCUMENT_INTERVAL) as pre_buyer_document_weighet,
    sum(LATEST_MEDIAN_INVENTORY_INTERVAL) as pre_buyer_inventory_weighet

  from
  final_with_days_with_buyer_temp_weights
  group by
    buyer_id
),

final_with_grades_and_weights as(
select 
  final_with_grades.* ,
  pre_buyer_order_weighet,
  pre_buyer_document_weighet,
  pre_buyer_inventory_weighet,
  pre_outlet_order_weighet*1/pre_buyer_order_weighet as outlet_order_weighet,
  pre_outlet_document_weighet*1/pre_buyer_document_weighet as outlet_document_weighet,
  pre_outlet_inventory_weighet*1/pre_buyer_inventory_weighet as outlet_inventory_weighet,  
  grade_order_outlet * (pre_outlet_order_weighet*1/pre_buyer_order_weighet) as outlet_order_weighted_grade,
  grade_document_outlet * (pre_outlet_document_weighet*1/pre_buyer_document_weighet) as outlet_documents_weighted_grade,
  grade_inventory_outlet * (pre_outlet_inventory_weighet*1/pre_buyer_inventory_weighet) as outlet_inventory_weighted_grade


from
  final_with_grades
left join
  final_with_days_with_buyer_weights
on
final_with_grades.buyer_id = final_with_days_with_buyer_weights.buyer_id
)
,

buyers_overall_orders_grade_view AS (
  select distinct 
    buyer_id,
    outlet_id,
    outlet_order_weighted_grade
from
  final_with_grades_and_weights
),

buyers_overall_documents_grade_view AS (
  select distinct 
    buyer_id,
    outlet_id,
    outlet_documents_weighted_grade
from
  final_with_grades_and_weights
),

buyers_overall_inventory_grade_view AS (
  select distinct 
    buyer_id,
    outlet_id,
    outlet_inventory_weighted_grade
from
  final_with_grades_and_weights
)
,

buyer_grades as(
select 
  coalesce(orders.buyer_id, documents.buyer_id, inventory.buyer_id) as buyer_id,
  sum(outlet_order_weighted_grade) as buyer_order_weighted_grade,
  sum(outlet_documents_weighted_grade) as buyer_documents_weighted_grade,
  sum(outlet_inventory_weighted_grade) as buyer_inventory_weighted_grade,
    (
    COALESCE(sum(outlet_order_weighted_grade), 0) + 
    COALESCE(sum(outlet_documents_weighted_grade), 0) + 
    COALESCE(sum(outlet_inventory_weighted_grade), 0)
  ) / 
  NULLIF(
    (CASE WHEN sum(outlet_order_weighted_grade) IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN sum(outlet_documents_weighted_grade) IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN sum(outlet_inventory_weighted_grade) IS NOT NULL THEN 1 ELSE 0 END),
    0
) AS overall_weighted_buyer_grade

from 
  buyers_overall_orders_grade_view orders

full outer join 
  buyers_overall_documents_grade_view documents
  ON 
    (orders.BUYER_ID = documents.BUYER_ID 
    and orders.OUTLET_ID = documents.OUTLET_ID
    and orders.OUTLET_ID is not null 
    and documents.OUTLET_ID is not null)
  or 
    (orders.BUYER_ID = documents.BUYER_ID 
    and orders.OUTLET_ID is null 
    and documents.OUTLET_ID is null)

full outer join 
  buyers_overall_inventory_grade_view inventory
  ON 
    (orders.BUYER_ID = inventory.BUYER_ID 
    and orders.OUTLET_ID = inventory.OUTLET_ID
    and orders.OUTLET_ID is not null 
    and inventory.OUTLET_ID is not null)
  or 
    (orders.BUYER_ID = inventory.BUYER_ID 
    and orders.OUTLET_ID is null 
    and inventory.OUTLET_ID is null)

group by 1
)

SELECT 
  final_with_grades_and_weights.*, 
  buyer_grades.buyer_order_weighted_grade,
  buyer_grades.buyer_documents_weighted_grade,
  buyer_grades.buyer_inventory_weighted_grade,
  buyer_grades.overall_weighted_buyer_grade
FROM final_with_grades_and_weights 
LEFT JOIN buyer_grades 
  ON final_with_grades_and_weights.buyer_id = buyer_grades.buyer_id
