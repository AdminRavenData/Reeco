-------------------------------------------
-- Adding some metrics to identify churn --
-------------------------------------------
with  buyer_outlet_metadata as(
SELECT DISTINCT
    BUYER_ID,
    OUTLET_ID,
    MIN(TRY_TO_DATE(CREATE_DATETIME)) OVER (PARTITION BY BUYER_ID, OUTLET_ID) AS min_date,
    MAX(TRY_TO_DATE(CREATE_DATETIME)) OVER (PARTITION BY BUYER_ID, OUTLET_ID) AS max_date,
    FIRST_VALUE(CHAIN_NAME) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME) AS CHAIN_NAME,
    FIRST_VALUE(BUYER_NAME) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME) AS BUYER_NAME,
    FIRST_VALUE(OUTLET_NAME) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME) AS OUTLET_NAME,
    FIRST_VALUE(CHAIN_ID) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME) AS CHAIN_ID,
    FIRST_VALUE(ROLE_PURCHASING) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME) AS ROLE_PURCHASING,
    FIRST_VALUE(ROLE_ACCOUNTS_PAYABLE) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME) AS ROLE_ACCOUNTS_PAYABLE,
    FIRST_VALUE(ROLE_INVENTORY) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME) AS ROLE_INVENTORY,
    FIRST_VALUE(ISDISABLED) OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME) AS ISDISABLED

    from
    {{ref('marts_daily_buyer')}}  
    -- REECO.ANALYTICS_DEV.MARTS_DAILY_BUYER
),

date_range as (
    -- Compute the minimum and maximum CREATE_DATETIME for each BUYER_ID and OUTLET_ID
    SELECT
        BUYER_ID,
        OUTLET_ID,
        MIN(CREATE_DATETIME) AS min_trade_datetime,
        MAX(CREATE_DATETIME) AS max_trade_datetime,
        DATEDIFF(day, MIN(CREATE_DATETIME), MAX(CREATE_DATETIME)) + 1 AS num_days
    FROM 
        {{ref('marts_daily_buyer')}}  
        -- REECO.ANALYTICS_DEV.MARTS_DAILY_BUYER
    GROUP BY BUYER_ID, OUTLET_ID
)
,

generated_dates as(
    -- Dynamically generate a series of dates for each BUYER_ID and OUTLET_ID
    SELECT
        dr.BUYER_ID,
        dr.OUTLET_ID,
        DATEADD(day, ROW_NUMBER() OVER (PARTITION BY dr.BUYER_ID, dr.OUTLET_ID ORDER BY NULL) - 1, dr.min_trade_datetime) AS CREATE_DATETIME
    FROM date_range dr
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => (5000))) -- Dynamically set ROWCOUNT
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dr.BUYER_ID, dr.OUTLET_ID ORDER BY NULL) <= dr.num_days

)
,

buyer_outlet_date_range as(
    SELECT 

        COALESCE(marts_buyer.CHAIN_NAME, bom.CHAIN_NAME) AS CHAIN_NAME,
        COALESCE(marts_buyer.BUYER_NAME, bom.BUYER_NAME) AS BUYER_NAME,
        COALESCE(marts_buyer.OUTLET_NAME, bom.OUTLET_NAME) AS OUTLET_NAME,
        gd.CREATE_DATETIME,
        marts_buyer.ORDERED_QUANTITY_ITEM,
        marts_buyer.CHECKOUT_QUANTITY,
        marts_buyer.ORDERED_QUANTITY,
        marts_buyer.ORDERED_TOTAL_PRICE,
        marts_buyer.RECIEVED_TOTAL_PRICE,
        marts_buyer.DOCUMENT_QUANTITY,
        marts_buyer.TOTAL_PRICE_DOCUMENT,
        marts_buyer.INVENTORY_DAILY_COUNT,
        marts_buyer.INVENTORY_ITEM_TOTAL_VALUE,
        COALESCE(marts_buyer.CHAIN_ID, bom.CHAIN_ID) AS CHAIN_ID,
        gd.BUYER_ID,
        gd.OUTLET_ID,
        COALESCE(bom.ROLE_PURCHASING, marts_buyer.ROLE_PURCHASING) AS ROLE_PURCHASING,
        COALESCE(bom.ROLE_ACCOUNTS_PAYABLE, marts_buyer.ROLE_ACCOUNTS_PAYABLE) AS ROLE_ACCOUNTS_PAYABLE,
        COALESCE(bom.ROLE_INVENTORY, marts_buyer.ROLE_INVENTORY) AS ROLE_INVENTORY,
        COALESCE(bom.ISDISABLED, marts_buyer.ISDISABLED) AS ISDISABLED


    FROM generated_dates gd
    LEFT JOIN 
    {{ref('marts_daily_buyer')}} marts_buyer
    -- REECO.ANALYTICS_DEV.MARTS_DAILY_BUYER marts_buyer
    ON gd.BUYER_ID = marts_buyer.BUYER_ID
      AND gd.OUTLET_ID IS NOT DISTINCT FROM marts_buyer.OUTLET_ID
      AND gd.CREATE_DATETIME = marts_buyer.CREATE_DATETIME
    left join buyer_outlet_metadata bom
    ON gd.BUYER_ID = bom.BUYER_ID
        AND gd.OUTLET_ID IS NOT DISTINCT FROM bom.OUTLET_ID
    ORDER BY 1,2,3,4 DESC
    ),

previous_action as (
  select
    buyer_outlet_date_range.*,
      -- Previous order date
      MAX(CASE WHEN ORDERED_QUANTITY_ITEM IS NOT NULL THEN CREATE_DATETIME END)
        OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_order_date,

      -- Previous document date
      MAX(CASE WHEN document_quantity IS NOT NULL THEN CREATE_DATETIME END)
        OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_document_date,

      -- Previous inventory date (corrected spelling)
      MAX(CASE WHEN inventory_daily_count IS NOT NULL THEN CREATE_DATETIME END)
        OVER (PARTITION BY BUYER_ID, OUTLET_ID ORDER BY CREATE_DATETIME ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_inventory_date
        
  from buyer_outlet_date_range
),

deltas as(
      -- When there is an order, compute the day difference between current and previous CREATE_DATETIME.
    SELECT
      previous_action.*,
      -- Order delta calculation
      -- CASE WHEN (ORDERED_QUANTITY IS NOT NULL or  CREATE_DATETIME = DATEADD(day, -1, CURRENT_DATE())) THEN 
      DATEDIFF('day', previous_order_date, CREATE_DATETIME) 
      -- ELSE NULL END
      AS order_delta,

      -- Document delta calculation
      -- CASE WHEN (DOCUMENT_QUANTITY IS NOT NULL or  CREATE_DATETIME = DATEADD(day, -1, CURRENT_DATE())) THEN 
      DATEDIFF('day', previous_document_date, CREATE_DATETIME) 
      -- ELSE NULL END
      AS document_delta,

      -- Inventory delta calculation
      -- CASE WHEN (INVENTORY_DAILY_COUNT IS NOT NULL or  CREATE_DATETIME = DATEADD(day, -1, CURRENT_DATE())) THEN 
      DATEDIFF('day', previous_inventory_date, CREATE_DATETIME) 
      -- ELSE NULL END 
      as inventory_delta

    FROM 
      previous_action
),
buyer_outlet_periods AS (
  -- for each outlet filtering the dates the median will be computed on according to the outlet's lifetime in Reeco
  SELECT
    deltas.*,
    DATEDIFF('day', MIN(CREATE_DATETIME) over (partition by BUYER_ID, OUTLET_ID), CREATE_DATETIME) AS period_days_from_minimum,
    CASE 
      WHEN DATEDIFF('day', MIN(CREATE_DATETIME) over (partition by BUYER_ID, OUTLET_ID), CREATE_DATETIME) < 30 THEN 14
      WHEN DATEDIFF('day', MIN(CREATE_DATETIME) over (partition by BUYER_ID, OUTLET_ID), CREATE_DATETIME) < 90 THEN 30
      WHEN DATEDIFF('day', MIN(CREATE_DATETIME) over (partition by BUYER_ID, OUTLET_ID), CREATE_DATETIME) < 365 THEN 90
      ELSE 180
    END AS window_days
  FROM deltas
)
,

final_enriched as (
  -- Computing the medians over time for each outlet
  SELECT
      bop.*,
        (CASE 
            WHEN order_delta IS NOT NULL THEN 
          (
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bop2.order_delta)
            FROM buyer_outlet_periods bop2
            WHERE bop2.BUYER_ID = bop.BUYER_ID
              AND bop2.OUTLET_ID IS NOT DISTINCT FROM bop.OUTLET_ID
              AND bop2.CREATE_DATETIME BETWEEN DATEADD(day, -bop.window_days, bop.CREATE_DATETIME) AND bop.CREATE_DATETIME
          )
            ELSE NULL 
          END
        ) AS MEDIAN_order_interval_per_row,
      (CASE 
          WHEN document_delta IS NOT NULL THEN (
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bop2.document_delta)
            FROM buyer_outlet_periods bop2
            WHERE bop2.BUYER_ID = bop.BUYER_ID
              AND bop2.OUTLET_ID IS NOT DISTINCT FROM bop.OUTLET_ID
              AND bop2.CREATE_DATETIME BETWEEN DATEADD(day, -bop.window_days, bop.CREATE_DATETIME) AND bop.CREATE_DATETIME
          )  else null end
        ) AS MEDIAN_document_interval_per_row,

      (CASE 
          WHEN inventory_delta IS NOT NULL THEN (
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bop2.inventory_delta)
            FROM buyer_outlet_periods bop2
            WHERE bop2.BUYER_ID = bop.BUYER_ID
              AND bop2.OUTLET_ID IS NOT DISTINCT FROM bop.OUTLET_ID
              AND bop2.CREATE_DATETIME BETWEEN DATEADD(day, -bop.window_days, bop.CREATE_DATETIME) AND bop.CREATE_DATETIME
          )  else null end
        ) AS MEDIAN_inventory_interval_per_row

    FROM buyer_outlet_periods bop
  )
,

final_with_days as (
    -- computing the latest median per outlet- flow (orders, documents, inventory)
    select
      fe.*,
      -- -- For each group, if no previous order date exists (only one row), use the group's minimum CREATE_DATETIME.
      -- max(previous_order_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_order_date,
      -- max(previous_document_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_document_date,
      -- max(previous_inventory_date) over (partition by BUYER_ID, OUTLET_ID) as group_prev_inventory_date,

      -- -- returns the latest median for each BUYER_ID-OUTLET_ID
      -- FIRST_VALUE(MEDIAN_order_interval_per_row IGNORE NULLS) OVER (
      --   PARTITION BY BUYER_ID, OUTLET_ID
      --   ORDER BY CREATE_DATETIME DESC
      -- ) AS latest_median_order_interval,
      -- FIRST_VALUE(MEDIAN_document_interval_per_row IGNORE NULLS) OVER (
      --   PARTITION BY BUYER_ID, OUTLET_ID
      --   ORDER BY CREATE_DATETIME DESC
      -- ) AS latest_median_document_interval,
      -- FIRST_VALUE(MEDIAN_inventory_interval_per_row IGNORE NULLS) OVER (
      --   PARTITION BY BUYER_ID, OUTLET_ID
      --   ORDER BY CREATE_DATETIME DESC
      -- ) AS latest_median_inventory_interval
      -- ,
      AVG(
        CASE 
          WHEN CREATE_DATETIME BETWEEN DATEADD(day, -window_days, CREATE_DATETIME) AND CREATE_DATETIME
          THEN MEDIAN_order_interval_per_row  
          ELSE NULL 
        END
      ) OVER (PARTITION BY BUYER_ID, OUTLET_ID) AS avg_recent_order_median,

      AVG(
        CASE 
          WHEN CREATE_DATETIME BETWEEN DATEADD(day, -window_days, CREATE_DATETIME) AND CREATE_DATETIME
          THEN  MEDIAN_document_interval_per_row
          ELSE NULL 
        END
      ) OVER (PARTITION BY BUYER_ID, OUTLET_ID) AS avg_recent_document_median,

      AVG(
        CASE 
          WHEN CREATE_DATETIME BETWEEN DATEADD(day, -window_days, CREATE_DATETIME) AND CREATE_DATETIME
          THEN MEDIAN_INVENTORY_INTERVAL_PER_ROW 
          ELSE NULL 
        END
      ) OVER (PARTITION BY BUYER_ID, OUTLET_ID) AS avg_recent_inventory_median

    from final_enriched fe
)
,

final_with_grades as(
    -- the computations are to avoid grading for rows where the relevant action doesn't exist 
    -- (e.g., rows where there is inventory but no order - then you don't want to grade the order), 
    -- at least if it is the last observation of yesterday that you want to update all the actions' measures.
  select 
        final_with_days.*,
        DATEADD(day, ROUND(MEDIAN_order_interval_per_row), previous_order_date ) AS predicted_next_order_date,
        DATEADD(day, ROUND(MEDIAN_document_interval_per_row),  PREVIOUS_DOCUMENT_DATE ) AS predicted_next_document_date,
        DATEADD(day, ROUND(MEDIAN_INVENTORY_INTERVAL_PER_ROW),  previous_inventory_date ) AS predicted_next_inventory_date,
        
        DATEDIFF('day', CREATE_DATETIME, DATEADD(day, ROUND(MEDIAN_order_interval_per_row),  previous_order_date )) AS order_expected_real_delta,
        DATEDIFF('day', CREATE_DATETIME, DATEADD(day, ROUND(MEDIAN_document_interval_per_row), PREVIOUS_DOCUMENT_DATE)) AS document_expected_real_delta,
        DATEDIFF('day', CREATE_DATETIME, DATEADD(day, ROUND(MEDIAN_INVENTORY_INTERVAL_PER_ROW), previous_inventory_date )) AS inventory_expected_real_delta,

      -- DATEDIFF('day', current_date, DATEADD(day, MEDIAN_order_interval_per_row*2, CASE WHEN ORDERED_QUANTITY_ITEM > 0 THEN previous_order_date END ))  + 1 as days_till_order_desertion,
      -- DATEDIFF('day', current_date, DATEADD(day, MEDIAN_document_interval_per_row*2, CASE WHEN DOCUMENT_QUANTITY > 0 THEN PREVIOUS_DOCUMENT_DATE END) + 1 as days_till_document_desertion,
      -- DATEDIFF('day', current_date, DATEADD(day, MEDIAN_INVENTORY_INTERVAL_PER_ROW*2, CASE WHEN INVENTORY_DAILY_COUNT > 0 THEN previous_inventory_date END)) + 1 as days_till_inventory_desertion,
    -- Compute grade_order_outlet: only if ROLE_PURCHASING is true and TOTAL_PRICE_DOCUMENT is not null

    CASE 
      WHEN role_PURCHASING AND DATEDIFF(DAY, PREVIOUS_ORDER_DATE, CREATE_DATETIME ) < 90 THEN  
        LEAST(
          GREATEST(
            100 
            -  GREATEST( LEAST(100 * ((MEDIAN_order_interval_per_row - avg_recent_order_median) / NULLIF(avg_recent_order_median,0)),40),-40)
            - 20 * (
                CASE 
                  WHEN order_expected_real_delta > -1 THEN 0 
                  WHEN order_expected_real_delta < 0 THEN (-1 * order_expected_real_delta / NULLIF(MEDIAN_order_interval_per_row,0))
                END
            ),
            0
          ),
          130
        )
      ELSE NULL
    END AS grade_order_outlet, 

    CASE 
      WHEN ROLE_ACCOUNTS_PAYABLE AND DATEDIFF(DAY, PREVIOUS_DOCUMENT_DATE, CREATE_DATETIME) < 90 THEN 
        LEAST(
          GREATEST(
            100 
            -  GREATEST( LEAST(100 * ((MEDIAN_document_interval_per_row - avg_recent_document_median) / NULLIF(avg_recent_document_median,0)),40),-40)
            - 20 * (
                CASE 
                  WHEN document_expected_real_delta > -1 THEN 0 
                  ELSE (-1 * document_expected_real_delta / NULLIF(MEDIAN_document_interval_per_row,0))
                END
            ),
            0
          ),
          130
        )
      ELSE NULL
    END AS grade_document_outlet,

    CASE 
      WHEN ROLE_INVENTORY AND DATEDIFF(DAY, PREVIOUS_INVENTORY_DATE, CREATE_DATETIME) < 90 THEN 
        LEAST(
          GREATEST(
            100 
            -  GREATEST( LEAST(100 * ((MEDIAN_INVENTORY_INTERVAL_PER_ROW - avg_recent_inventory_median) / NULLIF(avg_recent_inventory_median,0)),40),-40)
            - 20 * (
                CASE 
                  WHEN inventory_expected_real_delta > -1 THEN 0 
                  ELSE (-1 * inventory_expected_real_delta / NULLIF(MEDIAN_INVENTORY_INTERVAL_PER_ROW,0))
                END
            ),
            0
          ),
          130
        )
      ELSE NULL
    END AS grade_inventory_outlet,

    
    -- vars to compute the outlets weights
    CASE 
      WHEN role_PURCHASING AND DATEDIFF(DAY, PREVIOUS_ORDER_DATE, CREATE_DATETIME ) < 90 THEN  
      avg(1/NULLIF(MEDIAN_order_interval_per_row,0)) OVER (PARTITION BY BUYER_ID, OUTLET_ID)  else null end as pre_outlet_order_weighet,
    CASE 
      WHEN ROLE_ACCOUNTS_PAYABLE AND DATEDIFF(DAY, PREVIOUS_DOCUMENT_DATE, CREATE_DATETIME) < 90 THEN 
    avg(1/NULLIF(MEDIAN_document_interval_per_row,0)) OVER (PARTITION BY BUYER_ID, OUTLET_ID)  else null end as pre_outlet_document_weighet,
    CASE 
      WHEN ROLE_INVENTORY AND DATEDIFF(DAY, PREVIOUS_INVENTORY_DATE, CREATE_DATETIME) < 90 THEN 
    avg(1/NULLIF(MEDIAN_INVENTORY_INTERVAL_PER_ROW,0)) OVER (PARTITION BY BUYER_ID, OUTLET_ID)  else null end as pre_outlet_inventory_weighet
  from final_with_days
  )
  ,

final_with_days_with_buyer_weights AS (
  select
    buyer_id, 
    sum(distinct pre_outlet_order_weighet) as pre_buyer_order_weighet,
    sum(distinct pre_outlet_document_weighet) as pre_buyer_document_weighet,
    sum(distinct pre_outlet_inventory_weighet) as pre_buyer_inventory_weighet
  from (
    select 
      distinct buyer_id, 
      outlet_id, 
      pre_outlet_order_weighet, 
      pre_outlet_document_weighet, 
      pre_outlet_inventory_weighet
    from 
      final_with_grades
  ) 
  group by
    buyer_id
)
,

final_with_grades_and_weights as(
select 
  final_with_grades.* ,
  DATEDIFF('day', CREATE_DATETIME, predicted_next_order_date) AS days_till_next_order,
  DATEDIFF('day', CREATE_DATETIME, predicted_next_document_date) AS days_till_next_document,
  DATEDIFF('day', CREATE_DATETIME, predicted_next_inventory_date) AS days_till_next_inventory,
  pre_buyer_order_weighet,
  pre_buyer_document_weighet,
  pre_buyer_inventory_weighet,
  pre_outlet_order_weighet*1/NULLIF(pre_buyer_order_weighet,0) as outlet_order_weighet,
  pre_outlet_document_weighet*1/NULLIF(pre_buyer_document_weighet,0) as outlet_document_weighet,
  pre_outlet_inventory_weighet*1/NULLIF(pre_buyer_inventory_weighet,0) as outlet_inventory_weighet,  
  grade_order_outlet * (pre_outlet_order_weighet*1/NULLIF(pre_buyer_order_weighet,0)) as outlet_order_weighted_grade,
  grade_document_outlet * (pre_outlet_document_weighet*1/NULLIF(pre_buyer_document_weighet,0)) as outlet_document_weighted_grade,
  grade_inventory_outlet * (pre_outlet_inventory_weighet*1/NULLIF(pre_buyer_inventory_weighet,0)) as outlet_inventory_weighted_grade,
  ( COALESCE(grade_order_outlet, 0) + 
    COALESCE(grade_document_outlet, 0) + 
    COALESCE(grade_inventory_outlet, 0)
  ) / 
  NULLIF(
    (CASE WHEN grade_order_outlet IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN grade_document_outlet IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN grade_inventory_outlet IS NOT NULL THEN 1 ELSE 0 END),
    0
  ) AS overall_weighted_outlet_grade


from
  final_with_grades
left join
  final_with_days_with_buyer_weights
on
final_with_grades.buyer_id = final_with_days_with_buyer_weights.buyer_id
)
,

buyers_overall_grade_view AS (
  select  
    buyer_id,
    CREATE_DATETIME,
    sum(OUTLET_ORDER_WEIGHTED_GRADE) as buyer_order_weighted_grade,
    sum(OUTLET_DOCUMENT_WEIGHTED_GRADE) as buyer_documents_weighted_grade,
    sum(OUTLET_INVENTORY_WEIGHTED_GRADE) as buyer_inventory_weighted_grade,
    (
      COALESCE(sum(outlet_order_weighted_grade), 0) + 
      COALESCE(sum(outlet_document_weighted_grade), 0) + 
      COALESCE(sum(outlet_inventory_weighted_grade), 0)
    ) / 
    NULLIF(
      (CASE WHEN sum(outlet_order_weighted_grade) IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN sum(outlet_document_weighted_grade) IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN sum(outlet_inventory_weighted_grade) IS NOT NULL THEN 1 ELSE 0 END),
      0
    ) AS overall_weighted_buyer_grade
from
  final_with_grades_and_weights
group by 
  buyer_id,
  CREATE_DATETIME 
)



select 
  final_with_grades_and_weights.* ,
  buyers_overall_grade_view.buyer_order_weighted_grade,
  buyers_overall_grade_view.buyer_documents_weighted_grade,
  buyers_overall_grade_view.buyer_inventory_weighted_grade,
  buyers_overall_grade_view.overall_weighted_buyer_grade,
  row_number() over (partition by final_with_grades_and_weights.buyer_id order by final_with_grades_and_weights.CREATE_DATETIME desc) as row_num_buyer,
  row_number() over (partition by final_with_grades_and_weights.buyer_id, final_with_grades_and_weights.outlet_id order by final_with_grades_and_weights.CREATE_DATETIME desc) as row_num_outlet
  from final_with_grades_and_weights
  left join buyers_overall_grade_view
  on final_with_grades_and_weights.buyer_id = buyers_overall_grade_view.buyer_id
  and final_with_grades_and_weights.CREATE_DATETIME = buyers_overall_grade_view.CREATE_DATETIME
  