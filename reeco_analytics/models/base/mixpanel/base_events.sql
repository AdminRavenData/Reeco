WITH latest_metrics AS (
    SELECT 
        buyer_id,
        buyer_name,
        chain_name,
        overall_weighted_buyer_grade AS latest_overall_weighted_buyer_grade
    FROM 
        {{ref('marts_daily_buyer_desertion_metrics')}}
    QUALIFY 
        ROW_NUMBER() OVER (
            PARTITION BY buyer_id 
            ORDER BY CREATE_DATETIME DESC
        ) = 1
)
SELECT 
    stg_events.*,
    case when event_name like '%Product added directly from store%' then 'Directly from store'
            when event_name like '%order guide button got toggled%' then 'order guide button got toggled'
            else null end as product_directly_vs_order_guide,
    -- Compute the next values for event_name parts within the same session_id partition
    LEAD(SPLIT_PART(event_name, ' | ', 1)) OVER (
        PARTITION BY session_id 
        ORDER BY session_order
    ) AS next_event_name_part_1,
    LEAD(SPLIT_PART(event_name, ' | ', 2)) OVER (
        PARTITION BY session_id 
        ORDER BY session_order
    ) AS next_event_name_part_2,
    LEAD(SPLIT_PART(event_name, ' | ', 3)) OVER (
        PARTITION BY session_id 
        ORDER BY session_order
    ) AS next_event_name_part_3,

    latest_metrics.buyer_name AS buyer_name,
    latest_metrics.chain_name AS chain_name,
    -- Add the latest overall weighted buyer grade for each buyer
    latest_metrics.latest_overall_weighted_buyer_grade AS latest_overall_weighted_buyer_grade

FROM
    {{ref('stg_events')}} AS stg_events
LEFT JOIN 
    latest_metrics
ON 
    stg_events.buyer_id = latest_metrics.buyer_id
