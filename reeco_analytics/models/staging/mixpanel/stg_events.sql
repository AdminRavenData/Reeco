WITH base_data AS (
    SELECT
        -- Basic event information
        DATA:device_id::string AS device_id,
        DATA:distinct_id::string AS session_id,
        DATA:event_name::string AS event_name,
        
        -- Convert Unix timestamp (seconds) to datetime
        CASE 
            WHEN TRY_CAST(DATA:time::string AS number) IS NOT NULL 
            THEN TIMESTAMPADD(second, DATA:time::number, '1970-01-01')::timestamp 
            ELSE NULL 
        END AS event_datetime,
        
        -- Properties with timestamps
        CASE 
            WHEN TRY_CAST(DATA:properties:"$mp_api_timestamp_ms"::string AS number) IS NOT NULL 
            THEN TIMESTAMPADD(millisecond, DATA:properties:"$mp_api_timestamp_ms"::number, '1970-01-01')::timestamp 
            ELSE NULL 
        END AS mp_api_datetime,
        
        CASE 
            WHEN TRY_CAST(DATA:properties:mp_processing_time_ms::string AS number) IS NOT NULL 
            THEN TIMESTAMPADD(millisecond, DATA:properties:mp_processing_time_ms::number, '1970-01-01')::timestamp 
            ELSE NULL 
        END AS mp_processing_datetime,
        
        -- Add row_number partitioned by distinct_id and ordered by mp_api_datetime
        ROW_NUMBER() OVER (
            PARTITION BY DATA:distinct_id::string 
            ORDER BY mp_api_datetime ASC
        ) AS session_order,
        
        -- Other properties
        DATA:insert_id::string AS insert_id,
        DATA:user_id::string AS user_id,
        DATA:properties:buyerId::string AS buyer_id,
        DATA:properties:chainId::string AS chain_id,
        DATA:properties:clickLocation::string AS click_location,
        DATA:properties:mp_country_code::string AS country_code,
        DATA:properties:mp_lib::string AS mp_lib,
        DATA:properties:mp_sent_by_lib_version::string AS mp_sent_by_lib_version,
        DATA:properties:productID::string AS product_id,
        DATA:properties:productName::string AS product_name,
        DATA:properties:searchTerm::string AS search_term,
        DATA:properties:username::string AS username,
        DATA:properties:"$browser"::string AS browser,
        DATA:properties:"$browser_version"::string AS browser_version,
        DATA:properties:"$city"::string AS city,
        DATA:properties:"$current_url"::string AS current_url,
        DATA:properties:"$device_id"::string AS properties_device_id,
        DATA:properties:"$initial_referrer"::string AS initial_referrer,
        DATA:properties:"$initial_referring_domain"::string AS initial_referring_domain,
        DATA:properties:"$insert_id"::string AS properties_insert_id,
        DATA:properties:"$lib_version"::string AS lib_version,
        DATA:properties:"$mp_api_endpoint"::string AS mp_api_endpoint,
        DATA:properties:"$os"::string AS os,
        DATA:properties:"$region"::string AS region,
        DATA:properties:"$user_id"::string AS properties_user_id,
        TRY_CAST(DATA:properties:location::string AS number) AS location,
        TRY_CAST(DATA:properties:"$screen_height"::string AS number) AS screen_height,
        TRY_CAST(DATA:properties:"$screen_width"::string AS number) AS screen_width
    FROM 
        REECO.MIXPANEL.MP_MASTER_EVENT_RAW
)
SELECT
    null as test2,*,
    -- Parse EVENT_NAME into components using SPLIT_PART
    SPLIT_PART(event_name, ' | ', 1) AS event_name_part_1,
    SPLIT_PART(event_name, ' | ', 2) AS event_name_part_2,
    SPLIT_PART(event_name, ' | ', 3) AS event_name_part_3,

    -- Add session_time: time difference between earliest mp_api_datetime and latest mp_processing_datetime
    DATEDIFF(
        'second',
        MIN(mp_api_datetime) OVER (PARTITION BY device_id,session_id),
        MAX(mp_processing_datetime) OVER (PARTITION BY device_id,session_id)
    ) AS session_time_seconed,

    DATEDIFF(
        'millisecond',
        MIN(mp_api_datetime) OVER (PARTITION BY device_id,session_id),
        MAX(mp_processing_datetime) OVER (PARTITION BY device_id,session_id)
    ) AS session_time_millisecond,

    -- Add event_time: difference between mp_processing_datetime and mp_api_datetime
    DATEDIFF(
        'second',
        mp_api_datetime,
        mp_processing_datetime
    ) AS event_time_seconed,
    DATEDIFF(
        'millisecond',
        mp_api_datetime,
        mp_processing_datetime
    ) AS event_time_millisecond
FROM 
    base_data