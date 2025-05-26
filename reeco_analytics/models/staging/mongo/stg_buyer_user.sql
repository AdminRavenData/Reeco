WITH Buyers_TEMP AS (
    SELECT
        *,
        MIN(DATEADD('millisecond',TO_NUMBER(RECORD_METADATA:"CreateTime"::STRING),'1970-01-01 00:00:00')) 
                    over (partition by _id order by RECORD_METADATA:"CreateTime"::STRING) AS BUYER_CREATED_AT,
        ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC ,__ts_ms  DESC) AS rn
    FROM 
        {{ source('reeco_mongo', 'BUYERSERVICE_BUYERS') }}
    WHERE ISDEMOACCOUNT = FALSE and ISDELETED = FALSE
    and _ID not in (select demo_id from  {{ref("stg_demo_ids")}})
),

users AS (
    SELECT
        USERS.value:_id::STRING AS user_id,
        OutletsIds.value::STRING AS outlet_id,
        _ID AS Buyer_id,
        BUYER_CREATED_AT AS Buyer_created_at,
        CHAINID AS Chain_id, 
        USERS.value:FirstName::STRING AS FirstName,
        USERS.value:LastName::STRING AS LastName,
        USERS.value:ContactInfo:Email::STRING AS Email,
        USERS.value:ContactInfo:Tel::STRING AS phone,
        USERS.value:JobTitle::STRING AS JobTitle


    FROM Buyers_TEMP,
    LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(USERS)) USERS,
    LATERAL FLATTEN(INPUT => USERS.VALUE:OutletsIds) OutletsIds
    where rn = 1
)


SELECT 
    *
FROM users