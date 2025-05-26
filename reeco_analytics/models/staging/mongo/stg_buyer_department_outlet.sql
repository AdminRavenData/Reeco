WITH Buyers_TEMP AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY _id ORDER BY updatedatetime desc , __ts_ms  DESC) AS rn
    FROM 
        {{ source('reeco_mongo', 'BUYERSERVICE_BUYERS') }}
    WHERE ISDEMOACCOUNT = FALSE and ISDELETED = FALSE
    and _ID not in (select demo_id from  {{ref("stg_demo_ids")}})
),

Departments_outlets AS (
    SELECT
        _ID AS Buyer_id,
        DEPT.VALUE:_id::STRING AS department_id,
        DEPT.VALUE:Name::STRING AS department_name,
        OUTLET.value:_id::STRING AS outlet_id,
        OUTLET.value:Name::STRING AS outlet_name,
        OUTLET.value:Code::STRING AS code_outlet,
        OUTLET.value:Description::STRING AS Description_outlet,
        OUTLET.value:Budget:Value::STRING AS Budget_Value_outlet

    FROM Buyers_TEMP,
    LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(BUYERDEPARTMENTS)) DEPT,
    LATERAL FLATTEN(INPUT => DEPT.VALUE:Outlets) OUTLET
    where rn = 1
),

Buyers_Final AS (
    SELECT
        DISTINCT
        _ID AS Buyer_id,
        CHAINID AS Chain_id, 
        NAME AS Buyer_name,
        TRY_PARSE_JSON(ADDRESS):City::STRING AS buyer_City,
        TRY_PARSE_JSON(ADDRESS):Country::STRING AS buyer_Country,
        TRY_PARSE_JSON(TIMEZONE):_id::STRING AS TIMEZONE,
        CREATEDATETIME AS time_created,  
        UPDATEDATETIME as time_updated,
        ALLOWEDAUTHROLESIDS,
        ISDISABLED
    FROM Buyers_TEMP
      where rn = 1
),

chain_names as (
    select 
    NAME as chain_name,
    ACCOUNT_ID as Chain_id
    from 
    reeco.analytics_prod.HOTEL_CHAINS
)

SELECT 
    B.Chain_id,
    B.Buyer_id,
    D.department_id,
    D.outlet_id,
    c.chain_name,
    B.Buyer_name,
    D.department_name,
    D.outlet_name,
    B.buyer_City,
    B.buyer_Country,
    B.TIMEZONE,
    B.time_created,
    B.time_updated,
    D.code_outlet,
    D.Description_outlet,
    D.Budget_Value_outlet,
    B.ALLOWEDAUTHROLESIDS,
    B.ISDISABLED

    
FROM Buyers_Final B
LEFT JOIN Departments_outlets D
    ON B.Buyer_id = D.Buyer_id

LEFT JOIN chain_names c
    ON B.Chain_id = c.Chain_id

