WITH flattened_roles AS ( 
    -- Handle rows where ALLOWEDAUTHROLESIDS is NOT NULL
    SELECT 
        b.CHAIN_ID,
        b.BUYER_ID,
        b.BUYER_NAME,
        b.Buyer_created_at,
        b.BUYER_CITY,
        b.BUYER_COUNTRY,
        b.TIMEZONE,
        b.ISDISABLED,
        ids.VALUE::STRING AS role_id
    FROM 
        {{ref("stg_buyer_department_outlet")}} b,
        LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(b.ALLOWEDAUTHROLESIDS)) ids

    UNION ALL

    -- Handle rows where ALLOWEDAUTHROLESIDS IS NULL (to prevent dropping)
    SELECT 
        b.CHAIN_ID,
        b.BUYER_ID,
        b.BUYER_NAME,
        b.Buyer_created_at,
        b.BUYER_CITY,
        b.BUYER_COUNTRY,
        b.TIMEZONE,
        b.ISDISABLED,
        NULL AS role_id
    FROM 
        {{ref("stg_buyer_department_outlet")}} b
    WHERE 
        b.ALLOWEDAUTHROLESIDS  = {}
)

SELECT 
    fr.CHAIN_ID,
    fr.BUYER_ID,
    fr.BUYER_NAME,
    min(Buyer_created_at) AS Buyer_created_at,
    MAX(fr.BUYER_CITY) AS BUYER_CITY,
    MAX(fr.BUYER_COUNTRY) AS BUYER_COUNTRY,
    MAX(fr.TIMEZONE) AS TIMEZONE,
    MAX(fr.ISDISABLED) AS ISDISABLED,
    -- Boolean flags for modules, ensuring NULL values return FALSE
    COALESCE(MAX(IFF(s.Module = 'Purchasing', TRUE, FALSE)), FALSE) AS Purchasing,
    COALESCE(MAX(IFF(s.Module = 'Accounts Payable', TRUE, FALSE)), FALSE) AS Accounts_Payable,
    COALESCE(MAX(IFF(s.Module = 'Inventory', TRUE, FALSE)), FALSE) AS Inventory,
    COALESCE(MAX(IFF(s.Module = 'Recipes', TRUE, FALSE)), FALSE) AS Recipes

FROM 
    flattened_roles fr
LEFT JOIN 
    {{ref("stg_role_mapping")}} s 
    ON fr.role_id = s.Id

GROUP BY 
    fr.BUYER_ID,
    fr.CHAIN_ID,
    fr.BUYER_NAME