WITH Buyers_TEMP AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC) AS rn
    FROM 
        REECO.MONGO.BUYERSERVICE_BUYERS
),
Departments AS (
    SELECT
        _ID AS Buyer_id,
        DEPT.VALUE:_id::STRING AS department_id,
        DEPT.VALUE:Name::STRING AS department_name,
        OUTLET.value:_id::STRING AS outlet_id,
        OUTLET.value:Name::STRING AS outlet_name
    FROM Buyers_TEMP,
    LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(BUYERDEPARTMENTS)) DEPT,
    LATERAL FLATTEN(INPUT => DEPT.VALUE:Outlets) OUTLET
    WHERE ISDEMOACCOUNT = FALSE
      AND rn = 1
),
Buyers_Final AS (
    SELECT
        DISTINCT
        NAME AS Buyer_name,
        _ID AS Buyer_id,
        TRY_PARSE_JSON(ADDRESS):City::STRING AS buyer_City,
        TRY_PARSE_JSON(ADDRESS):Country::STRING AS buyer_Country,
        CHAINID AS Chainid, 
        CREATEDATETIME AS time_created,  
        UPDATEDATETIME as time_updated
    FROM Buyers_TEMP
    WHERE ISDEMOACCOUNT = FALSE
      AND rn = 1
)
SELECT 
    B.Buyer_name,
    D.department_name,
    D.outlet_name,
    B.Chainid,
    B.buyer_City,
    B.buyer_Country,
    B.time_created,
    B.time_updated,
    B.Buyer_id,
    D.department_id,
    D.outlet_id
    
FROM Buyers_Final B
LEFT JOIN Departments D
    ON B.Buyer_id = D.Buyer_id
