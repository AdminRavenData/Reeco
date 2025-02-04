WITH sup_TEMP AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC) AS rn
    FROM 
        REECO.MONGO.SUPPLIERSERVICE_SUPPLIERS
)


SELECT
    NAME AS supplier_name,
    CREATEDATETIME AS suppliers_joining_time,
    RATING AS rating,


    -- Extract new variables from the ADDRESS JSONREECO
    ADDRESS:City AS supplier_city,
    ADDRESS:Country AS supplier_country,
    _ID AS supplier_id,
    SUPPLIERGROUPID AS supplier_group_id,

FROM sup_TEMP

where rn = 1 and ISDEMOACCOUNT = False and __DELETED = False
    
