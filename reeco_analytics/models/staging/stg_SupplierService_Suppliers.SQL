WITH sup_TEMP AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC) AS rn
    FROM 
        REECO.MONGO.SUPPLIERSERVICE_SUPPLIERS
)


SELECT
    _ID AS supplier_id,
    NAME AS supplier_name,
    CREATEDATETIME AS suppliers_joining_time,
    SUPPLIERGROUPID AS supplier_group,
    RATING AS rating,
    CATEGORIES AS categories_supplying,
    __DELETED AS is_deleted,
    ISDEMOACCOUNT AS is_demoaccount,
    SUPPLIERGROUPID,
    
    -- Extract new variables from the ADDRESS JSONREECO
    ADDRESS:City AS supplier_city,
    ADDRESS:Country AS supplier_country

FROM sup_TEMP

where rn = 1
    
