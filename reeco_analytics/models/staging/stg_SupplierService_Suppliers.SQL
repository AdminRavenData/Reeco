SELECT
    _ID AS supplier_id,
    NAME AS supplier_name,
    CREATEDATETIME AS suppliers_joining_time,
    SUPPLIERGROUPID AS supplier_group,
    RATING AS rating,
    CATEGORIES AS categories_supplying,
    __DELETED AS is_deleted,
    ISDEMOACCOUNT AS is_demoaccount,
    
    -- Extract new variables from the ADDRESS JSONREECO
    ADDRESS:State AS supplier_state,
    ADDRESS:City AS supplier_city,
    ADDRESS:Country AS supplier_country

FROM 
    REECO.MONGO.SUPPLIERSERVICE_SUPPLIERS
