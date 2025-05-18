with suppliers_groups_temp AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC, __ts_ms  DESC) AS rn
    FROM 
        REECO.MONGO.SUPPLIERSERVICE_SUPPLIERGROUPS
    WHERE ISDEMOACCOUNT = FALSE and ISDELETED = FALSE
)
    select 
        _ID as suppliers_group_id,   
        NAME as suppliers_group_name,

    from
        suppliers_groups_temp
    where rn = 1
    group by 1,2