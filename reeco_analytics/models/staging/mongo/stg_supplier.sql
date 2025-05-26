with sup_temp AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC, __ts_ms  DESC) AS rn
    FROM 
    {{source('reeco_mongo', 'SUPPLIERSERVICE_SUPPLIERGROUPS')}}
    WHERE ISDEMOACCOUNT = FALSE and ISDELETED = FALSE
    and _ID not in (select demo_id from  {{ref("stg_demo_ids")}})
)

select
    _ID as supplier_id,
    SUPPLIERGROUPID suppliers_group_id,
    OPCO

from
sup_temp
where rn = 1
group by 1,2,3
