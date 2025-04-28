WITH sup_buy_temp AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATEDATETIME DESC) AS rn
    FROM 
        REECO.SQL.CATALOGPROD_BUYERSUPPLIERS
    WHERE
        ISDELETED = False 
        And BUYERID not in (select demo_id from  {{ref("stg_demo_ids")}})
        And SUPPLIERID not in (select demo_id from  {{ref("stg_demo_ids")}})

)


SELECT
    ID AS BUYER_SUPPLIER_ID,
    BUYERID as BUYER_ID,
    SUPPLIERID as SUPPLIER_ID


    FROM sup_buy_temp

where rn = 1 
order by 1

