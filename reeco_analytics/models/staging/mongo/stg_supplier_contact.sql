with 
sup_contacts_temp AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY UPDATEDATETIME DESC, __ts_ms  DESC) AS rn
    FROM 
    {{ source('reeco_sql', 'CATALOGPROD_BUYERSUPPLIERCONTACTS') }}
    WHERE ISDELETED = FALSE
)

    select
        ID as user_id,
        BUYERSUPPLIERID as BUYER_SUPPLIER_ID,
        FIRSTNAME,
        LASTNAME,
        EMAILADDRESS as EMAIL,
        PHONENUMBER,
        AUTOSENDEMAIL,
        AUTOSENDSMS

    from
        sup_contacts_temp
    WHERE
        rn = 1

