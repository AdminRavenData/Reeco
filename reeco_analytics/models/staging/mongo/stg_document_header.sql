WITH document_temp AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC ,__ts_ms  DESC) AS rn
    FROM 
        REECO.MONGO.DOCUMENTSERVICE_DOCUMENTS
    WHERE ISDEMO = FALSE 
      AND ISDELETED = FALSE
      And BUYERID not in (select demo_id from  {{ref("stg_demo_ids")}})

),

stg_buyer_timezone as(
    select 
        BUYER_ID,
        timezone 
    from
        {{ref("stg_buyer_timezone")}} 
)



select 
    _ID as DOCUMENT_ID,
    BUYERID as BUYER_ID,
    OUTLETID as OUTLET_ID,
    REECOSUPPLIER:SupplierId::STRING as reeco_Supplier_Id,
    JSON_EXTRACT_PATH_TEXT(TOTALPRODUCTSPRICE, 'Value') AS TOTAL_PRODUCT_PRICE,
    JSON_EXTRACT_PATH_TEXT(TOTALAMOUNT, 'Value') AS TOTAL_AMOUNT,
    JSON_EXTRACT_PATH_TEXT(TOTALDISCOUNT, 'Value') AS TOTAL_DISCOUNT_PRICE,
    JSON_EXTRACT_PATH_TEXT(TOTALTAX, 'Value') AS TOTAL_TAX,
    JSON_EXTRACT_PATH_TEXT(TOTALSHIPPING, 'Value') AS TOTAL_SHIPPING,
    CONVERT_TIMEZONE('UTC', tm.timezone,
        CASE 
        WHEN REGEXP_LIKE(JSON_EXTRACT_PATH_TEXT(INVOICEDATE, 'Value')::STRING, '^[0-9]+$')
        THEN TO_TIMESTAMP_NTZ(CAST(JSON_EXTRACT_PATH_TEXT(INVOICEDATE, 'Value')::STRING AS NUMBER) / 1000)
        ELSE NULL 
    END) AS INVOICE_DATE,
    CONVERT_TIMEZONE('UTC', tm.timezone, CREATEDATETIME) AS CREATE_DATETIME,
    CONVERT_TIMEZONE('UTC', tm.timezone,
    CASE 
        WHEN REGEXP_LIKE(JSON_EXTRACT_PATH_TEXT(DUEDATE, 'Value'), '^[0-9]+$')
        THEN TO_TIMESTAMP_NTZ(CAST(JSON_EXTRACT_PATH_TEXT(DUEDATE, 'Value') AS NUMBER) / 1000)
        ELSE NULL 
    END) AS DUEDATE,    
    STATUS,
    ORDERID as ORDERID,
    SOURCE,
    ISEXPORTED,
    CONVERT_TIMEZONE('UTC', tm.timezone,
    CASE 
        WHEN REGEXP_LIKE(PARSE_JSON(EXPORTS): _0: ExportDateTime::STRING, '^[0-9]+$')
        THEN TO_TIMESTAMP_NTZ(CAST(PARSE_JSON(EXPORTS): _0: ExportDateTime::STRING AS NUMBER) / 1000)
        ELSE NULL 
    END) AS export_date
    
from document_temp

left join 
stg_buyer_timezone  tm
on
document_temp.BUYERID = tm.BUYER_ID

where  rn = 1
-- for updating dates to be in the buyers timezone
