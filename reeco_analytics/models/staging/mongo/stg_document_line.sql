WITH document_temp AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC, __ts_ms  DESC) AS rn
    FROM REECO.MONGO.DOCUMENTSERVICE_DOCUMENTS
    WHERE ISDEMO = FALSE 
      AND ISDELETED = FALSE
    And BUYERID not in (select demo_id from  {{ref("stg_demo_ids")}})
)



select 
    _ID as DOCUMENT_ID,
    BUYERID as BUYER_ID,
    OUTLETID as OUTLET_ID,
    REECOSUPPLIER:SupplierId::STRING as reeco_Supplier_Id,
    flattened_items.VALUE:Sku:Value::STRING AS Sku_document,
    flattened_items.VALUE:Name:Value::STRING AS item_name_document,
    flattened_items.VALUE:OrderQuantity:Value::integer Quantity_ordered_Item_document,
    flattened_items.VALUE:ShippedQuantity:Value::integer Shipped_Quantity_item_document,
    flattened_items.VALUE:PricePerUnit:Value::integer Price_Per_unit_item_document,
    flattened_items.VALUE:TotalPrice:Value::integer Total_Price_item_document,
    flattened_items.VALUE:GlAccount::STRING GlAccount,
    flattened_items.VALUE:ExpenseName::STRING ExpenseName,
    flattened_items.VALUE:CatalogItemId::STRING AS CatalogItemId


from document_temp,
LATERAL FLATTEN(INPUT => LINEITEMS) AS flattened_items
    

where  rn = 1
-- And reeco_Supplier_Id not in (select demo_id from  {{ref("stg_demo_ids")}})
