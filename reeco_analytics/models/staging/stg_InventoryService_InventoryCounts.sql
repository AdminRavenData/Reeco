-- CREATE OR REPLACE VIEW stg_OrderService_Orders AS
with inventory_temp as(
    select *,
    ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC) AS rn
    from 
    REECO.MONGO.inventoryservice_inventorycounts

    )

select 
_id as inventory_id,

flattened_items.VALUE:InventoryCountCatalogItem:Name::STRING AS Name,
flattened_items.VALUE:InventoryCountCatalogItem:CatalogItemId::STRING AS Catalog_Item_Id,
flattened_items.VALUE:InventoryCountCatalogItem:Category::STRING AS Category,
flattened_items.VALUE:InventoryCountCatalogItem:SubCategory::STRING AS SubCategory,
COALESCE(flattened_items.VALUE:ItemCounts:_0:CountUnit::STRING,flattened_items.VALUE:ItemCounts:_1:CountUnit::STRING ,flattened_items.VALUE:ItemCounts:_2:CountUnit::STRING) AS CountUnit,
COALESCE(flattened_items.VALUE:ItemCounts:_0:CountValue::STRING,flattened_items.VALUE:ItemCounts:_1:CountValue::STRING ,flattened_items.VALUE:ItemCounts:_2:CountValue::STRING) AS CountValue,
COALESCE(flattened_items.VALUE:ItemCounts:_0:TotalValue::STRING,flattened_items.VALUE:ItemCounts:_1:TotalValue::STRING ,flattened_items.VALUE:ItemCounts:_2:TotalValue::STRING) AS TotalValue,
flattened_items.VALUE:AvgPrice::STRING AS AvgPrice,
CREATEDATETIME,
UPDATEDATETIME,
ISDELETED,
flattened_items.VALUE:InventoryCountCatalogItem:IsReportedMissing::STRING AS is_reported_missing,
STATUS,
BUYERNAME,
OUTLETNAME,
BUYERID,
OUTLETID,
flattened_items.VALUE:_id::STRING AS inventory_item_id



from inventory_temp,
    LATERAL FLATTEN(INPUT => INVENTORYCOUNTITEMS) AS flattened_items



where rn=1