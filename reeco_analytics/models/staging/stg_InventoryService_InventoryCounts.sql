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
flattened_items.VALUE:ItemCounts:_0:TotalValue::STRING as CASE_TOTAL_VALUE,
flattened_items.VALUE:ItemCounts:_0:CountValue::STRING as CASE_Count_Value,
flattened_items.VALUE:ItemCounts:_1:TotalValue::STRING  AS UNIT_TOTAL_VALUE,
flattened_items.VALUE:ItemCounts:_1:CountValue::STRING  AS UNIT_Count_Value,
flattened_items.VALUE:ItemCounts:_2:TotalValue::STRING AS EACH_TOTAL_VALUE,
flattened_items.VALUE:ItemCounts:_2:CountValue::STRING AS EACH_Count_Value,
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