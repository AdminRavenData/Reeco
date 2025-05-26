with inventory_temp as(
    select *,
    ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC, __ts_ms  DESC) AS rn
    from 
        {{source('reeco_mongo', 'inventoryservice_inventorycounts')}}

    WHERE ISDELETED = FALSE 
    And BUYERID not in (select demo_id from  {{ref("stg_demo_ids")}})
    )

select 
    _id as INVENTORY_count_id,
    flattened_items.VALUE:_id::STRING as INVENTORY_item_id,
    flattened_items.VALUE:InventoryCountCatalogItem:CatalogItemId::STRING AS Catalog_Item_Id,
    COALESCE(flattened_items.VALUE:ItemCounts:_0:TotalValue::integer,0) as CASE_TOTAL_VALUE,
    COALESCE(flattened_items.VALUE:ItemCounts:_0:CountValue::integer,0) as CASE_Count_Value,
    COALESCE(flattened_items.VALUE:ItemCounts:_1:TotalValue::integer,0)  AS UNIT_TOTAL_VALUE,
    COALESCE(flattened_items.VALUE:ItemCounts:_1:CountValue::integer,0)  AS UNIT_Count_Value,
    COALESCE(flattened_items.VALUE:ItemCounts:_2:TotalValue::integer,0) AS EACH_TOTAL_VALUE,
    COALESCE(flattened_items.VALUE:ItemCounts:_2:CountValue::integer,0) AS EACH_Count_Value,
        COALESCE(flattened_items.VALUE:ItemCounts:_0:TotalValue::integer,0) +
        COALESCE(flattened_items.VALUE:ItemCounts:_1:TotalValue::integer,0) +
        COALESCE(flattened_items.VALUE:ItemCounts:_2:TotalValue::integer,0) 
    AS inventory_item_total_value,
    flattened_items.VALUE:AvgPrice::STRING AS Avg_Price



from inventory_temp,
    LATERAL FLATTEN(INPUT => INVENTORYCOUNTITEMS) AS flattened_items

where rn=1