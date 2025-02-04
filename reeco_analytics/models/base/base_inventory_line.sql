select

    INVENTORY_count_id,
    INVENTORY_item_id,
    Catalog_Item_Id,
    CASE_TOTAL_VALUE,
    CASE_Count_Value,
    UNIT_TOTAL_VALUE,
    UNIT_Count_Value,
    EACH_TOTAL_VALUE,
    EACH_Count_Value,
    inventory_item_total_value,
    Avg_Price,
    CREATE_DATETIME



from     {{ref("base_inventory_unified")}}

