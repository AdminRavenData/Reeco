select 
    DOCUMENT_ID,
    BUYER_ID,
    OUTLET_ID,
    reeco_Supplier_Id,
    Sku_document,
    item_name_document,
    Quantity_ordered_Item_document,
    Shipped_Quantity_item_document,
    Price_Per_unit_item_document,
    Total_Price_item_document,
    GlAccount,
    ExpenseName,
    CatalogItemId


from 

{{ref("base_document_unified")}}
