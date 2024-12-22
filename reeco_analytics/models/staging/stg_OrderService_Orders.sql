-- CREATE OR REPLACE VIEW stg_OrderService_Orders AS
with ordered_items_temp as(
    select *,
    ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC) AS rn
    from 
    REECO.MONGO.ORDERSERVICE_ORDERS

    ),

order_recieved_temp as(
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC) AS rn


FROM 
    REECO.MONGO.ORDERSERVICE_ORDERS,

),

ordered_items as ( 
SELECT 
    _id AS order_id,  
    flattened_items.VALUE:CatalogItem:Name::STRING AS item_name,
    flattened_items.VALUE:CatalogItem:Thumbnail:CatalogItemId::STRING AS item_id,
    flattened_items.VALUE:CatalogItem:Category:StringValue::STRING AS category,
    flattened_items.VALUE:CatalogItem:SubCategory:StringValue::STRING AS subcategory,
    flattened_items.VALUE:IsRemovedFromOrder::STRING AS is_removed_from_order,
    flattened_items.VALUE:IsReportedMissing::STRING AS is_reported_missing,
    flattened_items.VALUE:CatalogItem:CategoryGlAccount::STRING AS GLCode,
    flattened_items.VALUE:_id::STRING AS ORDER_CATALOG_ITEM_ID,
    flattened_items.VALUE:SupplierPlan:OriginalSku::STRING AS ORDER_CATALOG_ITEM_SKU,
    CAST(flattened_items.VALUE:PricePerPacking::STRING AS NUMERIC(38,2)) AS price_per_packing_ordered, 
    CAST(flattened_items.VALUE:Quantity::STRING AS NUMERIC(38,0)) AS packs_quantity_ordered,
    BUYERID,
    BUYERSUPPLIERINFO:SupplierId::STRING AS supplier_id,
    STATUS AS order_status,
    CREATEDATETIME AS order_created_date,
    UPDATEDATETIME AS order_updated_date,
    CASE 
        WHEN REGEXP_LIKE(flattened_items.VALUE:DeleteDateTime::STRING, '^[0-9]+$')
        THEN TO_TIMESTAMP_NTZ(CAST(flattened_items.VALUE:DeleteDateTime::STRING AS NUMBER) / 1000)
        ELSE NULL 
    END AS order_delete_date,
    ISDELETED AS ISDELETED,
    ORDERDOCUMENTS:"_0":"DocumentId"::STRING AS document_id, 
    price_per_packing_ordered * packs_quantity_ordered as item_price_ordered,
    ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC) AS rn

FROM 
    ordered_items_temp,
    LATERAL FLATTEN(INPUT => ORDERCATALOGITEMS) AS flattened_items
where  rn = 1
) ,

order_recieved as (
select
    _id AS order_id, 
    flattened_items.VALUE:ExpenseName::STRING AS item_name,
    flattened_items.VALUE:GLCode::STRING AS GLCode,
    flattened_items.VALUE:IsUrgent::STRING AS is_urgent,
    flattened_items.VALUE:OrderCatalogItemId::STRING AS ORDER_CATALOG_ITEM_ID,
    CASE 
        WHEN REGEXP_LIKE(RECEIVEDORDER:ReceivedDateTime::STRING, '^[0-9]+$')
        THEN TO_TIMESTAMP_NTZ(CAST(RECEIVEDORDER:ReceivedDateTime::STRING AS NUMBER) / 1000)
        ELSE NULL 
    END AS received_date,    
    CAST(flattened_items.VALUE:TotalPrice::STRING AS NUMERIC(38,2)) AS item_price_received,
    CAST(flattened_items.VALUE:Quantity::STRING AS NUMERIC(38,0)) AS item_quantity_received,
    ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC) AS rn


FROM 
    order_recieved_temp,
    LATERAL FLATTEN(INPUT => RECEIVEDORDER:CatalogItems) AS flattened_items

where  rn = 1
)


select
    COALESCE(ordered_items.order_id, order_recieved.order_id) as order_id,
    BUYERID,
    supplier_id,
    ordered_items.item_id as item_id,
    COALESCE(ordered_items.ORDER_CATALOG_ITEM_ID, order_recieved.ORDER_CATALOG_ITEM_ID) as ORDER_CATALOG_ITEM_ID,
    ORDER_CATALOG_ITEM_SKU,
    COALESCE(ordered_items.item_name, order_recieved.item_name) as item_name,
    category,
    subcategory,
    is_removed_from_order,      
    is_reported_missing,
    COALESCE(ordered_items.GLCode, order_recieved.GLCode) as GLCode,
    price_per_packing_ordered,
    packs_quantity_ordered,
    item_quantity_received,
    item_price_ordered,
    item_price_received,
    order_created_date,
    order_updated_date,
    order_delete_date,
    received_date,
    order_status,
    document_id


from ordered_items inner join order_recieved 
on
ordered_items.order_id = order_recieved.order_id
and
ordered_items.ORDER_CATALOG_ITEM_ID = order_recieved.ORDER_CATALOG_ITEM_ID

    
