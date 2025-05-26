-- CREATE OR REPLACE VIEW stg_OrderService_Orders AS
with ordered_items_temp as(
    select *,
    ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC, __ts_ms  DESC) AS rn 
    FROM {{ source('reeco_mongo', 'ORDERSERVICE_ORDERS') }}
    WHERE ISDEMO = FALSE and ISDELETED = FALSE
      And BUYERID not in (select demo_id from  {{ref("stg_demo_ids")}})

    ),
    

ordered_items as ( 
SELECT 
    _id AS order_id,  
    CREATEDATETIME AS order_created_date,
    DELIVERYDATETIME as order_DELIVERY_DATETIME,
    flattened_items.VALUE:CatalogItem:Name::STRING AS item_name,
    flattened_items.VALUE:CatalogItem:_id::STRING AS Catalog_item_id,
    CASE 
        WHEN REGEXP_LIKE(flattened_items.VALUE:CreateDateTime::STRING, '^[0-9]+$')
        THEN TO_TIMESTAMP_NTZ(CAST(flattened_items.VALUE:CreateDateTime::STRING AS NUMBER) / 1000)
        ELSE NULL 
    END AS created_date_item,

    flattened_items.VALUE:GLCode::STRING AS GLCode_item,
    flattened_items.VALUE:ExpenseName::STRING AS Expense_Name_item,
    flattened_items.VALUE:Plan:Brand::STRING AS brand_item, 
    flattened_items.VALUE:Plan:Sku::STRING AS sku_item,
    CAST(flattened_items.VALUE:Quantity::STRING AS NUMERIC(38,0)) AS quantity_packing_ordered_item,
    CAST(flattened_items.VALUE:PricePerPacking::STRING AS NUMERIC(38,2)) AS price_per_packing_ordered_item, 
    price_per_packing_ordered_item * quantity_packing_ordered_item as total_price_ordered_item,
    flattened_items.VALUE:_id::STRING AS Catalog_ordered_item_id



FROM 
    ordered_items_temp,
    LATERAL FLATTEN(INPUT => ORDERCATALOGITEMS) AS flattened_items
where  rn = 1
) ,

recieved_items as (
select
    _id AS order_id,  
    CREATEDATETIME AS order_created_date,
    DELIVERYDATETIME as order_DELIVERY_DATETIME,
    flattened_items.VALUE:ExpenseName::STRING AS item_name,
    flattened_items.VALUE:OrderCatalogItemId::STRING AS Catalog_item_id,
    flattened_items.VALUE:CreateDateTime::STRING AS created_date_item,
    flattened_items.VALUE:GLCode::STRING AS GLCode_item,
    flattened_items.VALUE:ExpenseName::STRING AS Expense_Name_item,
    CAST(flattened_items.VALUE:Quantity::STRING AS NUMERIC(38,2)) AS quantity_packing_recieved_item,
    COALESCE(CAST(flattened_items.VALUE:TotalPrice::STRING AS NUMERIC(38,2)) / NULLIF(CAST(flattened_items.VALUE:Quantity::STRING AS NUMERIC(38,2)), 0), 0) AS price_per_packing_recieved_item,
    CAST(flattened_items.VALUE:TotalPrice::STRING AS NUMERIC(38,2)) as total_price_recieved_item,
    CASE 
        WHEN REGEXP_LIKE(RECEIVEDORDER:ReceivedDateTime::STRING, '^[0-9]+$')
        THEN TO_TIMESTAMP_NTZ(CAST(RECEIVEDORDER:ReceivedDateTime::STRING AS NUMBER) / 1000)
        ELSE NULL 
    END AS recieved_date_item


FROM 
    ordered_items_temp,
    LATERAL FLATTEN(INPUT => RECEIVEDORDER:CatalogItems) AS flattened_items

where  rn = 1
)



select
    COALESCE(ordered_items.order_id, recieved_items.order_id) as order_id,
    COALESCE(ordered_items.order_created_date, recieved_items.order_created_date) as order_created_date,
    -- COALESCE(ordered_items.order_DELIVERY_DATETIME, recieved_items.order_DELIVERY_DATETIME) as order_DELIVERY_DATETIME,
    COALESCE(ordered_items.item_name, recieved_items.item_name) as item_name,
	COALESCE(ordered_items.Catalog_item_id, recieved_items.Catalog_item_id) as Catalog_item_id,
    COALESCE(ordered_items.created_date_item, recieved_items.created_date_item) as created_date_item,
    recieved_date_item,
    COALESCE(ordered_items.GLCode_item, recieved_items.GLCode_item) as GLCode_item,
    COALESCE(ordered_items.Expense_Name_item, recieved_items.Expense_Name_item) as Expense_Name_item,
	ordered_items.brand_item as brand_item,
	ordered_items.sku_item as sku_item,

    quantity_packing_ordered_item,
    quantity_packing_recieved_item,
    price_per_packing_ordered_item ,
    round(price_per_packing_recieved_item, 2) as price_per_packing_recieved_item,
	total_price_ordered_item,
	total_price_recieved_item
	
	
	

from ordered_items full outer join recieved_items 
on
ordered_items.order_id = recieved_items.order_id
and
ordered_items.Catalog_ordered_item_id = recieved_items.Catalog_item_id

