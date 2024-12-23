with documents_view as (
    select 
        ORDERID,
        document_Sku,
        CatalogItemId,
        1 as is_document,
        INVOICENUMBER,
        INVOICEDATE as document_date,
        UPDATEDATETIME as document_updated_date,
        SUPPLIER_id,
        SUPPLIER_NAME,
        BUYER_ID,
        DOCUMENT_ID,
        document_item_name,
        document_Item_Quantity,
        document_Item_Price,
        TOTALPRODUCTSPRICE as order_price_document
    from 
        {{ref("stg_DocumentService_Documents")}} 
    -- group by ORDERID, document_Sku, CatalogItemId, is_document
),

order_flattened as (
    select 
        o.*, 
        1 as is_order,
        f.value as flattened_document_id,
    from 
         {{ref("stg_OrderService_Orders")}} o,
        lateral flatten(input => split(coalesce(o.document_ids_list, ''), ',')) f
    where f.value is not null or o.document_ids_list is null
),

merged_df as(
select 
    ORDER_ID,
    DOCUMENT_ID,
    coalesce(o.ITEM_ID, d.CatalogItemId) as ITEM_ID ,
    coalesce(o.ITEM_NAME, d.document_item_name) as ITEM_NAME ,
    coalesce(o.ORDER_CATALOG_ITEM_SKU, d.document_Sku) as sku ,
    CATEGORY,
    SUBCATEGORY,
    IS_REMOVED_FROM_ORDER,
    IS_REPORTED_MISSING,
    GLCODE,
    packs_quantity_ordered as ITEM_QUANTITY_ORDERED,
    ITEM_QUANTITY_RECEIVED,
    document_Item_Quantity as ITEM_QUANTITY_documen,
    ITEM_PRICE_ORDERED,
    ITEM_PRICE_RECEIVED,
    DOCUMENT_ITEM_PRICE as ITEM_PRICE_DOCUMENT,
    ORDER_CREATED_DATE,
    ORDER_UPDATED_DATE,
    ORDER_DELETE_DATE,
    document_date,
    document_updated_date,
    RECEIVED_DATE,
    ORDER_STATUS,
    ORDER_CATALOG_ITEM_ID,
    coalesce(o.BUYERID, d.BUYER_ID) as BUYER_ID,
    case when DEPARTMENT_ID = 'null' then NULL
    else DEPARTMENT_ID end as DEPARTMENT_ID,
    case when OUTLET_ID = 'null' then NULL
    else OUTLET_ID end as OUTLET_ID,
    coalesce(o.SUPPLIER_ID, d.SUPPLIER_id) as SUPPLIER_id,
    SUPPLIER_NAME,
    INVOICENUMBER,
    row_number() over (partition by o.order_id, o.item_id, o.ORDER_CATALOG_ITEM_SKU, DOCUMENT_ID,document_Sku,document_item_name, ITEM_PRICE_DOCUMENT  order by o.is_order, d.is_document) as rn
from 
    order_flattened o
full outer join documents_view d
    on o.flattened_document_id = d.DOCUMENT_ID
    and (o.item_id = d.CatalogItemId or  o.ORDER_CATALOG_ITEM_SKU = d.document_Sku)
)

-- keep only muched orders-documents and unmutched. this query drops duplicates needed to be created for the unification of documents with orders with more then 1 document
select * EXCLUDE(rn) from merged_df 
where rn = 1 
