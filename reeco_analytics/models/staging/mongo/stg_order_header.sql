WITH orders_temp AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY _id ORDER BY UPDATEDATETIME DESC ,__ts_ms  DESC) AS rn
    FROM {{ source('reeco_mongo', 'ORDERSERVICE_ORDERS') }}
    WHERE ISDEMO = FALSE 
      AND ISDELETED = FALSE
      And BUYERID not in (select demo_id from  {{ref("stg_demo_ids")}})
),

issues_counter AS (
    -- Flatten CatalogItems to check if any IssueType exists
    SELECT 
        _id AS order_id,
        COALESCE(MAX(CASE 
            WHEN flattened_items.VALUE:IssueType IS NOT NULL AND flattened_items.VALUE:IssueType != '' THEN TRUE 
            ELSE FALSE 
        END), FALSE) AS has_issues
    FROM orders_temp,
    LATERAL FLATTEN(INPUT => RECEIVEDORDER:CatalogItems) AS flattened_items 
    WHERE rn = 1
    GROUP BY _id
),

stg_buyer_timezone as(
    select 
        BUYER_ID,
        timezone 
    from
        {{ref("stg_buyer_timezone")}} 
),

checkout_ordes_map as(
    select
        _ID AS order_id,
        JSON_EXTRACT_PATH_TEXT(CHECKOUTDATA, 'CheckoutDisplayId') AS Checkout_ID
FROM 
    orders_temp

WHERE rn = 1
),

orders AS (
    SELECT 
        o._id AS order_id,  
        o.BUYERID AS BUYER_ID,
        JSON_EXTRACT_PATH_TEXT(o.BUYERINFO, 'SenderOutletId') AS outlet_id,
        JSON_EXTRACT_PATH_TEXT(o.SUPPLIERINFO, 'SupplierId') AS Supplier_Id,
        JSON_EXTRACT_PATH_TEXT(o.BUYERSUPPLIERINFO, '_id') AS Buyer_supplier_Id,
        CONVERT_TIMEZONE('UTC', tm.timezone, o.CREATEDATETIME) AS order_created_date,
        CONVERT_TIMEZONE('UTC', tm.timezone, o.DELIVERYDATETIME) AS order_delivery_datetime,
            CONVERT_TIMEZONE('UTC', tm.timezone, o.CUTOFFDATETIME) AS cut_off_datetime,
        CONVERT_TIMEZONE('UTC', tm.timezone,
        CASE 
            WHEN REGEXP_LIKE(o.ordersenttosupplierdatetime::STRING, '^[0-9]+$')
            THEN TO_TIMESTAMP_NTZ(CAST(o.ordersenttosupplierdatetime AS NUMBER) / 1000)
            ELSE NULL 
        END) AS order_sent_to_supplier_datetime,
            CONVERT_TIMEZONE('UTC', tm.timezone, o.closedorderdatetime) AS order_closed_datetime,
        o.status AS status,
        o.isautoclosed AS is_auto_closed,
        JSON_EXTRACT_PATH_TEXT(o.BUYERINFO, 'SenderUserId') AS user_Id,
        JSON_EXTRACT_PATH_TEXT(o.backofficeorderassignee, 'UserId') AS backoffice_assignee_user_Id,
        COALESCE(ARRAY_SIZE(OBJECT_KEYS(o.APPROVERLISTS)),0) AS approvers_count,
        Checkout_ID,
        ORDERDOCUMENTS:"_0"."DocumentId" as Document_Id
        
    FROM orders_temp o

    left join 
        stg_buyer_timezone  tm
    on
        o.BUYERID = tm.BUYER_ID

    left join 
        checkout_ordes_map  ck
    on
        o._id = ck.order_id


    WHERE rn = 1
    And Supplier_Id not in (select demo_id from  {{ref("stg_demo_ids")}})

)

SELECT o.*, COALESCE(i.has_issues, FALSE) AS closed_with_issues
FROM orders o
LEFT JOIN issues_counter i ON o.order_id = i.order_id
