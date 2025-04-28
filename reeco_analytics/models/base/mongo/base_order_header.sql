select
    ORDER_ID,
    BUYER_ID,
    outlet_id,
    SUPPLIER_ID,
    Buyer_supplier_Id,
    ORDER_CREATED_DATE,
    ORDER_DELIVERY_DATETIME,
    CUT_OFF_DATETIME,
    ORDER_SENT_TO_SUPPLIER_DATETIME,
    ORDER_CLOSED_DATETIME,
    STATUS,
    IS_AUTO_CLOSED,
    USER_ID,
    BACKOFFICE_ASSIGNEE_USER_ID,
    APPROVERS_COUNT,
    CLOSED_WITH_ISSUES,
    Document_Id,

    sum(TOTAL_PRICE_ORDERED_ITEM) as order_total_value,
    sum(TOTAL_PRICE_RECIEVED_ITEM) as recieved_total_value,
    COUNT(DISTINCT CASE WHEN QUANTITY_PACKING_ORDERED_ITEM > 0 THEN CATALOG_ITEM_ID END) AS count_items_ordered,
    COUNT(DISTINCT CASE WHEN QUANTITY_PACKING_RECIEVED_ITEM > 0 THEN CATALOG_ITEM_ID END) AS count_items_recieved

from
    {{ref("base_order_unified")}}

    group BY
        ORDER_ID,
        BUYER_ID,
        outlet_id,
        SUPPLIER_ID,
        Buyer_supplier_Id,
        ORDER_CREATED_DATE,
        ORDER_DELIVERY_DATETIME,
        CUT_OFF_DATETIME,
        ORDER_SENT_TO_SUPPLIER_DATETIME,
        ORDER_CLOSED_DATETIME,
        STATUS,
        IS_AUTO_CLOSED,
        USER_ID,
        BACKOFFICE_ASSIGNEE_USER_ID,
        APPROVERS_COUNT,
        CLOSED_WITH_ISSUES,
        Document_Id