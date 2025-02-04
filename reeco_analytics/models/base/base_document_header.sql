select 
  
    DOCUMENT_ID,
    BUYER_ID,
    OUTLET_ID,
    reeco_Supplier_Id,


    max(TOTAL_PRODUCT_PRICE) TOTAL_PRODUCT_PRICE,
    max(TOTAL_AMOUNT) TOTAL_AMOUNT,
    max(TOTAL_DISCOUNT_PRICE) TOTAL_DISCOUNT_PRICE,
    max(TOTAL_TAX) TOTAL_TAX,
    max(TOTAL_SHIPPING) TOTAL_SHIPPING,
    min(INVOICE_DATE) INVOICE_DATE,
    min(CREATE_DATETIME) CREATE_DATETIME,
    max(DUEDATE) DUEDATE,    
    max(STATUS) STATUS,
    max(ORDERID) ORDERID,
    max(SOURCE) SOURCE,
    max(ISEXPORTED) ISEXPORTED,
    max(export_date) export_date
    
from 

{{ref("base_document_unified")}}

group By 

    DOCUMENT_ID,
    BUYER_ID,
    OUTLET_ID,
    reeco_Supplier_Id