with stg_documents_header as(
    select * 
    from
    {{ref("stg_document_header")}}
),

stg_documents_line as (
    select * 
    from
    {{ref("stg_document_line")}}
)

select 
h.*,
l.* exclude (DOCUMENT_ID,BUYER_ID,OUTLET_ID,reeco_Supplier_Id)
 from stg_documents_header h
left join
stg_documents_line l
on 
h.DOCUMENT_ID = l.DOCUMENT_ID