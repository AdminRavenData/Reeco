with documents_view as(
select 
max(date(INVOICEDATE)) AS last_uploaded_invoice,
BUYER_ID

from 
 {{ref("stg_DocumentService_Documents")}}
where isdeleted = false and isdemo is null 
group by BUYER_ID
)

select b.*, d.last_uploaded_invoice 
from  {{ref("stg_buyer_department_outlet")}} b
left join
documents_view d
on 
b.BUYER_ID = d.BUYER_ID