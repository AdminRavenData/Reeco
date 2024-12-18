with documents_view as(
select 
max(date(INVOICEDATE)) AS last_uploaded_invoice,
BUYERID

from 
 {{ref("stg_DocumentService_Documents")}}
where isdeleted = false and isdemo is null and totalproductsprice > 0
group by BUYERID
)

select b.*, d.last_uploaded_invoice 
from  {{ref("stg_BuyerService_Buyers")}} b
left join
documents_view d
on 
b.BUYER_ID = d.buyerid