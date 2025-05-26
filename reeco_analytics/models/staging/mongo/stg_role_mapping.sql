select
* 
from
{{source('reeco_dwh', 'BUYER_ROLE_MAPPING')}} as role_mapping
