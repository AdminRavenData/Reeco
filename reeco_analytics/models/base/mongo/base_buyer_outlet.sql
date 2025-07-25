select 
CHAIN_ID,
BUYER_ID,
Buyer_created_at,
buyer_deleted_at,
DEPARTMENT_ID,
OUTLET_ID,
chain_name,
Buyer_name,
DEPARTMENT_NAME,
OUTLET_NAME,
CODE_OUTLET,
DESCRIPTION_OUTLET,
BUDGET_VALUE_OUTLET

from
{{ref("stg_buyer_department_outlet")}}

group by 
CHAIN_ID,
BUYER_ID,
Buyer_created_at,
buyer_deleted_at,
DEPARTMENT_ID,
OUTLET_ID,
chain_name,
Buyer_name,
DEPARTMENT_NAME,
OUTLET_NAME,
CODE_OUTLET,
DESCRIPTION_OUTLET,
BUDGET_VALUE_OUTLET