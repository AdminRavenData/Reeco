select 
CHAIN_ID,
CHAIN_NAME,
IS_DISABLED_CHAIN,
IS_DEMO_CHAIN,
count(distinct BUYER_ID) as count_distinct_buyers
from
{{ref("stg_buyer_department_outlet")}}

group by 
CHAIN_ID,
CHAIN_NAME,
IS_DISABLED_CHAIN,
IS_DEMO_CHAIN
