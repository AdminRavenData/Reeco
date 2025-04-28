select  
    BUYER_ID, 
    max(timezone) as timezone 
from 
        {{ref("stg_buyer_department_outlet")}} 
group by 1