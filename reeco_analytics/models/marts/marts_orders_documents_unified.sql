with view_buyers as(
select 
BUYER_NAME, DEPARTMENT_NAME,OUTLET_NAME, Chain_id, BUYER_CITY, BUYER_COUNTRY,BUYER_ID, DEPARTMENT_ID,OUTLET_ID
from
{{ref("base_buyers")}}
group by BUYER_NAME, DEPARTMENT_NAME,OUTLET_NAME, Chain_id, BUYER_CITY, BUYER_COUNTRY,BUYER_ID, DEPARTMENT_ID,OUTLET_ID
),

view_buyers_not_outlet as(
select 
BUYER_NAME, DEPARTMENT_NAME, Chain_id, BUYER_CITY, BUYER_COUNTRY,BUYER_ID, DEPARTMENT_ID,
null as OUTLET_NAME
from
view_buyers
group by BUYER_NAME, DEPARTMENT_NAME, Chain_id, BUYER_CITY, BUYER_COUNTRY,BUYER_ID, DEPARTMENT_ID
),

view_buyers_not_outlet_not_dep as(
select 
BUYER_NAME, Chain_id, BUYER_CITY, BUYER_COUNTRY,BUYER_ID,
null as DEPARTMENT_NAME,
null as OUTLET_NAME
from
view_buyers
group by BUYER_NAME, Chain_id, BUYER_CITY, BUYER_COUNTRY,BUYER_ID
),

view_suppliers as(
select 
SUPPLIER_ID,
supplier_name,
SUPPLIER_CITY,
SUPPLIER_COUNTRY 

from 
{{ref("stg_SupplierService_Suppliers")}}
)

select 
base_orders_documents.* EXCLUDE(SUPPLIER_ID,SUPPLIER_NAME),
coalesce(view_buyers.BUYER_NAME, view_buyers_not_outlet.BUYER_NAME,view_buyers_not_outlet_not_dep.BUYER_NAME) as BUYER_NAME,
coalesce(view_buyers.DEPARTMENT_NAME, view_buyers_not_outlet.DEPARTMENT_NAME,view_buyers_not_outlet_not_dep.DEPARTMENT_NAME) as DEPARTMENT_NAME,
coalesce(view_buyers.OUTLET_NAME, view_buyers_not_outlet.OUTLET_NAME,view_buyers_not_outlet_not_dep.OUTLET_NAME) as OUTLET_NAME,
coalesce(view_buyers.BUYER_COUNTRY, view_buyers_not_outlet.BUYER_COUNTRY,view_buyers_not_outlet_not_dep.BUYER_COUNTRY) as BUYER_COUNTRY,
coalesce(view_buyers.BUYER_CITY, view_buyers_not_outlet.BUYER_CITY,view_buyers_not_outlet_not_dep.BUYER_CITY) as BUYER_CITY,
view_suppliers.* EXCLUDE(supplier_name),
coalesce( base_orders_documents.SUPPLIER_NAME, view_suppliers.supplier_name) as supplier_name,
coalesce(view_buyers.Chain_id, view_buyers_not_outlet.Chain_id,view_buyers_not_outlet_not_dep.Chain_id) as Chain_id


from 
{{ref("base_orders_documents")}}
left join view_buyers
on

(base_orders_documents.BUYER_ID = view_buyers.BUYER_ID
and base_orders_documents.DEPARTMENT_ID = view_buyers.DEPARTMENT_ID
and base_orders_documents.OUTLET_ID = view_buyers.OUTLET_ID 
and base_orders_documents.DEPARTMENT_ID is not null
and base_orders_documents.OUTLET_ID  is not null)

left join view_buyers_not_outlet on
(
base_orders_documents.BUYER_ID = view_buyers_not_outlet.BUYER_ID
and base_orders_documents.DEPARTMENT_ID = view_buyers_not_outlet.DEPARTMENT_ID
and base_orders_documents.DEPARTMENT_ID is not null
and base_orders_documents.OUTLET_ID  is null
)
left join view_buyers_not_outlet_not_dep on

(
base_orders_documents.BUYER_ID = view_buyers_not_outlet_not_dep.BUYER_ID
and base_orders_documents.DEPARTMENT_ID is null
and base_orders_documents.OUTLET_ID  is null
)

left join 
view_suppliers
on
(base_orders_documents.SUPPLIER_ID = view_suppliers.SUPPLIER_ID)
or
(base_orders_documents.SUPPLIER_NAME = view_suppliers.supplier_name)


