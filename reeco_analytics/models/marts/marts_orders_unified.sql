with view_buyers as(
select 
BUYER_ID,
max(BUYER_NAME) BUYER_NAME,
max(CITY) as buyer_city,
max(COUNTRY) as buyer_country,
max(CHAINID) as CHAINID

from
{{ref("base_buyers")}}
group by 1

),

view_suppliers as(
select 
SUPPLIER_ID,
max(supplier_name) supplier_name,
max(SUPPLIER_CITY) SUPPLIER_CITY,
max(SUPPLIER_COUNTRY) SUPPLIER_COUNTRY 

from 
{{ref("stg_SupplierService_Suppliers")}}
group by 1
)

select 
base_orders.*,
view_buyers.* EXCLUDE(BUYER_ID),
view_suppliers.* EXCLUDE(SUPPLIER_ID)


from 
{{ref("base_orders")}}
left join view_buyers
on
base_orders.buyerid = view_buyers.BUYER_ID

left join 
view_suppliers
on
base_orders.SUPPLIER_ID = view_suppliers.SUPPLIER_ID

