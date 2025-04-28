with supplier_buyer as(
    select * from {{ref("stg_supplier_buyer")}}
),

contact as(
    select * from {{ref("stg_supplier_contact")}} 
),

group_names as(
    select * from  {{ref("stg_supplier_group")}} 
)

select 
    sup.supplier_id,
    sup.OPCO,
    sup.suppliers_group_id,
    group_names.suppliers_group_name,
    supplier_buyer.BUYER_ID,
    contact.user_id,
    contact.FIRSTNAME,
    contact.LASTNAME,
    contact.EMAIL,
    contact.PHONENUMBER,
    contact.AUTOSENDEMAIL,
    contact.AUTOSENDSMS


from
{{ref("stg_supplier")}} sup

left join 
  supplier_buyer
on
sup.supplier_id = supplier_buyer.supplier_id

left join 
group_names
on 
sup.suppliers_group_id = group_names.suppliers_group_id

left join 
 contact
on
supplier_buyer.BUYER_SUPPLIER_ID = contact.BUYER_SUPPLIER_ID

