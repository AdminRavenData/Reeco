select
    SUPPLIERS_GROUP_ID,
    SUPPLIER_ID,
    BUYER_ID,
    USER_ID,
    FIRSTNAME,
    LASTNAME,
    EMAIL,
    PHONENUMBER,
    AUTOSENDEMAIL,
    AUTOSENDSMS

    from

    {{ref("base_supplier_unified")}}
where 
USER_ID is not null