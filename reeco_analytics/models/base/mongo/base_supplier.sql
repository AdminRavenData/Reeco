select
    SUPPLIER_ID,
    SUPPLIERS_GROUP_ID,
    max(OPCO) OPCO,
    count(distinct BUYER_ID) as num_buyers_connected,
    count(distinct USER_ID) as num_users_connected,



    from

    {{ref("base_supplier_unified")}}

    group by 1,2