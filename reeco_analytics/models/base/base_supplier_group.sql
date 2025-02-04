select
    suppliers_group_id,
    SUPPLIERS_GROUP_NAME,
    count(distinct SUPPLIER_ID) as num_connected_suppliers
    
    from

    {{ref("base_supplier_unified")}}

    group by 1,2