select 
catalog.*,
gl_code.GLCODEID,
changes.*

from REECO.SQL.STG_CATALOGPROD_CATALOGITEMS catalog
left join 
REECO.SQL.STG_CATALOGPROD_BUYERCATALOGITEMGLCODES  gl_code
on
catalog.ITEM_ID = gl_code.catalogitemid
left join
REECO.SQL.STG_CHECKOUTSERVICE_CHECKOUTCATALOGITEMCHANGES changes
on 
catalog.ITEM_ID = changes.catalogitemid