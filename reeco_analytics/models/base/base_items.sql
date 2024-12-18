select 
catalog.*,
gl_code.GLCODEID,
changes.*
from {{ref("stg_CatalogProd_CatalogItems")}} catalog
left join 
 {{ref("stg_CatalogProd_BuyerCatalogItemGLCodes")}} gl_code
on
catalog.ITEM_ID = gl_code.catalogitemid
left join
 {{ref("stg_CheckoutService_CheckoutCatalogItemChanges")}}  changes
on 
catalog.ITEM_ID = changes.catalogitemid
