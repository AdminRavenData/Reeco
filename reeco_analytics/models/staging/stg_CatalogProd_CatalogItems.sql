select 
ID as item_id,
NAME as name,
DATEADD(MILLISECOND, CREATEDATETIME, '1970-01-01 00:00:00'::TIMESTAMP) AS created_datetime,
DATEADD(MILLISECOND, UPDATEDATETIME, '1970-01-01 00:00:00'::TIMESTAMP) AS updated_datetime,
DATEADD(MILLISECOND, DELETEDATETIME, '1970-01-01 00:00:00'::TIMESTAMP) AS deleted_datetime,
ISDELETED as is_deleted,
CATEGORY as category,
SUBCATEGORY as subcategory,
ISORGANIC as is_organic,
BRAND as brand,
ISVEGETARIAN as is_vegetarian,
ISKOSHER as is_kosher,
STORAGEINSTRUCTIONS as storage_instructions

from reeco.SQL.CATALOGPROD_CATALOGITEMS
