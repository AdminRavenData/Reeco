{{
  config(
    materialized= "view"
  )
}}

WITH recent_activity AS (
SELECT
buyer_id,
MAX(create_datetime) AS last_document_activity_date
FROM {{ref("marts_daily_buyer")}}
WHERE document_quantity > 0
GROUP BY buyer_id
),
recent_mdb AS (
SELECT buyer_id, SUM(document_quantity) AS total_doc_qty
FROM  {{ref("marts_daily_buyer")}}
WHERE create_datetime BETWEEN DATEADD(day, -7, CURRENT_DATE()) AND CURRENT_DATE()
GROUP BY buyer_id
)
SELECT
c.name AS chain_name,
b.buyer_name,
ra.last_document_activity_date AS last_invoice_update_date,
b.buyer_created_at
FROM reeco.analytics_prod.base_buyer AS b
LEFT JOIN recent_mdb AS r
ON b.buyer_id = r.buyer_id
LEFT JOIN reeco.mongo.buyerservice_chains AS c
ON b.chain_id = c._id
AND c.__deleted = FALSE
AND c.isdeleted = FALSE
LEFT JOIN recent_activity AS ra
ON b.buyer_id = ra.buyer_id
WHERE b.isdisabled = FALSE
AND b.accounts_payable = TRUE
AND b.buyer_created_at <= DATEADD(day, -7, CURRENT_DATE())
AND COALESCE(r.total_doc_qty, 0) = 0
ORDER BY
c.name,
b.buyer_created_at