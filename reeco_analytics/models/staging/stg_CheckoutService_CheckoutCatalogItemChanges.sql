
WITH normalized_data AS (
    SELECT 
        CATALOGITEMID,
        value:"OfferId"::STRING AS OfferId,
        value:"SupplierId"::STRING AS SupplierId
    FROM reeco.mongo.CHECKOUTSERVICE_CHECKOUTCATALOGITEMCHANGES,
         LATERAL FLATTEN(input => OFFERCHANGES)
),
offer_counts AS (
    SELECT 
        CATALOGITEMID,
        OfferId,
        SupplierId,
        COUNT(*) AS offer_count
    FROM normalized_data
    GROUP BY CATALOGITEMID, OfferId, SupplierId
),
ranked_offers AS (
    SELECT 
        CATALOGITEMID,
        OfferId,
        SupplierId,
        offer_count,
        ROW_NUMBER() OVER (PARTITION BY CATALOGITEMID ORDER BY offer_count DESC, OfferId ASC) AS rank
    FROM offer_counts
)
SELECT 
    CATALOGITEMID,
    -- OfferId and SupplierId for Top 1, 2, 3
    MAX(CASE WHEN rank = 1 THEN OfferId END) AS offer_change_1,
    MAX(CASE WHEN rank = 1 THEN SupplierId END) AS offer_change_1_supplier,
    MAX(CASE WHEN rank = 2 THEN OfferId END) AS offer_change_2,
    MAX(CASE WHEN rank = 2 THEN SupplierId END) AS offer_change_2_supplier,
    MAX(CASE WHEN rank = 3 THEN OfferId END) AS offer_change_3,
    MAX(CASE WHEN rank = 3 THEN SupplierId END) AS offer_change_3_supplier,

    -- Count for Top 1, 2, 3
    MAX(CASE WHEN rank = 1 THEN offer_count END) AS offer_change_1_count,
    MAX(CASE WHEN rank = 2 THEN offer_count END) AS offer_change_2_count,
    MAX(CASE WHEN rank = 3 THEN offer_count END) AS offer_change_3_count
FROM ranked_offers
GROUP BY CATALOGITEMID
