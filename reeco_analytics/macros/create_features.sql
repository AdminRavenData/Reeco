{% macro create_features() %}
  {# Create or replace the features table BUYER_OUTLET_FEATURES_FINAL #}
  {% set sql %}
    CREATE OR REPLACE TABLE reeco.analytics_prod.BUYER_OUTLET_FEATURES_FINAL AS
    WITH 
      chain_flags AS (
        SELECT 
          CHAIN_ID,
          CASE WHEN COUNT(DISTINCT BUYER_ID) > 2 THEN 1 ELSE 0 END AS chain_buyer_flag
        FROM REECO.ANALYTICS_PROD.MARTS_DAILY_BUYER
        GROUP BY CHAIN_ID
      ),
      aggregated AS (
        SELECT
          CHAIN_ID,
          BUYER_NAME,
          COALESCE(OUTLET_NAME, 'NA') AS outlet_name,
          MIN(CREATE_DATETIME) AS first_create_date,
          COALESCE(AVG(MEDIAN_ORDER_INTERVAL_PER_ROW), 40) AS avg_median_order_interval,
          COALESCE(SUM(
            CASE 
              WHEN CREATE_DATETIME BETWEEN DATEADD(day, -30, CURRENT_DATE()) AND CURRENT_DATE()
              THEN ORDERED_TOTAL_PRICE ELSE 0 END
          ), 0) AS sum_ordered_total_price
        FROM REECO.ANALYTICS_PROD.MARTS_DAILY_BUYER
        WHERE CREATE_DATETIME BETWEEN DATEADD(month, -4, CURRENT_DATE()) AND CURRENT_DATE()
        GROUP BY CHAIN_ID, BUYER_NAME, COALESCE(OUTLET_NAME, 'NA')
      ),
      buyer_features AS (
        SELECT
          a.CHAIN_ID,
          a.BUYER_NAME,
          a.outlet_name,
          COALESCE(cf.chain_buyer_flag, 0) AS chain_buyer_flag,
          DATEDIFF('day', a.first_create_date, CURRENT_DATE()) AS days_since_first,
          a.avg_median_order_interval,
          a.sum_ordered_total_price
        FROM aggregated a
        LEFT JOIN chain_flags cf ON a.CHAIN_ID = cf.CHAIN_ID
      ),
      outlet_categories AS (
        SELECT
          COALESCE(OUTLET_NAME, 'NA') AS outlet_name,
          MAX(CASE WHEN OUTLET_CATEGORY = 'Operations & Maintenance' THEN 1 ELSE 0 END) AS flag_operations,
          MAX(CASE WHEN OUTLET_CATEGORY = 'Inventory & Procurement' THEN 1 ELSE 0 END) AS flag_inventory,
          MAX(CASE WHEN OUTLET_CATEGORY = 'Food & Beverage' THEN 1 ELSE 0 END) AS flag_food_beverage,
          MAX(CASE WHEN OUTLET_CATEGORY = 'Finance, Admin & Sales' THEN 1 ELSE 0 END) AS flag_finance,
          MAX(CASE WHEN OUTLET_CATEGORY = 'Banquets & Catering' THEN 1 ELSE 0 END) AS flag_banquets,
          MAX(CASE WHEN OUTLET_CATEGORY = 'Category6' THEN 1 ELSE 0 END) AS flag_category6,
          MAX(CASE WHEN OUTLET_CATEGORY = 'Category7' THEN 1 ELSE 0 END) AS flag_category7
        FROM REECO.DWH.HIGH_LEVEL_CATEGORIES_MAPPING
        GROUP BY COALESCE(OUTLET_NAME, 'NA')
      )
    SELECT 
      bf.CHAIN_ID,
      bf.BUYER_NAME,
      COALESCE(bf.outlet_name, 'NA') AS OUTLET_NAME,
      bf.chain_buyer_flag,
      bf.days_since_first,
      bf.avg_median_order_interval,
      bf.sum_ordered_total_price,
      COALESCE(oc.flag_operations, 0) AS flag_operations,
      COALESCE(oc.flag_inventory, 0) AS flag_inventory,
      COALESCE(oc.flag_food_beverage, 0) AS flag_food_beverage,
      COALESCE(oc.flag_finance, 0) AS flag_finance,
      COALESCE(oc.flag_banquets, 0) AS flag_banquets,
      COALESCE(oc.flag_category6, 0) AS flag_category6,
      COALESCE(oc.flag_category7, 0) AS flag_category7
    FROM buyer_features bf
    LEFT JOIN outlet_categories oc
      ON bf.outlet_name = oc.outlet_name
  {% endset %}
  {{ run_query(sql) }}
  {{ log("Features table created successfully.", info=True) }}
{% endmacro %}
