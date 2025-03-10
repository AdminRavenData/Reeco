{% macro create_kmeans_procedure() %}
  {# Create or replace the Python stored procedure for k-means clustering #}
  {% set sql %}
    CREATE OR REPLACE PROCEDURE reeco.ANALYTICS_PROD.MY_KMEANS_CLUSTERING()
      RETURNS STRING
      LANGUAGE PYTHON
      RUNTIME_VERSION = '3.8'
      PACKAGES = ('snowflake-snowpark-python','scikit-learn','pandas','numpy')
      HANDLER='MY_KMEANS_CLUSTERING'
    AS
    $$
def MY_KMEANS_CLUSTERING(session):
    import pandas as pd
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    # Read data from the features table
    df_snow = session.table("BUYER_OUTLET_FEATURES_FINAL").select(
        "CHAIN_ID",
        "BUYER_NAME",
        "OUTLET_NAME",
        "CHAIN_BUYER_FLAG",
        "DAYS_SINCE_FIRST",
        "AVG_MEDIAN_ORDER_INTERVAL",
        "SUM_ORDERED_TOTAL_PRICE",
        "FLAG_OPERATIONS",
        "FLAG_INVENTORY",
        "FLAG_FOOD_BEVERAGE",
        "FLAG_FINANCE",
        "FLAG_BANQUETS",
        "FLAG_CATEGORY6",
        "FLAG_CATEGORY7"
    )

    pdf = df_snow.to_pandas()

    # Create a unique identifier for each buyer-outlet combination
    pdf['buyer_outlet'] = pdf['BUYER_NAME'] + '-' + pdf['OUTLET_NAME']

    # Select numeric features for clustering
    features = pdf[[
        'CHAIN_BUYER_FLAG', 
        'DAYS_SINCE_FIRST', 
        'AVG_MEDIAN_ORDER_INTERVAL', 
        'SUM_ORDERED_TOTAL_PRICE',
        'FLAG_OPERATIONS', 
        'FLAG_INVENTORY', 
        'FLAG_FOOD_BEVERAGE', 
        'FLAG_FINANCE', 
        'FLAG_BANQUETS', 
        'FLAG_CATEGORY6', 
        'FLAG_CATEGORY7'
    ]]

    # Standardize features
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)

    # Perform K-Means clustering
    n_clusters = 7
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    pdf['cluster'] = kmeans.fit_predict(features_scaled)

    # Log cluster information
    print("Cluster Centers (standardized):", kmeans.cluster_centers_)
    print("Cluster counts:", pdf['cluster'].value_counts().to_dict())

    # Save results with original features + cluster
    result_df = session.create_dataframe(pdf)
    result_df.write.mode("overwrite").save_as_table("REECO.ANALYTICS_PROD.BUYER_OUTLET_CLUSTERED_FINAL")

    return "KMeans clustering completed. Results stored in table REECO.ANALYTICS_PROD.BUYER_OUTLET_CLUSTERED_FINAL."
    $$
  {% endset %}

  -- Debugging: Print the SQL statement
  {{ log("Executing the following SQL:", info=True) }}
  {{ log(sql, info=True) }}

  -- Execute the SQL statement
  {{ run_query(sql) }}

  {{ log("KMeans procedure created successfully.", info=True) }}
{% endmacro %}
