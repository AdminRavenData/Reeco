{% macro call_kmeans_procedure() %}
  {# Call the stored procedure to perform clustering #}
  {% set sql %}
    CALL REECO.ANALYTICS_PROD.MY_KMEANS_CLUSTERING();
  {% endset %}
  {{ run_query(sql) }}
  {{ log("KMeans procedure called successfully.", info=True) }}
{% endmacro %}
