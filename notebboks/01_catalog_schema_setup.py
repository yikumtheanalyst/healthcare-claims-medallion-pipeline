# Databricks notebook source
# MAGIC %md
# MAGIC ##### Author/Developer: Yikum Shiferaw
# MAGIC ##### Date: Feb 16,2026
# MAGIC ##### Purpose: Data Engineer | Analytics | Automation
# MAGIC ##### History:

# COMMAND ----------

# MAGIC %md
# MAGIC #### Medallion Architecture structures data pipelines into progressive refinement layers — Bronze for raw ingestion, Silver for validated transformation, and Gold for analytics-ready modeling — ensuring data quality, traceability, and scalable business reporting.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A Catalog and Schema must be defined to load data used in each layer

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup Catalog and Schema

# COMMAND ----------

# DBTITLE 1,Create Catalog and Schema
spark.sql("DROP CATALOG IF EXISTS bronze_layer CASCADE")
spark.sql("DROP CATALOG IF EXISTS silver_layer CASCADE")
spark.sql("DROP CATALOG IF EXISTS gold_layer CASCADE")

try:
    spark.sql("CREATE CATALOG IF NOT EXISTS bronze_layer")
    spark.sql("CREATE CATALOG IF NOT EXISTS silver_layer")
    spark.sql("CREATE CATALOG IF NOT EXISTS gold_layer")

    print("Catalogs dropped if they existed.")

except Exception as e:
    print(e)
    raise e
try:    
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze_layer.healthcare_claims")
    spark.sql("CREATE SCHEMA IF NOT EXISTS silver_layer.healthcare_claims")
    spark.sql("CREATE SCHEMA IF NOT EXISTS gold_layer.healthcare_claims")
    print("Schema created.")

except Exception as e:
    print(e)
    raise e