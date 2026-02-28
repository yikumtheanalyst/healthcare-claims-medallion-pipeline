# Databricks notebook source
# MAGIC %md
# MAGIC ## Pipeline to ingest, clean and curate data

# COMMAND ----------

# Clear widgets (safe for interactive use; harmless in jobs)
dbutils.widgets.removeAll()

dbutils.widgets.text("reference_workspace_path", "[your profile]/healthcare-claims-medallion-pipeline/notebooks/")
notebook_path = dbutils.widgets.get("reference_workspace_path")
notebook_path

# COMMAND ----------

# This should only run once to create  Catalog and Schema Setup
dbutils.notebook.run(f"{notebook_path}/01_Medallion Architecture Catalog and Schema Setup", 600)
# Run the healthcare claims bronze layer notebook
dbutils.notebook.run(f"{notebook_path}/02_healthcare_claims_bronze_final", 600)
# Run the healthcare claims silver layer notebook
dbutils.notebook.run(f"{notebook_path}/03_healthcare_claims_silver_clean_Final", 600)
# # # Run the healthcare claims gold layer notebook
dbutils.notebook.run(f"{notebook_path}/04_healthcare_claims_gold_final", 600)