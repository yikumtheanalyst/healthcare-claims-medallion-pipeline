# Databricks notebook source
# MAGIC %md
# MAGIC # Project Type: Enterprise-Style Synthetic Healthcare Claims Data Platform

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01 — Bronze Layer: healthcare Claims Ingestion (Clean Template)
# MAGIC
# MAGIC **Author:** Yikum Shiferaw  
# MAGIC **Purpose:** Bronze (raw) ingestion for synthetic healthcare claims + reference code sets  
# MAGIC **Architecture:** Medallion (Bronze → Silver → Gold)
# MAGIC
# MAGIC ### Bronze Principles Applied
# MAGIC - Preserve raw truth (no business transformations, joins, or aggregations)
# MAGIC - Append-only loads (supports replay/auditing)
# MAGIC - Add ingestion metadata (run_id, ingestion_timestamp, source identifier)
# MAGIC - Minimal validations + structured run logging
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Imports

# COMMAND ----------

from datetime import datetime
import uuid
import pandas as pd

from pyspark.sql import functions as F

try:
    import openpyxl
    import xlrd 
    
except ImportError:
    %pip install openpyxl;
    %pip install xlrd;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Parameters (Widgets)

# COMMAND ----------

# Clear widgets (safe for interactive use; harmless in jobs)
dbutils.widgets.removeAll()

# Source (Unity Catalog table)
dbutils.widgets.text("source_prefix", "bronze_layer")       # catalog
dbutils.widgets.text("source_schema", "healthcare_claims")  # schema

# Destination (your Bronze schema)
dbutils.widgets.text("destination_prefix", "bronze_layer")  # catalog
dbutils.widgets.text("destination_schema", "healthcare_claims") # schema

# Optional: reference files location (Unity Catalog Volumes)
dbutils.widgets.text("reference_volume_path", "/Volumes/workspace/default/healthcare_claims_raw/")

# Optional: write mode (append recommended for Bronze)
dbutils.widgets.dropdown("write_mode", "append", ["append", "overwrite"])


# COMMAND ----------

# -------------------------------------------------
# Bronze Source Configuration
# -------------------------------------------------

REFERENCE_FILE = '/Volumes/workspace/default/healthcare_claims_raw/ref_hcpcs_icd10_diagnosis_Place of Services_ref_revenue_codes.xlsx'

dbutils.widgets.text("source_path", "/Volumes/workspace/default/healthcare_claims_raw/synthetic_healthcare_claims.csv")
source_path = dbutils.widgets.get("source_path")

# -------------------------------------------------
# Read Raw CSV (Bronze Ingestion)
# -------------------------------------------------
healthcare_claims  = (
    spark.read
        .format("csv")
        .option("header", True)
        .option("inferSchema", False)   # Always false in Bronze
        .option("multiLine", False)
        .load(source_path)
        )

healthcare_claims.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{dbutils.widgets.get('destination_prefix')}.{dbutils.widgets.get('destination_schema')}.synthetic_healthcare_claims")

rows_read = healthcare_claims .count()

print(f"Rows Read from Source: {rows_read}")
# display(healthcare_claims.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Derived Variables + Run Context

# COMMAND ----------

run_id = str(uuid.uuid4())
start_time = datetime.now()

source_table = (
    f"{dbutils.widgets.get('source_prefix')}."
    f"{dbutils.widgets.get('source_schema')}."
    "synthetic_healthcare_claims"
)

dest_catalog = dbutils.widgets.get("destination_prefix")
dest_schema  = dbutils.widgets.get("destination_schema")

# Bronze target tables (separated by domain)
claims_bronze_table      = f"{dest_catalog}.{dest_schema}.claims_raw"
icd10_bronze_table       = f"{dest_catalog}.{dest_schema}.ref_icd10_diagnosis"
hcpcs_bronze_table       = f"{dest_catalog}.{dest_schema}.ref_hcpcs"
revenue_bronze_table     = f"{dest_catalog}.{dest_schema}.ref_revenue_codes"
pos_bronze_table         = f"{dest_catalog}.{dest_schema}.ref_place_of_service"


plans_bronze_table        = f"{dest_catalog}.{dest_schema}.health_plans_raw"
members_bronze_table      = f"{dest_catalog}.{dest_schema}.members_raw"
providers_bronze_table    = f"{dest_catalog}.{dest_schema}.providers_raw"
pharmacies_bronze_table   = f"{dest_catalog}.{dest_schema}.pharmacies_raw"

# Run log table
run_log_table            = f"{dest_catalog}.{dest_schema}.pipeline_run_log"

reference_path = dbutils.widgets.get("reference_volume_path")
write_mode = dbutils.widgets.get("write_mode").lower().strip()

print(f"run_id: {run_id}")
print(f"source_table: {source_table}")
print(f"reference_path: {reference_path}")
print(f"write_mode: {write_mode}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Create the Run Log Table (if not exists)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {run_log_table} (
  run_id STRING,
  pipeline_name STRING,
  layer STRING,
  source STRING,
  target STRING,
  status STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_seconds DOUBLE,
  rows_read LONG,
  rows_written LONG,
  error_message STRING
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Helper Functions (Write + Logging)

# COMMAND ----------

import re

def write_delta(df, target_table: str, mode: str = "append") -> int:
    """Write a dataframe to a Delta table and return the final row count of the target."""   
    rows_written = df.count()
    (df.write
       .format("delta")
       .mode(mode)
       .option("mergeSchema", "true")
       .saveAsTable(target_table))

    return rows_written

def log_run(
    pipeline_name: str,
    layer: str,
    source: str,
    target: str,
    status: str,
    start_time_dt: datetime,
    rows_read: int,
    rows_written: int,
    error_message: str = None
):
    end_time_dt = datetime.now()
    duration_seconds = (end_time_dt - start_time_dt).total_seconds()

    log_df = spark.createDataFrame([(
        run_id,
        pipeline_name,
        layer,
        source,
        target,
        status,
        start_time_dt,
        end_time_dt,
        float(duration_seconds),
        int(rows_read) if rows_read is not None else None,
        int(rows_written) if rows_written is not None else None,
        error_message
    )], schema="""
        run_id string, pipeline_name string, layer string, source string, target string,
        status string, start_time timestamp, end_time timestamp, duration_seconds double,
        rows_read long, rows_written long, error_message string
    """)

    (log_df.write
          .format("delta")
          .mode("append")
          .saveAsTable(run_log_table))
    
def clean_columns(df):
    df.columns = (
        df.columns
          .str.strip()                    # remove leading/trailing spaces
          .str.lower()                    # lowercase
          .str.replace(r'[^a-z0-9]', '_', regex=True)  # replace ALL non-alphanumeric with _
          .str.replace(r'_+', '_', regex=True)         # collapse multiple underscores
          .str.strip('_')                 # remove leading/trailing underscores
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Ingest Claims (Bronze: raw + metadata only)

# COMMAND ----------

pipeline_name = "healthcare_Claims_Pipeline_Bronze"
layer = "bronze"

rows_read = None
rows_written = None

try:
    if not spark.catalog.tableExists(source_table):
        raise ValueError(f"Source table not found: {source_table}")

    df_claims = spark.table(source_table)
    rows_read = df_claims.count()

    if rows_read == 0:
        raise ValueError(f"Source table is empty: {source_table}")

    # Bronze metadata only (no business transforms)
    df_claims_bronze = (
        df_claims
        .withColumn("run_id", F.lit(run_id))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_table", F.lit(source_table))
    )

    rows_written = write_delta(df_claims_bronze, claims_bronze_table, mode=write_mode)

    log_run(
        pipeline_name=pipeline_name,
        layer=layer,
        source=source_table,
        target=claims_bronze_table,
        status="SUCCESS",
        start_time_dt=start_time,
        rows_read=rows_read,
        rows_written=rows_written
    )

    print(f"Claims loaded. rows_read={rows_read}, rows_written={rows_written}")

except Exception as e:
    log_run(
        pipeline_name=pipeline_name,
        layer=layer,
        source=source_table,
        target=claims_bronze_table,
        status="FAILED",
        start_time_dt=start_time,
        rows_read=rows_read if rows_read is not None else 0,
        rows_written=rows_written if rows_written is not None else 0,
        error_message=str(e)[:4000]
    )
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Ingest Reference Tables (small lookups)
# MAGIC These are loaded as separate Bronze reference tables. They are typically small, so Pandas is acceptable here.
# MAGIC If you prefer 100% Spark ingestion, we can switch to Spark Excel readers or pre-convert to CSV.

# COMMAND ----------

import pandas as pd

def bronze_ref_from_pandas(pdf: pd.DataFrame, target_table: str, source_name: str):
    """Convert a pandas dataframe to Spark and write to Delta with Bronze metadata."""
    df = spark.createDataFrame(pdf)
    df = (df
          .withColumn("run_id", F.lit(run_id))
          .withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_file", F.lit(source_name)))
    return write_delta(df, target_table, mode=write_mode)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.0 Health Plans, Membership, Prescribers and Pharmacies (CSV)

# COMMAND ----------

for file_name in ["health_plans_raw","members_raw","providers_raw","pharmacies_raw"]:
    bronze_table= f"bronze_layer.healthcare_claims.{file_name}"
    
    file_path = f"{file_name}.csv"
    file_path_source = reference_path + file_path

    print(f"{bronze_table} {file_path} {file_path_source}")

    rows_read = 0
    rows_written = 0
    ref_start = datetime.now()

    try:
        
        pdf_icd102 = pd.read_csv(file_path_source )
        rows_read = len(pdf_icd102)

        rows_written = bronze_ref_from_pandas(pdf_icd102, bronze_table, file_path)

        log_run(pipeline_name, layer, file_path_source, bronze_table, "SUCCESS", ref_start, rows_read, rows_written)
        print(f"{file_name} loaded. rows_read={rows_read}, rows_written={rows_written}")

    except Exception as e:
        log_run(pipeline_name, layer, file_path_source, bronze_table, "FAILED", ref_start, rows_read, rows_written, str(e)[:4000])
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 ICD-10 Diagnosis Codes

# COMMAND ----------

rows_read = 0
rows_written = 0
ref_start = datetime.now()

try:
    
    pdf_icd10 = pd.read_excel(
        REFERENCE_FILE,
        sheet_name='ref_icd10_diagnosis',
        names=["DIAGNOSIS_CODE", "SHORT_DESCRIPTION", "LONG_DESCRIPTION"]
    )
    rows_read = len(pdf_icd10)

    rows_written = bronze_ref_from_pandas(pdf_icd10, icd10_bronze_table, REFERENCE_FILE)

    log_run(pipeline_name, layer, REFERENCE_FILE, icd10_bronze_table, "SUCCESS", ref_start, rows_read, rows_written)
    print(f"ICD10 loaded. rows_read={rows_read}, rows_written={rows_written}")

except Exception as e:
    log_run(pipeline_name, layer, REFERENCE_FILE, icd10_bronze_table, "FAILED", ref_start, rows_read, rows_written, str(e)[:4000])
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 HCPCS / Procedure Codes

# COMMAND ----------

rows_read = 0
rows_written = 0
ref_start = datetime.now()

try:
    pdf_hcpcs = pd.read_excel(REFERENCE_FILE, sheet_name='ref_hcpcs')
    rows_read = len(pdf_hcpcs)
    
    pdf_rev=clean_columns(pdf_hcpcs)

    rows_written = bronze_ref_from_pandas(pdf_hcpcs, hcpcs_bronze_table, REFERENCE_FILE)

    log_run(pipeline_name, layer, REFERENCE_FILE, hcpcs_bronze_table, "SUCCESS", ref_start, rows_read, rows_written)
    print(f"HCPCS loaded. rows_read={rows_read}, rows_written={rows_written}")

except Exception as e:
    log_run(pipeline_name, layer, REFERENCE_FILE, hcpcs_bronze_table, "FAILED", ref_start, rows_read, rows_written, str(e)[:4000])
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Revenue Codes

# COMMAND ----------

rows_read = 0
rows_written = 0
ref_start = datetime.now()

try:
    pdf_rev = pd.read_excel(
        REFERENCE_FILE,
        sheet_name="ref_revenue_codes",
        names=["CODE", "DESCRIPTION"]
    )
    rows_read = len(pdf_rev)

    rows_written = bronze_ref_from_pandas(pdf_rev, revenue_bronze_table, REFERENCE_FILE)

    pdf_rev=clean_columns(pdf_rev)

    log_run(pipeline_name, layer, REFERENCE_FILE, revenue_bronze_table, "SUCCESS", ref_start, rows_read, rows_written)
    print(f"Revenue codes loaded. rows_read={rows_read}, rows_written={rows_written}")

except Exception as e:
    log_run(pipeline_name, layer, REFERENCE_FILE, revenue_bronze_table, "FAILED", ref_start, rows_read, rows_written, str(e)[:4000])
    raise


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.4 Place of Service (CSV)

# COMMAND ----------

rows_read = 0
rows_written = 0
ref_start = datetime.now()

try:
    df_pos = spark.createDataFrame(pd.read_excel(REFERENCE_FILE,
                                       sheet_name='Place of Services',
                                       names=["Place_of_Service_Code", "Place_of_Service_Name", "Place_of_Service_Description"],
                                       dtype={"Place_of_Service_Code": "string", "Place_of_Service_Name": "string", "Place_of_Service_Description": "string"}
                                       )
                         )
        
    rows_read = df_pos.count()

    df_pos_bronze = (df_pos
                     .withColumn("run_id", F.lit(run_id))
                     .withColumn("ingestion_timestamp", F.current_timestamp())
                     .withColumn("source_file", F.lit(REFERENCE_FILE)))

    rows_written = write_delta(df_pos_bronze, pos_bronze_table, mode=write_mode)

    log_run(pipeline_name, layer, REFERENCE_FILE, pos_bronze_table, "SUCCESS", ref_start, rows_read, rows_written)
    print(f"Place of service loaded. rows_read={rows_read}, rows_written={rows_written}")

except Exception as e:
    log_run(pipeline_name, layer, REFERENCE_FILE, pos_bronze_table, "FAILED", ref_start, rows_read, rows_written, str(e)[:4000])
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Basic Validation (No EDA)
# MAGIC Only confirm that tables exist and contain records. Detailed profiling belongs in Silver/Gold or a separate profiling notebook.

# COMMAND ----------

tables = [
    claims_bronze_table,
    icd10_bronze_table,
    hcpcs_bronze_table,
    revenue_bronze_table,
    pos_bronze_table,
    run_log_table
]

for t in tables:
    exists = spark.catalog.tableExists(t)
    cnt = spark.table(t).count() if exists else 0
    print(f"{t}: exists={exists}, rows={cnt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9) Review Latest Run Log

# COMMAND ----------

(spark.table(run_log_table)
      .orderBy(F.col("end_time").desc())
      .limit(50)
      .display()
)