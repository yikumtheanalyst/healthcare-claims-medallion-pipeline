# Databricks notebook source
# MAGIC %md
# MAGIC # Project Type: Enterprise-Style Synthetic Healthcare Claims Data Platform

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 — Gold Layer: healthcare Claims Star Schema + Power BI Marts (Clean Template)
# MAGIC
# MAGIC **Purpose:** Build **business-ready** Gold tables from Silver entities (Facts, Dimensions, and BI marts/views).  
# MAGIC **Architecture:** Medallion (Bronze → Silver → Gold)
# MAGIC
# MAGIC ## Gold Principles Applied
# MAGIC - Read from Silver clean entity tables (trusted data)
# MAGIC - Apply **business rules** and modeling decisions (grain, surrogate keys, conformed dimensions)
# MAGIC - Create **Facts + Dimensions** optimized for BI (Power BI)
# MAGIC - Create optional marts/views for performance and ease of use
# MAGIC - Structured run logging with **run-level** metrics
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Imports

# COMMAND ----------

from datetime import datetime
import uuid

from pyspark.sql import functions as F
from pyspark.sql import types as T


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Parameters (Widgets)

# COMMAND ----------

dbutils.widgets.removeAll()

# Silver source
dbutils.widgets.text("source_prefix", "silver_layer")          # catalog
dbutils.widgets.text("source_schema", "healthcare_claims")       # schema

# Gold destination
dbutils.widgets.text("destination_prefix", "gold_layer")       # catalog
dbutils.widgets.text("destination_schema", "healthcare_claims")  # schema

# Which Silver batch to process
# - "latest" = process most recent silver_run_id from claims_clean
# - or provide an explicit silver_run_id UUID
dbutils.widgets.text("process_silver_run_id", "latest")

# Gold write mode: typically overwrite (rebuild marts/dims/facts each run)
dbutils.widgets.dropdown("write_mode", "overwrite", ["overwrite", "append"])


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Derived Variables + Run Context

# COMMAND ----------

gold_run_id = str(uuid.uuid4())
start_time = datetime.now()

src_catalog = dbutils.widgets.get("source_prefix")
src_schema  = dbutils.widgets.get("source_schema")

dst_catalog = dbutils.widgets.get("destination_prefix")
dst_schema  = dbutils.widgets.get("destination_schema")

process_silver_run_id = dbutils.widgets.get("process_silver_run_id").strip().lower()
write_mode = dbutils.widgets.get("write_mode").strip().lower()

# Silver source tables
silver_claims_table    = f"{src_catalog}.{src_schema}.claims_clean"
silver_claims_rejects  = f"{src_catalog}.{src_schema}.claims_rejects"
silver_diagnosis_table     = f"{src_catalog}.{src_schema}.ref_icd10_diagnosis"
silver_hcpcs_table     = f"{src_catalog}.{src_schema}.ref_hcpcs"
silver_rev_table       = f"{src_catalog}.{src_schema}.ref_revenue_codes"
silver_pos_table       = f"{src_catalog}.{src_schema}.ref_place_of_service"

silver_health_plans_table  = f"{src_catalog}.{src_schema}.health_plans"
silver_members_table        = f"{src_catalog}.{src_schema}.members"
silver_providers_table      = f"{src_catalog}.{src_schema}.providers"
silver_pharmacies_table     = f"{src_catalog}.{src_schema}.pharmacies"

# Gold star schema tables
gold_fact_claims          = f"{dst_catalog}.{dst_schema}.fact_claims"
gold_dim_members          = f"{dst_catalog}.{dst_schema}.dim_members"
gold_dim_providers        = f"{dst_catalog}.{dst_schema}.dim_providers"
gold_dim_pharmacies       = f"{dst_catalog}.{dst_schema}.dim_pharmacies"
gold_dim_plans            = f"{dst_catalog}.{dst_schema}.dim_plans"
gold_dim_diagnosis_codes  = f"{dst_catalog}.{dst_schema}.dim_diagnosis_codes"
gold_dim_procedures_codes = f"{dst_catalog}.{dst_schema}.dim_procedures_codes"
gold_dim_revenue_codes    = f"{dst_catalog}.{dst_schema}.dim_revenue_codes"
gold_dim_place_of_service = f"{dst_catalog}.{dst_schema}.dim_place_of_service"

gold_dim_date          = f"{dst_catalog}.{dst_schema}.dim_date"

# Gold marts (optional)
gold_mart_plan_month   = f"{dst_catalog}.{dst_schema}.mart_plan_month"

# Gold run log + DQ issues table
run_log_table          = f"{dst_catalog}.{dst_schema}.pipeline_run_log"
dq_issues_table        = f"{dst_catalog}.{dst_schema}.dq_issues"

print(f"gold_run_id: {gold_run_id}")
print(f"process_silver_run_id: {process_silver_run_id}")
print(f"write_mode: {write_mode}")
print(f"silver_claims_table: {silver_claims_table}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Create Gold Run Log + DQ Tables (if not exists)

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
  rows_rejected LONG,
  error_message STRING
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {dq_issues_table} (
  gold_run_id STRING,
  issue_timestamp TIMESTAMP,
  issue_type STRING,
  table_name STRING,
  record_count LONG,
  details STRING
)
USING DELTA
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Helper Functions (Logging, Writing, Keys, DQ)

# COMMAND ----------

def log_run(
    pipeline_name: str,
    layer: str,
    source: str,
    target: str,
    status: str,
    start_time_dt: datetime,
    rows_read: int,
    rows_written: int,
    rows_rejected: int = 0,
    error_message: str = None
):
    end_time_dt = datetime.now()
    duration_seconds = (end_time_dt - start_time_dt).total_seconds()

    log_df = spark.createDataFrame([(
        gold_run_id,
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
        int(rows_rejected) if rows_rejected is not None else 0,
        error_message
    )], schema="""
        run_id string, pipeline_name string, layer string, source string, target string,
        status string, start_time timestamp, end_time timestamp, duration_seconds double,
        rows_read long, rows_written long, rows_rejected long, error_message string
    """)

    (log_df.write
          .format("delta")
          .option("overwriteSchema", "true")
          .option("mergeSchema", "true")
          .mode("append")
          .saveAsTable(run_log_table))


def write_delta(df, target_table: str, mode: str = "overwrite") -> int:
    """Write a dataframe to Delta and return rows written for THIS write."""
    rows_written = df.count()
    (df.write
       .format("delta")
       .mode(mode)
       .option("overwriteSchema", "true" if mode == "overwrite" else "false")
       .option("mergeSchema", "true")
       .saveAsTable(target_table))
    return rows_written


def stable_key(*cols) -> F.Column:
    """Create a deterministic surrogate key from one or more business key columns."""
    exprs = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
    return F.sha2(F.concat_ws("||", *exprs), 256)


def record_dq(issue_type: str, table_name: str, record_count: int, details: str):
    df_issue = spark.createDataFrame([(
        gold_run_id,
        datetime.now(),
        issue_type,
        table_name,
        int(record_count),
        details
    )], schema="""
        gold_run_id string, issue_timestamp timestamp, issue_type string,
        table_name string, record_count long, details string
    """)
    (
    df_issue.write
        .format("delta")
        .mode("append")
        .option("overwriteSchema", "true")
        .option("mergeSchema", "true")
        .saveAsTable(dq_issues_table)
)


def get_latest_silver_run_id(df_silver):
    if "silver_run_id" not in df_silver.columns or "silver_processed_timestamp" not in df_silver.columns:
        raise ValueError("Silver table must contain silver_run_id and silver_processed_timestamp columns.")
    r = (df_silver
         .orderBy(F.col("silver_processed_timestamp").desc())
         .select("silver_run_id")
         .limit(1)
         .collect())
    if not r:
        raise ValueError("Silver claims table is empty.")
    return r[0]["silver_run_id"]


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6) Read Silver Claims and Select Batch to Process

# COMMAND ----------

if not spark.catalog.tableExists(silver_claims_table):
    raise ValueError(f"Silver claims table not found: {silver_claims_table}")

df_claims_silver_all = spark.table(silver_claims_table)
df_claims_silver_all = df_claims_silver_all.withColumnRenamed("place_of_service_std_id", "place_of_service_code")

if process_silver_run_id == "latest":
    silver_run_id_to_process = get_latest_silver_run_id(df_claims_silver_all)
else:
    silver_run_id_to_process = dbutils.widgets.get("process_silver_run_id").strip()

df_claims_silver = df_claims_silver_all.filter(F.col("silver_run_id") == silver_run_id_to_process)

rows_claims_read = df_claims_silver.count()
if rows_claims_read == 0:
    raise ValueError(f"No rows found for silver_run_id={silver_run_id_to_process} in {silver_claims_table}")

print(f"Processing silver_run_id: {silver_run_id_to_process}")
print(f"claims rows_read: {rows_claims_read}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7) Apply Gold Business Rules (Derived Measures + Flags)
# MAGIC Gold is where domain rules belong. Keep original Silver columns and add derived columns/flags.

# COMMAND ----------

pipeline_name = "healthcare_Claims_Pipeline"
layer = "gold"

df = df_claims_silver

# Example business rule: impute null monetary amounts to 0 (transparent flags)
money_cols = ["paid_amount", "allowed_amount", "billed_amount", "line_paid", "line_allowed", "line_charge", "member_cost_share"]
for c in money_cols:
    if c in df.columns:
        df = df.withColumn(f"{c}_imputed_flag", F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0)))
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0.00).cast("decimal(18,2)")))

# Null units/quantity => 0 (transparent flags)
for c in ["units", "quantity", "days_supply"]:
    if c in df.columns:
        df = df.withColumn(f"{c}_imputed_flag", F.when(F.col(c).isNull(), F.lit(1)).otherwise(F.lit(0)))
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

# Claim type flag
if "claim_type" in df.columns:
    df = df.withColumn("is_rx_claim", F.when(F.upper(F.col("claim_type")).isin("RX","PHARMACY"), F.lit(1)).otherwise(F.lit(0)))
else:
    df = df.withColumn("is_rx_claim", F.lit(None).cast("int"))

# Gold metadata
df = (df
      .withColumn("gold_run_id", F.lit(gold_run_id))
      .withColumn("gold_processed_timestamp", F.current_timestamp())
     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8) Build Dimensions (Conformed)

# COMMAND ----------

# Determine candidate columns based on available schema
member_cols         = [c for c in ["member_id", "member_name", "member_dob", "member_gender","member_age"] if c in spark.table(silver_members_table).columns]
provider_cols       = [c for c in ["provider_id", "npi", "provider_specialty", "provider_state"] if c in spark.table(silver_providers_table).columns]
pharm_cols          = [c for c in ["pharmacy_id", "pharmacy_name", "pharmacy_state"] if c in spark.table(silver_pharmacies_table).columns]
plan_cols           = [c for c in ["plan_id", "plan_name","plan_state","payer_type"] if c in spark.table(silver_health_plans_table).columns]
diagnosis_cols      = [c for c in ['DIAGNOSIS_CODE','SHORT_DESCRIPTION','LONG_DESCRIPTION'] if c in spark.table(silver_diagnosis_table).columns]

hcps_df = spark.table(silver_hcpcs_table)
hcps_df = hcps_df.withColumnRenamed("hcpc", "procedure_code")
hcpcs_icd10_cols      = [c for c in ['procedure_code', 'long_description', 'short_description',] if c in hcps_df.columns]

rev_df = spark.table(silver_rev_table)
rev_df = rev_df.withColumnRenamed("CODE", "revenue_code")
revenue_codes_cols  = [c for c in ['revenue_code','DESCRIPTION',] if c in spark.table(silver_rev_table).columns]

placeofservice_cols = [c for c in ['Place_of_Service_Code', 'Place_of_Service_Name', 'Place_of_Service_Description'] if c in spark.table(silver_pos_table).columns]

def build_dim(source_df, id_col: str, cols: list, key_col: str, key_from: list, target_table: str):
    if id_col not in str(source_df.columns).lower():
        print(f"{id_col} not found; skipping {target_table}")
    return 0

def build_dim(source_df, id_col: str, cols: list, key_col: str, key_from: list, target_table: str):
    if id_col not in df.columns:
        print(f"{id_col} not found; skipping {target_table}")
        return 0
    dim_df = (source_df.select(*cols)
              .dropDuplicates([id_col])
              .withColumn(key_col, stable_key(*key_from))
              .withColumn("gold_run_id", F.lit(gold_run_id))
              .withColumn("gold_processed_timestamp", F.current_timestamp()))
    return write_delta(dim_df, target_table, mode=write_mode)

t0 = datetime.now()
try:
    mem_df = spark.table(silver_members_table)    
    rw = build_dim(mem_df,"member_id"               , member_cols           , "member_key"          , ["member_id"]             , gold_dim_members)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_members, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_members, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim provider
t0 = datetime.now()
try:
    prov_df = spark.table(silver_providers_table)
    rw = build_dim(prov_df,"provider_id", provider_cols, "provider_key", ["provider_id"], gold_dim_providers)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_providers, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_providers, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim pharmacy
t0 = datetime.now()
try:
    phar_df = spark.table(silver_pharmacies_table)    
    rw = build_dim(phar_df,"pharmacy_id", pharm_cols, "pharmacy_key", ["pharmacy_id"], gold_dim_pharmacies)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_pharmacies, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_pharmacies, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim Diagnosis 
t0 = datetime.now()
try:
    diag_df = spark.table(silver_diagnosis_table)
    rw = build_dim(diag_df,"diagnosis_code", diagnosis_cols, "diagnosis_code_key", ["diagnosis_code"], gold_dim_diagnosis_codes)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_diagnosis_codes, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_diagnosis_codes, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim Procedure Codes
t0 = datetime.now()
try:    
    rw = build_dim(hcps_df,"procedure_code", hcpcs_icd10_cols, "procedure_code_key", ["procedure_code"], gold_dim_procedures_codes)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_procedures_codes, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_procedures_codes, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim Revenue Codes

t0 = datetime.now()
try:    
    rw = build_dim(rev_df,"revenue_code", revenue_codes_cols, "revenue_code_key", ["revenue_code"],  gold_dim_revenue_codes)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_revenue_codes, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_revenue_codes, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim Place of Service
t0 = datetime.now()
try:
    pos_df = spark.table(silver_pos_table)
    rw = build_dim(pos_df,"place_of_service_code", placeofservice_cols, "place_of_service_key", ["place_of_service_code"],  gold_dim_place_of_service)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_place_of_service, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_place_of_service, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise
    
# Dim plan
t0 = datetime.now()
try:
    plans_df = spark.table(silver_health_plans_table)    
    rw = build_dim(plans_df,"plan_id",              plan_cols,              "plan_key",             ["plan_id"], gold_dim_plans)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_plans, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_plans, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## 9) Build Health Plan and Member relationship. A member must belong to a health plan.

# COMMAND ----------

dim_members_df = spark.table("gold_layer.healthcare_claims.dim_members")

member_df = spark.table("gold_layer.healthcare_claims.dim_members").select("member_key").distinct().collect()
plans_df = spark.table("gold_layer.healthcare_claims.dim_plans").select("plan_key").distinct().collect()

import random

# Extract member_keys and plan_keys
member_keys = [row["member_key"] for row in member_df if row["member_key"] is not None]
plan_keys = [row["plan_key"] for row in plans_df if row["plan_key"] is not None]

if not plan_keys:
    raise ValueError("No plan_key values found in plans_df.")

# Assign a random plan_key to each member_key
member_plan_pairs = [(member_key, random.choice(plan_keys)) for member_key in member_keys]

# Create DataFrame with member_key and assigned plan_key
df_member_plan = spark.createDataFrame(member_plan_pairs, ["member_key", "plan_key"])

dim_members_df_final = dim_members_df.join(df_member_plan, "member_key", "inner")

dim_members_df_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{dst_catalog}.{dst_schema}.dim_members")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10) Build dim_date

# COMMAND ----------

t0 = datetime.now()
try:
    if "admission_date" not in df.columns:
        raise ValueError("admission_date missing; cannot build dim_date.")

    mm = df.select(F.min("admission_date").cast("date").alias("min_d"), F.max("admission_date").cast("date").alias("max_d")).collect()[0]
    min_d, max_d = mm["min_d"], mm["max_d"]
    if min_d is None or max_d is None:
        raise ValueError("No non-null admission_date values found.")

    df_dates = spark.sql(f"SELECT explode(sequence(to_date('{min_d}'), to_date('{max_d}'), interval 1 day)) AS date_day")

    df_dim_date = (df_dates
    .withColumn("date_key", F.date_format("date_day", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date_day"))
    .withColumn("quarter", F.quarter("date_day"))
    .withColumn("month", F.month("date_day"))
    .withColumn("month_name", F.date_format("date_day", "MMMM"))
    .withColumn("day", F.dayofmonth("date_day"))
    .withColumn("day_of_week_num", F.dayofweek("date_day"))
    .withColumn("day_of_week_name", F.date_format("date_day", "E"))
    .withColumn("week_of_year", F.weekofyear("date_day"))
    .withColumn("is_weekend", F.when(F.col("day_of_week_num").isin([1,7]), 1).otherwise(0)  )
    .withColumn("gold_run_id", F.lit(gold_run_id))
    .withColumn("gold_processed_timestamp", F.current_timestamp())
)    

    rw = write_delta(df_dim_date, gold_dim_date, mode=write_mode)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_date, "SUCCESS", t0, rows_claims_read, rw, 0)

except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_date, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise


# COMMAND ----------

# MAGIC %md
# MAGIC ## 11) Build fact_claims (Grain + Measures + Keys)

# COMMAND ----------

t0 = datetime.now()
try:
    # Determine grain key
    if "claim_line_id" in df.columns:
        df_fact = df.withColumn("claim_fact_key", stable_key("claim_line_id"))
        grain_cols = ["claim_line_id"]
    else:
        # Fallback grain
        required = [c for c in ["claim_id", "member_id", "admission_date","service_line_number"] if c in df.columns]
        if len(required) < 2:
            raise ValueError("Cannot determine fact grain. Need claim_id/member_id (and ideally admission_date).")
        df_fact = df.withColumn("claim_fact_key", stable_key(*required))
        grain_cols = required

    # Surrogate keys for dims
    if "member_id" in df_fact.columns:
        df_fact = df_fact.withColumn("member_key", stable_key("member_id"))
    if "provider_id" in df_fact.columns:
        df_fact = df_fact.withColumn("provider_key", stable_key("provider_id"))
    if "pharmacy_id" in df_fact.columns:
        df_fact = df_fact.withColumn("pharmacy_key", stable_key("pharmacy_id"))    
    if "diagnosis_code" in df_fact.columns:
        df_fact = df_fact.withColumn("diagnosis_code_key", stable_key("diagnosis_code"))
    if "procedure_code" in df_fact.columns:
        df_fact = df_fact.withColumn("procedure_code_key", stable_key("procedure_code"))
    if "revenue_code" in df_fact.columns:
        df_fact = df_fact.withColumn("revenue_code_key", stable_key("revenue_code"))
    if "place_of_service_code" in df_fact.columns:
        df_fact = df_fact.withColumn("place_of_service_key", stable_key("place_of_service_code"))
    
    plan_id_col = "health_plan_id" if "health_plan_id" in df_fact.columns else ("plan_id" if "plan_id" in df_fact.columns else None)
    if plan_id_col:
        df_fact = df_fact.withColumn("plan_key", stable_key(plan_id_col))

    # Date key
    df_fact = df_fact.withColumn("date_key", F.date_format(F.col("admission_date"), "yyyyMMdd").cast("int"))

    # DQ: duplicate grain count
    dup_cnt = (df_fact.groupBy(*grain_cols).count().filter(F.col("count") > 1).count())
    if dup_cnt > 0:
        record_dq("DUPLICATE_GRAIN", gold_fact_claims, dup_cnt, f"Duplicate grain found on {grain_cols}")
        print(f"WARNING: duplicate grain rows found: {dup_cnt}")

    # Select columns for the fact table
    keep = [
        "claim_fact_key", "claim_id", "date_key", "admission_date",
        "member_key", "provider_key", "pharmacy_key", "plan_key",'diagnosis_code_key', "procedure_code_key", "revenue_code_key","place_of_service_key",
        "is_rx_claim","claim_type","discharge_date" ]

    measures = ["paid_amount","allowed_amount","billed_amount","line_paid","line_allowed","line_charge","member_cost_share",
                "units","quantity","days_supply"]
    for c in measures:
        if c in df_fact.columns:
            keep.append(c)

    # Operational troubleshooting codes (optional)
    # for c in ["diagnosis_code","procedure_code","revenue_code","place_of_service_code","ndc_code"]:
    #     if c in df_fact.columns and c not in keep:
    #         keep.append(c)

    keep = [c for c in keep if c in df_fact.columns]

    df_fact_out = (df_fact.select(*keep)
                   .withColumn("gold_run_id", F.lit(gold_run_id))
                   .withColumn("gold_processed_timestamp", F.current_timestamp()))

    rw = write_delta(df_fact_out, gold_fact_claims, mode=write_mode)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_fact_claims, "SUCCESS", t0, rows_claims_read, rw, 0)

except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_fact_claims, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# display(df_fact.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12) Build Gold Marts (Aggregations for BI Performance)

# COMMAND ----------

# Mart: Plan-Month summary
t0 = datetime.now()
try:
    if not spark.catalog.tableExists(gold_fact_claims):
        raise ValueError(f"Gold fact not found: {gold_fact_claims}")

    df_fact = spark.table(gold_fact_claims)
    rr = df_fact.count()

    if "plan_key" in df_fact.columns and "admission_date" in df_fact.columns:
        df_mart = (df_fact
                   .withColumn("year_month", F.date_format(F.col("admission_date"), "yyyy-MM"))
                   .groupBy("plan_key", "year_month")
                   .agg(
                       F.countDistinct("claim_id").alias("claims_cnt"),
                       F.sum(F.coalesce(F.col("line_allowed"), F.lit(0))).alias("paid_amount"),                       
                       F.sum(F.coalesce(F.col("line_charge"), F.lit(0))).alias("billed_amount")
                   )
                   .withColumn("gold_run_id", F.lit(gold_run_id))
                   .withColumn("gold_processed_timestamp", F.current_timestamp())
                   )

        rw = write_delta(df_mart, gold_mart_plan_month, mode=write_mode)
        log_run(pipeline_name, layer, gold_fact_claims, gold_mart_plan_month, "SUCCESS", t0, rr, rw, 0)
    else:
        print("plan_key/admission_date missing; skipping mart_plan_month")

except Exception as e:
    log_run(pipeline_name, layer, gold_fact_claims, gold_mart_plan_month, "FAILED", t0, 0, 0, 0, str(e)[:4000]); raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13) Create Power BI Friendly Views

# COMMAND ----------

# Create/replace an enriched view for convenience (optional)
# This view joins Fact -> Dimensions where keys exist.
vw_fact_claims_enriched_df =spark.sql(f"""
SELECT
f.claim_fact_key
,f.claim_id as encounter_id
,f.date_key
,f.admission_date
,f.member_key
,f.provider_key
,f.pharmacy_key
,f.plan_key
,f.diagnosis_code_key
,f.procedure_code_key
,f.revenue_code_key
,f.place_of_service_key
,f.is_rx_claim
,f.claim_type
,f.discharge_date
,f.line_allowed
,f.line_charge
,pl.plan_id
,pl.plan_name
,pl.plan_state
,pl.payer_type
,m.member_id
,m.member_name
,m.member_dob
,m.member_gender
,m.member_age
,pr.provider_id
,pr.npi as prescriber_npi
,pr.provider_specialty
,pr.provider_state
,ph.pharmacy_id
,ph.pharmacy_name
,ph.pharmacy_state
,d.DIAGNOSIS_CODE
,d.SHORT_DESCRIPTION as DIAG_SHORT_DESCRIPTION
,d.LONG_DESCRIPTION as DIAG_LONG_DESCRIPTION
,pc.procedure_code
,pc.short_description as procedure_short_description
,pc.long_description as  procedure_long_description
,rc.revenue_code
,rc.DESCRIPTION as revenue_description
,pos.Place_of_Service_Code
,pos.Place_of_Service_Name
,pos.Place_of_Service_Description

FROM {gold_fact_claims} f
LEFT JOIN {gold_dim_plans} pl ON f.plan_key = pl.plan_key
LEFT JOIN {gold_dim_members} m ON f.member_key = m.member_key
LEFT JOIN {gold_dim_providers} pr ON f.provider_key = pr.provider_key
left join {gold_dim_pharmacies} ph ON f.pharmacy_key = ph.pharmacy_key
left join {gold_dim_diagnosis_codes} d on f.diagnosis_code_key = d.diagnosis_code_key
left join {gold_dim_procedures_codes} pc on f.procedure_code_key = pc.procedure_code_key
left JOIN {gold_dim_revenue_codes} rc on f.revenue_code_key = rc.revenue_code_key
left join {gold_dim_place_of_service} pos on f.place_of_service_key = pos.place_of_service_key
""")

# Make all columns as proper case
vw_fact_claims_enriched_df = vw_fact_claims_enriched_df.select([F.col(x).alias(x.upper()) for x in vw_fact_claims_enriched_df.columns])

# vw_fact_claims_enriched_df.createOrReplaceGlobalTempView(f"{dst_catalog}.{dst_schema}.vw_fact_claims_enriched") 
#[NOT_SUPPORTED_WITH_SERVERLESS] GLOBAL TEMPORARY VIEW is not supported on serverless compute. SQLSTATE: 0A000

# If you want to persist the view as a table, uncomment the following line:
vw_fact_claims_enriched_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{dst_catalog}.{dst_schema}.bi_fact_claims_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14) Basic Validation + Latest Logs

# COMMAND ----------

tables = [
    gold_dim_plans,
    gold_dim_members,
    gold_dim_providers,
    gold_dim_pharmacies,        
    gold_dim_diagnosis_codes,
    gold_dim_procedures_codes,
    gold_dim_revenue_codes,
    gold_dim_place_of_service,
    gold_dim_date,
    gold_fact_claims,
    gold_mart_plan_month,    
    run_log_table,
    dq_issues_table
]

for t in tables:
    if spark.catalog.tableExists(t):
        print(f"{t}: rows={spark.table(t).count()}")
    else:
        print(f"{t}: (not found)")

print("\nLatest Gold run logs:")
(spark.table(run_log_table)
      .filter(F.col("layer") == "gold")
      .orderBy(F.col("end_time").desc())
      .limit(50)
      .display())

print("\nLatest DQ issues:")
(spark.table(dq_issues_table)
      .orderBy(F.col("issue_timestamp").desc())
      .limit(50)
      .display())
