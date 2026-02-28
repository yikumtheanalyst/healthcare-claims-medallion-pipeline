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
silver_icd10_table     = f"{src_catalog}.{src_schema}.ref_icd10_diagnosis"
silver_hcpcs_table     = f"{src_catalog}.{src_schema}.ref_hcpcs"
silver_rev_table       = f"{src_catalog}.{src_schema}.ref_revenue_codes"
silver_pos_table       = f"{src_catalog}.{src_schema}.ref_place_of_service"

silver_health_plans_table  = f"{src_catalog}.{src_schema}.health_plans"
silver_members_table        = f"{src_catalog}.{src_schema}.members"
silver_providers_table      = f"{src_catalog}.{src_schema}.providers"
silver_pharmacies_table     = f"{src_catalog}.{src_schema}.pharmacies"

# Gold star schema tables
gold_fact_claims       = f"{dst_catalog}.{dst_schema}.fact_claims"
gold_dim_member        = f"{dst_catalog}.{dst_schema}.dim_member"
gold_dim_provider      = f"{dst_catalog}.{dst_schema}.dim_provider"
gold_dim_pharmacy      = f"{dst_catalog}.{dst_schema}.dim_pharmacy"
gold_dim_plan          = f"{dst_catalog}.{dst_schema}.dim_plan"

gold_dim_diagnosis        = f"{dst_catalog}.{dst_schema}.dim_diagnosis"
gold_dim_procedure        = f"{dst_catalog}.{dst_schema}.dim_procedure"
gold_dim_revenue_codes        = f"{dst_catalog}.{dst_schema}.dim_revenue_codes"
gold_dim_place_of_service        = f"{dst_catalog}.{dst_schema}.dim_place_of_service"

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

    # (df_issue.write
    #       .format("delta")
    #       .mode("append")
    #       .option("overwriteSchema", "true")
    #       .option("mergeSchema", "true"          
    #       .saveAsTable(dq_issues_table)))


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

# Standard analytic date for trending
# admission_date = F.coalesce(    
#     F.col("admission_date") if "admission_date" in df.columns else F.lit(None).cast("date")#,
#     # F.col("admission_date") if "admission_date" in df.columns else F.lit(None).cast("date"),
#     # F.col("claim_date") if "claim_date" in df.columns else F.lit(None).cast("date"),
#     # F.col("fill_date") if "fill_date" in df.columns else F.lit(None).cast("date")
# )
# df = df.withColumn("admission_date", admission_date)

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

if not spark.catalog.tableExists(silver_icd10_table):
    raise ValueError(f"Silver claims table not found: {silver_icd10_table}")

df_silver_icd10_table = spark.table(silver_icd10_table)
df_silver_icd10_table = df_silver_icd10_table.toDF(*[c.lower() for c in df_silver_icd10_table.columns])

if not spark.catalog.tableExists(silver_hcpcs_table):
    raise ValueError(f"Silver claims table not found: {silver_hcpcs_table}")

df_silver_hcpcs_table = spark.table(silver_hcpcs_table)
df_silver_hcpcs_table = df_silver_hcpcs_table.toDF(*[c.lower() for c in df_silver_hcpcs_table.columns])

if not spark.catalog.tableExists(silver_rev_table):
    raise ValueError(f"Silver claims table not found: {silver_rev_table}")

df_silver_rev_table = spark.table(silver_rev_table)
df_silver_rev_table = df_silver_rev_table.toDF(*[c.lower() for c in df_silver_rev_table.columns])

if not spark.catalog.tableExists(silver_pos_table):
    raise ValueError(f"Silver claims table not found: {silver_pos_table}")

df_silver_pos_table= spark.table(silver_pos_table)
df_silver_pos_table = df_silver_pos_table.toDF(*[c.lower() for c in df_silver_pos_table.columns])

# COMMAND ----------

# Determine candidate columns based on available schema
member_cols   = [c for c in ["member_id", "member_name", "member_dob", "member_gender","member_age"] if c in spark.table(silver_members_table).columns]
provider_cols = [c for c in ["provider_id", "npi", "provider_specialty", "provider_state"] if c in spark.table(silver_providers_table).columns]
pharm_cols    = [c for c in ["pharmacy_id", "pharmacy_name", "pharmacy_state"] if c in spark.table(silver_pharmacies_table).columns]

plan_cols     = [c for c in ["plan_id", "plan_name","plan_state","payer_type"] if c in spark.table(silver_health_plans_table).columns]

diagnosis_cols   = [c for c in ["diagnosis_code"] if c in df.columns] 
icd10_cols   = [c for c in ["procedure_code"] if c in df.columns]
revenue_codes_cols   = [c for c in ["revenue_code"] if c in df.columns] 
placeofservice_cols   = [c for c in ["place_of_service_std_id"] if c in df.columns]

def build_dim(id_col: str, cols: list, key_col: str, key_from: list, target_table: str):
    if id_col not in df.columns:
        print(f"{id_col} not found; skipping {target_table}")
        return 0
    dim_df = (df.select(*cols)
              .dropDuplicates([id_col])
              .withColumn(key_col, stable_key(*key_from))
              .withColumn("gold_run_id", F.lit(gold_run_id))
              .withColumn("gold_processed_timestamp", F.current_timestamp()))
    return write_delta(dim_df, target_table, mode=write_mode)


t0 = datetime.now()
try:
    rw = build_dim("member_id", member_cols, "member_key", ["member_id"], gold_dim_member)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_member, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_member, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim provider
t0 = datetime.now()
try:
    rw = build_dim("provider_id", provider_cols, "provider_key", ["provider_id"], gold_dim_provider)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_provider, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_provider, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim pharmacy
t0 = datetime.now()
try:
    rw = build_dim("pharmacy_id", pharm_cols, "pharmacy_key", ["pharmacy_id"], gold_dim_pharmacy)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_pharmacy, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_pharmacy, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim Diagnosis 
t0 = datetime.now()
try:
    rw = build_dim("diagnosis_code", diagnosis_cols, "diagnosis_key", ["diagnosis_code"], gold_dim_diagnosis)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_diagnosis, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_diagnosis, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim Procedure Codes
t0 = datetime.now()
try:
    rw = build_dim("procedure_code", icd10_cols, "procedure_key", ["procedure_code"], gold_dim_procedure)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_procedure, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_procedure, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim Revenue Codes
t0 = datetime.now()
try:
    rw = build_dim("revenue_code", revenue_codes_cols, "revenue_key", ["revenue_code"], gold_dim_revenue_codes)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_revenue_codes, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_revenue_codes, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# Dim Place of Service
t0 = datetime.now()
try:
    rw = build_dim("place_of_service_std_id", placeofservice_cols, "place_of_service_key", ["place_of_service_std_id"], gold_dim_place_of_service)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_place_of_service, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_place_of_service, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise
    
# Dim plan

t0 = datetime.now()
try:
    rw = build_dim("plan_id", plan_cols, "plan_key", ["plan_id"], gold_dim_plan)
    log_run(pipeline_name, layer, f"{silver_claims_table} (silver_run_id={silver_run_id_to_process})", gold_dim_plan, "SUCCESS", t0, rows_claims_read, rw, 0)
except Exception as e:
    log_run(pipeline_name, layer, silver_claims_table, gold_dim_plan, "FAILED", t0, rows_claims_read, 0, 0, str(e)[:4000]); raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9) Build dim_date

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
# MAGIC ## 10) Build fact_claims (Grain + Measures + Keys)

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
        df_fact = df_fact.withColumn("diagnosis_key", stable_key("diagnosis_code"))
    if "procedure_code" in df_fact.columns:
        df_fact = df_fact.withColumn("procedure_key", stable_key("procedure_code"))
    if "revenue_code" in df_fact.columns:
        df_fact = df_fact.withColumn("revenue_key", stable_key("revenue_code"))
    if "place_of_service_std_id" in df_fact.columns:
        df_fact = df_fact.withColumn("place_of_service_key", stable_key("place_of_service_std_id"))
    
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
        "member_key", "provider_key", "pharmacy_key", "plan_key",'diagnosis_key', "procedure_key", "revenue_key","place_of_service_key",
        "is_rx_claim"
    ]

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
# MAGIC ## 11) Build Gold Marts (Aggregations for BI Performance)

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
# MAGIC ## 12) Create Power BI Friendly Views

# COMMAND ----------

# Create/replace an enriched view for convenience (optional)
# This view joins Fact -> Dimensions where keys exist.
spark.sql(f"""
CREATE OR REPLACE VIEW {dst_catalog}.{dst_schema}.vw_fact_claims_enriched AS
SELECT
  f.*
  ,m.member_id
--   ,m.member_state
  ,m.member_gender
  ,m.member_age
  ,pr.provider_id
  ,pr.provider_specialty  
  ,ph.pharmacy_id
  ,ph.pharmacy_name
 ,pl.plan_id
 ,pl.plan_name
 ,pl.plan_state
 ,pl.payer_type
,d.diagnosis_code
,pc.procedure_code
,rc.revenue_code
,pos.place_of_service_std_id as place_of_service_code
FROM {gold_fact_claims} f
LEFT JOIN {gold_dim_member} m ON f.member_key = m.member_key
LEFT JOIN {gold_dim_provider} pr ON f.provider_key = pr.provider_key
left join {gold_dim_pharmacy} ph ON f.pharmacy_key = ph.pharmacy_key
LEFT JOIN {gold_dim_plan} pl ON f.plan_key = pl.plan_key
left join {gold_dim_diagnosis} d on f.diagnosis_key = d.diagnosis_key
left join {gold_dim_procedure} pc on f.procedure_key = pc.procedure_key
left JOIN {gold_dim_revenue_codes} rc on f.revenue_key = rc.revenue_key
left join {gold_dim_place_of_service} pos on f.place_of_service_key = pos.place_of_service_key
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13) Basic Validation + Latest Logs

# COMMAND ----------

tables = [
    gold_dim_plan,
    gold_dim_member,
    gold_dim_provider,
    gold_dim_pharmacy,        
    gold_dim_diagnosis,
    gold_dim_procedure,
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
