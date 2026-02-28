# Databricks notebook source
# MAGIC %md
# MAGIC # Project Type: Enterprise-Style Synthetic Healthcare Claims Data Platform

# COMMAND ----------

# MAGIC %md
# MAGIC #### Author: Yikum Shiferaw
# MAGIC #### Date: 2/28/2026
# MAGIC #### History:  2/28/26 initial code creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Synthetic Healthcare Claims Generator (Real Code References)
# MAGIC
# MAGIC This notebook reads real-world healthcare code reference tables and generates a configurable number of synthetic healthcare claims.
# MAGIC
# MAGIC Update `RECORD_COUNT` to generate more records.

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

# dbutils.widgets.text("source_path", "/Volumes/workspace/default/healthcare_claims_raw/synthetic_healthcare_claims_10000_real_codes.csv")

source_path = dbutils.widgets.get("reference_volume_path")
source_path


# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup reference file path

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Faker for realistic synthetic people

# COMMAND ----------

# Faker produces natural‑looking names, DOBs, addresses, phone numbers, etc. It’s perfect for Bronze‑layer demographic tables.
# Install Faker (if needed)
try:
    !pip install faker
    dbutils.library.restartPython();
except Exception as e:    
    print("Faker already installed")

# COMMAND ----------

# Generate random names + DOB

from faker import Faker;
import pandas as pd
import random
import numpy as np
import random
import re
from datetime import datetime, timedelta

fake = Faker()

num_people = 1000

people = []

for _ in range(num_people):
    name = fake.name()
    dob = fake.date_of_birth(minimum_age=0, maximum_age=100)  # realistic DOB
    gender = random.choice(["M", "F"])
    
    people.append((name, dob, gender))

people_df = pd.DataFrame(people, columns=["name", "dob", "gender"])

# import datetime
# from datetime import timedelta
from datetime import datetime
# Calculate age
people_df['member_age'] = pd.to_datetime(people_df['dob']).apply(lambda x: (datetime.now() - x).days // 365)

# people_df['member_age'] = people_df['member_dob'].apply(lambda x: (datetime.now()

# display(people_df)

# COMMAND ----------

# =============================
# CONFIGURATION
# =============================

REFERENCE_FILE = '/Volumes/workspace/default/healthcare_claims_raw/ref_hcpcs_icd10_diagnosis_Place of Services_ref_revenue_codes.xlsx'
OUTPUT_FILE = '/Volumes/workspace/default/healthcare_claims_raw/synthetic_healthcare_claims.csv'
RECORD_COUNT = 3000  # <<< CHANGE THIS

np.random.seed(42)
random.seed(42)
REFERENCE_FILE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load reference tables to obtain the codes used to generate the synthetic claims

# COMMAND ----------

df_hcpcs = pd.read_excel(REFERENCE_FILE, sheet_name='ref_hcpcs')
df_icd10 = pd.read_excel(REFERENCE_FILE, sheet_name='ref_icd10_diagnosis')
df_pos   = pd.read_excel(REFERENCE_FILE, sheet_name='Place of Services')

# Rename a column in a pandas DataFrame
df_pos = df_pos.rename(columns={'Place of Service Code(s)': 'Place_of_Service_Code',
                                "Place of Service Name": 'Place_of_Service_Name', 
                                "Place of Service Description": 'Place_of_Service_Description'}
                                )

df_rev   = pd.read_excel(REFERENCE_FILE, sheet_name='ref_revenue_codes')

hcpcs_codes = df_hcpcs['HCPC'].dropna().astype(str).str.strip().tolist()
icd_codes   = df_icd10['DIAGNOSIS_CODE'].dropna().astype(str).str.strip().tolist()
rev_codes   = df_rev['CODE'].dropna().astype(str).str.strip().tolist()

pos_codes = (
    df_pos["Place_of_Service_Code"]
      .dropna()
      .astype(str)
      .str.extract(r"(\d+)")[0]   # grab first number (handles "73-80")
      .dropna()
      .tolist()
)

if not pos_codes:
    raise ValueError("POS extraction failed: pos_codes is empty. Check column name and sheet.")

print("POS codes count:", len(pos_codes))
print("POS codes sample:", pos_codes[:20])
print("df_pos columns:", list(df_pos.columns))

# COMMAND ----------

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def random_npi():
    return str(random.randint(1000000000, 9999999999))

def random_zip3():
    return str(random.randint(100, 999))

def random_state():
    return random.choice(['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY'])

def random_gender():
    return random.choice(['M','F'])

# COMMAND ----------

plans = [
    (100,'PLAN_A', 'Managed healthcare',random_state()),
    (200,'PLAN_B', 'Managed healthcare',random_state()),
    (300,'PLAN_C', 'healthcare',random_state()),
    (400,'PLAN_D', 'healthcare',random_state()),
    (500,'PLAN_E', 'MEDICARE',random_state()),
    (600,'PLAN_F', 'UNSPECIFIED',random_state()),
    (700,'PLAN_G', 'MEDICARE SUPPLEMENT',random_state()),
    (800,'PLAN_H', 'COMMERCIAL',random_state()),
    (900,'PLAN_I', 'healthcare',random_state()),
    (1000,'PLAN_J', 'healthcare',random_state()),
    (1100,'PLAN_K', 'MEDICARE',random_state()),
    (1200,'PLAN_L', 'MEDICARE',random_state()),
    (1300,'PLAN_M', 'COMMERCIAL',random_state()),
    (1400,'PLAN_N', 'healthcare',random_state())
]
health_plans_pdf = pd.DataFrame(plans, columns=['plan_id', 'plan_name','payer_type','plan_state'])
# display(health_plans_pdf)

# Membership
rows = []

for _ in range(1000):   # generate 1000 members
    member_id = f"MBR{random.randint(100000, 130000)}"
    name,dob,gender,age = random.choice(list(people_df.itertuples(index=False, name=None)))
    rows.append((member_id, name,dob,gender,age))
    

membership_pdf = pd.DataFrame(rows, columns=["member_id", "member_name", "member_dob", "member_gender","member_age"])

# display(membership_pdf)

pharmacy_id = [f'PHARM_{i:04d}' for i in range(1000, 1060)]

pharmacy_names = ["CVS",
                  "Walgreens",
                  "Krogers",
                  "Target",
                  "Rite Aid",
                  "Wal-Mart",
                  "Save On"
                  ]

pharmacy_names_df = [random.choice(pharmacy_names) for _ in pharmacy_id]
pharmacy_state_df = [random_state() for _ in pharmacy_id]

pharmacies_pdf = pd.DataFrame({
    "pharmacy_id": pharmacy_id,
    "pharmacy_name": pharmacy_names_df,
    "pharmacy_state": pharmacy_state_df
})
specialty = [
     'Cardiologist',
    'Psychiatrist',
     'Psychologist',       
    'Therapist',
     'Dermatologist',
     'Endocrinologist',
     'Gastroenterologist',
     'Geriatrician',
     'Hematologist',
     'Infectious Disease Specialist',
     'Nephrologist',
     'Neurologist',
     'Obstetrician',
     'Oncologist',
     'Ophthalmologist',
     'Orthopedic Surgeon',
     'Pain Management Specialist',
     'Pulmonologist',
     'Rheumatologist',     
     'Allergist',
     'Anesthesiologist',
     'Bariatric Surgeon',
     'Psychiatric Nurse',
     'Psychiatric Social Worker',       
     'Speech-Language Pathologist',
     'Eumunologist',
     'Nurse Practitioner'    
]

taxonomies = [
    '207Q00000X',
    '207R00000X',
    '207RC0000X',
    '207X00000X',
    '207RG0100X',
    '207RX0202X',
    '207P00000X',
    '208000000X',
    '208100000X',
    '208200000X',
    '208300000X',
    '208400000X',
    '208500000X',
    '208600000X',
    '208700000X',
    '208800000X',
]

provider_ids = [f'PROVIDER_{i:04d}' for i in range(1000, 2000)]
npi_ids = [f'{i:04d}' for i in range(1000, 2000)]
taxonomies_types = [random.choice(taxonomies) for _ in provider_ids]
provider_specialty = [random.choice(specialty) for _ in provider_ids]
provider_state_df = [random_state() for _ in provider_ids]

providers_pdf = pd.DataFrame({
    "provider_id": provider_ids,
    "npi": npi_ids,
    "taxonomies": taxonomies_types,
    "provider_specialty": provider_specialty,
    "provider_state":provider_state_df
})

# COMMAND ----------

# Generate synthetic medical claims
start_date = datetime(2024, 1, 1)
end_date   = datetime(2025, 12, 31)

rows = []

for i in range(RECORD_COUNT):

    claim_id = f'CLM{1000000+i}'
    member_id, name,dob,gender,age = random.choice(list(membership_pdf.itertuples(index=False, name=None)))

    plan_id, plan_name,payer_type, plan_state = random.choice(list(health_plans_pdf.itertuples(index=False, name=None)))

    provider_id, npi, provider_specialty,taxonomies_id,provider_state = random.choice(list(providers_pdf.itertuples(index=False, name=None)))

    pharmacy_id, pharmacy_name, pharmacy_state = random.choice(list(pharmacies_pdf.itertuples(index=False, name=None)))    
    
    diagnosis_code = random.choice(icd_codes)
    procedure_code = random.choice(hcpcs_codes)
    place_of_service_std_id = int(random.choice(pos_codes))
    revenue_code = random.choice(rev_codes)

    line_charge = round(random.uniform(50, 5000), 2)
    line_allowed = round(line_charge * random.uniform(0.55, 0.95), 2)

    admission_date = pd.to_datetime(str(random_date(start_date, end_date).date()))
    discharge_date =  pd.to_datetime(admission_date) + pd.Timedelta(days=random.randint(1, 45)) 
        
    rows.append({
        'line_id': random.randint(1,10),
        'claim_id': claim_id,
        'claim_type': random.choice(['IP','OP','PROF']), # IP = inpatient facility, OP = outpatient facility, PROF = professional claim (physician billing)
        'admission_date': admission_date,
        'discharge_date': discharge_date,
        'diagnosis_code': diagnosis_code,
        'procedure_code': procedure_code,
        'patient_gender': random_gender(),
        'patient_year_of_birth': random.randint(1940, 2015),
        'patient_zip3': random_zip3(),
        'patient_state': random_state(),
        'inst_admit_type_std_id': random.randint(1,5),
        'inst_admit_source_std_id': random.randint(1,5),
        'inst_discharge_status_std_id': random.randint(1,5),
        'inst_type_of_bill_std_id': random.randint(110, 899),
        'inst_drg_std_id': random.randint(1, 999),
        'place_of_service_std_id': place_of_service_std_id,
        'service_line_number': random.randint(1,5),
        'diagnosis_code_qual': 'ICD10',
        'admit_diagnosis_ind': random.choice([0,1]),
        'procedure_code_qual': 'HCPCS',
        'procedure_units_billed': random.randint(1,4),
        'procedure_modifier_1': None,
        'procedure_modifier_2': None,
        'procedure_modifier_3': None,
        'procedure_modifier_4': None,
        'revenue_code': revenue_code,
        'line_charge': line_charge,
        'line_allowed': line_allowed,
        'prov_rendering_npi': npi,
        'prov_billing_npi': npi,
        'prov_referring_npi': npi,
        'prov_rendering_std_taxonomy': taxonomies_id,
        'prov_billing_std_taxonomy': taxonomies_id,
        'prov_referring_std_taxonomy': taxonomies_id,
        'member_id': member_id,
        'member_name': name,
        'member_dob': dob,
        'member_gender': gender,
        "member_age": age,
        'plan_id': plan_id,
        'plan_name': plan_name,
        'payer_type': payer_type,
        'plan_state': plan_state,
        'provider_id': provider_id,
        'provider_state': provider_state,
        'npi': npi,
        "provider_specialty": provider_specialty,        
        'pharmacy_id': pharmacy_id,
        'pharmacy_name': pharmacy_name,
        'pharmacy_state': pharmacy_state
    })   

df_out = pd.DataFrame(rows).drop_duplicates(['plan_id','member_id','claim_id','service_line_number','admission_date'])
df_out.to_csv(OUTPUT_FILE, index=False)

print('File generated:', OUTPUT_FILE)
print('Total records:', len(df_out))


health_plans_raw_OUTPUT_FILE = f"{dbutils.widgets.get('reference_volume_path')}health_plans_raw.csv"
health_plans_pdf.to_csv(health_plans_raw_OUTPUT_FILE, index=False)

members_raw_OUTPUT_FILE = f"{dbutils.widgets.get('reference_volume_path')}members_raw.csv"
membership_pdf.to_csv(members_raw_OUTPUT_FILE, index=False)

providers_raw_OUTPUT_FILE = f"{dbutils.widgets.get('reference_volume_path')}providers_raw.csv"
providers_pdf.to_csv(providers_raw_OUTPUT_FILE, index=False)

pharmacies_raw_OUTPUT_FILE = f"{dbutils.widgets.get('reference_volume_path')}pharmacies_raw.csv"
pharmacies_pdf.to_csv(pharmacies_raw_OUTPUT_FILE, index=False)

# COMMAND ----------

from pyspark.sql import functions as F
display(spark.createDataFrame(df_out).groupBy("claim_id").count().filter(F.col("count") > 1).display())