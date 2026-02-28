# Enterprise | Claims Medallion Pipeline

## Overview

End-to-end | claims data platform built using Medallion Architecture (Bronze → Silver → Gold) in Databricks.

This project generates realistic synthetic Medicaid medical claims using real-world ICD-10, CPT/HCPCS, Revenue Code, and Place of Service reference tables and produces a dimensional star schema ready for Power BI analytics.

---

## Architecture

### Bronze Layer
- Raw ingestion from CSV
- Schema enforcement
- Column normalization
- Run-level logging

### Silver Layer
- Data typing
- Null handling and imputations
- Business rule transformations
- Standardization

### Gold Layer
- Star schema modeling
- Surrogate key generation
- Fact grain validation
- Data Quality framework
- Duplicate grain detection

---

## Fact Table Grain

Fact table is built at:


Duplicate grain detection is implemented to prevent improper fact inflation.

---

## Data Quality Controls

- Duplicate grain validation
- Row count validation
- Null checks
- Run-level audit logging

---

## Workflow Orchestration

Databricks job executes:

1. Catalog & Schema Setup
2. Bronze Layer
3. Silver Layer
4. Gold Layer

---

## Technologies Used

- Databricks
- PySpark
- Delta Lake
- Medallion Architecture
- Dimensional Modeling
- Healthcare Claims Domain
- Power BI (downstream analytics)

---

## Sample Dataset

A small synthetic dataset (~3,000 claims) is included for reproducibility.

In production environments, raw data would reside in cloud storage (e.g., Databricks Volumes, ADLS, S3), not within the repository.

---

## What This Demonstrates

- Production-style Medallion architecture
- Fact table grain enforcement
- Healthcare domain modeling
- Synthetic data engineering
- Data quality framework implementation
- Star schema dimensional modeling

---

## Author

Yikum Shiferaw  
Senior Data Analyst / Data Engineering Focus
