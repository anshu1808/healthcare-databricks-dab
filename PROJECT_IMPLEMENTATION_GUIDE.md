# Healthcare Lakehouse on Databricks
## DLT + DAB Implementation Guide

### Project Overview

Built an end-to-end Healthcare Lakehouse using:

- Auto Loader
- Delta Live Tables (DLT)
- Bronze / Silver / Gold Architecture
- CDC (SCD Type 2)
- Audit Framework
- Databricks Asset Bundles (DAB)
- GitHub Integration
- DEV / QA / PROD Deployments

---

## Project Structure

```text
my_project
│
├── databricks.yml
│
├── resources
│   ├── jobs.yml
│   └── pipeline.yml
│
├── src
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   ├── audit.py
│   └── transformations
│       ├── 03_silver_claims_cdc.py
│       └── 04_advanced_claims_pipeline.py
│
└── tests

Healthcare Lakehouse on Databricks
DLT + DAB Implementation Guide
Project Overview
Built an end-to-end Healthcare Lakehouse using:

Auto Loader
Delta Live Tables (DLT)
Bronze / Silver / Gold Architecture
CDC (SCD Type 2)
Audit Framework
Databricks Asset Bundles (DAB)
GitHub Integration
DEV / QA / PROD Deployments
Project Structure
my_project
│
├── databricks.yml
│
├── resources
│   ├── jobs.yml
│   └── pipeline.yml
│
├── src
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│   ├── audit.py
│   └── transformations
│       ├── 03_silver_claims_cdc.py
│       └── 04_advanced_claims_pipeline.py
│
└── tests

=========================================================
HEALTHCARE LAKEHOUSE PROJECT
DATABRICKS DLT + DAB IMPLEMENTATION DOCUMENTATION
=========================================================

AUTHOR:
Anshu Kumar

PROJECT TYPE:
End-to-End Healthcare Lakehouse using Databricks

TECHNOLOGIES:
- Databricks
- Delta Live Tables (DLT)
- Databricks Asset Bundles (DAB)
- Unity Catalog
- Auto Loader
- Delta Lake
- GitHub
- Python
- PySpark

=========================================================
1. PROJECT OBJECTIVE
=========================================================

Build an Enterprise Healthcare Lakehouse using:

- Auto Loader
- Bronze / Silver / Gold Architecture
- CDC (SCD Type 2)
- Data Quality Expectations
- Audit Framework
- Reconciliation Framework
- Monitoring Framework
- Databricks Asset Bundle Deployment
- Dev / QA / Prod Environments
- Git Integration

=========================================================
2. DATABRICKS CLI INSTALLATION
=========================================================

Install Databricks CLI:

pip install databricks

Verify installation:

databricks -v

=========================================================
3. AUTHENTICATION
=========================================================

Login:

databricks auth login --host https://dbc-23c305ec-be26.cloud.databricks.com

Verify profiles:

databricks auth profiles

Profiles Found:

DEFAULT
dbc-23c305ec-be26

Important:

DEFAULT profile lacked access-management permission.

Final deployments used:

dbc-23c305ec-be26

=========================================================
4. CREATE ASSET BUNDLE
=========================================================

Create bundle:

databricks bundle init

Project Name:

my_project

Project Structure Created:

my_project
|
|-- databricks.yml
|-- resources
|-- src
|-- tests
|-- README.md

=========================================================
5. FINAL PROJECT STRUCTURE
=========================================================

my_project
|
|-- databricks.yml
|
|-- resources
|   |-- jobs.yml
|   |-- pipeline.yml
|
|-- src
|   |-- bronze.py
|   |-- silver.py
|   |-- gold.py
|   |-- audit.py
|   |
|   |-- transformations
|       |-- 03_silver_claims_cdc.py
|       |-- 04_advanced_claims_pipeline.py
|
|-- tests
|
|-- README.md

=========================================================
6. SOURCE DATA
=========================================================

Volume Path:

/Volumes/main/healthcare_project_pipe/raw_data

Datasets:

members
claims
providers
pharmacy
billing
appointments
eligibility
crm
labs
fraud

=========================================================
7. BRONZE LAYER
=========================================================

File:

src/bronze.py

Technology:

Auto Loader

Implementation:

spark.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")

Metadata Columns Added:

ingestion_time

source_file

ingestion_date

Bronze Tables:

bronze_members
bronze_claims_v2
bronze_providers
bronze_pharmacy
bronze_billing
bronze_appointments
bronze_eligibility
bronze_crm
bronze_labs
bronze_fraud

=========================================================
8. SILVER LAYER
=========================================================

File:

src/silver.py

Purpose:

- Cleanse Data
- Standardize Data
- Data Quality Checks
- Deduplicate Records

Features:

Uppercase Conversion

trim()

Date Conversion

Record Hash

Audit Columns

created_ts

updated_ts

=========================================================
9. CDC IMPLEMENTATION
=========================================================

File:

src/transformations/03_silver_claims_cdc.py

Technology:

SCD Type 2

Implementation:

dlt.create_streaming_table(
    name="silver_claims_scd2"
)

dlt.apply_changes(
    target="silver_claims_scd2",
    source="claims_cdc_view",
    keys=["claim_id"],
    sequence_by=col("ingestion_time"),
    stored_as_scd_type=2
)

History Tracking:

claim_amount

claim_status

=========================================================
10. ADVANCED CLAIMS PIPELINE
=========================================================

File:

src/transformations/04_advanced_claims_pipeline.py

Features:

CDC

Data Quality

Rejected Records

Audit Tracking

Tables:

claims_cleaned_view

rejected_claims1

silver_claims_scd2

=========================================================
11. GOLD LAYER
=========================================================

File:

src/gold.py

Business Tables:

gold_member_claim_summary

gold_provider_performance

gold_monthly_claim_summary

gold_fraud_analytics

gold_executive_dashboard

Purpose:

Business Reporting

Executive Reporting

Analytics

KPIs

=========================================================
12. AUDIT LAYER
=========================================================

File:

src/audit.py

Tables:

audit_claims_reconciliation

audit_pipeline_monitoring

audit_watermark_control

audit_run_history

audit_data_quality_scorecard

=========================================================
13. RECONCILIATION FRAMEWORK
=========================================================

Table:

audit_claims_reconciliation

Columns:

bronze_count

silver_count

difference

reconciliation_status

reconciliation_timestamp

=========================================================
14. WATERMARK FRAMEWORK
=========================================================

Table:

audit_watermark_control

Purpose:

Track last processed timestamp

Track total processed records

Detect incremental loads

=========================================================
15. PIPELINE MONITORING
=========================================================

Table:

audit_pipeline_monitoring

Metrics:

record_count

pipeline_name

run_timestamp

=========================================================
16. RUN HISTORY
=========================================================

Table:

audit_run_history

Metrics:

pipeline_name

status

run_timestamp

=========================================================
17. DATA QUALITY SCORECARD
=========================================================

Table:

audit_data_quality_scorecard

Metrics:

table_name

rejected_records

quality_percentage

run_timestamp

=========================================================
18. DATABRICKS ASSET BUNDLE
=========================================================

File:

databricks.yml

Environments:

DEV

QA

PROD

Variables:

catalog

schema

pipeline_development

=========================================================
19. DEV ENVIRONMENT
=========================================================

Catalog:

main

Schema:

healthcare_project_pipe_dev

Development Mode:

true

Deployment:

databricks bundle deploy --target dev

=========================================================
20. QA ENVIRONMENT
=========================================================

Catalog:

main

Schema:

healthcare_project_pipe_qa

Development Mode:

false

Deployment:

databricks bundle deploy --target qa

=========================================================
21. PROD ENVIRONMENT
=========================================================

Catalog:

main

Schema:

healthcare_project_pipe_prod

Development Mode:

false

Deployment:

databricks bundle deploy --target prod

=========================================================
22. PIPELINE RESOURCE
=========================================================

File:

resources/pipeline.yml

Key Settings:

serverless: true

continuous: false

development: ${var.pipeline_development}

Pipeline Names:

healthcare-pipeline_combined-dev

healthcare-pipeline_combined-qa

healthcare-pipeline_combined-prod

=========================================================
23. JOB RESOURCE
=========================================================

File:

resources/jobs.yml

Job Names:

healthcare-pipeline_combined-job-dev

healthcare-pipeline_combined-job-qa

healthcare-pipeline_combined-job-prod

=========================================================
24. DATABRICKS BUNDLE COMMANDS
=========================================================

Validate:

databricks bundle validate

Deploy DEV:

databricks bundle deploy --target dev

Deploy QA:

databricks bundle deploy --target qa

Deploy PROD:

databricks bundle deploy --target prod

Run Job:

databricks bundle run healthcare_pipeline_combined_job --target dev

Summary:

databricks bundle summary --target dev

=========================================================
25. GIT INTEGRATION
=========================================================

Initialize Repository:

git init

Add Files:

git add .

Commit:

git commit -m "Initial healthcare DLT project with Databricks Asset Bundle"

Add Remote:

git remote add origin https://github.com/anshu1808/healthcare-databricks-dab.git

Verify:

git remote -v

Rename Branch:

git branch -M main

Push:

git push -u origin main

=========================================================
26. ISSUES FACED AND RESOLUTIONS
=========================================================

Issue:
NO_TABLES_IN_PIPELINE

Cause:
DLT source files not available correctly.

Fix:
Copied DLT files into src folder and configured libraries properly.

---------------------------------------------------------

Issue:
Serverless Compute Required

Error:
You must use serverless compute.

Fix:

serverless: true

---------------------------------------------------------

Issue:
Duplicate Pipeline Name

Error:
RESOURCE_CONFLICT

Fix:

healthcare-pipeline_combined-${bundle.target}

---------------------------------------------------------

Issue:
silver_claims_scd2 Not Found

Cause:
Schema mismatch or missing dependency.

Fix:
Corrected references in audit layer and CDC layer.

---------------------------------------------------------

Issue:
Production Deployment Permission Error

Error:
access-management scope missing.

Fix:
Used Databricks profile:

dbc-23c305ec-be26

instead of

DEFAULT

=========================================================
27. FINAL ACHIEVEMENTS
=========================================================

Successfully Implemented:

✓ Auto Loader

✓ Bronze Layer

✓ Silver Layer

✓ Gold Layer

✓ Audit Layer

✓ Data Quality Expectations

✓ Rejected Records

✓ CDC

✓ SCD Type 2

✓ Reconciliation

✓ Watermark Control

✓ Monitoring

✓ Run History

✓ Data Quality Scorecard

✓ Delta Live Tables

✓ Databricks Asset Bundle

✓ GitHub Integration

✓ Serverless DLT

✓ DEV Deployment

✓ QA Deployment

✓ PROD Deployment

✓ Multi-Environment Architecture

=========================================================
28. RESUME DESCRIPTION
=========================================================

Built an end-to-end Healthcare Lakehouse solution on Databricks using Auto Loader, Delta Live Tables (DLT), SCD Type 2 CDC, Medallion Architecture (Bronze/Silver/Gold), Data Quality Expectations, Audit & Reconciliation Framework, Serverless Pipelines, GitHub, and Databricks Asset Bundles with DEV, QA, and PROD deployments.