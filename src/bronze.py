import dlt

from pyspark.sql.functions import *

base_path = "/Volumes/main/healthcare_project_pipe/raw_data"

members_path = f"{base_path}/members/"
claims_path = f"{base_path}/claims/"
providers_path = f"{base_path}/providers/"
pharmacy_path = f"{base_path}/pharmacy/"
billing_path = f"{base_path}/billing/"
appointments_path = f"{base_path}/appointments/"
eligibility_path = f"{base_path}/eligibility/"
crm_path = f"{base_path}/crm/"
labs_path = f"{base_path}/labs/"
fraud_path = f"{base_path}/fraud/"


@dlt.table(
    name="bronze_members",
    comment="Raw members data"
)
def bronze_members():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(members_path)
        .withColumn("ingestion_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )


@dlt.table(
    name="bronze_claims_v2"
)
def bronze_claims_v2():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(claims_path)
        .withColumn("ingestion_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )


@dlt.table(
    name="bronze_providers"
)
def bronze_providers():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(providers_path)
        .withColumn("ingestion_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )


@dlt.table(
    name="bronze_pharmacy"
)
def bronze_pharmacy():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(pharmacy_path)
        .withColumn("ingestion_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )

@dlt.table(
    name="bronze_billing"
)
def bronze_billing():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(billing_path)
        .withColumn("ingestion_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )

@dlt.table(
    name="bronze_appointments"
)
def bronze_appointments():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(appointments_path)
        .withColumn("ingestion_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )

@dlt.table(
    name="bronze_eligibility"
)
def bronze_eligibility():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(eligibility_path)
        .withColumn("ingestion_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )

@dlt.table(
    name="bronze_crm"
)
def bronze_crm():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(crm_path)
        .withColumn("ingestion_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )

@dlt.table(
    name="bronze_labs"
)
def bronze_labs():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(labs_path)
        .withColumn("ingestion_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )

@dlt.table(
    name="bronze_fraud"
)
def bronze_fraud():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(fraud_path)
        .withColumn("ingestion_time", col("_metadata.file_modification_time"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("ingestion_date", to_date(col("ingestion_time")))
    )

