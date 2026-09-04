import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="audit_pipeline_monitoring"
)
def audit_pipeline_monitoring():

    return (
        dlt.read("silver_claims_scd2")
        .agg(
            count("*").alias("record_count")
        )
        .withColumn(
            "pipeline_name",
            lit("healthcare_pipeline")
        )
        .withColumn(
            "load_timestamp",
            current_timestamp()
        )
    )

@dlt.table(
    name="audit_watermark_control"
)
def audit_watermark_control():

    return (
        dlt.read("silver_claims_scd2")
        .agg(
            max(col("ingestion_time")).alias("last_watermark"),
            count("*").alias("record_count")
        )
        .withColumn("table_name", lit("silver_claims_scd2"))
        .withColumn(
            "pipeline_name",
            lit("healthcare_pipeline")
        )
        .withColumn("run_ts",
            current_timestamp()
        )
    )
        
@dlt.table(
    name="audit_run_history"
)
def audit_run_history():

    return (
        dlt.read("silver_claims_combined")
        .agg(
            count("*").alias("record_count")
        )
        .withColumn(
            "pipeline_name",
            lit("healthcare_pipeline")
        )
        .withColumn(
            "status",
            lit("SUCCESS")
        )
        .withColumn(
            "run_timestamp",
            current_timestamp()
        )
    )

@dlt.table(
    name="audit_claims_reconciliation"
)
def audit_claims_reconciliation():

    bronze_count = (
        dlt.read("bronze_claims_v2")
        .agg(count("*").alias("bronze_count"))
    )

    silver_count = (
        dlt.read("silver_claims_combined")
        .agg(count("*").alias("silver_count"))
    )

    return (
        bronze_count
        .crossJoin(silver_count)
        .withColumn(
            "difference_br_si",
            col("bronze_count") - col("silver_count")
        )
        .withColumn(
            "difference_si_br",
            col("silver_count") - col("bronze_count")
        )
        .withColumn(
            "reconciliation_ts",
            current_timestamp()
        )
        .withColumn(
            "reconciliation_ts",
            current_timestamp()
        )
        .withColumn(
            "reconciliation_status",
            when(
                col("bronze_count") == col("silver_count"),
                "PASS"
            ).otherwise("FAIL")
        )
    )