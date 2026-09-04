import dlt

from pyspark.sql.functions import *

@dlt.view(
    name = "claims_cdc_view"
)

@dlt.expect("valid_claim_id","claim_id IS NOT NULL")
@dlt.expect("valid_member_id","member_id IS NOT NULL")
@dlt.expect("positive_claim_amount","claim_amount > 0")
@dlt.expect("valid_claim_status","claim_status IN ('APPROVED', 'DENIED', 'PENDING')")
@dlt.expect("valid_claim_date","claim_date IS NOT NULL")

def claims_cdc_view():
  df = dlt.read_stream("bronze_claims_v2")  
  return (
        df
        #.filter(col("claim_id").isNotNull())
        #.filter(col("member_id").isNotNull())
        #.filter(col("claim_amount") > 0)
        .withColumn(
            "claim_amount",
            col("claim_amount").cast("double")
        )
        .withColumn(
            "claim_status",
            upper(trim(col("claim_status")))
        )
        #.filter(col("claim_date").isNotNull())
        .withColumn(
            "claim_date",
            to_date(col("claim_date"))
        )
        .withColumn(
            "created_ts",
            current_timestamp()
        )
        .withColumn(
            "updated_ts",
            current_timestamp()
        )
    )
  
dlt.create_streaming_table(
    name="silver_claims_scd2"
)

dlt.apply_changes(
    target = "silver_claims_scd2",
    source = "claims_cdc_view",
    keys = ["claim_id"],
    sequence_by = col("ingestion_time"),
    stored_as_scd_type = 2,
    track_history_column_list=[
        "claim_status","claim_amount","claim_date"
    ]
)
