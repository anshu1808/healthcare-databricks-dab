import dlt

from pyspark.sql.functions import *

@dlt.view(
    name="claims_cleaned_view"
)
@dlt.expect(
    "valid_claim_amount",
    "claim_amount > 0"
)
def claims_cleaned_view():

    df = dlt.read_stream("bronze_claims_v2")

    return (
        df
        # Remove null claim ids
        .filter(col("claim_id").isNotNull())
        # Remove null member ids
        .filter(col("member_id").isNotNull())
        # Apply all transformations in one operation
        .withColumns({
            "claim_status": upper(trim(col("claim_status"))),
            "claim_amount": col("claim_amount").cast("double"),
            "claim_date": to_date(col("claim_date")),
            "processing_time": current_timestamp(),
            "claim_year": year(col("claim_date")),
            "claim_month": month(col("claim_date")),
            "record_hash": sha2(
                concat_ws(
                    "||",
                    col("claim_id"),
                    col("claim_amount"),
                    col("claim_status")
                ),
                256
            ),
            "created_ts": current_timestamp(),
            "updated_ts": current_timestamp(),
            "source_file": col("source_file"),
            "ingestion_time": col("ingestion_time")
        })
    )

@dlt.table(
    name="rejected_claims1"
)
@dlt.expect_or_drop(
    "valid_claim_id",
    "claim_id IS NOT NULL"
)
@dlt.expect_or_drop(
    "valid_member_id",
    "member_id IS NOT NULL"
)
@dlt.expect_or_drop(
    "valid_claim_amount",
    "claim_amount > 0"
)
@dlt.expect_or_drop(
    "valid_claim_date",
    "claim_date IS NOT NULL"
)
def rejected_claims():

    df = dlt.read_stream("bronze_claims_v2")

    return (
        df
        .filter(
            (col("claim_amount") <= 0) |
            (col("member_id").isNull()) |
            (col("claim_date") == "-")
        )
        .withColumns({
            "rejection_reason": when(col("claim_amount") <= 0, "INVALID_CLAIM_AMOUNT")
                .when(col("member_id").isNull(), "NULL_MEMBER_ID")
                .otherwise("INVALID_CLAIM_DATE"),
            "rejected_time": current_timestamp(),
            "record_hash": sha2(
                concat_ws(
                    "||",
                    col("claim_id"),
                    col("claim_amount"),
                    col("claim_status")
                ),
                256
            ),
            "created_ts": current_timestamp(),
            "updated_ts": current_timestamp(),
            "source_file": col("source_file"),
            "ingestion_time": col("ingestion_time")
        })
    )

dlt.create_streaming_table(
    name="silver_claims_scd2_advanced"
)

dlt.apply_changes(
    target = "silver_claims_scd2_advanced",
    source = "claims_cleaned_view",
    keys = ["claim_id"],
    sequence_by = col("ingestion_time"),
    stored_as_scd_type = 2,
    track_history_column_list = [
        "claim_amount", "claim_status"
    ]
)
