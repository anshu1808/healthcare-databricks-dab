import dlt

from pyspark.sql.functions import *

@dlt.table(
    name="gold_member_claim_summary"
)
def gold_member_claim_summary():

    claims_df = dlt.read("silver_claims_combined")

    members_df = dlt.read("silver_members")

    joined_df = claims_df.join(
        members_df,
        "member_id",
        "left"
    )

    return (
        joined_df.groupBy(
            "member_id",
            "member_name",
            "state"
        ).agg(
            count("claim_id").alias("total_claims"),
            sum("claim_amount").alias("total_claim_amount"),
            avg("claim_amount").alias("avg_claim_amount")
        )
        .withColumn(
            "gold_load_time",
            current_timestamp()
        )
    )

@dlt.table(
    name="gold_provider_performance"
)
def gold_provider_performance():

    claims_df = dlt.read("silver_claims_combined")

    providers_df = dlt.read("silver_providers")

    joined_df = claims_df.join(
        providers_df,
        "provider_id",
        "left"
    )

    return (
        joined_df.groupBy(
            "provider_id",
            "provider_name",
            "specialty"
        ).agg(
            count("claim_id").alias("total_claims"),
            sum("claim_amount").alias("total_claim_amount"),
            avg("claim_amount").alias("avg_claim_amount")
        )
    )

@dlt.table(
    name="gold_monthly_claim_summary"
)
def gold_monthly_claim_summary():

    claims_df = dlt.read("silver_claims_combined")

    return (
        claims_df.groupBy(
            "claim_year",
            "claim_month"
        ).agg(
            count("claim_id").alias("total_claims"),
            sum("claim_amount").alias("total_claim_amount")
        )
    )

@dlt.table(
    name="gold_fraud_analytics"
)
def gold_fraud_analytics():

    fraud_df = dlt.read("silver_fraud")

    claims_df = dlt.read("silver_claims_combined")

    joined_df = fraud_df.join(
        claims_df,
        "claim_id",
        "left"
    )

    return (
        joined_df.groupBy(
            "fraud_flag"
        ).agg(
            count("claim_id").alias("fraud_claim_count"),
            sum("claim_amount").alias("fraud_claim_amount")
        )
    )

@dlt.table(
    name="gold_executive_dashboard"
)
def gold_executive_dashboard():

    claims_df = dlt.read("silver_claims_combined")

    members_df = dlt.read("silver_members")

    providers_df = dlt.read("silver_providers")

    return (
        claims_df
        .join(members_df, "member_id", "left")
        .join(providers_df, "provider_id", "left")
        .groupBy(
            "state",
            "specialty"
        )
        .agg(
            countDistinct("member_id").alias("active_members"),
            count("claim_id").alias("total_claims"),
            sum("claim_amount").alias("total_claim_amount"),
            avg("claim_amount").alias("avg_claim_amount")
        )
    )

@dlt.table(
    name="rejected_claims"
)
def rejected_claims():

    df = dlt.read("bronze_claims_v2")

    return (
        df.filter(
            (col("claim_id").isNull()) |
            (col("claim_amount") <= 0)
        )
    )

@dlt.table(
    name="rejected_members"
)
def rejected_members():

    df = dlt.read("bronze_members")

    return (
        df.filter(
            (col("member_id").isNull()) |
            (col("member_id") == '')
        )
    )

@dlt.table(
    name="rejected_providers"
)
def rejected_providers():

    df = dlt.read("bronze_providers")

    return (
        df.filter(
            (col("provider_id").isNull()) |
            (col("provider_id") == 0)
        )
    )



