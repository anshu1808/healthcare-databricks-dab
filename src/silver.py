import dlt

from pyspark.sql.functions import *

@dlt.table(
    name="silver_members"
)
@dlt.expect("valid_member_id", "member_id IS NOT NULL")
def silver_members():

    df = dlt.read("bronze_members")

    return (
        df
        .filter(col("member_id").isNotNull())
        .dropDuplicates(["member_id"])
        .withColumn(
            "member_name",
            upper(trim(col("member_name")))
        )
        .withColumn(
            "city",
            upper(trim(col("city")))
        )
        .withColumn(
            "state",
            upper(trim(col("state")))
        )
        .withColumn(
            "join_date",
            to_date(col("join_date"), "yyyy-MM-dd")
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

@dlt.table(
    name="silver_claims_combined"
)

@dlt.expect("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect("valid_amount", "claim_amount > 0")
#@dlt.expect_or_drop("valid_member", "member_id IS NOT NULL")
#@dlt.expect_or_fail("valid_date", "claim_date <= current_date()")
def silver_claims_combined():

    df = dlt.read("bronze_claims_v2")

    return (
        df
        .filter(col("claim_id").isNotNull())
        .filter(col("member_id").isNotNull())
        .filter(col("claim_amount") > 0)
        .dropDuplicates(["claim_id"])
        .withColumn(
            "claim_status",
            upper(trim(col("claim_status")))
        )
        .withColumn(
            "claim_date",
            to_date(col("claim_date"), "yyyy-MM-dd")
        )
        .filter(col("claim_date").isNotNull())
        .withColumn(
            "claim_year",
            year(col("claim_date"))
        )
        .withColumn(
            "claim_month",
            month(col("claim_date"))
        )
        .withColumn(
            "created_ts",
            current_timestamp()
        )
        .withColumn(
            "updated_ts",
            current_timestamp()
        )
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    col("claim_id"),
                    col("claim_amount"),
                    col("claim_status")
        ),
        256
    )
)
    )

@dlt.table(
    name="silver_providers"
)
@dlt.expect(
    "valid_provider_id",
    "provider_id IS NOT NULL"
)
def silver_providers():

    df = dlt.read("bronze_providers")

    return (
        df
        .filter(col("provider_id").isNotNull())
        .dropDuplicates(["provider_id"])
        .withColumn(
            "provider_name",
            upper(trim(col("provider_name")))
        )
        .withColumn(
            "specialty",
            upper(trim(col("specialty")))
        )
        .withColumn(
            "city",
            upper(trim(col("city")))
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

@dlt.table(
    name="silver_pharmacy"
)
@dlt.expect(
    "positive_rx_cost",
    "rx_cost > 0"
)
def silver_pharmacy():

    df = dlt.read("bronze_pharmacy")

    return (
        df
        .filter(col("rx_id").isNotNull())
        .filter(col("rx_cost") > 0)
        .dropDuplicates(["rx_id"])
        .withColumn(
            "drug_name",
            upper(trim(col("drug_name")))
        )
        .withColumn(
            "rx_date",
            to_date(col("rx_date"), "yyyy-MM-dd")
        )
        .filter(col("rx_date").isNotNull())
        .withColumn(
            "created_ts",
            current_timestamp()
        )
        .withColumn(
            "updated_ts",
            current_timestamp()
        )
    )

@dlt.table(
    name="silver_billing"
)
@dlt.expect(
    "valid_bill_id",
    "bill_id IS NOT NULL"
)
def silver_billing():

    df = dlt.read("bronze_billing")

    return (
        df
        .filter(col("bill_id").isNotNull())
        .filter(col("payment_amount") > 0)
        .dropDuplicates(["bill_id"])
        .withColumn(
            "payment_status",
            upper(trim(col("payment_status")))
        )
        .withColumn(
            "payment_date",
            to_date(col("payment_date"), "yyyy-MM-dd")
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

@dlt.table(
    name="silver_appointments"
)
def silver_appointments():

    df = dlt.read("bronze_appointments")

    return (
        df
        .filter(col("appointment_id").isNotNull())
        .dropDuplicates(["appointment_id"])
        .withColumn(
            "appointment_date",
            to_date(col("appointment_date"), "yyyy-MM-dd")
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

@dlt.table(
    name="silver_crm"
)
def silver_crm():

    df = dlt.read("bronze_crm")

    return (
        df
        .filter(col("ticket_id").isNotNull())
        .dropDuplicates(["ticket_id"])
        .withColumn(
            "ticket_status",
            upper(trim(col("ticket_status")))
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

@dlt.table(
    name="silver_fraud"
)
def silver_fraud():

    df = dlt.read("bronze_fraud")

    return (
        df
        .filter(col("fraud_id").isNotNull())
        .dropDuplicates(["fraud_id"])
        .withColumn(
            "created_ts",
            current_timestamp()
        )
        .withColumn(
            "updated_ts",
            current_timestamp()
        )
    )
    

    