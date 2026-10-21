from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame

@dp.table(
    name='finguard.silver.customers',
    comment='parse and cleaned customers silver table'
)
@dp.expect_or_drop('invalid_customers','customer_id is not null')
def silver_customers() -> DataFrame:
    bronze_cust = spark.readStream.table('finguard.bronze.customers')

    transformed_df = bronze_cust.select(
        col('customer_id'),
        col('first_name'),
        col('last_name'),
        col('gender'),
        col('age'),
        col('city'),
        col('state'),
        col('country'),
        col('annual_income'),
        col('customer_segment'),
        to_date(col('account_open_date'), 'yyyy-MM-dd').alias('account_open_date'),
        col('risk_score'),
        col('preferred_spending_min'),
        col('preferred_spending_max'),
        col('preferred_city'),
        col('preferred_country'),
        col('trusted_device_id'),
        col('card_number'),
        col('card_type'),
        col('email'),
        col('transaction_limit'),
        col('update_timestamp'),
        current_timestamp().alias('silver_ingestion_timestamp')
    )

    return transformed_df