from pyspark import pipelines as dp
from pyspark.sql.functions import *
import json
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name='finguard.silver.transactions',
    comment='cleaned and parsed transactions from bronze table')


@dp.expect_or_drop('invalid_transactions','transaction_id is not null')
@dp.expect_or_drop('invalid_customers',"customer_id is not null")
@dp.expect_or_drop('invalid_merchant',"merchant_id is not null")
@dp.expect_or_drop('invalid_card_number',"card_number is not null")
@dp.expect('invalid_amount',"amount > 0")

def silver_transactions() -> DataFrame:
    bronze_df=spark.readStream.table('finguard.bronze.transactions')

    schema=StructType([
        StructField('transaction_id',StringType()),
        StructField('customer_id',StringType()),
        StructField('card_number',StringType()),
        StructField('merchant_id',StringType()),
        StructField('merchant_name',StringType()),
        StructField('merchant_category',StringType()),
        StructField('amount',FloatType()),
        StructField('currency',StringType()),
        StructField('transaction_type',StringType()),
        StructField('payment_channel',StringType()),
        StructField('device_id',StringType()),
        StructField('city',StringType()),
        StructField('country',StringType()),
        StructField('transaction_timestamp',TimestampType()),
        StructField('is_international',BooleanType()),
        StructField('status',StringType())
    ])

    transformed_df=bronze_df.select(
        from_json(col('value'),schema).alias('data'),
        col('topic').alias('kafka_topic'),
        col('partition').alias('kafka_partition'),
        col('offset').alias('kafka_offset'),
        col('timestamp').alias('kafka_timestamp'),
        current_timestamp().alias('silver_ingestion_timestamp')
    ).select(
        col('data.*'),
        col('kafka_topic'),
        col('kafka_partition'),
        col('kafka_offset'),
        col('kafka_timestamp'),
        col('silver_ingestion_timestamp')

            )
    return transformed_df
        