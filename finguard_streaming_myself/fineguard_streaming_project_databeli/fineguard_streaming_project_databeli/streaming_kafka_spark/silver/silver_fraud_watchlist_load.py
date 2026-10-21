from pyspark import pipelines as dp 
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame

@dp.table(
    name='finguard.silver.fraud_watchlist',
    comment='cleaned fraud_list data'
)
def cleaned_fraud_watchlist() -> DataFrame:

    bronze_df=spark.readStream.table('finguard.bronze.fraud_watchlist')

    cleaned_df=bronze_df.select(
    upper(col("watchlist_id")).alias("watchlist_id"),
    col("watch_type"),
    upper(col("entity_id")).alias("entity_id"),
    upper(col("risk_level")).alias("risk_level"),
    upper(col("action")).alias("action"),
    col("reason_code"),
    col("reason_description"),
    col("status"),
    to_timestamp(col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
    col("reported_by"),
    col("reported_source"),
    col("country"),
    col("city"),
    col("source_file"),
    col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
    current_timestamp().alias("silver_ingestion_timestamp")
    )

    return cleaned_df