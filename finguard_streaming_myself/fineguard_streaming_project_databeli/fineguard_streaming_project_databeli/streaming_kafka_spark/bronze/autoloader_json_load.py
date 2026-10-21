from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame

@dp.table(
    name='finguard.bronze.fraud_watchlist',
    comment='raw json files brionze ingestion through autolaoder'
    )
def fraud_watchlist_autoloader() -> DataFrame:

    streaming_df=(spark.readStream.format('cloudFiles')
               .option('cloudFiles.format','json')
               .option('cloudFiles.inferColumnTypes','true')
               .load('/Volumes/finguard/source/finguard_watchlist/data/')
    )

    parsed_streaming_df=streaming_df.select(
    col("watchlist_id"),
    col("watch_type"),
    col("entity_id"),
    col("risk_level"),
    col("action"),
    col("reason_code"),
    col("reason_description"),
    col("status"),
    col("effective_from"),
    col("reported_by"),
    col("reported_source"),
    col("country"),
    col("city"),
    col("_rescued_data"),
    col("_metadata.file_path").alias("source_file"),
    current_timestamp().alias("ingestion_timestamp")
    )

    return parsed_streaming_df