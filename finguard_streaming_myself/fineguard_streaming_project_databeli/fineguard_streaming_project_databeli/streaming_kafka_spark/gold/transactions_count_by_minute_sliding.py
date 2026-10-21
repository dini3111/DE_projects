from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *

@dp.table(name='finguard.gold.transactions_by_minute_sliding',
          comment='Count transactions per minute')
def trnsactions_per_minute() -> DataFrame:
    transactions=spark.readStream.table('finguard.silver.transactions')

    transactions_with_watermark=transactions.withWatermark('transaction_timestamp','5 minutes')

    transactions_count_df=transactions_with_watermark.groupBy(
        window('transaction_timestamp','5 minutes','1 minute')).agg(
            count('*').alias('transaction_count')).select(
                col('window.start').alias('window_start'),
                col('window.end').alias('windo_end'),
                col('transaction_count')

        )

    return transactions_count_df
    
