from pyspark import pipelines as dp 
from pyspark.sql.functions import *
from pyspark.sql.dataframe import DataFrame

@dp.table(
    name='finguard.gold.customer_high_transaction_alert',
    comment='fraud transactions alert table '
)
def customers_fraud_trans_alert() -> DataFrame:
    transactions=spark.readStream.table('finguard.silver.transactions')
    customers=spark.read.table('finguard.silver.customers')

    joined_df=transactions.alias('t').join(customers.alias('c'),col('t.customer_id')==col('c.customer_id'),'left')

    high_valued_transactions=joined_df.filter(col('t.amount')>col('c.transaction_limit'))\
        .select(
            concat_ws('-',lit('ALERT'),col('t.transaction_id')).alias('alert_id'),
            lit('HIGH_VALUE_TRANSACTION').alias('alert_type'),
            current_timestamp().alias('alert_timestamp'),
            't.transaction_id',
            't.customer_id',
            col('c.email').alias('customer_email_address'),
            concat_ws(' ',col('c.first_name'),col('c.last_name')).alias('customer_name'),
            col('t.amount').alias('transaction_amount'),
            col('c.transaction_limit').alias('transaction_limit'),
            't.currency',
            't.merchant_name',
            't.merchant_category',
            't.transaction_type',
            't.payment_channel',
            't.country',
            't.city',
            't.is_international',
            't.transaction_timestamp',
            't.status'
        )
    return high_valued_transactions


