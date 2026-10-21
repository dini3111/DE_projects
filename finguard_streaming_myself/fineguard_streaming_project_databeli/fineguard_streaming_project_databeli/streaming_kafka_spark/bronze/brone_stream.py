from pyspark import pipelines as dp
from pyspark.sql.functions import *
import json
from pyspark.sql.dataframe import DataFrame


@dp.table(name='finguard.bronze.transactions',comment='raw stream from kafka source bronze table')
def bronze_transactions() -> DataFrame:
    kafka_connection_json=dbutils.secrets.get(scope='finguard-scope',key='kafka_connection_details')
    kafka_config=json.loads(kafka_connection_json)
    bootstrap_servers=kafka_config['bootstrap_servers']
    api_key=kafka_config['api_key']
    secret=kafka_config['api_secret']
    topic=kafka_config['topic']

    jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{secret}";'

    stream_df=(spark.readStream.format('kafka')
          .option('kafka.bootstrap.servers',bootstrap_servers)
          .option('subscribe',topic)
          .option('kafka.security.protocol','SASL_SSL')
          .option('kafka.sasl.jaas.config',jaas_config)
          .option('kafka.sasl.mechanism','PLAIN')
          .option('kafka.ssl.endpoint.identification.algorithm','https')
          .option('startingOffsets','earliest')
          .load())
    
    parsed_stream_df=(stream_df.select(col('key').cast('string'),
                                   col('value').cast('string'),
                                col('topic'),
                                col('partition'),
                                col('offset'),
                                col('timestamp'),
                                col('timestampType'),
                                current_timestamp().alias('bronze_ingestion_timestamp')))
    return parsed_stream_df