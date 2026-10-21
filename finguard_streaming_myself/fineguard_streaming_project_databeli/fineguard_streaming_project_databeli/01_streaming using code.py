# Databricks notebook source
# bootstrap_servers='pkc-7prvp.centralindia.azure.confluent.cloud:9092'

# topic='credit_card_transactions'

# COMMAND ----------

import json
kafka_connection_json=dbutils.secrets.get(scope='finguard-scope',key='kafka_connection_details')

kafka_config=json.loads(kafka_connection_json)
bootstrap_servers=kafka_config['bootstrap_servers']
api_key=kafka_config['api_key']
secret=kafka_config['api_secret']
topic=kafka_config['topic']

# COMMAND ----------

jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{secret}";'

# COMMAND ----------

batch_df=(spark.read.format('kafka')
          .option('kafka.bootstrap.servers',bootstrap_servers)
          .option('subscribe',topic)
          .option('kafka.security.protocol','SASL_SSL')
          .option('kafka.sasl.jaas.config',jaas_config)
          .option('kafka.sasl.mechanism','PLAIN')
          .option('kafka.ssl.endpoint.identification.algorithm','https')
          .option('startingOffsets','earliest')
          .load())

# COMMAND ----------

batch_df.count()

# COMMAND ----------

display(batch_df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

parsed_batch_df=(batch_df.select(col('key').cast('string')
                                 ,col('value').cast('string'),
                                col('topic'),
                                col('partition'),
                                col('offset'),
                                col('timestamp'),
                                col('timestampType'))
                 )

# COMMAND ----------

display(parsed_batch_df)

# COMMAND ----------

parsed_batch_df.write.mode('overwrite').saveAsTable('finguard.bronze.credit_card_transactions_batch')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.bronze.credit_card_transactions_batch

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming**

# COMMAND ----------

stream_df=(spark.readStream.format('kafka')
          .option('kafka.bootstrap.servers',bootstrap_servers)
          .option('subscribe',topic)
          .option('kafka.security.protocol','SASL_SSL')
          .option('kafka.sasl.jaas.config',jaas_config)
          .option('kafka.sasl.mechanism','PLAIN')
          .option('kafka.ssl.endpoint.identification.algorithm','https')
          .option('startingOffsets','earliest')
          .load())

# COMMAND ----------

parsed_stream_df=(stream_df.select(col('key').cast('string'),
                                   col('value').cast('string'),
                                col('topic'),
                                col('partition'),
                                col('offset'),
                                col('timestamp'),
                                col('timestampType'))
                 )

# COMMAND ----------

streaming_query=(parsed_stream_df.writeStream.format('delta')
         .outputMode('append')
         .option('checkpointLocation','/Volumes/finguard/source/finguard_vol/checkpoint_v3/')
         .trigger(availableNow=True)
         .toTable('finguard.bronze.credit_card_transactions_stream')
         )
print('streaming_query_id :',streaming_query.id)
streaming_query.awaitTermination()
print('Streaming query completed. Data written to stream table.')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.bronze.credit_card_transactions_stream

# COMMAND ----------

