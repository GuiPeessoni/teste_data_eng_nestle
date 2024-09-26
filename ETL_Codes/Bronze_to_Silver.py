# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as sf
import pyspark.sql.types as st
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ./functions/funcs

# COMMAND ----------

df_orders = spark.table('guilherme_pesssoni_catalog.testes_estudos.orders')
#df_clients = spark.table('guilherme_pesssoni_catalog.testes_estudos.clientes')
#df_product =  spark.table('guilherme_pesssoni_catalog.testes_estudos.produtos')

# COMMAND ----------

df_clients = TratamentoETL.deduplica_dados(df_clients, ['id'],['datalog'])
df_address = TratamentoETL.explode_map(df_clients, 'id','addresses')

df_ca = TratamentoETL.explode_map(df_clients, 'id', 'custom_attributes')
df_ca = df_ca.groupBy("id").pivot("attribute_code").agg(sf.first("value"))

df_ext_att = TratamentoETL.explode_map(df_clients, 'id','extension_attributes')

df_clients = df_clients.drop(*['addresses','custom_attributes','extension_attributes'])

df_clients = df_clients.join(df_address, on=['id'], how='left')
df_clients = df_clients.join(df_ca, on=['id'], how='left')
df_clients = df_clients.join(df_ext_att, on=['id'], how='left')


window_spec = Window.partitionBy('id').orderBy('updated_at')
df_clients = df_clients.withColumn("cli_rn", sf.row_number().over(window_spec))


SaveDados(df_clients, 
          database_name='catalog_db',
          schema_name='context_layer',
          table_name='clientes').save_delta_table()

# COMMAND ----------

df_product = TratamentoETL.deduplica_dados(df_product, ['code_product_id'],['datalog'])
df_product = TratamentoETL.converte_medida(df_product, 'product_size')

SaveDados(df_product, 
          database_name='catalog_db',
          schema_name='context_layer',
          table_name='produtos').save_delta_table()

# COMMAND ----------

df_orders = TratamentoETL.deduplica_dados(df_orders, ['code_order_id'],['datalog'])
df_itens = TratamentoETL.explode_struct(df_orders, 'code_order_id','items')

SaveDados(df_orders, 
          database_name='guilherme_pesssoni_catalog',
          schema_name='testes_estudos',
          table_name='order').save_delta_table()

df_itens.write.mode("overwrite").saveAsTable("guilherme_pesssoni_catalog.testes_estudos.order_items")

# COMMAND ----------


