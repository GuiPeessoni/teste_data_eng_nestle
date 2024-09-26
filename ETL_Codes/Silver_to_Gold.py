# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ./functions/funcs

# COMMAND ----------

df_orders = spark.table('guilherme_pesssoni_catalog.context_schema.order')
df_orders_items = spark.table('guilherme_pesssoni_catalog.context_schema.order_items')
df_clients = spark.table('guilherme_pesssoni_catalog.context_schema.clientes')
df_product =  spark.table('guilherme_pesssoni_catalog.context_schema.produtos')

# COMMAND ----------

df_oip = df_orders_items.join(df_product, df_orders_items.product_id == df_product.code_product_id, 'inner').drop(*['code_product_id','year_partition','month_partition','day_partition'])

display(df_oip)

SaveDados(df_oip, 
          database_name='catalog_db',
          schema_name='context_layer',
          table_name='order_items_products').save_delta_table()

# COMMAND ----------

df_oc = df_orders.join(df_clients, df_orders.code_customer_id == df_clients.id, 'left').drop(*['year_partition','month_partition','day_partition'])
display(df_oc)

SaveDados(df_oc, 
          database_name='catalog_db',
          schema_name='context_layer',
          table_name='order_clientes').save_delta_table()

# COMMAND ----------

path = '/Volumes/catalog_db/context_layer/output_files'

df_oip.write.csv(f'{path}/df_oip.csv', header=True)
df_oc.write.csv(f'{path}/df_oc.csv', header=True)
df_clients.write.csv(f'{path}/df_clients.csv', header=True)
df_product.write.csv(f'{path}/df_product.csv', header=True)
