# Databricks notebook source
!pip install PyYAML

# COMMAND ----------

import yaml
import json
from pyspark.sql import DataFrame
import pyspark.sql.functions as sf
import pyspark.sql.types as st

# COMMAND ----------

# MAGIC %run ./functions/funcs

# COMMAND ----------

df_yaml = LeituraArquivos('/base_order.yaml').read_yaml('orders')
df_json = LeituraArquivos('/base_clientes.json').read_json('customers')
df_csv =  LeituraArquivos('/base_produtos.csv').read_csv('¢')

# COMMAND ----------

df_yaml = TratamentoETL.cria_colunas_particionamento(df_yaml)
df_json = TratamentoETL.cria_colunas_particionamento(df_json)
df_csv  = TratamentoETL.cria_colunas_particionamento(df_csv)

# COMMAND ----------

SaveDados(df_csv, 
          database_name='catalog_db',
          schema_name='raw_layer',
          table_name='produtos').save_delta_table()

# COMMAND ----------

SaveDados(df_json, 
          database_name='catalog_db',
          schema_name='raw_layer',
          table_name='clientes').save_delta_table()

# COMMAND ----------

SaveDados(df_yaml, 
          database_name='catalog_db',
          schema_name='raw_layer',
          table_name='orders').save_delta_table()
