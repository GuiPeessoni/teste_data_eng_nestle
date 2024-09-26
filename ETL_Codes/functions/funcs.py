# Databricks notebook source
yaml_schema = st.StructType([
    st.StructField("code_customer_id", st.IntegerType(), True),
    st.StructField("code_order_id", st.IntegerType(), True),
    st.StructField("date_create", st.StringType(), True),
    st.StructField("date_delivered", st.StringType(), True),
    st.StructField("date_update", st.StringType(), True),
    st.StructField("items", st.ArrayType(
        st.StructType([
            st.StructField("base_price", st.StringType(), True),
            st.StructField("item_id", st.StringType(), True),
            st.StructField("product_id", st.StringType(), True),
            st.StructField("qty_canceled", st.StringType(), True),
            st.StructField("qty_invoiced", st.StringType(), True),
            st.StructField("qty_ordered", st.StringType(), True)
        ])), True),
    st.StructField("region_delivery", st.StringType(), True),
    st.StructField("status", st.StringType(), True),
    st.StructField("val_ordered", st.DoubleType(), True),
    st.StructField("val_paid", st.DoubleType(), True)
])

# COMMAND ----------

class LeituraArquivos:
    def __init__(self,  file_name:str,) -> None:
        self.path = '/Volumes/guilherme_pesssoni_catalog/testes_estudos/input_files'
        self.file_name = file_name
        self.file_path = self.path + self.file_name
        self.spark = spark

    def read_yaml(self,key:str= None)-> DataFrame:
        """
        Função que lê um YAML e transforma ele em Spark Dataframe
        """
        with open(self.file_path) as yaml_file:
            try:
                arq_yaml = yaml.safe_load(yaml_file).get(key)
            except yaml.YAMLError as exc:
                print(exc)
            else:
                df_yaml = spark.createDataFrame(arq_yaml,schema=yaml_schema)
                df_yaml = df_yaml.withColumn('datalog', sf.current_timestamp())
                df_yaml = df_yaml.withColumn('table_infos', sf.struct(sf.lit('code_order_id').alias('table_pk'), 
                                                          sf.lit('datalog').alias('date_partition'),
                                                          sf.lit('year_partition,month_partition,day_partition').alias('field_partition'),))
                return df_yaml

    def read_json(self,key:str= None) -> DataFrame:
      """
      Função que lê um JSON e transforma ele em Spark Dataframe
      """
      with open(self.file_path, 'r', encoding='utf-8-sig') as json_file:
          arq_json = json.load(json_file)
          df_json = self.spark.createDataFrame(arq_json[key])
          df_json = df_json.withColumn('datalog', sf.current_timestamp())
          df_json = df_json.withColumn('table_infos', sf.struct(sf.lit('id').alias('table_pk'), 
                                                          sf.lit('datalog').alias('date_partition'),
                                                          sf.lit('year_partition,month_partition,day_partition').alias('field_partition'),))
          return df_json

    def read_csv(self, delimiter:str = ',') -> DataFrame:
      """
      Função que lê um CSV e transforma ele em Spark Dataframe
      """
      df_csv = self.spark.read.csv(self.file_path, inferSchema=True, header=True, sep=delimiter)
      df_csv = df_csv.withColumn('datalog', sf.current_timestamp())
      df_csv = df_csv.withColumn('table_infos', sf.struct(sf.lit('code_product_id').alias('table_pk'), 
                                                          sf.lit('datalog').alias('date_partition'),
                                                          sf.lit('year_partition,month_partition,day_partition').alias('field_partition'),))
      return df_csv

# COMMAND ----------

class SaveDados:
    def __init__(self, df: DataFrame, schema_name: str = None, table_name: str = None, database_name: str = None) -> None:
        self.spark = spark
        self.df = df
        self.schema_name = schema_name
        self.table_name = table_name
        self.database_name = database_name

    def save_delta_table(self):
        try:
            get_part_cols = self.df.select('table_infos.field_partition').limit(1).collect()[0][0]
            part_cols = get_part_cols.split(',')
        except:
            part_cols = ['year_partition','month_partition','day_partition']
        self.df.write.format("delta").mode("overwrite").partitionBy(part_cols).saveAsTable(f'{self.database_name}.{self.schema_name}.{self.table_name}')

        


# COMMAND ----------

class TratamentoETL:
    def cria_colunas_particionamento(df: DataFrame)-> DataFrame:
        col_part = df.select('table_infos.date_partition').limit(1).collect()[0][0]
        df = df.withColumn('year_partition', sf.year(sf.col(col_part)).cast('string'))
        df = df.withColumn('month_partition', sf.lpad(sf.month(sf.col(col_part)).cast('string'), 2, "0"))
        df = df.withColumn('day_partition', sf.lpad(sf.dayofmonth(sf.col(col_part)).cast('string'), 2, "0"))
        return df

    def explode_map(df: DataFrame, col_pk: str, col_exp: str) -> DataFrame:
      if isinstance(df.schema[col_exp].dataType, st.ArrayType):
        df = df.withColumn(col_exp, sf.explode(sf.col(col_exp)))

      keys = df.select(sf.explode(sf.map_keys(col_exp))).distinct().collect()
      keys = [row[0] for row in keys] 
      df = df.select(
          col_pk,
          *[sf.col(col_exp).getItem(key).alias(f'{key}') for key in keys]
      )
      return df
  
    def explode_struct(df: DataFrame, col_pk: str, col_exp: str) -> DataFrame:
      df = df.withColumn(col_exp, sf.explode(col_exp))
      df = df.select(col_pk, sf.col(f"{col_exp}.*"))
      return df

    def deduplica_dados(df: DataFrame, col_dup: list, col_order: list) -> DataFrame:
        window_spec = Window.partitionBy(col_dup).orderBy([sf.col(c).desc() for c in col_order])
        df = df.withColumn("row_number", sf.row_number().over(window_spec)).drop('row_number')
        df = df.drop(*['table_infos','datalog'])
        return df

    def converte_medida(df: DataFrame, col_convert: str) -> DataFrame:
        df = df.fillna(0, subset=col_convert)
        df = df.withColumn(col_convert, sf.regexp_replace(col_convert, ',', '.'))
        df = df.withColumn('peso_em_g',
                  sf.when(sf.lower(sf.col('product_size')).like('%kg%'),
                          sf.regexp_replace(sf.lower(sf.col('product_size')), 'kg', '').cast('float') * 1000)
                 .when(sf.col('product_size').like('%mg%'),
                          sf.regexp_replace(sf.col('product_size'), 'mg', '').cast('float') / 1000)
                 .when(sf.col('product_size').like('%caps%'),
                          sf.regexp_replace(sf.regexp_replace(sf.col('product_size'), 'caps', ''), 'g', '').cast('float'))
                 .otherwise(sf.regexp_replace(sf.col('product_size'), '[g|r]', '').cast('float'))
            )
        df = df.fillna(0, subset='peso_em_g')
        return df


# COMMAND ----------


