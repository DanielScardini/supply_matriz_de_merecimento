# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import List, Optional

# Inicialização do Spark
spark = SparkSession.builder.appName("analise_resultados_factuais").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestão de bases

# COMMAND ----------

df_matriz_nova = {}

df_matriz_nova['TELAS'] = (
  spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas')
)

df_matriz_nova['TELEFONIA'] = (
  spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular')
)

df_matriz_neogrid = (
    spark.createDataFrame(
        pd.read_csv(
            "/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/(DRP)_MATRIZ_20250829135142.csv",
            delimiter=";",
        )
    )
    .select(
        F.col("CODIGO").cast("int").alias("CdSku"),
        F.regexp_replace(F.col("CODIGO_FILIAL"), ".*_", "").cast("int").alias("CdFilial"),
        F.regexp_replace(F.col("PERCENTUAL_MATRIZ"), ",", ".").cast("float").alias("PercMatrizNeogrid"),
        F.col("CLUSTER").cast("string").alias("is_Cluster"),
    )
    .dropDuplicates()
)

df_base_calculo_factual = (
  spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3')
  .filter(F.col('DtAtual') >= '2025-08-01')
)

df_matriz_nova['TELEFONIA'].cache()
df_matriz_nova['TELAS'].cache()
df_matriz_neogrid.cache()

# COMMAND ----------

df_matriz_nova['TELAS'].display()

# COMMAND ----------

df_matriz_nova['TELAS'].groupBy('CdSku').agg(F.sum('Merecimento_Final_Media180_Qt_venda_sem_ruptura')).display()


# COMMAND ----------

df_matriz_neogrid.display()
df_matriz_neogrid.count()

# COMMAND ----------

df_matriz_nova['TELAS'].display()
