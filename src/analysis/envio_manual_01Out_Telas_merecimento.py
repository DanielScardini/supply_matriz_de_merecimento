# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any
import plotly.graph_objects as go

!pip install openpyxl

# Inicialização do Spark
spark = SparkSession.builder.appName("envio_manual_lojas_telas").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura das demandas de unidade

# COMMAND ----------

df_demanda_envio = (
    spark.createDataFrame(
        pd.read_excel('/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/Envio adicional Telas.xlsx', engine='openpyxl', skiprows=1)
    )
    .withColumnRenamed("CODIGO_ITEM", "CdSku")
)

df_demanda_envio.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matrizes de merecimento on e off

# COMMAND ----------

df_merecimento_on = (
  spark.table
)
