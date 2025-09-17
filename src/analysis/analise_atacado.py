# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any
import plotly.graph_objects as go

# Inicialização do Spark
spark = SparkSession.builder.appName("calculo_matriz_merecimento_unificado").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

GRUPOS_TESTE = ['Telef pp', 'TV 50 ALTO P', 'TV 55 ALTO P']
print(GRUPOS_TESTE)

data_inicio = "2025-08-29"
fim_baseline = "2025-09-05"

inicio_teste = "2025-09-05"

categorias_teste = ['TELAS', 'TELEFONIA']

dict_diretorias = {
  'TELAS': 'TVS',
  'TELEFONIA': 'TELEFONIA CELULAR'
}



# COMMAND ----------

from pyspark.sql import functions as F

df = (
    spark.table("databox.int_negocios_op_comum.pozzani_atacado_faturado")
    .groupBy("CdFilialVenda", "FlagAtacado")
    .count()
)

# total por filial
df_total = df.groupBy("CdFilialVenda").agg(F.sum("count").alias("TotalFilial"))

# junta total e calcula %
df_result = (
    df.join(df_total, on="CdFilialVenda", how="left")
      .withColumn("PercAtacado",
                  F.when(F.col("FlagAtacado") == 'VAREJO',
                         (F.col("count") / F.col("TotalFilial")) * 100)
                   .otherwise(F.lit(0.0)))
)

df_result.display()
