# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date

data_inicio = '2025-01-01'

# COMMAND ----------

df_base_ON = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4_online')
    .filter(F.col("DtAtual") >= data_inicio)
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA DE TELAS')
    .filter(F.col("CdFilial").isin('1885', ''))
)
df_base_ON.display()
