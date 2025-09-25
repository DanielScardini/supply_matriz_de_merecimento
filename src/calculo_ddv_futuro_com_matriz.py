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

data_inicio = datetime.now() - timedelta(days=30)
data_inicio_str = data_inicio.strftime("%Y-%m-%d")
data_inicio_int = int(data_inicio.strftime("%Y%m%d"))

GRUPOS_TESTE = ['TV 50 ALTO P', 'TV 55 ALTO P']

# COMMAND ----------

df_vendas_robustas = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
    .filter(F.col('DtAtual') >= data_inicio_str)
    .filter(F.col('DtAtual') <= hoje_str)
    .join(
        spark.table('databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia')
        .filter(F.col('grupo_de_necessidade').isin(GRUPOS_TESTE))
        .select('CdSku', 'grupo_de_necessidade'),
        how="inner",
        on="CdSku"
    )
    .groupBy("grupo_de_necessidade", "CdSku")
    .agg(
        F.round(F.sum(F.col('QtMercadoria') + F.col("deltaRuptura")),0).alias("demanda_total"),
        F.countDistinct("DtAtual").alias("dias"),
        F.round(F.col("demanda_total")/F.col("dias") ,3).alias("demanda_diarizada"),
    )
    .filter(F.col("demanda_diarizada") > 1)
    .orderBy(F.desc("demanda_diarizada"))
    .join(
        spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas_teste1009')
        .select(
            "CdSku", "CdFilial",
            F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura").alias("merecimento_final")),
        on="CdSku",
        how="inner"
    )
    .withColumn("DDV_futuro_filial",
                F.round(F.col("demanda_diarizada") * F.col("merecimento_final"), 3)
    )    
).display()
