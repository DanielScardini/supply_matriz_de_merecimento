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

GRUPOS_TESTE = [
    "Telef pp",
    "TV 50 ALTO P",
    "TV 55 ALTO P",
    "TV 43 PP",
    "Telef Alto",
    "Telef Medio 256GB",
    "Telef Medio 128GB"+
]

dt_inicio = "2025-08-01"
dt_fim = "2025-10-01"

# Calcular top 80% por ESPÉCIE (SKUs) - apenas PORTATEIS
df_demanda_especie = (
  spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
  .filter(F.col("NmSetorGerencial") == "PORTATEIS")
  .filter(F.col("DtAtual") >= dt_inicio)
  .filter(F.col("DtAtual") < dt_fim)
  .groupBy("NmEspecieGerencial")
  .agg(
    F.sum(F.col("QtMercadoria")).alias("QtDemanda"),
    F.sum(F.col("Receita")).alias("Receita")
  )
)

# calcular totais com window
w_total = W.rowsBetween(W.unboundedPreceding, W.unboundedFollowing)

# window para cumulativo
w_cum = W.orderBy(F.col("PercDemanda").desc()).rowsBetween(W.unboundedPreceding, 0)

# TOP 80% POR ESPÉCIE (SKUs) - apenas PORTATEIS
df_demanda_especie = (
    df_demanda_especie
    .withColumn("TotalDemanda", F.sum("QtDemanda").over(w_total))
    .withColumn("TotalReceita", F.sum("Receita").over(w_total))
    .withColumn("PercDemanda", F.round((F.col("QtDemanda") / F.col("TotalDemanda")) * 100, 0))
    .withColumn("PercReceita", F.round((F.col("Receita") / F.col("TotalReceita")) * 100, 0))
    .drop("TotalDemanda", "TotalReceita")
    .withColumn("PercDemandaCumulativo", F.sum("PercDemanda").over(w_cum))
    .withColumn("PercReceitaCumulativo", F.sum("PercReceita").over(w_cum))
)

especies_top80 = (
    df_demanda_especie
    .filter(F.col("PercDemandaCumulativo") <= 80)
    .select("NmEspecieGerencial")
    .rdd.flatMap(lambda x: x)
    .collect()
)


print("🔝 ESPÉCIES TOP 80% PORTATEIS:")
print(especies_top80)
print(f"Total de espécies: {len(especies_top80)}")

# SKUs das espécies top 80%
skus_especies_top80 = (
    spark.table('data_engineering_prd.app_venda.mercadoria')
    .select(
        F.col("CdSkuLoja").alias("CdSku"),
        F.col("NmEspecieGerencial")
    )
    .filter(F.col("NmEspecieGerencial").isin(especies_top80))
    .filter(F.col("CdSku") != -1)
    .select("CdSku")
    .rdd.flatMap(lambda x: x)
    .collect()
)

print(f"\n📊 SKUs das espécies top 80%: {len(skus_especies_top80)} SKUs")

# Validação dos percentuais
print("\n📈 VALIDAÇÃO PERCENTUAIS:")
print("ESPÉCIES TOP 80%:")
df_demanda_especie.filter(F.col("NmEspecieGerencial").isin(especies_top80)).agg(F.sum("PercDemanda")).show()

# COMMAND ----------


