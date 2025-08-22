# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta

# COMMAND ----------

spark.table('databox.bcg_comum.supply_base_merecimento_diario').display()


# COMMAND ----------

spark.table('databox.bcg_comum.supply_base_merecimento_diario').columns


# COMMAND ----------

df_vendas_estoque_linha_leve = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA LINHA LEVE')
    .filter(F.col("DtAtual") >= "2025-06-01")
    .withColumn(
        "year_month",
        F.date_format(F.col("DtAtual"), "yyyyMM").cast("int")
    )
    .fillna(0, subset=["Receita", "QtMercadoria", "TeveVenda"])
)
df_vendas_estoque_linha_leve.cache()
df_vendas_estoque_linha_leve.display()

# COMMAND ----------

df_vendas_estoque_linha_leve_agg_CD = (
    df_vendas_estoque_linha_leve
    .filter(F.col("year_month")< 202508)
    .groupBy("year_month", "NmEspecieGerencial", "NmCidade_UF_primario")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria")
    )
    .dropna()
    
)

df_vendas_estoque_linha_leve_agg_CD.display()

# COMMAND ----------

from pyspark.sql import functions as F, Window

w = Window.partitionBy("year_month", "NmEspecieGerencial")

df_pct_cd = (
    df_vendas_estoque_linha_leve_agg_CD
    .withColumn("Qt_total_mes_especie", F.sum("QtMercadoria").over(w))
    .withColumn(
        "pct_vendas", 
        F.when(F.col("Qt_total_mes_especie") > 0,
               F.col("QtMercadoria") / F.col("Qt_total_mes_especie"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn("pct_vendas_perc", F.round(F.col("pct_vendas") * 100, 2))
    .select("year_month", "NmEspecieGerencial", "NmCidade_UF_primario",
            "QtMercadoria", "Qt_total_mes_especie", "pct_vendas", "pct_vendas_perc")
    .fillna(0, subset=["Qt_total_mes_especie", "pct_vendas", "pct_vendas_perc"])
)

df_pct_cd.display()
