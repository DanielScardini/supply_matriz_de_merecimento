# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta

# COMMAND ----------

df_vendas_estoque_telefonia = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA TELEFONIA CELULAR')
    .filter(F.col("DtAtual") >= "2025-06-01")
    .withColumn(
        "year_month",
        F.date_format(F.col("DtAtual"), "yyyyMM").cast("int")
    )
    .fillna(0, subset=["Receita", "QtMercadoria", "TeveVenda"])
)
df_vendas_estoque_telefonia.cache()
df_vendas_estoque_telefonia.display()

# COMMAND ----------

df_vendas_estoque_telefonia_agg_CD = (
    df_vendas_estoque_telefonia
    .filter(F.col("year_month")< 202508)
    .groupBy("year_month", "NmEspecieGerencial", "Cd_primario", "NmCidade_UF_primario")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria"),
        F.round(F.sum("Receita"),2).alias("Receita"),
        F.round(F.sum("Media90_Qt_venda_estq"), 0).alias("QtdDemanda"),
        F.round(F.median("PrecoMedio90"), 2).alias("PrecoMedio90")

    )
    .filter(~F.col("NmEspecieGerencial").contains("CHIP"))
    
    .dropna()
    
)

df_vendas_estoque_telefonia_agg_CD.display()

# COMMAND ----------

de_para_filial_cd = df_vendas_estoque_telefonia.select("CdFilial", "Cd_primario", "NmCidade_UF_primario").distinct()


# COMMAND ----------

from pyspark.sql import functions as F, Window

# janela por mês e espécie
w = Window.partitionBy("year_month", "NmEspecieGerencial")

df_pct_cd = (
    df_vendas_estoque_telefonia_agg_CD
    # totais no mês/especie
    .withColumn("Qt_total_mes_especie", F.sum("QtMercadoria").over(w))
    .withColumn("Demanda_total_mes_especie", F.sum("QtdDemanda").over(w))
    
    # percentuais de venda e demanda
    .withColumn(
        "pct_vendas", 
        F.when(F.col("Qt_total_mes_especie") > 0,
               F.col("QtMercadoria") / F.col("Qt_total_mes_especie"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "pct_demanda", 
        F.when(F.col("Demanda_total_mes_especie") > 0,
               F.col("QtdDemanda") / F.col("Demanda_total_mes_especie"))
         .otherwise(F.lit(None).cast("double"))
    )
    
    # percentuais em %
    .withColumn("pct_vendas_perc", F.round(F.col("pct_vendas") * 100, 2))
    .withColumn("pct_demanda_perc", F.round(F.col("pct_demanda") * 100, 2))
    
    # selecionar colunas finais
    .select(
        "year_month", "NmEspecieGerencial", "NmCidade_UF_primario",
        "QtMercadoria", "QtdDemanda", "PrecoMedio90",
        "Qt_total_mes_especie", "Demanda_total_mes_especie",
        "pct_vendas", "pct_vendas_perc",
        "pct_demanda", "pct_demanda_perc"
    )
    .fillna(0, subset=[
        "Qt_total_mes_especie", "Demanda_total_mes_especie",
        "pct_vendas", "pct_vendas_perc", 
        "pct_demanda", "pct_demanda_perc"
    ])
)

df_pct_cd.display()

# COMMAND ----------

import pandas as pd

!pip install pyxlsb
import pandas as pd

df_matriz_telefonia_pd = pd.read_excel("/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/matriz_telefonia.xlsx")
# Convert 'DATA_VALIDADE_RELACAO' to string to avoid conversion errors
if 'DATA_VALIDADE_RELACAO' in df_matriz_telefonia_pd.columns:
    df_matriz_telefonia_pd['DATA_VALIDADE_RELACAO'] = df_matriz_telefonia_pd['DATA_VALIDADE_RELACAO'].astype(str)

df_matriz_telefonia = (
    spark.createDataFrame(df_matriz_telefonia_pd)
    .withColumnRenamed("ESPECIE", "NmEspecieGerencial")
    .withColumnRenamed("CODIGO_FILIAL", "CdFilial")
    .filter(F.col("TIPO_FILIAL") != 'CD')
)

df_matriz_telefonia_especie = (
    df_matriz_telefonia
    .join(de_para_filial_cd, on=["CdFilial"])
    .groupBy("NmEspecieGerencial", "CdFilial", "NmEspecieGerencial")
    .agg(F.sum("PERCENTUAL_MATRIZ").alias("PercentualMatriz"))
)

df_matriz_telefonia_especie.display()