# Databricks notebook source
# MAGIC %md
# MAGIC # Imports e constants

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Análise de participação de vendas - grupo de necessidade de Linha Leve

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# janela factual (últimos 8 dias)
janela_factual = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
print(janela_factual)

# janela para total (sem partição = mesmo total para todas as linhas)
w_total = Window.partitionBy()

# base agregada por grupo_de_necessidade + percentuais
df_proporcao_factual = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
    .filter(F.col('DtAtual') >= janela_factual)
    .filter(F.col("NmAgrupamentoDiretoriaSetor").isin('DIRETORIA LINHA LEVE'))
    .fillna(0, subset=['deltaRuptura', 'QtMercadoria'])
    .withColumn("QtDemanda", F.col("QtMercadoria") + F.col("deltaRuptura"))
    .withColumn("DsVoltagem_filled", F.coalesce(F.col("DsVoltagem"), F.lit("")))
    .withColumn(
        "grupo_de_necessidade",
        F.concat(F.coalesce(F.col('NmEspecieGerencial'), F.lit("SEM_GN")), F.lit("_"), F.col("DsVoltagem_filled"))
    )
    .groupBy("NmSetorGerencial")
    .agg(
        F.sum("Receita").alias("Receita"),
        F.sum("QtDemanda").alias("QtDemanda")
    )
    # totais via janela global
    .withColumn("total_receita", F.sum("Receita").over(w_total))
    .withColumn("total_demanda", F.sum("QtDemanda").over(w_total))
    # percentuais
    .withColumn("perc_receita", F.when(F.col("total_receita") > 0, F.col("Receita") / F.col("total_receita")).otherwise(F.lit(0.0)))
    .withColumn("perc_demanda", F.when(F.col("total_demanda") > 0, F.col("QtDemanda") / F.col("total_demanda")).otherwise(F.lit(0.0)))
    # opcional: arredondar
    .withColumn("perc_receita", F.round(100*F.col("perc_receita"), 1))
    .withColumn("perc_demanda", F.round(100*F.col("perc_demanda"), 1))
)

(
    df_proporcao_factual
    .select(
        "NmSetorGerencial",
        "perc_receita", 
        "perc_demanda"
    )
    .orderBy(F.desc("perc_receita"))
    #.limit(10)
    .display()
)

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# janela factual (últimos 8 dias)
janela_factual = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
print(janela_factual)

# janela para total (sem partição = mesmo total para todas as linhas)
w_total = Window.partitionBy()

# base agregada por grupo_de_necessidade + percentuais
df_proporcao_factual = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
    .filter(F.col('DtAtual') >= janela_factual)
    .filter(F.col("NmAgrupamentoDiretoriaSetor").isin('DIRETORIA LINHA LEVE'))
    .fillna(0, subset=['deltaRuptura', 'QtMercadoria'])
    .withColumn("QtDemanda", F.col("QtMercadoria") + F.col("deltaRuptura"))
    .withColumn("DsVoltagem_filled", F.coalesce(F.col("DsVoltagem"), F.lit("")))
    .withColumn(
        "grupo_de_necessidade",
        F.concat(F.coalesce(F.col('NmEspecieGerencial'), F.lit("SEM_GN")), F.lit("_"), F.col("DsVoltagem_filled"))
    )
    .groupBy("grupo_de_necessidade")
    .agg(
        F.sum("Receita").alias("Receita"),
        F.sum("QtDemanda").alias("QtDemanda")
    )
    # totais via janela global
    .withColumn("total_receita", F.sum("Receita").over(w_total))
    .withColumn("total_demanda", F.sum("QtDemanda").over(w_total))
    # percentuais
    .withColumn("perc_receita", F.when(F.col("total_receita") > 0, F.col("Receita") / F.col("total_receita")).otherwise(F.lit(0.0)))
    .withColumn("perc_demanda", F.when(F.col("total_demanda") > 0, F.col("QtDemanda") / F.col("total_demanda")).otherwise(F.lit(0.0)))
    # opcional: arredondar
    .withColumn("perc_receita", F.round(100*F.col("perc_receita"), 1))
    .withColumn("perc_demanda", F.round(100*F.col("perc_demanda"), 1))
)

(
    df_proporcao_factual
    .select(
        "grupo_de_necessidade",
        "perc_receita", 
        "perc_demanda"
    )
    .orderBy(F.desc("perc_receita"))
    .limit(10)
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de scale up - Telas e telefonia

# COMMAND ----------

janela_factual = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

print(janela_factual)

# partindo do df_proporcao_factual já agregado por CdFilial × grupo_de_necessidade
w_grp = Window.partitionBy("NmAgrupamentoDiretoriaSetor")

df_proporcao_factual = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
    .filter(F.col('DtAtual') >= janela_factual)    
    .filter(F.col("NmAgrupamentoDiretoriaSetor").isin('DIRETORIA TELEFONIA CELULAR', 'DIRETORIA DE TELAS'))
    .fillna(0, subset=['deltaRuptura', 'QtMercadoria'])
    .withColumn("QtDemanda", F.col("QtMercadoria") + F.col("deltaRuptura"))
    .join(
        spark.table('databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia'),
          how='left',
          on='CdSku')
    .fillna("SEM_GN", subset=["grupo_de_necessidade"])
    .dropna(subset='grupo_de_necessidade')
    .withColumn('grupo_de_necessidade', F.col("gemeos"))
    .groupBy('grupo_de_necessidade', "NmAgrupamentoDiretoriaSetor")
    .agg(
        F.round(F.sum('QtDemanda'), 0).alias('QtDemanda'),
        F.round(F.sum('Receita'), 0).alias('Receita')
    )
    .withColumn("Total_QtDemanda", F.round(F.sum(F.col("QtDemanda")).over(w_grp), 0))
    .withColumn("Total_Receita", F.round(F.sum(F.col("Receita")).over(w_grp), 0))

    .withColumn(
        "Proporcao_Interna_Receita",
        F.when(F.col("Total_Receita") > 0, F.col("Receita") / F.col("Total_Receita")).otherwise(F.lit(0.0))
    )
    .withColumn(
        "Proporcao_Interna_QtDemanda",
        F.when(F.col("Total_QtDemanda") > 0, F.col("QtDemanda") / F.col("Total_QtDemanda")).otherwise(F.lit(0.0))
    )
    .withColumn("Percentual_QtDemanda", F.round(F.col("Proporcao_Interna_QtDemanda") * 100.0, 2))
    .withColumn("Percentual_Receita", F.round(F.col("Proporcao_Interna_Receita") * 100.0, 2))

    .select('grupo_de_necessidade', 'Percentual_QtDemanda', 'Percentual_Receita')
    #.filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
).display()
