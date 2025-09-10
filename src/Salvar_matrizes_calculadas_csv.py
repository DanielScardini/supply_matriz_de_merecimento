# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any

# Inicialização do Spark
spark = SparkSession.builder.appName("salvar_matrizes_merecimento_unificadas").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

categorias_list = [
    "DIRETORIA DE TELAS",
    "DIRETORIA TELEFONIA CELULAR", 
    #...
    ]

# COMMAND ----------

from pyspark.sql import functions as F, Window as W

# ---------- OFFLINE ----------
df_offline = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_teste1009')
    .select(
        "CdFilial","NmPorteLoja","NmRegiaoGeografica","CdSku","grupo_de_necessidade",
        (100*F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura")).alias("Merecimento_Percentual_offline_raw")
    )
    .filter(F.col("CdSku")==5286301)
    .filter(F.col("grupo_de_necessidade")=="Telef pp")
    .join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial","NmFilial"),
        on="CdFilial", how="left"
    )
)

win_off = W.partitionBy("CdSku")
tot_off = F.sum("Merecimento_Percentual_offline_raw").over(win_off)

df_offline_norm = (
    df_offline
    .withColumn(
        "Merecimento_Percentual_offline",
        F.round(F.when(tot_off>0, F.col("Merecimento_Percentual_offline_raw")*(100.0/tot_off)).otherwise(0.0), 3)
    )
)

# linhas normalizadas
df_offline_norm.drop("Merecimento_Percentual_offline_raw").display()

# conferência raw vs normalizada
(
    df_offline_norm.groupBy("CdSku")
    .agg(
        F.round(F.sum("Merecimento_Percentual_offline_raw"),2).alias("Soma_Raw"),
        F.round(F.sum("Merecimento_Percentual_offline"),2).alias("Soma_Normalizada")
    )
).display()


# ---------- ONLINE ----------
df_online = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_online_teste0809')
    .select(
        "CdFilial","CdSku","grupo_de_necessidade",
        (100*F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura")).alias("Merecimento_Percentual_online_raw")
    )
    .filter(F.col("grupo_de_necessidade")=="Telef pp")
    .filter(F.col("CdSku")==5286301)
    .join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial","NmFilial","NmPorteLoja","NmRegiaoGeografica"),
        on="CdFilial", how="left"
    )
)

win_on = W.partitionBy("CdSku")
tot_on = F.sum("Merecimento_Percentual_online_raw").over(win_on)

df_online_norm = (
    df_online
    .withColumn(
        "Merecimento_Percentual_online",
        F.round(F.when(tot_on>0, F.col("Merecimento_Percentual_online_raw")*(100.0/tot_on)).otherwise(0.0), 3)
    )
)

# linhas normalizadas
df_online_norm.drop("Merecimento_Percentual_online_raw").display()

# conferência raw vs normalizada
(
    df_online_norm.groupBy("CdSku")
    .agg(
        F.round(F.sum("Merecimento_Percentual_online_raw"),2).alias("Soma_Raw"),
        F.round(F.sum("Merecimento_Percentual_online"),2).alias("Soma_Normalizada")
    )
).display()

# COMMAND ----------

from pyspark.sql import functions as F, Window as W

# ---------- OFFLINE ----------
df_offline = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas_teste1009')
    .select(
        "CdFilial","NmPorteLoja","NmRegiaoGeografica","CdSku","grupo_de_necessidade",
        (100*F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura")).alias("Merecimento_Percentual_offline_raw")
    )
    .filter(F.col("CdSku")==5338182)
    .filter(F.col("grupo_de_necessidade").isin("TV 50 ALTO P", "TV 55 ALTO P"))
    .join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial","NmFilial"),
        on="CdFilial", how="left"
    )
)

win_off = W.partitionBy("CdSku")
tot_off = F.sum("Merecimento_Percentual_offline_raw").over(win_off)

df_offline_norm = (
    df_offline
    .withColumn(
        "Merecimento_Percentual_offline",
        F.round(F.when(tot_off>0, F.col("Merecimento_Percentual_offline_raw")*(100.0/tot_off)).otherwise(0.0), 3)
    )
)

# linhas normalizadas
df_offline_norm.drop("Merecimento_Percentual_offline_raw").display()

# conferência raw vs normalizada
(
    df_offline_norm.groupBy("CdSku")
    .agg(
        F.round(F.sum("Merecimento_Percentual_offline_raw"),3).alias("Soma_Raw"),
        F.round(F.sum("Merecimento_Percentual_offline"),3).alias("Soma_Normalizada")
    )
).display()


# ---------- ONLINE ----------
df_online = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas_online_teste0809')
    .select(
        "CdFilial","CdSku","grupo_de_necessidade",
        (100*F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura")).alias("Merecimento_Percentual_online_raw")
    )
    .filter(F.col("grupo_de_necessidade").isin("TV 50 ALTO P", "TV 55 ALTO P"))
    .filter(F.col("CdSku")==5338182)
    .join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial","NmFilial","NmPorteLoja","NmRegiaoGeografica"),
        on="CdFilial", how="left"
    )
)

win_on = W.partitionBy("CdSku")
tot_on = F.sum("Merecimento_Percentual_online_raw").over(win_on)

df_online_norm = (
    df_online
    .withColumn(
        "Merecimento_Percentual_online",
        F.round(F.when(tot_on>0, F.col("Merecimento_Percentual_online_raw")*(100.0/tot_on)).otherwise(0.0), 3)
    )
)

# linhas normalizadas
df_online_norm.drop("Merecimento_Percentual_online_raw").display()

# conferência raw vs normalizada
(
    df_online_norm.groupBy("CdSku")
    .agg(
        F.round(F.sum("Merecimento_Percentual_online_raw"),3).alias("Soma_Raw"),
        F.round(F.sum("Merecimento_Percentual_online"),3).alias("Soma_Normalizada")
    )
).display()

# COMMAND ----------


(
    spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_teste0809')
    .select(
            "CdFilial", 
#             #"NmPorteLoja", 
#             #"NmRegiaoGeografica", 
            "CdSku", 
            "grupo_de_necessidade", 
            F.round(100*F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura"), 3).alias("Merecimento_Percentual_online"),
            )
      .filter(F.col("grupo_de_necessidade").isin('Telef pp'))
      #.filter(F.col("CdSku").isin(52))
#       .join(
#               spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
#               .select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica"),
#               on="CdFilial", how="left"
#       )

     #.groupBy("CdSku").agg(F.sum("Merecimento_Percentual_online"))
).display()

# COMMAND ----------


