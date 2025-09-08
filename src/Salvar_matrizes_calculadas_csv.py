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

(
    spark.table('databox.bcg_comum.supply_matriz_merecimento_linha_leve')
    .join(
        spark.table('data_engineering_prd.app_venda.mercadoria')
        .select('CdSkuLoja', 'NmSetorGerencial')
        .distinct()
        .filter(F.col("NmSetorGerencial") == 'PORTATEIS')
        .filter(F.col("StUltimaVersaoMercadoria") == 'Y')
        , on=F.col('CdSku') == F.col('CdSkuLoja'),
        how='inner'
        )
    .select(
            "CdFilial", "NmPorteLoja", "NmRegiaoGeografica", "CdSku", "grupo_de_necessidade", "NmSetorGerencial",
            F.round(100*F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura"), 4).alias("Merecimento_Percentual_offline"),
            #F.round(100*F.col("Merecimento_Final_Media180_Qt_venda_sem_ruptura"), 2).alias("Merecimento_MedMovel180")
            )
    # .groupBy("CdSku").agg(
    #     F.sum("Merecimento_Percentual_offline"),
    #     F.count("CdFilial"))
    .filter(~F.col("grupo_de_necessidade").isin("FRITADEIRA ELETRICA (CAPSULA)_220 VOLTS                               "))
    .filter(~F.col("grupo_de_necessidade").isin("FRITADEIRA ELETRICA (CAPSULA)_110 VOLTS                               "))
    .groupBy("CdSku").agg(F.sum("Merecimento_Percentual_offline"))

).display()

# COMMAND ----------

(
    spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_teste0309')
    .select(
            "CdFilial", "NmPorteLoja", "NmRegiaoGeografica", "CdSku", "grupo_de_necessidade", 
            F.round(100*F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura"), 3).alias("Merecimento_Percentual_offline"),
            #F.round(100*F.col("Merecimento_Final_Media180_Qt_venda_sem_ruptura"), 2).alias("Merecimento_MedMovel180")
            )
    #.filter(F.col("CdFilial").isin(2528, 3604))

    #.filter(F.col("grupo_de_necessidade").isin('Telef pp'))
   # .groupBy("CdSku").agg(F.sum("Merecimento_Percentual_offline"))
).display()
    

# (
#     spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_online_teste0309')
#     .select(
#             "CdFilial", "NmPorteLoja", "NmRegiaoGeografica", "CdSku", "grupo_de_necessidade", 
#             F.round(100*F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura"), 3).alias("Merecimento_Percentual_online"),
#             #F.round(100*F.col("Merecimento_Final_Media180_Qt_venda_sem_ruptura"), 2).alias("Merecimento_MedMovel180")
#             )
#     .filter(F.col("grupo_de_necessidade").isin('Telef pp'))
#      #.groupBy("CdSku").agg(F.sum("Merecimento_Percentual_online"))
# )#.display()

# COMMAND ----------

(
    spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas_teste0309')
    .select(
            "CdFilial", "NmPorteLoja", "NmRegiaoGeografica", "CdSku", "grupo_de_necessidade", 
            F.round(100*F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura"), 3).alias("Merecimento_Percentual_offline"),
            #F.round(100*F.col("Merecimento_Final_Media180_Qt_venda_sem_ruptura"), 2).alias("Merecimento_MedMovel180")
            )
    #.filter(F.col("grupo_de_necessidade").isin('TV 50 ALTO P', 'TV 55 ALTO P'))
    #.filter(F.col("CdFilial").isin(2528, 3604))

    #.groupBy("CdSku").agg(F.sum("Merecimento_Percentual_offline"))
).display()
    

# (
#     spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas_online_teste0509')
#     .select(
#             "CdFilial", "NmPorteLoja", "NmRegiaoGeografica", "CdSku", "grupo_de_necessidade", 
#             F.round(100*F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura"), 4).alias("Merecimento_Percentual_online"),
#             #F.round(100*F.col("Merecimento_Final_Media180_Qt_venda_sem_ruptura"), 2).alias("Merecimento_MedMovel180")
#             )
#     .filter(F.col("grupo_de_necessidade").isin('TV 50 ALTO P', 'TV 55 ALTO P'))
#     #.groupBy("CdSku").agg(F.sum("Merecimento_Percentual_online"))
# )#.display()

# COMMAND ----------

(
    spark.table('databox.bcg_comum.supply_matriz_merecimento_linha_leve')
    .groupBy('CdSku').agg(
        F.sum('Merecimento_Final_Media360_Qt_venda_sem_ruptura')
        )
).display()

# COMMAND ----------

(
    spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular')
    .filter(F.col("CdSKu").isin(
        5316979, 5316987, 5316995,
        5327580, 5327571, 5327598,
        5327563))
    .select(
        'CdFilial', 
        'grupo_de_necessidade', 
        'CdSku', 
        F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 3).alias('merecimento_percentual_nova_matriz')
    ).orderBy('CdSku', 'CdFilial')
).display()

# COMMAND ----------

(
    spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas')
    .filter(F.col("CdSKu").isin(
        5307708, 5284546, 5313619,))
    .select(
        'CdFilial', 
        'grupo_de_necessidade', 
        'CdSku', 
        F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 3).alias('merecimento_percentual_nova_matriz')
    ).orderBy('CdSku', 'CdFilial')
).display()

# .groupBy('CdSku').agg(F.sum('merecimento_percentual_nova_matriz'))

# COMMAND ----------

for categoria in categorias_list:
    ## TODO SALVAR TABELAS EM ARQUIVOS CSV COM DATA DE HOJE
  print(categoria)

# COMMAND ----------


