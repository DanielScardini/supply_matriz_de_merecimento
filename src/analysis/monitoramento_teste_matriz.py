# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoramento do teste

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e constants

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any

# Inicialização do Spark
spark = SparkSession.builder.appName("calculo_matriz_merecimento_unificado").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

GRUPOS_TESTE = ['Telef pp', 'TV 50 ALTO P', 'TV 55 ALTO P']
print(GRUPOS_TESTE)

data_inicio = "2025-08-20"
inicio_teste = "2025-09-05"

categorias_teste = ['TELAS', 'TELEFONIA']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos merecimentos

# COMMAND ----------

spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_teste0509').display()


# COMMAND ----------

df_merecimento_offline = {}
df_merecimento_online = {}


df_merecimento_offline['TELAS'] = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas_teste0509')
    .select('CdFilial', 'grupo_de_necessidade', 'CdSku',
            F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('merecimento_percentual')
    ).dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade','CdSku',])
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmRegiaoGeografica", "NmPorteLoja").distinct(),
          how="left",
          on="CdFilial")
    .filter(F.col('grupo_de_necessidade').isin(GRUPOS_TESTE))
)

df_merecimento_online['TELAS'] = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas_online_teste0809')
    .select('CdFilial', 'grupo_de_necessidade', 'CdSku',
            F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('merecimento_percentual')
    ).dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade', 'CdSku',])
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmRegiaoGeografica", "NmPorteLoja").distinct(),
          how="left",
          on="CdFilial")
    .filter(F.col('grupo_de_necessidade').isin(GRUPOS_TESTE))
)


df_merecimento_offline['TELEFONIA'] = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_teste0509')
    .select('CdFilial', 'grupo_de_necessidade', 'CdSku',
            F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('merecimento_percentual')
    ).dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade', 'CdSku',])
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmRegiaoGeografica", "NmPorteLoja").distinct(),
          how="left",
          on="CdFilial")       
    .filter(F.col('grupo_de_necessidade').isin(GRUPOS_TESTE))
)

df_merecimento_online['TELEFONIA'] = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_online_teste0809')
    .select('CdFilial', 'grupo_de_necessidade', 'CdSku',
            F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('merecimento_percentual')
    ).dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade', 'CdSku',])
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmRegiaoGeografica", "NmPorteLoja").distinct(),
          how="left",
          on="CdFilial")       
    .filter(F.col('grupo_de_necessidade').isin(GRUPOS_TESTE))
)

df_merecimento_offline['TELAS']#.display()
df_merecimento_offline['TELEFONIA']#.display()
df_merecimento_online['TELAS']#.display()
df_merecimento_online['TELEFONIA']#.display()


produtos_do_teste = {}
produtos_do_teste['TELAS'] = df_merecimento_offline['TELAS'].select("CdSku", "grupo_de_necessidade").distinct()
produtos_do_teste['TELEFONIA'] = df_merecimento_offline['TELEFONIA'].select("CdSku", "grupo_de_necessidade").distinct()

# COMMAND ----------

produtos_do_teste['TELEFONIA'].display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos estoques

# COMMAND ----------

def load_estoque_loja_data(spark: SparkSession, categoria: str) -> DataFrame:
    """
    Carrega dados de estoque das lojas ativas.
    
    Args:
        spark: Sessão do Spark
        current_year: Ano atual para filtro de partição
        
    Returns:
        DataFrame com dados de estoque das lojas, incluindo:
        - Informações da filial e SKU
        - Dados de estoque e classificação
        - Métricas de DDE e faixas
    """
    return (
        spark.read.table("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
        .filter(F.col("DtAtual") >= data_inicio)
        .filter(F.col("StLoja") == "ATIVA")
        .filter(F.col("DsEstoqueLojaDeposito") == "L")
        .select(
            "CdFilial", 
            "CdSku",
            "DsSku",
            "DsSetor",
            "DsCurva",
            "DsCurvaAbcLoja",
            "StLinha",
            "DsObrigatorio",
            "DsVoltagem",
            F.col("DsTipoEntrega").alias("TipoEntrega"),
            F.col("CdEstoqueFilialAbastecimento").alias("QtdEstoqueCDVinculado"),
            (F.col("VrTotalVv")/F.col("VrVndCmv")).alias("DDE"),
            F.col("QtEstoqueBoaOff").alias("EstoqueLoja"),
            F.col("DsFaixaDde").alias("ClassificacaoDDE"),
            F.col("data_ingestao"),
            F.date_format(F.col("data_ingestao"), "yyyy-MM-dd").alias("DtAtual")    
        )
        .join(
            F.broadcast(produtos_do_teste[categoria]),
            on="CdSku",
            how="left" 
        )
        .withColumn(
            "grupo",
            F.when(
                F.col("grupo_de_necessidade").isNotNull(), F.lit("teste")
            )
            .otherwise(F.lit("controle"))
        
            
        )
        .dropDuplicates(["DtAtual", "CdSku", "CdFilial"])
        .withColumn("periodo_analise",
                    F.when(
                        F.col("DtAtual") <= inicio_teste, F.lit('baseline')
                    )
                    .otherwise(F.lit('piloto'))
                    )
        .withColumn("DtAtual", F.to_date(F.col("DtAtual")))
    )

df_estoque_loja = {}

df_estoque_loja['TELAS'] = load_estoque_loja_data(spark, 'TELAS')
df_estoque_loja['TELAS'].cache()
df_estoque_loja['TELAS'].limit(1).display()

df_estoque_loja['TELEFONIA'] = load_estoque_loja_data(spark, 'TELEFONIA')
df_estoque_loja['TELEFONIA'].cache()
df_estoque_loja['TELEFONIA'].limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de DDE

# COMMAND ----------

df_estoque_loja['TELEFONIA'].display()

# COMMAND ----------

df_analise = {}

for categoria in categorias_teste:
    
    df_analise[categoria]  =  (
            df_estoque_loja[categoria]
            .groupBy("periodo_analise", "grupo")
            .agg(
                F.round(F.median("DDE"), 1).alias("DDE_medio"),
                F.round((100*F.sum(F.when(F.col("ClassificacaoDDE") == "RUPTURA", 1)))/F.count("ClassificacaoDDE"), 1).alias("PctRuptura")
            )
            .orderBy(F.desc("grupo_de_necessidade"))
    )

    df_analise[categoria].display()

    
