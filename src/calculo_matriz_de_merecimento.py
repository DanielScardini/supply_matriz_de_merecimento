# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Optional

meses_atipicos = [202501, 202411, 202412]

# COMMAND ----------

df_vendas_estoque_telefonia = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA TELEFONIA CELULAR')
    .filter(F.col("DtAtual") >= "2024-01-01")
    .filter(~F.col("year_month").isin(meses_atipicos))
    .withColumn(
        "year_month",
        F.date_format(F.col("DtAtual"), "yyyyMM").cast("int")
    )
    .fillna(0, subset=["Receita", "QtMercadoria", "TeveVenda"])
)
df_vendas_estoque_telefonia.cache()

print("Dados de vendas e estoque de telefonia carregados:")
df_vendas_estoque_telefonia.limit(1).display()

# COMMAND ----------

# Mapeamento de modelos e tecnologia
de_para_modelos_tecnologia = (
    pd.read_csv('dados_analise/MODELOS_AJUSTE (1).csv', 
                delimiter=';')
    .drop_duplicates()
)

# Normalização de nomes de colunas
de_para_modelos_tecnologia.columns = (
    de_para_modelos_tecnologia.columns
    .str.strip()            # remove leading/trailing spaces
    .str.lower()            # lowercase
    .str.replace(r"[^\w]+", "_", regex=True)  # non-alphanumeric -> "_"
    .str.strip("_")         # remove leading/trailing underscores
)

# Mapeamento de produtos similares (gêmeos)
de_para_gemeos_tecnologia = (
    pd.read_csv('dados_analise/ITENS_GEMEOS 2.csv',
                delimiter=";",
                encoding='iso-8859-1')
    .drop_duplicates()
)

# Normalização de nomes de colunas
de_para_gemeos_tecnologia.columns = (
    de_para_gemeos_tecnologia
    .columns
    .str.strip()            # remove leading/trailing spaces
    .str.lower()            # lowercase
    .str.replace(r"[^\w]+", "_", regex=True)  # non-alphanumeric -> "_"
    .str.strip("_")         # remove leading/trailing underscores
)

# Renomeação e merge dos mapeamentos
de_para_modelos_tecnologia = (
    de_para_modelos_tecnologia.rename(columns={'codigo_item': 'sku_loja'})
)

de_para_modelos_gemeos_tecnologia = (
    spark.createDataFrame(
        pd.merge(
            de_para_modelos_tecnologia,
            de_para_gemeos_tecnologia,
            on='sku_loja',
            how="outer"
        )
        [['sku_loja', 'item', 'modelos', 'setor_gerencial', 'gemeos']]
        .drop_duplicates()
    )
    .withColumnRenamed("sku_loja", "CdSku")
)

print("Mapeamentos de produtos carregados:")
de_para_modelos_gemeos_tecnologia.limit(1).display()

# COMMAND ----------

df_vendas_estoque_telefonia_gemeos_modelos = (
    df_vendas_estoque_telefonia
    .join(
        de_para_modelos_gemeos_tecnologia,
        on=['CdSku'],
        how='left'
    )
)

df_vendas_estoque_telefonia_gemeos_modelos.limit(1).display()

# COMMAND ----------


