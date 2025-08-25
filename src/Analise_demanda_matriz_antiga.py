# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta
import pandas as pd

# COMMAND ----------

from pyspark.sql import functions as F, Window
from typing import List, Optional

def add_allocation_metrics(
    df,
    y_col: str,                  # coluna real (y)
    yhat_col: str,               # coluna previsto/alocado (ŷ)
    group_cols: Optional[List[str]] = None,  # ex.: ["year_month","NmEspecieGerencial"]
    gamma: float = 2.0,          # penalização extra para under no wMAPE assimétrico
    epsilon: float = 1e-12       # proteção numérica para KL
):
    """
    Retorna um DataFrame agregado por group_cols contendo:
      - wMAPE
      - SE (Share Error)
      - UAPE (Underallocation Penalty)
      - wMAPE_asym (assimétrico com fator gamma)
      - KL_divergence (Kullback-Leibler entre shares)
    """
    if group_cols is None:
        group_cols = []

    w = Window.partitionBy(*group_cols) if group_cols else Window.partitionBy(F.lit(1))

    y      = F.col(y_col).cast("double")
    yhat   = F.col(yhat_col).cast("double")

    # Totais por grupo
    Y_tot    = F.sum(y).over(w)
    Yhat_tot = F.sum(yhat).over(w)

    # Shares com proteção para divisões por zero
    p    = F.when(Y_tot > 0,  y / Y_tot).otherwise(F.lit(0.0))
    phat = F.when(Yhat_tot > 0, yhat / Yhat_tot).otherwise(F.lit(0.0))

    # Termos linha-a-linha
    abs_err   = F.abs(y - yhat)
    under     = F.greatest(F.lit(0.0), y - yhat)
    weight    = F.when(yhat < y, F.lit(gamma) * y).otherwise(y)
    w_abs     = weight * abs_err

    # KL: p * log(p/phat) com eps
    p_eps    = F.when(p > 0, p).otherwise(F.lit(0.0)) + F.lit(0.0)
    phat_eps = F.when(phat > 0, phat).otherwise(F.lit(0.0)) + F.lit(epsilon)
    kl_term  = F.when(p_eps > 0, p_eps * F.log(p_eps / phat_eps)).otherwise(F.lit(0.0))

    # Agregações por grupo
    agg = (
        df
        .withColumn("__Y_tot__", Y_tot)
        .withColumn("__Yhat_tot__", Yhat_tot)
        .withColumn("__p__", p)
        .withColumn("__phat__", phat)
        .withColumn("__abs_err__", abs_err)
        .withColumn("__under__", under)
        .withColumn("__w_abs__", w_abs)
        .withColumn("__kl_term__", kl_term)
        .groupBy(*group_cols) if group_cols else
        df
        .withColumn("__Y_tot__", Y_tot)
        .withColumn("__Yhat_tot__", Yhat_tot)
        .withColumn("__p__", p)
        .withColumn("__phat__", phat)
        .withColumn("__abs_err__", abs_err)
        .withColumn("__under__", under)
        .withColumn("__w_abs__", w_abs)
        .withColumn("__kl_term__", kl_term)
        .groupBy()
    )

    res = agg.agg(
        F.sum("__abs_err__").alias("_sum_abs_err"),
        F.sum(F.col(y_col).cast("double")).alias("_sum_y"),
        F.sum(F.col(yhat_col).cast("double")).alias("_sum_yhat"),
        F.sum("__under__").alias("_sum_under"),
        F.sum("__w_abs__").alias("_sum_w_abs"),
        F.sum(F.abs(F.col("__p__") - F.col("__phat__"))).alias("SE"),
        F.sum("__kl_term__").alias("_kl_sum")
    )

    # Métricas finais com salvaguardas
    res = (
        res
        .withColumn(
            "wMAPE",
            F.when(F.col("_sum_y") > 0, F.col("_sum_abs_err") / F.col("_sum_y")).otherwise(F.lit(0.0))
        )
        .withColumn(
            "UAPE",
            F.when(F.col("_sum_y") > 0, F.col("_sum_under") / F.col("_sum_y")).otherwise(F.lit(0.0))
        )
        .withColumn(
            "wMAPE_asym",
            F.when(F.col("_sum_y") > 0, F.col("_sum_w_abs") / F.col("_sum_y")).otherwise(F.lit(0.0))
        )
        .withColumn(
            "KL_divergence",
            F.when((F.col("_sum_y") > 0) & (F.col("_sum_yhat") > 0), F.col("_kl_sum")).otherwise(F.lit(0.0))
        )
        .select(
            *(group_cols if group_cols else []),
            "wMAPE", "SE", "UAPE", "wMAPE_asym", "KL_divergence"
        )
    )

    return res

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
df_vendas_estoque_telefonia.limit(1).display()

# COMMAND ----------

de_para_modelos_tecnologia = (
    pd.read_csv('/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/MODELOS_AJUSTE (1).csv', 
                delimiter=';')
    .drop_duplicates()
)

de_para_modelos_tecnologia.columns = (
    de_para_modelos_tecnologia.columns
    .str.strip()            # remove leading/trailing spaces
    .str.lower()            # lowercase
    .str.replace(r"[^\w]+", "_", regex=True)  # non-alphanumeric -> "_"
    .str.strip("_")         # remove leading/trailing underscores
)

de_para_gemeos_tecnologia = (
    pd.read_csv('/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/ITENS_GEMEOS 2.csv',
                delimiter=";",
                encoding='iso-8859-1')
    .drop_duplicates()

)

de_para_gemeos_tecnologia.columns = (
    de_para_gemeos_tecnologia
    .columns
    .str.strip()            # remove leading/trailing spaces
    .str.lower()            # lowercase
    .str.replace(r"[^\w]+", "_", regex=True)  # non-alphanumeric -> "_"
    .str.strip("_")         # remove leading/trailing underscores
)

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
)

de_para_modelos_gemeos_tecnologia.limit(1).display()

# COMMAND ----------


df_vendas_estoque_telefonia_agg_CD = (
    df_vendas_estoque_telefonia
    .filter(F.col("year_month")< 202508)
    .filter(~F.col("NmEspecieGerencial").contains("CHIP"))
    .join(
        de_para_modelos_gemeos_tecnologia
        .withColumnRenamed("sku_loja", "CdSku"),
        how="left",
        on="CdSku"
    )
    .groupBy("year_month", "modelos", "gemeos", "Cd_primario")#"NmCidade_UF_primario")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria"),
        F.round(F.sum("Receita"),2).alias("Receita"),
        F.round(F.sum("Media90_Qt_venda_estq"), 0).alias("QtdDemanda"),
        F.round(F.median("PrecoMedio90"), 2).alias("PrecoMedio90")

    )
    
    .dropna()
    
)

df_vendas_estoque_telefonia_agg_CD.limit(1).display()

# COMMAND ----------


df_vendas_estoque_telefonia_agg = (
    df_vendas_estoque_telefonia
    .filter(F.col("year_month")< 202508)
    .filter(~F.col("NmEspecieGerencial").contains("CHIP"))
    .join(
        de_para_modelos_gemeos_tecnologia
        .withColumnRenamed("sku_loja", "CdSku"),
        how="left",
        on="CdSku"
    )
    .groupBy("year_month", "modelos", "gemeos", "Cd_primario", "CdFilial")#"NmCidade_UF_primario")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria"),
        F.round(F.sum("Receita"),2).alias("Receita"),
        F.round(F.sum("Media90_Qt_venda_estq"), 0).alias("QtdDemanda"),
        F.round(F.median("PrecoMedio90"), 2).alias("PrecoMedio90")

    )
)

df_vendas_estoque_telefonia_agg.limit(1).display()

# COMMAND ----------

from pyspark.sql import functions as F, Window

# janela por mês e espécie
w = Window.partitionBy("year_month", "modelos", "gemeos")

df_pct_cd_telefonia = (
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
        "year_month", "modelos", "gemeos", "Cd_primario", #"NmCidade_UF_primario",
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
    .filter(F.col("year_month") == 202507)
)

df_pct_cd_telefonia.limit(1).display()

# COMMAND ----------

from pyspark.sql import functions as F, Window

# janela por mês e espécie
w = Window.partitionBy("year_month", "modelos", "gemeos")

df_pct_telefonia = (
    df_vendas_estoque_telefonia_agg
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
        "year_month", "modelos", "gemeos", "Cd_primario", "CdFilial", #"NmCidade_UF_primario",
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
    .filter(F.col("year_month") == 202507)
)

df_pct_telefonia.limit(1).display()

# COMMAND ----------

import pandas as pd

!pip install pyxlsb
import pandas as pd

df_matriz_telefonia_pd = pd.read_excel("/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/(DRP)_MATRIZ_20250825174952.csv.xlsx", sheet_name="(DRP)_MATRIZ_20250825174952")
# Convert 'DATA_VALIDADE_RELACAO' to string to avoid conversion errors
if 'DATA_VALIDADE_RELACAO' in df_matriz_telefonia_pd.columns:
    df_matriz_telefonia_pd['DATA_VALIDADE_RELACAO'] = df_matriz_telefonia_pd['DATA_VALIDADE_RELACAO'].astype(str)

df_matriz_telefonia = (
    spark.createDataFrame(df_matriz_telefonia_pd)
    .withColumnRenamed("CODIGO", "CdSku")
    .join(
        de_para_modelos_gemeos_tecnologia
        .withColumnRenamed("sku_loja", "CdSku"),
        how="left",
        on="CdSku"
    )
    .withColumnRenamed("CODIGO_FILIAL", "CdFilial")
    .filter(F.col("TIPO_FILIAL") != 'CD')
    .filter(F.col("CANAL") == 'OFFLINE')
    .withColumn(
        "CdFilial",
    F.col("CdFilial").substr(6, 20).cast("int"))
)

# df_matriz_telefonia_especie = (
#     df_matriz_telefonia
#     .join(de_para_filial_cd, on=["CdFilial"])
#     .groupBy("modelos","gemeos", "Cd_primario")#"NmCidade_UF_primario")
#     .agg(F.sum("PERCENTUAL_MATRIZ").alias("PercentualMatriz"))
# )

df_matriz_telefonia.limit(1).display()

# COMMAND ----------

(
    df_matriz_telefonia
    #.filter(F.col("modelos") == 'S25 5G 256GB')
    #.filter(F.col("CdSku") == 5355427)
    .withColumn("TIPO_FILIAL_V2",
                F.when(F.col("TIPO_FILIAL") != "CD",
                F.lit("LOJA"))
                .otherwise(F.lit("CD")))
    .groupBy("CdSku", "TIPO_FILIAL_V2", "CANAL")
    .agg(F.sum("PERCENTUAL_MATRIZ"))
).display()

# COMMAND ----------

df_matriz_telefonia_metricas = (
    df_matriz_telefonia
    .join(
        df_pct_telefonia,
        how="inner",
        on=["gemeos", "modelos", "CdFilial"]# "NmCidade_UF_primario",]
    )
    .select("gemeos", "modelos", "CdFilial",#, "NmCidade_UF_primario", 
            F.round(F.col("PERCENTUAL_MATRIZ"), 2).alias("Percentual_matriz_fixa"),
            "pct_vendas_perc",
            "pct_demanda_perc",
            "QtMercadoria", "QtdDemanda", "Qt_total_mes_especie", "Demanda_total_mes_especie",
)
)

df_matriz_telefonia_metricas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Análise para telas

# COMMAND ----------

de_para_gemeos_telas = (
    pd.read_excel('/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/Base analise- Telas.xlsx', sheet_name='Base')
    [['ITEM', 'VOLTAGEM_ITEM', 'ESPECIE ( GEF)', 'FAIXA DE PREÇO', 'MODELO ', 'GEMEOS']]
    .drop_duplicates()
)

# Normalize column names
de_para_gemeos_telas.columns = (
    de_para_gemeos_telas.columns
    .str.strip()            # remove leading/trailing spaces
    .str.lower()            # lowercase
    .str.replace(r"[^\w]+", "_", regex=True)  # non-alphanumeric -> "_"
    .str.strip("_")         # remove leading/trailing underscores
)

(
    spark.createDataFrame(de_para_gemeos_telas).write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable('databox.bcg_comum.supply_de_para_gemeos_modelos')
)

# COMMAND ----------

df_vendas_estoque_telas = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA DE TELAS')
    .filter(F.col("DtAtual") >= "2025-06-01")
    .withColumn(
        "year_month",
        F.date_format(F.col("DtAtual"), "yyyyMM").cast("int")
    )
    .fillna(0, subset=["Receita", "QtMercadoria", "TeveVenda"])
)
df_vendas_estoque_telas.cache()
df_vendas_estoque_telas.limit(1).display()

# COMMAND ----------


df_vendas_estoque_telas_agg_CD = (
    df_vendas_estoque_telas
    .filter(F.col("year_month")< 202508)
    .join(
        de_para_modelos_gemeos_tecnologia
        .withColumnRenamed("sku_loja", "CdSku"),
        how="left",
        on="CdSku"
    )
    .groupBy("year_month", "NmEspecieGerencial", "Cd_primario")#, "NmCidade_UF_primario")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria"),
        F.round(F.sum("Receita"),2).alias("Receita"),
        F.round(F.sum("Media90_Qt_venda_estq"), 0).alias("QtdDemanda"),
        F.round(F.median("PrecoMedio90"), 2).alias("PrecoMedio90")

    )    
    .dropna()
    
)

df_vendas_estoque_telas_agg_CD.limit(1).display()
