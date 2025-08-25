# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Análise de Efetividade da Matriz de Merecimento - Telefonia
# MAGIC 
# MAGIC Este notebook analisa a efetividade da matriz de merecimento atual comparando as alocações previstas 
# MAGIC com o comportamento real de vendas e demanda para produtos de telefonia celular.
# MAGIC 
# MAGIC **Objetivo**: Identificar gaps entre alocações previstas e realidade para otimização da matriz futura.
# MAGIC **Escopo**: Apenas produtos de telefonia celular no nível de filial (loja).

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Imports e Configurações Iniciais

# COMMAND ----------
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Optional

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Função para Cálculo de Métricas de Alocação
# MAGIC 
# MAGIC Esta função calcula métricas sofisticadas para avaliar a qualidade das alocações:
# MAGIC - **wMAPE**: Erro percentual absoluto médio ponderado
# MAGIC - **SE (Share Error)**: Erro na distribuição de participações
# MAGIC - **UAPE**: Penalização para subalocações
# MAGIC - **wMAPE assimétrico**: Versão que penaliza mais os erros de subalocação
# MAGIC - **KL Divergence**: Medida de divergência entre distribuições reais e previstas

# COMMAND ----------
def add_allocation_metrics(
    df,
    y_col: str,                  # coluna real (y)
    yhat_col: str,               # coluna previsto/alocado (ŷ)
    group_cols: Optional[List[str]] = None,  # ex.: ["year_month","modelos","gemeos"]
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
# MAGIC %md
# MAGIC ## 3. Leitura e Preparação dos Dados de Telefonia
# MAGIC 
# MAGIC Carregamos a base de dados de vendas e estoque para produtos de telefonia celular,
# MAGIC filtrando apenas a diretoria específica e período relevante.

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

print("Dados de vendas e estoque de telefonia carregados:")
df_vendas_estoque_telefonia.limit(1).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Carregamento dos Mapeamentos de Produtos
# MAGIC 
# MAGIC Carregamos os arquivos de mapeamento que relacionam SKUs com modelos, 
# MAGIC espécies gerenciais e grupos de produtos similares ("gêmeos").

# COMMAND ----------
# Mapeamento de modelos e tecnologia
de_para_modelos_tecnologia = (
    pd.read_csv('/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/MODELOS_AJUSTE (1).csv', 
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
    pd.read_csv('/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/ITENS_GEMEOS 2.csv',
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
)

print("Mapeamentos de produtos carregados:")
de_para_modelos_gemeos_tecnologia.limit(1).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Agregação dos Dados de Telefonia por Filial
# MAGIC 
# MAGIC Agregamos os dados por mês, modelo, gêmeos e filial para análise no nível de loja.
# MAGIC Excluímos produtos de chip e filtramos apenas o período de análise.

# COMMAND ----------
df_vendas_estoque_telefonia_agg = (
    df_vendas_estoque_telefonia
    .filter(F.col("year_month") < 202508)  # Filtro de período
    .filter(~F.col("NmEspecieGerencial").contains("CHIP"))  # Excluir chips
    .join(
        de_para_modelos_gemeos_tecnologia
        .withColumnRenamed("sku_loja", "CdSku"),
        how="left",
        on="CdSku"
    )
    .groupBy("year_month", "modelos", "gemeos", "CdFilial")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria"),
        F.round(F.sum("Receita"), 2).alias("Receita"),
        F.round(F.sum("Media90_Qt_venda_estq"), 0).alias("QtdDemanda"),
        F.round(F.median("PrecoMedio90"), 2).alias("PrecoMedio90")
    )
)

print("Dados agregados por filial:")
df_vendas_estoque_telefonia_agg.limit(1).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Cálculo de Percentuais de Vendas e Demanda
# MAGIC 
# MAGIC Calculamos os percentuais de participação nas vendas e demanda por mês, 
# MAGIC modelo e grupo de produtos similares.

# COMMAND ----------
# Janela por mês, modelo e gêmeos
w = Window.partitionBy("year_month", "modelos", "gemeos")

df_pct_telefonia = (
    df_vendas_estoque_telefonia_agg
    # Totais no mês/especie
    .withColumn("Qt_total_mes_especie", F.sum("QtMercadoria").over(w))
    .withColumn("Demanda_total_mes_especie", F.sum("QtdDemanda").over(w))
    
    # Percentuais de venda e demanda
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
    
    # Percentuais em %
    .withColumn("pct_vendas_perc", F.round(F.col("pct_vendas") * 100, 2))
    .withColumn("pct_demanda_perc", F.round(F.col("pct_demanda") * 100, 2))
    
    # Selecionar colunas finais
    .select(
        "year_month", "modelos", "gemeos", "CdFilial",
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
    .filter(F.col("year_month") == 202507)  # Foco no mês de análise
)

print("Percentuais calculados por filial:")
df_pct_telefonia.limit(1).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Carregamento da Matriz de Merecimento Atual
# MAGIC 
# MAGIC Carregamos a matriz de merecimento atual para comparar com os dados reais.
# MAGIC Filtramos apenas lojas offline (não CDs) e aplicamos os mapeamentos de produtos.

# COMMAND ----------
# Leitura da matriz de merecimento
df_matriz_telefonia_pd = pd.read_excel(
    "/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/(DRP)_MATRIZ_20250825174952.csv.xlsx", 
    sheet_name="(DRP)_MATRIZ_20250825174952"
)

# Conversão para evitar erros de conversão
if 'DATA_VALIDADE_RELACAO' in df_matriz_telefonia_pd.columns:
    df_matriz_telefonia_pd['DATA_VALIDADE_RELACAO'] = df_matriz_telefonia_pd['DATA_VALIDADE_RELACAO'].astype(str)

# Criação do DataFrame Spark com mapeamentos
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
    .filter(F.col("TIPO_FILIAL") != 'CD')  # Apenas lojas
    .filter(F.col("CANAL") == 'OFFLINE')   # Apenas canal offline
    .withColumn(
        "CdFilial",
        F.col("CdFilial").substr(6, 20).cast("int")
    )
)

print("Matriz de merecimento carregada:")
df_matriz_telefonia.limit(1).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Validação da Matriz por Tipo de Filial
# MAGIC 
# MAGIC Verificamos a distribuição da matriz por tipo de filial para garantir 
# MAGIC que estamos analisando apenas lojas.

# COMMAND ----------
print("Distribuição da matriz por tipo de filial:")
(
    df_matriz_telefonia
    .withColumn("TIPO_FILIAL_V2",
                F.when(F.col("TIPO_FILIAL") != "CD",
                F.lit("LOJA"))
                .otherwise(F.lit("CD")))
    .groupBy("CdSku", "TIPO_FILIAL_V2", "CANAL")
    .agg(F.sum("PERCENTUAL_MATRIZ"))
).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Join entre Matriz e Dados Reais
# MAGIC 
# MAGIC Realizamos o join entre a matriz de merecimento e os dados reais de vendas/demanda
# MAGIC para comparar alocações previstas vs. realidade.

# COMMAND ----------
df_matriz_telefonia_metricas = (
    df_matriz_telefonia
    .join(
        df_pct_telefonia,
        how="inner",
        on=["gemeos", "modelos", "CdFilial"]
    )
    .select(
        "gemeos", "modelos", "CdFilial",
        F.round(F.col("PERCENTUAL_MATRIZ"), 2).alias("Percentual_matriz_fixa"),
        "pct_vendas_perc",
        "pct_demanda_perc",
        "QtMercadoria", "QtdDemanda", 
        "Qt_total_mes_especie", "Demanda_total_mes_especie"
    )
)

print("Dados consolidados para análise de métricas:")
df_matriz_telefonia_metricas.limit(1).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Cálculo das Métricas de Avaliação
# MAGIC 
# MAGIC Calculamos as métricas linha a linha e agregadas para avaliar a qualidade
# MAGIC das alocações da matriz de merecimento.

# COMMAND ----------
# COMMAND ----------
# MAGIC %md
# MAGIC ### 10.1 Métricas Linha a Linha
# MAGIC 
# MAGIC Calculamos métricas para cada linha individual para análise detalhada.

# COMMAND ----------
# Janela para cálculo de shares (sobre todo o dataframe)
w_total = Window.partitionBy(F.lit(1))

# Parâmetros das métricas
GAMMA = 2.0
EPSILON = 1e-12

df_with_metrics = (
    df_matriz_telefonia_metricas
    # Totais globais para cálculo de shares
    .withColumn("total_mercadoria", F.sum("QtMercadoria").over(w_total))
    .withColumn("total_demanda", F.sum("QtdDemanda").over(w_total))
    
    # Shares com proteção para divisões por zero
    .withColumn(
        "p", 
        F.when(F.col("total_demanda") > 0, 
               F.col("QtdDemanda") / F.col("total_demanda"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "phat", 
        F.when(F.col("total_mercadoria") > 0, 
               F.col("QtMercadoria") / F.col("total_mercadoria"))
         .otherwise(F.lit(0.0))
    )
    
    # Métricas linha a linha
    .withColumn("abs_err", F.abs(F.col("QtdDemanda") - F.col("QtMercadoria")))
    .withColumn("under", F.greatest(F.lit(0.0), F.col("QtdDemanda") - F.col("QtMercadoria")))
    .withColumn(
        "weight", 
        F.when(F.col("QtMercadoria") < F.col("QtdDemanda"), 
               F.lit(GAMMA) * F.col("QtdDemanda"))
         .otherwise(F.col("QtdDemanda"))
    )
    .withColumn("w_abs", F.col("weight") * F.col("abs_err"))
    
    # KL divergence term
    .withColumn(
        "kl_term", 
        F.when(
            (F.col("p") > 0) & (F.col("phat") > 0),
            F.col("p") * F.log((F.col("p") + F.lit(EPSILON)) / (F.col("phat") + F.lit(EPSILON)))
        ).otherwise(F.lit(0.0))
    )
    
    # Seleção das colunas finais
    .select(
        "year_month", "modelos", "gemeos", "CdFilial",
        "Percentual_matriz_fixa", "pct_vendas_perc", "pct_demanda_perc",
        "QtMercadoria", "QtdDemanda", 
        "Qt_total_mes_especie", "Demanda_total_mes_especie",
        "abs_err", "under", "weight", "w_abs", "p", "phat", "kl_term"
    )
)

print("Métricas linha a linha calculadas:")
df_with_metrics.limit(1).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 10.2 Métricas Agregadas
# MAGIC 
# MAGIC Calculamos as métricas agregadas para o dataframe inteiro usando a função
# MAGIC `add_allocation_metrics` que criamos no início.

# COMMAND ----------
# Métricas agregadas usando a função
df_agg_metrics = add_allocation_metrics(
    df=df_matriz_telefonia_metricas,
    y_col="QtdDemanda",           # Valor real (demanda)
    yhat_col="QtMercadoria",      # Valor previsto/alocado
    group_cols=None               # Agregação global
)

print("Métricas agregadas calculadas:")
df_agg_metrics.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 11. Análise de Telas (Incompleto - Mantido por Referência)
# MAGIC 
# MAGIC Esta seção está incompleta e é mantida apenas para referência futura.
# MAGIC Foca na análise de produtos de tela (eletrodomésticos e eletrônicos).

# COMMAND ----------
# MAGIC %md
# MAGIC ### 11.1 Mapeamento de Produtos de Tela

# COMMAND ----------
de_para_gemeos_telas = (
    pd.read_excel('/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/Base analise- Telas.xlsx', sheet_name='Base')
    [['ITEM', 'VOLTAGEM_ITEM', 'ESPECIE ( GEF)', 'FAIXA DE PREÇO', 'MODELO ', 'GEMEOS']]
    .drop_duplicates()
)

# Normalização de nomes de colunas
de_para_gemeos_telas.columns = (
    de_para_gemeos_telas.columns
    .str.strip()            # remove leading/trailing spaces
    .str.lower()            # lowercase
    .str.replace(r"[^\w]+", "_", regex=True)  # non-alphanumeric -> "_"
    .str.strip("_")         # remove leading/trailing underscores
)

# Salvamento na tabela Delta
(
    spark.createDataFrame(de_para_gemeos_telas).write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable('databox.bcg_comum.supply_de_para_gemeos_modelos')
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 11.2 Dados de Vendas e Estoque de Telas

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

print("Dados de vendas e estoque de telas carregados:")
df_vendas_estoque_telas.limit(1).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 11.3 Agregação de Dados de Telas por CD (Incompleto)

# COMMAND ----------
df_vendas_estoque_telas_agg_CD = (
    df_vendas_estoque_telas
    .filter(F.col("year_month") < 202508)
    .join(
        de_para_modelos_gemeos_tecnologia
        .withColumnRenamed("sku_loja", "CdSku"),
        how="left",
        on="CdSku"
    )
    .groupBy("year_month", "NmEspecieGerencial", "Cd_primario")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria"),
        F.round(F.sum("Receita"), 2).alias("Receita"),
        F.round(F.sum("Media90_Qt_venda_estq"), 0).alias("QtdDemanda"),
        F.round(F.median("PrecoMedio90"), 2).alias("PrecoMedio90")
    )    
    .dropna()
)

print("Dados de telas agregados por CD (incompleto):")
df_vendas_estoque_telas_agg_CD.limit(1).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 12. Resumo da Análise
# MAGIC 
# MAGIC **Análise Concluída:**
# MAGIC - ✅ Dados de telefonia carregados e processados
# MAGIC - ✅ Mapeamentos de produtos aplicados
# MAGIC - ✅ Agregações por filial calculadas
# MAGIC - ✅ Matriz de merecimento carregada e validada
# MAGIC - ✅ Métricas linha a linha calculadas
# MAGIC - ✅ Métricas agregadas calculadas
# MAGIC 
# MAGIC **Próximos Passos Recomendados:**
# MAGIC 1. Análise detalhada das métricas por modelo/gêmeos
# MAGIC 2. Identificação de padrões de sub/super alocação
# MAGIC 3. Recomendações para otimização da matriz
# MAGIC 4. Completar análise de telas se necessário
