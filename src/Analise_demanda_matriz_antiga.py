# Databricks notebook source
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

from pyspark.sql import functions as F, Window
from typing import List, Optional

def add_allocation_metrics(
    df,
    y_col: str,                             # real (y)
    yhat_col: str,                          # previsto (ŷ)
    group_cols: Optional[List[str]] = None, # ex.: ["year_month","modelo","gemeo"]
    gamma: float = 1.5,                     # >1 penaliza under (share e volume)
    epsilon: float = 1e-12
):
    if group_cols is None:
        group_cols = []

    w = Window.partitionBy(*group_cols) if group_cols else Window.partitionBy(F.lit(1))

    y, yhat = F.col(y_col).cast("double"), F.col(yhat_col).cast("double")

    # Totais por grupo (para shares)
    Y_tot    = F.sum(y).over(w)
    Yhat_tot = F.sum(yhat).over(w)

    # Shares (fração)
    p    = F.when(Y_tot    > 0, y    / Y_tot   ).otherwise(F.lit(0.0))
    phat = F.when(Yhat_tot > 0, yhat / Yhat_tot).otherwise(F.lit(0.0))

    # Termos (volume)
    abs_err = F.abs(y - yhat)
    under   = F.greatest(F.lit(0.0), y - yhat)

    # Peso escalar assimétrico (volume)
    weight_scalar = F.when(yhat < y, F.lit(gamma)).otherwise(F.lit(1.0))
    w_abs = weight_scalar * abs_err

    # KL em shares
    kl_term = F.when(p > 0, p * F.log((p + F.lit(epsilon)) / (phat + F.lit(epsilon)))).otherwise(F.lit(0.0))

    # Termos (share ponderado por volume)
    abs_err_share   = F.abs(p - phat)
    under_err_share = F.when(phat < p, p - phat).otherwise(F.lit(0.0))
    w_abs_share     = abs_err_share * y
    w_abs_share_as  = F.when(phat < p, F.lit(gamma) * abs_err_share * y).otherwise(abs_err_share * y)
    w_under_share   = under_err_share * y

    base = (df
        .withColumn("__y__", y).withColumn("__yhat__", yhat)
        .withColumn("__p__", p).withColumn("__phat__", phat)
        .withColumn("__abs_err__", abs_err).withColumn("__under__", under).withColumn("__w_abs__", w_abs)
        .withColumn("__kl_term__", kl_term)
        .withColumn("__abs_err_share__", abs_err_share)
        .withColumn("__w_abs_share__", w_abs_share)
        .withColumn("__w_abs_share_as__", w_abs_share_as)
        .withColumn("__w_under_share__", w_under_share)
    )

    agg = base.groupBy(*group_cols) if group_cols else base.groupBy()

    res = agg.agg(
        # volume
        F.sum("__abs_err__").alias("_sum_abs_err"),
        F.sum("__under__").alias("_sum_under"),
        F.sum("__w_abs__").alias("_sum_w_abs"),
        F.sum("__y__").alias("_sum_y"),
        F.sum("__yhat__").alias("_sum_yhat"),
        # shares
        F.sum(F.abs(F.col("__p__") - F.col("__phat__"))).alias("_SE"),
        F.sum("__kl_term__").alias("_KL"),
        F.sum("__w_abs_share__").alias("_num_wmape_share"),
        F.sum("__w_abs_share_as__").alias("_num_wmape_share_as"),
        F.sum("__w_under_share__").alias("_num_uape_share")
    ).withColumn(
        # volume (%)
        "wMAPE_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_sum_abs_err")/F.col("_sum_y")).otherwise(0.0) * 100, 4)
    ).withColumn(
        "UAPE_perc",  F.round(F.when(F.col("_sum_y") > 0, F.col("_sum_under") /F.col("_sum_y")).otherwise(0.0) * 100, 4)
    ).withColumn(
        "wMAPE_asym_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_sum_w_abs")/F.col("_sum_y")).otherwise(0.0) * 100, 4)
    ).withColumn(
        # shares (% e pp)
        "SE_pp", F.round(F.col("_SE") * 100, 4)  # 0–200 p.p.
    ).withColumn(
        "wMAPE_share_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_num_wmape_share")/F.col("_sum_y")).otherwise(0.0) * 100, 4)
    ).withColumn(
        "wMAPE_share_asym_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_num_wmape_share_as")/F.col("_sum_y")).otherwise(0.0) * 100, 4)
    ).withColumn(
        "UAPE_share_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_num_uape_share")/F.col("_sum_y")).otherwise(0.0) * 100, 4)
    ).withColumn(
        "KL_divergence", F.when((F.col("_sum_y") > 0) & (F.col("_sum_yhat") > 0), F.col("_KL")).otherwise(F.lit(0.0))
    ).select(
        *(group_cols if group_cols else []),
        # volume
        #"wMAPE_perc","UAPE_perc","wMAPE_asym_perc",
        # shares ponderados por volume
        "SE_pp","wMAPE_share_perc","wMAPE_share_asym_perc","UAPE_share_perc",
        # distância de distribuição
        "KL_divergence"
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

!pip install openpyxl

# Leitura da matriz de merecimento
df_matriz_telefonia_pd = pd.read_excel(
    "dados_analise/(DRP)_MATRIZ_20250825174952.csv.xlsx", 
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
        "year_month", "gemeos", "modelos", "CdFilial",
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

# MAGIC %md
# MAGIC ### 10.1 Métricas Linha a Linha
# MAGIC
# MAGIC Calculamos métricas para cada linha individual para análise detalhada.
# MAGIC **Nota**: Usamos percentuais da matriz (Percentual_matriz_fixa) vs. percentuais reais da demanda (pct_demanda_perc).

# COMMAND ----------

# Parâmetros das métricas
GAMMA = 1.5
EPSILON = 1e-12

# APLICA FILTROS PRIMEIRO
df_filtered = (
    df_matriz_telefonia_metricas
    .filter(F.col("Demanda_total_mes_especie") > 50)  # Seu filtro aqui
    # Adicione outros filtros conforme necessário
)

# DEPOIS calcula métricas sobre os dados filtrados
w_filtered = Window.partitionBy(F.lit(1))  # Janela sobre dados filtrados

df_with_metrics = (
    df_filtered
    # Totais sobre dados FILTRADOS (não globais)
    .withColumn("total_matriz_filtrado", F.sum("Percentual_matriz_fixa").over(w_filtered))
    .withColumn("total_demanda_real_filtrado", F.sum("pct_demanda_perc").over(w_filtered))
    
    # Shares sobre dados filtrados
    .withColumn(
        "p", 
        F.when(F.col("total_demanda_real_filtrado") > 0, 
               F.col("pct_demanda_perc") / F.col("total_demanda_real_filtrado"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "phat", 
        F.when(F.col("total_matriz_filtrado") > 0, 
               F.col("Percentual_matriz_fixa") / F.col("total_matriz_filtrado"))
         .otherwise(F.lit(0.0))
    )
    
    # Métricas linha a linha
    .withColumn("abs_err", F.abs(F.col("pct_demanda_perc") - F.col("Percentual_matriz_fixa")))
    .withColumn("under", F.greatest(F.lit(0.0), F.col("pct_demanda_perc") - F.col("Percentual_matriz_fixa")))
    .withColumn(
        "weight", 
        F.when(F.col("Percentual_matriz_fixa") < F.col("pct_demanda_perc"), 
               F.lit(GAMMA) * F.col("pct_demanda_perc"))
         .otherwise(F.col("pct_demanda_perc"))
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

print("Métricas linha a linha calculadas (sobre dados filtrados):")
df_with_metrics.limit(1).display()


# COMMAND ----------

de_para_filial_cd = (
  spark.table('databox.bcg_comum.supply_base_merecimento_diario')
  .select('CdFilial', 'Cd_primario')
  .distinct()
  .dropna()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Métricas Agregadas
# MAGIC
# MAGIC Calculamos as métricas agregadas para o dataframe inteiro usando a função
# MAGIC `add_allocation_metrics` que criamos no início.
# MAGIC **Nota**: Métricas calculadas comparando percentuais da matriz vs. percentuais reais da demanda.

# COMMAND ----------

# Métricas agregadas sobre dados FILTRADOS
df_agg_metrics = add_allocation_metrics(
    df=df_filtered,  # Usa dados filtrados
    y_col="pct_demanda_perc",     
    yhat_col="Percentual_matriz_fixa",  
    group_cols=None               
)

print("Métricas agregadas calculadas (sobre dados filtrados):")
df_agg_metrics.display()

# COMMAND ----------

# Métricas agregadas sobre dados FILTRADOS
df_agg_metrics = add_allocation_metrics(
    df=df_filtered.join(de_para_filial_cd, how="left", on="CdFilial"),  # Usa dados filtrados
    y_col="pct_demanda_perc",     
    yhat_col="Percentual_matriz_fixa",  
    group_cols=["Cd_primario"]            
).dropna(subset=["Cd_primario"])

print("Métricas agregadas calculadas (sobre dados filtrados):")
df_agg_metrics.display()

# COMMAND ----------

# Métricas agregadas sobre dados FILTRADOS
df_agg_metrics = add_allocation_metrics(
    df=df_filtered.join(de_para_filial_cd, how="left", on="CdFilial"),  # Usa dados filtrados
    y_col="pct_demanda_perc",     
    yhat_col="Percentual_matriz_fixa",  
    group_cols=["CdFilial"]            
).dropna(subset=["CdFilial"])

print("Métricas agregadas calculadas (sobre dados filtrados):")
df_agg_metrics.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Visualização e Análise por CD Primário
# MAGIC 
# MAGIC Criamos visualizações para analisar a distribuição de erros por filial,
# MAGIC mostrando como os erros se distribuem entre diferentes CDs primários
# MAGIC e permitindo identificar padrões de sub/super alocação.

# COMMAND ----------
# MAGIC %md
# MAGIC ### 11.1 Preparação dos Dados para Visualização
# MAGIC 
# MAGIC Preparamos os dados com métricas de erro calculadas por filial,
# MAGIC incluindo informações de CD primário para análise geográfica.

# COMMAND ----------
# Carregar dados de mapeamento de filial para CD primário
df_filial_cd = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .select("CdFilial", "Cd_primario", "NmCidade_UF_primario")
    .distinct()
    .filter(F.col("Cd_primario").isNotNull())
)

# Join com as métricas calculadas para obter informações de CD primário
df_metricas_completo = (
    df_with_metrics
    .join(
        df_filial_cd,
        on="CdFilial",
        how="left"
    )
    .withColumn(
        "erro_percentual", 
        F.col("pct_demanda_perc") - F.col("Percentual_matriz_fixa")
    )
    .withColumn(
        "tipo_erro",
        F.when(F.col("erro_percentual") < 0, "Underallocation")
         .when(F.col("erro_percentual") > 0, "Overallocation")
         .otherwise("Perfeito")
    )
    .withColumn(
        "abs_erro_percentual",
        F.abs(F.col("erro_percentual"))
    )
)

print("Dados preparados para visualização:")
df_metricas_completo.limit(1).display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 11.2 Análise de Erros por CD Primário
# MAGIC 
# MAGIC Analisamos a distribuição de erros por centro de distribuição primário,
# MAGIC ordenando por métrica de erro para identificar padrões.

# COMMAND ----------
# Análise agregada por CD primário
df_erros_por_cd = (
    df_metricas_completo
    .groupBy("Cd_primario", "NmCidade_UF_primario")
    .agg(
        F.count("*").alias("total_filiais"),
        F.avg("erro_percentual").alias("erro_medio"),
        F.stddev("erro_percentual").alias("desvio_padrao_erro"),
        F.avg("abs_erro_percentual").alias("erro_absoluto_medio"),
        F.sum(F.when(F.col("tipo_erro") == "Underallocation", 1).otherwise(0)).alias("filiais_under"),
        F.sum(F.when(F.col("tipo_erro") == "Overallocation", 1).otherwise(0)).alias("filiais_over"),
        F.sum(F.when(F.col("tipo_erro") == "Perfeito", 1).otherwise(0)).alias("filiais_perfeitas")
    )
    .withColumn(
        "pct_under", 
        F.round(F.col("filiais_under") / F.col("total_filiais") * 100, 2)
    )
    .withColumn(
        "pct_over", 
        F.round(F.col("filiais_over") / F.col("total_filiais") * 100, 2)
    )
    .withColumn(
        "pct_perfeitas", 
        F.round(F.col("filiais_perfeitas") / F.col("total_filiais") * 100, 2)
    )
    .orderBy(F.desc("erro_absoluto_medio"))
)

print("Análise de erros por CD primário (ordenado por erro absoluto médio):")
df_erros_por_cd.display()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 11.3 Visualização Scatter Plot por CD Primário
# MAGIC 
# MAGIC Criamos um scatter plot mostrando a distribuição de erros por filial,
# MAGIC com foco na análise por CD primário e visualização da nuvem de pontos
# MAGIC em torno do eixo y = 0.

# COMMAND ----------
# Converter para pandas para visualização
df_metricas_pd = df_metricas_completo.toPandas()

# Instalar plotly se necessário
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    print("Plotly já está disponível")
except ImportError:
    print("Instalando plotly...")
    import subprocess
    subprocess.check_call(["pip", "install", "plotly"])
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

# COMMAND ----------
# MAGIC %md
# MAGIC #### 11.3.1 Scatter Plot Principal - Distribuição de Erros por Filial

# COMMAND ----------
# Scatter plot principal mostrando erro percentual vs. filial
fig_scatter = px.scatter(
    df_metricas_pd,
    x="CdFilial",
    y="erro_percentual",
    color="Cd_primario",
    hover_data=["modelos", "gemeos", "NmCidade_UF_primario"],
    title="Distribuição de Erros de Alocação por Filial - Análise por CD Primário",
    labels={
        "CdFilial": "Código da Filial",
        "erro_percentual": "Erro Percentual (Demanda Real - Matriz)",
        "Cd_primario": "CD Primário"
    },
    color_discrete_sequence=px.colors.qualitative.Set3
)

# Adicionar linha horizontal em y = 0
fig_scatter.add_hline(
    y=0, 
    line_dash="dash", 
    line_color="red",
    annotation_text="Linha de Referência (Sem Erro)"
)

# Adicionar linhas horizontais para zonas de tolerância
fig_scatter.add_hline(y=5, line_dash="dot", line_color="orange", annotation_text="Tolerância +5%")
fig_scatter.add_hline(y=-5, line_dash="dot", line_color="orange", annotation_text="Tolerância -5%")

# Configurar layout
fig_scatter.update_layout(
    height=600,
    showlegend=True,
    legend_title="CD Primário",
    xaxis_title="Código da Filial",
    yaxis_title="Erro Percentual (%)",
    hovermode="closest"
)

# Mostrar o gráfico
fig_scatter.show()

# COMMAND ----------
# MAGIC %md
# MAGIC #### 11.3.2 Scatter Plot por CD Primário - Análise Detalhada (Ordenado por CD e Erro)

# COMMAND ----------
# Criar subplots para cada CD primário
cds_unicos = sorted(df_metricas_pd["Cd_primario"].unique())
n_cds = len(cds_unicos)

# Determinar layout de subplots
if n_cds <= 4:
    cols = 2
    rows = (n_cds + 1) // 2
else:
    cols = 3
    rows = (n_cds + 2) // 3

fig_subplots = make_subplots(
    rows=rows, 
    cols=cols,
    subplot_titles=[f"CD {cd}" for cd in cds_unicos],
    vertical_spacing=0.1,
    horizontal_spacing=0.1
)

# Adicionar scatter plots para cada CD
for i, cd in enumerate(cds_unicos):
    df_cd = df_metricas_pd[df_metricas_pd["Cd_primario"] == cd].copy()
    
    # Ordenar filiais por desvio de alocação decrescente (maior erro absoluto primeiro)
    df_cd = df_cd.sort_values("abs_erro_percentual", ascending=False)
    
    # Criar índice arbitrário para o eixo X (valores não importam)
    df_cd["indice_ordenado"] = range(len(df_cd))
    
    row = (i // cols) + 1
    col = (i % cols) + 1
    
    # Definir cores baseadas no tipo de erro
    cores = []
    for erro in df_cd["erro_percentual"]:
        if erro < 0:  # Underallocation
            cores.append("red")
        elif erro > 0:  # Overallocation
            cores.append("blue")
        else:  # Perfeito
            cores.append("green")
    
    fig_subplots.add_trace(
        go.Scatter(
            x=df_cd["indice_ordenado"],
            y=df_cd["erro_percentual"],
            mode="markers",
            name=f"CD {cd}",
            marker=dict(
                size=10,
                color=cores,
                line=dict(width=1, color="black")
            ),
            text=df_cd["CdFilial"].astype(str),
            hovertemplate="<b>Filial: %{text}</b><br>" +
                         "Erro: %{y:.2f}%<br>" +
                         "Modelo: " + df_cd["modelos"] + "<br>" +
                         "Gêmeos: " + df_cd["gemeos"] + "<br>" +
                         "<extra></extra>"
        ),
        row=row, col=col
    )
    
    # Adicionar linha horizontal em y = 0 para cada subplot
    fig_subplots.add_hline(
        y=0, 
        line_dash="dash", 
        line_color="black",
        line_width=2,
        row=row, col=col
    )
    
    # Adicionar linhas de tolerância
    fig_subplots.add_hline(y=5, line_dash="dot", line_color="orange", line_width=1, row=row, col=col)
    fig_subplots.add_hline(y=-5, line_dash="dot", line_color="orange", line_width=1, row=row, col=col)

# Configurar layout
fig_subplots.update_layout(
    height=300 * rows,
    title_text="Distribuição de Erros por CD Primário - Filiais Ordenadas por Desvio Decrescente",
    showlegend=False
)

# Atualizar eixos - omitir valores do eixo X
fig_subplots.update_xaxes(
    title_text="",  # Sem título
    showticklabels=False,  # Omitir valores
    showgrid=False  # Sem grid no eixo X
)
fig_subplots.update_yaxes(title_text="Erro Percentual (%)")

fig_subplots.show()

# COMMAND ----------
# MAGIC %md
# MAGIC #### 11.3.3 Scatter Plot Principal - Filiais Ordenadas por CD e Erro

# COMMAND ----------
# Preparar dados ordenados para o scatter plot principal
df_metricas_ordenado = df_metricas_pd.copy()

# Criar índice ordenado por CD primário e erro percentual
df_metricas_ordenado["indice_ordenado"] = 0
posicao_atual = 0

for cd in sorted(df_metricas_ordenado["Cd_primario"].unique()):
    df_cd = df_metricas_ordenado[df_metricas_ordenado["Cd_primario"] == cd].copy()
    # Ordenar por desvio decrescente (maior erro absoluto primeiro)
    df_cd = df_cd.sort_values("abs_erro_percentual", ascending=False)
    
    # Atribuir posições ordenadas
    for idx, row in df_cd.iterrows():
        df_metricas_ordenado.loc[idx, "indice_ordenado"] = posicao_atual
        posicao_atual += 1

# Definir cores baseadas no tipo de erro
cores_principais = []
for erro in df_metricas_ordenado["erro_percentual"]:
    if erro < 0:  # Underallocation
        cores_principais.append("red")
    elif erro > 0:  # Overallocation
        cores_principais.append("blue")
    else:  # Perfeito
        cores_principais.append("green")

# Scatter plot principal com filiais ordenadas
fig_ordenado = px.scatter(
    df_metricas_ordenado,
    x="indice_ordenado",
    y="erro_percentual",
    color="Cd_primario",
    hover_data=["CdFilial", "modelos", "gemeos", "NmCidade_UF_primario"],
    title="Distribuição de Erros por Filial - Ordenadas por CD Primário e Desvio Decrescente",
    labels={
        "indice_ordenado": "Posição da Filial (Ordenada)",
        "erro_percentual": "Erro Percentual (Demanda Real - Matriz)",
        "Cd_primario": "CD Primário"
    },
    color_discrete_sequence=px.colors.qualitative.Set3
)

# Atualizar cores dos marcadores para vermelho/azul baseado no erro
fig_ordenado.update_traces(
    marker=dict(
        size=8,
        color=cores_principais,
        line=dict(width=1, color="black")
    )
)

# Adicionar linha horizontal em y = 0
fig_ordenado.add_hline(
    y=0, 
    line_dash="dash", 
    line_color="black",
    line_width=2,
    annotation_text="Linha de Referência (Sem Erro)"
)

# Adicionar linhas horizontais para zonas de tolerância
fig_ordenado.add_hline(y=5, line_dash="dot", line_color="orange", line_width=1, annotation_text="Tolerância +5%")
fig_ordenado.add_hline(y=-5, line_dash="dot", line_color="orange", line_width=1, annotation_text="Tolerância -5%")

# Configurar layout
fig_ordenado.update_layout(
    height=600,
    showlegend=True,
    legend_title="CD Primário",
    xaxis_title="",  # Sem título no eixo X
    yaxis_title="Erro Percentual (%)",
    hovermode="closest"
)

# Omitir valores do eixo X
fig_ordenado.update_xaxes(
    showticklabels=False,  # Omitir valores
    showgrid=False  # Sem grid no eixo X
)

# Mostrar o gráfico
fig_ordenado.show()

# COMMAND ----------
# MAGIC %md
# MAGIC #### 11.3.4 Box Plot - Distribuição de Erros por CD Primário

# COMMAND ----------
# Box plot mostrando distribuição de erros por CD primário
fig_box = px.box(
    df_metricas_pd,
    x="Cd_primario",
    y="erro_percentual",
    color="Cd_primario",
    title="Distribuição de Erros de Alocação por CD Primário",
    title_x=0.5,
    labels={
        "Cd_primario": "CD Primário",
        "erro_percentual": "Erro Percentual (%)"
    }
)

# Adicionar linha horizontal em y = 0
fig_box.add_hline(
    y=0, 
    line_dash="dash", 
    line_color="red",
    annotation_text="Linha de Referência (Sem Erro)"
)

# Configurar layout
fig_box.update_layout(
    height=500,
    showlegend=False,
    xaxis_title="CD Primário",
    yaxis_title="Erro Percentual (%)"
)

fig_box.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 11.4 Resumo da Análise Visual

# COMMAND ----------
print("=== RESUMO DA ANÁLISE VISUAL ===")
print()

# Estatísticas gerais
total_filiais = len(df_metricas_pd)
filiais_under = len(df_metricas_pd[df_metricas_pd["erro_percentual"] < 0])
filiais_over = len(df_metricas_pd[df_metricas_pd["erro_percentual"] > 0])
filiais_perfeitas = len(df_metricas_pd[df_metricas_pd["erro_percentual"] == 0])

print(f"📊 ESTATÍSTICAS GERAIS:")
print(f"   • Total de Filiais: {total_filiais}")
print(f"   • Filiais com Underallocation: {filiais_under} ({filiais_under/total_filiais*100:.1f}%)")
print(f"   • Filiais com Overallocation: {filiais_over} ({filiais_over/total_filiais*100:.1f}%)")
print(f"   • Filiais Perfeitas: {filiais_perfeitas} ({filiais_perfeitas/total_filiais*100:.1f}%)")
print()

# Análise por CD primário
print(f"🏢 ANÁLISE POR CD PRIMÁRIO:")
for _, row in df_erros_por_cd.toPandas().iterrows():
    cd = row["Cd_primario"]
    cidade = row["NmCidade_UF_primario"]
    erro_medio = row["erro_medio"]
    erro_abs = row["erro_absoluto_medio"]
    pct_under = row["pct_under"]
    pct_over = row["pct_over"]
    
    print(f"   • CD {cd} ({cidade}):")
    print(f"     - Erro Médio: {erro_medio:.2f}%")
    print(f"     - Erro Absoluto Médio: {erro_abs:.2f}%")
    print(f"     - Underallocation: {pct_under:.1f}% das filiais")
    print(f"     - Overallocation: {pct_over:.1f}% das filiais")
    print()

# COMMAND ----------
# MAGIC %md
# MAGIC ### 12.1 Mapeamento de Produtos de Tela

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
# MAGIC ### 12.2 Dados de Vendas e Estoque de Telas

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
# MAGIC ### 12.3 Agregação de Dados de Telas por CD (Incompleto)

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
# MAGIC ## 13. Resumo da Análise
# MAGIC
# MAGIC **Análise Concluída:**
# MAGIC - ✅ Dados de telefonia carregados e processados
# MAGIC - ✅ Mapeamentos de produtos aplicados
# MAGIC - ✅ Agregações por filial calculadas
# MAGIC - ✅ Matriz de merecimento carregada e validada
# MAGIC - ✅ Métricas linha a linha calculadas
# MAGIC - ✅ Métricas agregadas calculadas
# MAGIC - ✅ Visualizações por CD primário criadas
# MAGIC
# MAGIC **Próximos Passos Recomendados:**
# MAGIC 1. Análise detalhada das métricas por modelo/gêmeos
# MAGIC 2. Identificação de padrões de sub/super alocação por região
# MAGIC 3. Recomendações para otimização da matriz por CD primário
# MAGIC 4. Completar análise de telas se necessário
