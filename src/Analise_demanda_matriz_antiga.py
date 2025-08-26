# Databricks notebook source
# MAGIC %md
# MAGIC # Análise de Efetividade da Matriz de Merecimento - Telefonia
# MAGIC
# MAGIC Este notebook analisa a efetividade da matriz de merecimento atual comparando as alocações previstas 
# MAGIC com o comportamento real de vendas e demanda para produtos de telefonia celular.
# MAGIC
# MAGIC **Objetivo**: Identificar gaps entre alocações previstas e realidade para otimização da matriz futura.
# MAGIC **Escopo**: Apenas produtos de telefonia celular no nível de filial (loja).
# MAGIC
# MAGIC **Métricas de Avaliação:**
# MAGIC - **wMAPE**: Weighted Mean Absolute Percentage Error ponderado por volume
# MAGIC - **Cross Entropy**: Entropia cruzada para divergência de distribuições
# MAGIC - **Share Error (SE)**: Erro na distribuição de participações
# MAGIC - **KL Divergence**: Divergência de Kullback-Leibler para comparação

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
# MAGIC Esta função calcula métricas robustas para avaliar a qualidade das alocações:
# MAGIC - **wMAPE**: Weighted Mean Absolute Percentage Error ponderado por volume
# MAGIC - **SE (Share Error)**: Erro na distribuição de participações entre filiais
# MAGIC - **Cross Entropy**: Medida de divergência entre distribuições reais e previstas
# MAGIC - **KL Divergence**: Divergência de Kullback-Leibler para comparação de distribuições
# MAGIC
# MAGIC **Vantagens das métricas:**
# MAGIC - wMAPE é padrão da indústria e bem interpretável
# MAGIC - Cross Entropy é padrão em machine learning para avaliação de distribuições
# MAGIC - Foco em métricas fundamentais e robustas

# COMMAND ----------

from pyspark.sql import functions as F, Window
from typing import List, Optional

def add_allocation_metrics(
    df,
    y_col: str,
    yhat_col: str,
    group_cols: Optional[List[str]] = None,
    epsilon: float = 1e-12
):
    if group_cols is None:
        group_cols = []

    w = Window.partitionBy(*group_cols) if group_cols else Window.partitionBy(F.lit(1))
    y, yhat = F.col(y_col).cast("double"), F.col(yhat_col).cast("double")

    # Totais
    Y_tot    = F.sum(y).over(w)
    Yhat_tot = F.sum(yhat).over(w)

    # Shares
    p    = F.when(Y_tot    > 0, y    / Y_tot   ).otherwise(F.lit(0.0))
    phat = F.when(Yhat_tot > 0, yhat / Yhat_tot).otherwise(F.lit(0.0))

    # Erros
    abs_err = F.abs(y - yhat)
    mae_weighted_by_y = abs_err * y

    # sMAPE componentes
    smape_num = 2.0 * abs_err
    smape_den = F.when((y + yhat) > 0, y + yhat).otherwise(F.lit(0.0))

    # Distribuição
    cross_entropy_term = F.when((p > 0) & (phat > 0), -p * F.log(phat + F.lit(epsilon))).otherwise(F.lit(0.0))
    kl_term            = F.when((p > 0) & (phat > 0),  p * F.log((p + F.lit(epsilon)) / (phat + F.lit(epsilon)))).otherwise(F.lit(0.0))

    # Share
    abs_err_share = F.abs(p - phat)
    wmape_share   = abs_err_share * y  # ponderado por volume real

    base = (df
        .withColumn("__y__", y).withColumn("__yhat__", yhat)
        .withColumn("__p__", p).withColumn("__phat__", phat)
        .withColumn("__abs_err__", abs_err)
        .withColumn("__mae_w_by_y__", mae_weighted_by_y)
        .withColumn("__smape_num__", smape_num)
        .withColumn("__smape_den__", smape_den)
        .withColumn("__kl_term__", kl_term)
        .withColumn("__cross_entropy_term__", cross_entropy_term)
        .withColumn("__abs_err_share__", abs_err_share)
        .withColumn("__wmape_share__", wmape_share)
    )

    agg = base.groupBy(*group_cols) if group_cols else base.groupBy()
    res = (agg.agg(
            F.sum("__abs_err__").alias("_sum_abs_err"),
            F.sum("__mae_w_by_y__").alias("_sum_mae_w_by_y"),
            F.sum("__y__").alias("_sum_y"),
            F.sum("__yhat__").alias("_sum_yhat"),
            F.sum("__smape_num__").alias("_sum_smape_num"),
            F.sum("__smape_den__").alias("_sum_smape_den"),
            F.sum(F.abs(F.col("__p__") - F.col("__phat__"))).alias("_SE"),
            F.sum("__kl_term__").alias("_KL"),
            F.sum("__cross_entropy_term__").alias("_cross_entropy"),
            F.sum("__wmape_share__").alias("_num_wmape_share")
        )
        # WMAPE (%)
        .withColumn("wMAPE_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_sum_abs_err")/F.col("_sum_y")*100).otherwise(0.0), 4))
        # sMAPE (%)
        .withColumn("sMAPE_perc", F.round(F.when(F.col("_sum_smape_den") > 0, F.col("_sum_smape_num")/F.col("_sum_smape_den")*100).otherwise(0.0), 4))
        # MAE ponderado
        .withColumn("MAE_weighted_by_y", F.round(F.when(F.col("_sum_y") > 0, F.col("_sum_mae_w_by_y")/F.col("_sum_y")).otherwise(0.0), 4))
        # Shares
        .withColumn("SE_pp", F.round(F.col("_SE") * 100, 4))
        .withColumn("wMAPE_share_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_num_wmape_share")/F.col("_sum_y")*100).otherwise(0.0), 4))
        # Distribuição
        .withColumn("Cross_entropy", F.when(F.col("_sum_y") > 0, F.col("_cross_entropy")).otherwise(F.lit(0.0)))
        .withColumn("KL_divergence", F.when((F.col("_sum_y") > 0) & (F.col("_sum_yhat") > 0), F.col("_KL")).otherwise(F.lit(0.0)))
        .select(*(group_cols if group_cols else []),
                "wMAPE_perc","sMAPE_perc","MAE_weighted_by_y",
                "SE_pp","wMAPE_share_perc",
                "Cross_entropy","KL_divergence")
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
    .groupBy("year_month", "gemeos", "CdFilial")
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
w = Window.partitionBy("year_month", "gemeos")

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
        "year_month", "gemeos", "CdFilial",
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

df_pct_telefonia.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("databox.bcg_comum.supply_calculo_percentual_demanda_telefonia")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Carregamento da Matriz de Merecimento Atual
# MAGIC
# MAGIC Carregamos a matriz de merecimento atual para comparar com os dados reais.
# MAGIC Filtramos apenas lojas offline (não CDs) e aplicamos os mapeamentos de produtos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Instalação de Dependências
# MAGIC
# MAGIC Instalamos a biblioteca openpyxl necessária para leitura de arquivos Excel.

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Leitura da Matriz de Merecimento
# MAGIC
# MAGIC Carregamos a matriz de merecimento atual para comparar com os dados reais.
# MAGIC Filtramos apenas lojas offline (não CDs) e aplicamos os mapeamentos de produtos.

# COMMAND ----------

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

df_matriz_telefonia.count()

# COMMAND ----------

df_matriz_telefonia_metricas = (
    df_matriz_telefonia
    .join(
        df_pct_telefonia,
        how="inner",
        on=["gemeos", "CdFilial"]
    )
    .select(
        "year_month", "gemeos",  "CdFilial",
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
# MAGIC
# MAGIC **Métricas Principais:**
# MAGIC - **wMAPE**: Weighted Mean Absolute Percentage Error (nível volume e shares)
# MAGIC - **SE (Share Error)**: Erro na distribuição de participações
# MAGIC - **Cross Entropy**: Divergência entre distribuições reais e previstas
# MAGIC - **KL Divergence**: Medida de divergência para comparação

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 Métricas Linha a Linha
# MAGIC
# MAGIC Calculamos métricas para cada linha individual para análise detalhada.
# MAGIC **Nota**: Usamos percentuais da matriz (Percentual_matriz_fixa) vs. percentuais reais da demanda (pct_demanda_perc).
# MAGIC
# MAGIC **Métricas Calculadas:**
# MAGIC - **Erro Absoluto**: Diferença absoluta entre matriz e demanda real
# MAGIC - **wMAPE**: Erro ponderado pelo volume real da filial
# MAGIC - **Cross Entropy**: Divergência entre distribuições de participação

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
        "year_month", "gemeos", "CdFilial",
        "Percentual_matriz_fixa", "pct_vendas_perc", "pct_demanda_perc",
        "QtMercadoria", "QtdDemanda", 
        "Qt_total_mes_especie", "Demanda_total_mes_especie",
        "abs_err", "under", "weight", "w_abs", "p", "phat", "kl_term"
    )
    .dropDuplicates()
)

print("Métricas linha a linha calculadas (sobre dados filtrados):")
df_with_metrics.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1.1 Visualização das Métricas Linha a Linha
# MAGIC
# MAGIC Exibimos as métricas calculadas ordenadas por demanda total para identificar
# MAGIC os produtos com maior volume e suas respectivas métricas de erro.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1.2 Carregamento do Mapeamento Filial-CD
# MAGIC
# MAGIC Carregamos o mapeamento entre filiais e CDs primários para análise
# MAGIC agregada por centro de distribuição.

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
# MAGIC
# MAGIC **Métricas Agregadas por CD:**
# MAGIC - **wMAPE_perc**: Weighted Mean Absolute Percentage Error (nível volume)
# MAGIC - **wMAPE_share_perc**: Weighted Mean Absolute Percentage Error (nível shares)
# MAGIC - **SE_pp**: Share Error em pontos percentuais
# MAGIC - **Cross_entropy**: Entropia cruzada entre distribuições
# MAGIC - **KL_divergence**: Divergência de Kullback-Leibler

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

# MAGIC %md
# MAGIC ### 10.2.1 Métricas Agregadas por CD Primário
# MAGIC
# MAGIC Calculamos as métricas agregadas por centro de distribuição para identificar
# MAGIC quais CDs têm melhor performance na matriz de merecimento.

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

# MAGIC %md
# MAGIC ### 10.2.2 Métricas Agregadas por Filial e CD
# MAGIC
# MAGIC Calculamos as métricas agregadas por filial e CD para análise mais granular
# MAGIC da performance da matriz de merecimento.

# COMMAND ----------

# Métricas agregadas sobre dados FILTRADOS
df_agg_metrics = add_allocation_metrics(
    df=df_filtered.join(de_para_filial_cd, how="left", on="CdFilial"),  # Usa dados filtrados
    y_col="pct_demanda_perc",     
    yhat_col="Percentual_matriz_fixa",  
    group_cols=["CdFilial", "Cd_primario"]            
).dropna(subset=["CdFilial"])

print("Métricas agregadas calculadas (sobre dados filtrados):")
df_agg_metrics.display()

# COMMAND ----------

df_agg_metrics.agg(F.mean("sMAPE_perc")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Visualização dos Resultados
# MAGIC
# MAGIC Criamos visualizações para analisar a distribuição das métricas de erro
# MAGIC por centro de distribuição, permitindo identificar padrões e outliers.

# COMMAND ----------

# Converter colunas necessárias para Pandas
df_plot = df_agg_metrics.select("Cd_primario", "wMAPE_share_perc").toPandas()

import plotly.express as px

fig = px.box(
    df_plot,
    x="Cd_primario",
    y="wMAPE_share_perc",
    points=False,  # remove pontos/outliers
    title="Boxplot de wMAPE_share_perc por Cd_primario",
    template="plotly_white"
)

fig.update_traces(marker=dict(size=4, opacity=0.6, color="royalblue"))
fig.update_layout(
    yaxis_title="wMAPE Share (%)",
    xaxis_title="Cd_primario",
    title_font=dict(size=18),
    yaxis=dict(showgrid=True, gridcolor="lightgrey"),
    xaxis=dict(showgrid=False)
)

fig.show()
