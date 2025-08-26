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
# MAGIC ## 11. Visualização: Scatter Plot de Erro Percentual por Filial
# MAGIC
# MAGIC Criamos um scatter plot onde cada ponto representa uma filial, mostrando:
# MAGIC - **Eixo X**: Ordenação arbitrária baseada em Cd_primario (não tem significado específico)
# MAGIC - **Eixo Y**: Erro percentual agregado da filial (agregando todos os produtos)
# MAGIC
# MAGIC **Objetivo**: Identificar visualmente quais filiais têm maior discrepância entre matriz prevista e realidade.

# COMMAND ----------

# Cálculo do erro percentual agregado por filial
df_erro_por_filial = (
    df_filtered
    .join(de_para_filial_cd, how="left", on="CdFilial")
    .dropna(subset=["Cd_primario"])
    .groupBy("CdFilial", "Cd_primario")
    .agg(
        F.sum("pct_demanda_perc").alias("demanda_real_total"),
        F.sum("Percentual_matriz_fixa").alias("matriz_prevista_total"),
        F.count("*").alias("qtd_produtos")
    )
    .withColumn(
        "erro_percentual", 
        F.round(
            F.abs(F.col("demanda_real_total") - F.col("matriz_prevista_total")) / 
            F.greatest(F.col("demanda_real_total"), F.lit(0.01)) * 100, 2
        )
    )
    .orderBy("Cd_primario", "erro_percentual")
)

print("Erro percentual agregado por filial:")
df_erro_por_filial.display()

# COMMAND ----------

# Preparação dos dados para o scatter plot
df_scatter_plot = (
    df_erro_por_filial
    .withColumn(
        "indice_ordenacao", 
        F.monotonically_increasing_id()
    )
    .select(
        "indice_ordenacao",
        "CdFilial", 
        "Cd_primario",
        "erro_percentual",
        "demanda_real_total",
        "matriz_prevista_total",
        "qtd_produtos"
    )
    .orderBy("indice_ordenacao")
)

print("Dados preparados para scatter plot:")
df_scatter_plot.display()

# COMMAND ----------

# Conversão para pandas para visualização
df_scatter_pandas = df_scatter_plot.toPandas()

# Criação do scatter plot
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Scatter plot principal
fig = go.Figure()

# Adiciona os pontos (filiais)
fig.add_trace(
    go.Scatter(
        x=df_scatter_pandas['indice_ordenacao'],
        y=df_scatter_pandas['erro_percentual'],
        mode='markers',
        marker=dict(
            size=8,
            color=df_scatter_pandas['erro_percentual'],
            colorscale='Reds',
            showscale=True,
            colorbar=dict(title="Erro %")
        ),
        text=df_scatter_pandas['CdFilial'].astype(str),
        hovertemplate=(
            '<b>Filial:</b> %{text}<br>' +
            '<b>Cd_primario:</b> ' + df_scatter_pandas['Cd_primario'].astype(str) + '<br>' +
            '<b>Erro %:</b> %{y:.2f}%<br>' +
            '<b>Demanda Real:</b> ' + df_scatter_pandas['demanda_real_total'].round(2).astype(str) + '%<br>' +
            '<b>Matriz Prevista:</b> ' + df_scatter_pandas['matriz_prevista_total'].round(2).astype(str) + '%<br>' +
            '<b>Qtd Produtos:</b> ' + df_scatter_pandas['qtd_produtos'].astype(str) + '<br>' +
            '<extra></extra>'
        ),
        name="Filiais"
    )
)

# Linha de referência para erro médio
erro_medio = df_scatter_pandas['erro_percentual'].mean()
fig.add_hline(
    y=erro_medio, 
    line_dash="dash", 
    line_color="gray",
    annotation_text=f"Erro Médio: {erro_medio:.2f}%",
    annotation_position="top right"
)

# Configuração do layout
fig.update_layout(
    title={
        'text': 'Erro Percentual da Matriz de Merecimento por Filial',
        'x': 0.5,
        'xanchor': 'center',
        'font': {'size': 20}
    },
    xaxis_title="Índice de Ordenação (Cd_primario + Erro)",
    yaxis_title="Erro Percentual (%)",
    plot_bgcolor="#F8F8FF",
    paper_bgcolor="#F8F8FF",
    font=dict(size=12),
    height=600,
    showlegend=False
)

# Configuração dos eixos
fig.update_xaxes(
    showgrid=True, 
    gridwidth=1, 
    gridcolor='lightgray',
    zeroline=False
)
fig.update_yaxes(
    showgrid=True, 
    gridwidth=1, 
    gridcolor='lightgray',
    zeroline=True,
    zerolinecolor='black'
)

# Exibe o gráfico
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise do Scatter Plot
# MAGIC
# MAGIC **Interpretação dos resultados:**
# MAGIC - **Pontos altos**: Filiais com maior erro percentual (maior discrepância entre matriz e realidade)
# MAGIC - **Pontos baixos**: Filiais com menor erro percentual (matriz mais alinhada com realidade)
# MAGIC - **Linha tracejada**: Erro médio para referência
# MAGIC - **Cor dos pontos**: Escala de vermelho (mais escuro = maior erro)
# MAGIC
# MAGIC **Ações recomendadas:**
# MAGIC 1. **Filiais com erro > 2x média**: Revisar alocações prioritariamente
# MAGIC 2. **Filiais com erro < 0.5x média**: Considerar como benchmark
# MAGIC 3. **Padrões geográficos**: Analisar se erros altos se concentram em regiões específicas

# COMMAND ----------

# Estatísticas resumidas por faixa de erro
df_resumo_erro = (
    df_scatter_pandas
    .assign(
        faixa_erro=lambda x: pd.cut(
            x['erro_percentual'], 
            bins=[0, erro_medio/2, erro_medio, erro_medio*2, float('inf')],
            labels=['Baixo (<50% média)', 'Médio-Baixo', 'Médio-Alto', 'Alto (>200% média)']
        )
    )
    .groupby('faixa_erro')
    .agg({
        'CdFilial': 'count',
        'erro_percentual': ['mean', 'std'],
        'qtd_produtos': 'mean'
    })
    .round(2)
)

df_resumo_erro.columns = ['Qtd_Filiais', 'Erro_Medio', 'Erro_Desvio', 'Produtos_Medio']
df_resumo_erro = df_resumo_erro.reset_index()

print("Resumo por faixa de erro:")
display(df_resumo_erro)
