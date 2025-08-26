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
    
    # L1 Distance (Manhattan distance) - erro absoluto ponderado por volume
    l1_distance = abs_err * y
    
    # Cross Entropy - medida de divergência entre distribuições
    cross_entropy_term = F.when(
        (p > 0) & (phat > 0), 
        -p * F.log(phat + F.lit(epsilon))
    ).otherwise(F.lit(0.0))
    
    # KL Divergence (mantido para comparação)
    kl_term = F.when(
        (p > 0) & (phat > 0), 
        p * F.log((p + F.lit(epsilon)) / (phat + F.lit(epsilon)))
    ).otherwise(F.lit(0.0))

    # Termos (share ponderado por volume)
    abs_err_share = F.abs(p - phat)
    l1_share = abs_err_share * y  # L1 distance para shares

    base = (df
        .withColumn("__y__", y).withColumn("__yhat__", yhat)
        .withColumn("__p__", p).withColumn("__phat__", phat)
        .withColumn("__abs_err__", abs_err).withColumn("__l1_distance__", l1_distance)
        .withColumn("__kl_term__", kl_term)
        .withColumn("__cross_entropy_term__", cross_entropy_term)
        .withColumn("__abs_err_share__", abs_err_share)
        .withColumn("__l1_share__", l1_share)
    )

    agg = base.groupBy(*group_cols) if group_cols else base.groupBy()

    res = agg.agg(
        # volume
        F.sum("__abs_err__").alias("_sum_abs_err"),
        F.sum("__l1_distance__").alias("_sum_l1_distance"),
        F.sum("__y__").alias("_sum_y"),
        F.sum("__yhat__").alias("_sum_yhat"),
        # shares
        F.sum(F.abs(F.col("__p__") - F.col("__phat__"))).alias("_SE"),
        F.sum("__kl_term__").alias("_KL"),
        F.sum("__cross_entropy_term__").alias("_cross_entropy"),
        F.sum("__l1_share__").alias("_num_l1_share")
    ).withColumn(
        # volume (%)
        "L1_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_sum_l1_distance")/F.col("_sum_y")).otherwise(0.0) * 100, 4)
    ).withColumn(
        # shares (% e pp)
        "SE_pp", F.round(F.col("_SE") * 100, 4)  # 0–200 p.p.
    ).withColumn(
        "L1_share_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_num_l1_share")/F.col("_sum_y")).otherwise(0.0) * 100, 4)
    ).withColumn(
        "Cross_entropy", F.when(F.col("_sum_y") > 0, F.col("_cross_entropy")).otherwise(F.lit(0.0))
    ).withColumn(
        "KL_divergence", F.when((F.col("_sum_y") > 0) & (F.col("_sum_yhat") > 0), F.col("_KL")).otherwise(F.lit(0.0))
    ).select(
        *(group_cols if group_cols else []),
        # volume
        "L1_perc",
        # shares ponderados por volume
        "SE_pp", "L1_share_perc",
        # distância de distribuição
        "Cross_entropy", "KL_divergence"
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
        "erro_percentual_abs", 
        F.round(
            F.abs(F.col("demanda_real_total") - F.col("matriz_prevista_total")) / 
            F.greatest(F.col("demanda_real_total"), F.lit(0.01)) * 100, 2
        )
    )
    .withColumn(
        "erro_percentual_sinal", 
        F.round(
            (F.col("demanda_real_total") - F.col("matriz_prevista_total")) / 
            F.greatest(F.col("demanda_real_total"), F.lit(0.01)) * 100, 2
        )
    )
    .orderBy("Cd_primario", "erro_percentual_abs")
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
        "erro_percentual_abs",
        "erro_percentual_sinal",
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

# Adiciona os pontos (filiais) com cores baseadas no sinal do erro
fig.add_trace(
    go.Scatter(
        x=df_scatter_pandas['indice_ordenacao'],
        y=df_scatter_pandas['erro_percentual_sinal'],
        mode='markers',
        marker=dict(
            size=8,
            color=df_scatter_pandas['erro_percentual_sinal'],
            colorscale='RdBu',  # Vermelho para negativo, Azul para positivo
            showscale=True,
            colorbar=dict(title="Erro % com Sinal"),
            cmin=df_scatter_pandas['erro_percentual_sinal'].min(),
            cmax=df_scatter_pandas['erro_percentual_sinal'].max()
        ),
        text=df_scatter_pandas['CdFilial'].astype(str),
        hovertemplate=(
            '<b>Filial:</b> %{text}<br>' +
            '<b>Cd_primario:</b> ' + df_scatter_pandas['Cd_primario'].astype(str) + '<br>' +
            '<b>Erro % com Sinal:</b> %{y:.2f}%<br>' +
            '<b>Erro % Absoluto:</b> ' + df_scatter_pandas['erro_percentual_abs'].round(2).astype(str) + '%<br>' +
            '<b>Demanda Real:</b> ' + df_scatter_pandas['demanda_real_total'].round(2).astype(str) + '%<br>' +
            '<b>Matriz Prevista:</b> ' + df_scatter_pandas['matriz_prevista_total'].round(2).astype(str) + '%<br>' +
            '<b>Qtd Produtos:</b> ' + df_scatter_pandas['qtd_produtos'].astype(str) + '<br>' +
            '<extra></extra>'
        ),
        name="Filiais"
    )
)

# Linha de referência para erro médio com sinal
erro_medio_sinal = df_scatter_pandas['erro_percentual_sinal'].mean()
fig.add_hline(
    y=erro_medio_sinal, 
    line_dash="dash", 
    line_color="gray",
    annotation_text=f"Erro Médio: {erro_medio_sinal:.2f}%",
    annotation_position="top right"
)

# Linha de referência para zero (sem erro)
fig.add_hline(
    y=0, 
    line_dash="dot", 
    line_color="black",
    annotation_text="Sem Erro (0%)",
    annotation_position="bottom right"
)

# Configuração do layout
fig.update_layout(
    title={
        'text': 'Erro Percentual da Matriz de Merecimento por Filial (com Sinal)',
        'x': 0.5,
        'xanchor': 'center',
        'font': {'size': 20}
    },
    xaxis_title="Índice de Ordenação (Cd_primario + Erro)",
    yaxis_title="Erro Percentual com Sinal (%)",
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
# MAGIC - **Pontos acima de 0**: Filiais onde a matriz **SUBESTIMA** a demanda real (matriz prevê menos do que realidade)
# MAGIC - **Pontos abaixo de 0**: Filiais onde a matriz **SUPERESTIMA** a demanda real (matriz prevê mais do que realidade)
# MAGIC - **Linha tracejada cinza**: Erro médio para referência
# MAGIC - **Linha pontilhada preta**: Linha de zero (sem erro)
# MAGIC - **Cor dos pontos**: Escala RdBu (vermelho = negativo/superestimação, azul = positivo/subestimação)
# MAGIC
# MAGIC **Ações recomendadas:**
# MAGIC 1. **Filiais com erro positivo alto**: Aumentar alocações na matriz (demanda real > prevista)
# MAGIC 2. **Filiais com erro negativo alto**: Reduzir alocações na matriz (demanda real < prevista)
# MAGIC 3. **Filiais próximas de zero**: Matriz bem calibrada (manter como está)
# MAGIC 4. **Padrões geográficos**: Analisar se erros se concentram em regiões específicas

# COMMAND ----------

# Estatísticas resumidas por faixa de erro
df_resumo_erro = (
    df_scatter_pandas
    .assign(
        faixa_erro=lambda x: pd.cut(
            x['erro_percentual_sinal'], 
            bins=[float('-inf'), -erro_medio_sinal, -erro_medio_sinal/2, 0, erro_medio_sinal/2, erro_medio_sinal, float('inf')],
            labels=['Muito Negativo', 'Negativo', 'Levemente Negativo', 'Levemente Positivo', 'Positivo', 'Muito Positivo']
        )
    )
    .groupby('faixa_erro')
    .agg({
        'CdFilial': 'count',
        'erro_percentual_sinal': ['mean', 'std'],
        'erro_percentual_abs': 'mean',
        'qtd_produtos': 'mean'
    })
    .round(2)
)

df_resumo_erro.columns = ['Qtd_Filiais', 'Erro_Medio_Sinal', 'Erro_Desvio', 'Erro_Medio_Abs', 'Produtos_Medio']
df_resumo_erro = df_resumo_erro.reset_index()

print("Resumo por faixa de erro:")
display(df_resumo_erro)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Análise de Heterocedasticidade: Erros ao Nível de Loja vs. Agregação por CD
# MAGIC
# MAGIC **Problema identificado**: A agregação por CD pode mascarar problemas reais ao nível de loja individual.
# MAGIC **Objetivo**: Demonstrar que embora os wMAPEs agregados por CD pareçam "aceitáveis", 
# MAGIC ao nível de loja existem erros significativos em todas as direções (heterocedasticidade).
# MAGIC
# MAGIC **Hipótese**: Os erros se "cancelam" na agregação, mas individualmente são problemáticos.

# COMMAND ----------

# Cálculo de métricas ao nível de loja individual (sem agregação por CD)
df_metricas_loja_individual = add_allocation_metrics(
    df=df_filtered.join(de_para_filial_cd, how="left", on="CdFilial"),
    y_col="pct_demanda_perc",     
    yhat_col="Percentual_matriz_fixa",  
    group_cols=["CdFilial", "Cd_primario"]  # Agrupa por loja individual
).dropna(subset=["CdFilial", "Cd_primario"])

print("Métricas calculadas ao nível de loja individual:")
df_metricas_loja_individual.display()

# COMMAND ----------

# Comparação: Métricas agregadas por CD vs. Distribuição das métricas ao nível de loja
df_comparacao_agregacao = (
    df_metricas_loja_individual
    .groupBy("Cd_primario")
    .agg(
        F.avg("wMAPE_share_perc").alias("wMAPE_medio_CD"),
        F.stddev("wMAPE_share_perc").alias("wMAPE_desvio_CD"),
        F.min("wMAPE_share_perc").alias("wMAPE_min_CD"),
        F.max("wMAPE_share_perc").alias("wMAPE_max_CD"),
        F.count("*").alias("qtd_lojas_CD")
    )
    .orderBy("wMAPE_medio_CD")
)

print("Comparação: Agregação por CD vs. Distribuição ao nível de loja:")
df_comparacao_agregacao.display()

# COMMAND ----------

# Preparação dos dados para visualização da heterocedasticidade
df_heterocedasticidade = (
    df_metricas_loja_individual
    .select(
        "CdFilial", "Cd_primario", 
        "wMAPE_share_perc", "SE_pp", "KL_divergence"
    )
    .toPandas()
)

# Criação do gráfico de heterocedasticidade
fig_hetero = go.Figure()

# Adiciona os pontos de cada loja, coloridos por CD
for cd_primario in df_heterocedasticidade['Cd_primario'].unique():
        df_cd = df_heterocedasticidade[df_heterocedasticidade['Cd_primario'] == cd_primario]
        
        fig_hetero.add_trace(
            go.Scatter(
                x=df_cd['L1_share_perc'],
                y=df_cd['SE_pp'],
                mode='markers',
                marker=dict(
                    size=8,
                    symbol='circle',
                    opacity=0.7
                ),
                text=df_cd['CdFilial'].astype(str),
                hovertemplate=(
                    '<b>Loja:</b> %{text}<br>' +
                    '<b>CD Primário:</b> ' + str(cd_primario) + '<br>' +
                    '<b>L1 Share:</b> %{x:.4f}%<br>' +
                    '<b>SE (pp):</b> %{y:.2f}<br>' +
                    '<extra></extra>'
                ),
                name=f'CD {cd_primario}',
                showlegend=True
            )
        )

# Adiciona linha de referência para erro médio por CD
for cd_primario in df_heterocedasticidade['Cd_primario'].unique():
    df_cd = df_heterocedasticidade[df_heterocedasticidade['Cd_primario'] == cd_primario]
    l1_medio = df_cd['L1_share_perc'].mean()
    se_medio = df_cd['SE_pp'].mean()
    
    fig_hetero.add_trace(
        go.Scatter(
            x=[l1_medio],
            y=[se_medio],
            mode='markers',
            marker=dict(
                size=15,
                symbol='diamond',
                color='red',
                line=dict(color='black', width=2)
            ),
            text=f'CD {cd_primario} - Média',
            hovertemplate=(
                '<b>CD {cd_primario} - Média</b><br>' +
                '<b>L1 Share Médio:</b> ' + f'{l1_medio:.4f}%<br>' +
                '<b>SE Médio:</b> ' + f'{se_medio:.2f}<br>' +
                '<extra></extra>'
            ),
            name=f'CD {cd_primario} - Média',
            showlegend=False
        )
    )

# Configuração do layout
fig_hetero.update_layout(
    title={
        'text': 'Heterocedasticidade dos Erros: Loja Individual vs. Agregação por CD',
        'x': 0.5,
        'xanchor': 'center',
        'font': {'size': 18}
    },
    xaxis_title="L1 Share por Loja (%)",
    yaxis_title="Share Error (SE) por Loja (pp)",
    plot_bgcolor="#F8F8FF",
    paper_bgcolor="#F8F8FF",
    font=dict(size=12),
    height=700,
    showlegend=True,
    legend=dict(
        yanchor="top",
        y=0.99,
        xanchor="left",
        x=0.01
    )
)

# Configuração dos eixos
fig_hetero.update_xaxes(
    showgrid=True, 
    gridwidth=1, 
    gridcolor='lightgray',
    zeroline=True,
    zerolinecolor='black'
)
fig_hetero.update_yaxes(
    showgrid=True, 
    gridwidth=1, 
    gridcolor='lightgray',
    zeroline=True,
    zerolinecolor='black'
)

# Exibe o gráfico
fig_hetero.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise da Heterocedasticidade
# MAGIC
# MAGIC **O que o gráfico revela:**
# MAGIC
# MAGIC 1. **Dispersão dos Erros**: Cada CD (cor diferente) mostra lojas espalhadas em diferentes níveis de erro
# MAGIC 2. **Máscara da Agregação**: Os pontos vermelhos (médias dos CDs) parecem "aceitáveis", mas as lojas individuais têm erros altos
# MAGIC 3. **Heterocedasticidade**: Variância dos erros não é constante - algumas lojas têm erros muito maiores que outras
# MAGIC
# MAGIC **Implicações:**
# MAGIC - **Nível CD**: L1 pode parecer baixo, mas mascara problemas reais
# MAGIC - **Nível Loja**: Erros individuais podem ser 10x maiores
# MAGIC - **Cancelamento**: Erros positivos e negativos se "cancelam" na agregação

# COMMAND ----------

# Estatísticas detalhadas da heterocedasticidade
df_stats_hetero = (
    df_metricas_loja_individual
    .groupBy("Cd_primario")
    .agg(
        F.avg("L1_share_perc").alias("L1_medio"),
        F.stddev("L1_share_perc").alias("L1_desvio"),
        F.min("L1_share_perc").alias("L1_min"),
        F.max("L1_share_perc").alias("L1_max"),
        F.avg("SE_pp").alias("SE_medio"),
        F.stddev("SE_pp").alias("SE_desvio"),
        F.count("*").alias("qtd_lojas")
    )
    .withColumn(
        "coeficiente_variacao_L1", 
        F.round(F.col("L1_desvio") / F.col("L1_medio"), 4)
    )
    .withColumn(
        "coeficiente_variacao_SE", 
        F.round(F.col("SE_desvio") / F.col("SE_medio"), 4)
    )
    .orderBy("wMAPE_medio")
)

print("Estatísticas detalhadas da heterocedasticidade por CD:")
df_stats_hetero.display()

# COMMAND ----------

# Gráfico de barras mostrando a variabilidade interna de cada CD
df_stats_pandas = df_stats_hetero.toPandas()

fig_barras = go.Figure()

# Barras para L1
fig_barras.add_trace(
    go.Bar(
        x=df_stats_pandas['Cd_primario'].astype(str),
        y=df_stats_pandas['L1_medio'],
        name='L1 Médio',
        marker_color='lightblue',
        yaxis='y'
    )
)

# Barras para desvio padrão (sobrepostas)
fig_barras.add_trace(
    go.Bar(
        x=df_stats_pandas['Cd_primario'].astype(str),
        y=df_stats_pandas['L1_desvio'],
        name='Desvio Padrão',
        marker_color='red',
        opacity=0.7,
        yaxis='y'
    )
)

# Configuração do layout
fig_barras.update_layout(
    title={
        'text': 'Variabilidade Interna dos Erros por CD: Média vs. Desvio Padrão',
        'x': 0.5,
        'xanchor': 'center',
        'font': {'size': 18}
    },
    xaxis_title="CD Primário",
    yaxis_title="L1 Share (%)",
    plot_bgcolor="#F8F8FF",
    paper_bgcolor="#F8F8FF",
    font=dict(size=12),
    height=600,
    barmode='overlay',
    showlegend=True
)

# Configuração dos eixos
fig_barras.update_xaxes(
    showgrid=True, 
    gridwidth=1, 
    gridcolor='lightgray'
)
fig_barras.update_yaxes(
    showgrid=True, 
    gridwidth=1, 
    gridcolor='lightgray',
    zeroline=True,
    zerolinecolor='black'
)

# Exibe o gráfico
fig_barras.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusões sobre a Heterocedasticidade
# MAGIC
# MAGIC **Evidências encontradas:**
# MAGIC
# MAGIC 1. **Máscara da Agregação**: As métricas L1 agregadas por CD não refletem a realidade das lojas individuais
# MAGIC 2. **Variabilidade Interna**: CDs com L1 médio baixo podem ter lojas com erros 10-100x maiores
# MAGIC 3. **Cancelamento de Erros**: Erros positivos e negativos se compensam na agregação, mascarando problemas reais
# MAGIC
# MAGIC **Recomendações:**
# MAGIC - **Análise Granular**: Sempre analisar erros ao nível de loja individual
# MAGIC - **Métricas Complementares**: Usar desvio padrão e coeficiente de variação além das médias
# MAGIC - **Alertas por Loja**: Implementar monitoramento individual, não apenas agregado
# MAGIC - **Investigações Específicas**: Focar nas lojas com maior variabilidade interna
