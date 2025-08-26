# Databricks notebook source
# MAGIC %md
# MAGIC # Análise de Efetividade da Matriz de Merecimento - Telas
# MAGIC
# MAGIC Este notebook analisa a efetividade da matriz de merecimento atual comparando as alocações previstas 
# MAGIC com o comportamento real de vendas e demanda para produtos de telas e monitores.
# MAGIC
# MAGIC **Objetivo**: Identificar gaps entre alocações previstas e realidade para otimização da matriz futura.
# MAGIC **Escopo**: Apenas produtos de telas e monitores no nível de filial (loja).
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
    
    # wMAPE (Weighted Mean Absolute Percentage Error) - erro absoluto ponderado por volume
    wmape_distance = abs_err * y
    
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
    wmape_share = abs_err_share * y  # wMAPE para shares

    base = (df
        .withColumn("__y__", y).withColumn("__yhat__", yhat)
        .withColumn("__p__", p).withColumn("__phat__", phat)
        .withColumn("__abs_err__", abs_err).withColumn("__wmape_distance__", wmape_distance)
        .withColumn("__kl_term__", kl_term)
        .withColumn("__cross_entropy_term__", cross_entropy_term)
        .withColumn("__abs_err_share__", abs_err_share)
        .withColumn("__wmape_share__", wmape_share)
    )

    agg = base.groupBy(*group_cols) if group_cols else base.groupBy()

    res = agg.agg(
        # volume
        F.sum("__abs_err__").alias("_sum_abs_err"),
        F.sum("__wmape_distance__").alias("_sum_wmape_distance"),
        F.sum("__y__").alias("_sum_y"),
        F.sum("__yhat__").alias("_sum_yhat"),
        # shares
        F.sum(F.abs(F.col("__p__") - F.col("__phat__"))).alias("_SE"),
        F.sum("__kl_term__").alias("_KL"),
        F.sum("__cross_entropy_term__").alias("_cross_entropy"),
        F.sum("__wmape_share__").alias("_num_wmape_share")
    ).withColumn(
        # volume (%)
        "wMAPE_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_sum_wmape_distance")/F.col("_sum_y")).otherwise(0.0) * 100, 4)
    ).withColumn(
        # shares (% e pp)
        "SE_pp", F.round(F.col("_SE") * 100, 4)  # 0–200 p.p.
    ).withColumn(
        "wMAPE_share_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_num_wmape_share")/F.col("_sum_y")).otherwise(0.0) * 100, 4)
    ).withColumn(
        "Cross_entropy", F.when(F.col("_sum_y") > 0, F.col("_cross_entropy")).otherwise(F.lit(0.0))
    ).withColumn(
        "KL_divergence", F.when((F.col("_sum_y") > 0) & (F.col("_sum_yhat") > 0), F.col("_KL")).otherwise(F.lit(0.0))
    ).select(
        *(group_cols if group_cols else []),
        # volume
        "wMAPE_perc",
        # shares ponderados por volume
        "SE_pp", "wMAPE_share_perc",
        # distância de distribuição
        "Cross_entropy", "KL_divergence"
    )

    return res

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura e Preparação dos Dados de Telas
# MAGIC
# MAGIC Carregamos a base de dados de vendas e estoque para produtos de telas e monitores,
# MAGIC filtrando apenas a diretoria específica e período relevante.

# COMMAND ----------

df_vendas_estoque_telas = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA TELAS E MONITORES')
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
# MAGIC ## 4. Carregamento dos Mapeamentos de Produtos
# MAGIC
# MAGIC Carregamos os arquivos de mapeamento que relacionam SKUs com modelos, 
# MAGIC espécies gerenciais e grupos de produtos similares ("gêmeos").

# COMMAND ----------

# Mapeamento de modelos e tecnologia para telas
de_para_modelos_tecnologia_telas = (
    pd.read_csv('dados_analise/MODELOS_AJUSTE_TELAS.csv', 
                delimiter=';')
    .drop_duplicates()
)

# Normalização de nomes de colunas
de_para_modelos_tecnologia_telas.columns = (
    de_para_modelos_tecnologia_telas.columns
    .str.strip()            # remove leading/trailing spaces
    .str.lower()            # lowercase
    .str.replace(r"[^\w]+", "_", regex=True)  # non-alphanumeric -> "_"
    .str.strip("_")         # remove leading/trailing underscores
)

# Mapeamento de produtos similares (gêmeos) para telas
de_para_gemeos_tecnologia_telas = (
    pd.read_csv('dados_analise/ITENS_GEMEOS_TELAS.csv',
                delimiter=";",
                encoding='iso-8859-1')
    .drop_duplicates()
)

# Normalização de nomes de colunas
de_para_gemeos_tecnologia_telas.columns = (
    de_para_gemeos_tecnologia_telas
    .columns
    .str.strip()            # remove leading/trailing spaces
    .str.lower()            # lowercase
    .str.replace(r"[^\w]+", "_", regex=True)  # non-alphanumeric -> "_"
    .str.strip("_")         # remove leading/trailing underscores
)

# Renomeação e merge dos mapeamentos
de_para_modelos_tecnologia_telas = (
    de_para_modelos_tecnologia_telas.rename(columns={'codigo_item': 'sku_loja'})
)

de_para_modelos_gemeos_tecnologia_telas = (
    spark.createDataFrame(
        pd.merge(
            de_para_modelos_tecnologia_telas,
            de_para_gemeos_tecnologia_telas,
            on='sku_loja',
            how="outer"
        )
        [['sku_loja', 'item', 'modelos', 'setor_gerencial', 'gemeos']]
        .drop_duplicates()
    )
)

print("Mapeamentos de produtos de telas carregados:")
de_para_modelos_gemeos_tecnologia_telas.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Agregação dos Dados de Telas por Filial
# MAGIC
# MAGIC Agregamos os dados por mês, modelo, gêmeos e filial para análise no nível de loja.
# MAGIC Filtramos apenas o período de análise.

# COMMAND ----------

df_vendas_estoque_telas_agg = (
    df_vendas_estoque_telas
    .filter(F.col("year_month") < 202508)  # Filtro de período
    .join(
        de_para_modelos_gemeos_tecnologia_telas
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

print("Dados agregados por filial para telas:")
df_vendas_estoque_telas_agg.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cálculo de Percentuais de Vendas e Demanda
# MAGIC
# MAGIC Calculamos os percentuais de participação nas vendas e demanda por mês, 
# MAGIC modelo e grupo de produtos similares.

# COMMAND ----------

# Janela por mês, modelo e gêmeos
w = Window.partitionBy("year_month", "modelos", "gemeos")

df_pct_telas = (
    df_vendas_estoque_telas_agg
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

print("Percentuais calculados por filial para telas:")
df_pct_telas.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Carregamento da Matriz de Merecimento Atual
# MAGIC
# MAGIC Carregamos a matriz de merecimento atual para comparar com os dados reais.
# MAGIC Filtramos apenas lojas offline (não CDs) e aplicamos os mapeamentos de produtos.

# COMMAND ----------

# Leitura da matriz de merecimento para telas
df_matriz_telas_pd = pd.read_excel(
    "dados_analise/(DRP)_MATRIZ_TELAS_20250825174952.csv.xlsx", 
    sheet_name="(DRP)_MATRIZ_20250825174952"
)

# Conversão para evitar erros de conversão
if 'DATA_VALIDADE_RELACAO' in df_matriz_telas_pd.columns:
    df_matriz_telas_pd['DATA_VALIDADE_RELACAO'] = df_matriz_telas_pd['DATA_VALIDADE_RELACAO'].astype(str)

# Criação do DataFrame Spark com mapeamentos
df_matriz_telas = (
    spark.createDataFrame(df_matriz_telas_pd)
    .withColumnRenamed("CODIGO", "CdSku")
    .join(
        de_para_modelos_gemeos_tecnologia_telas
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

print("Matriz de merecimento para telas carregada:")
df_matriz_telas.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Join entre Matriz e Dados Reais
# MAGIC
# MAGIC Realizamos o join entre a matriz de merecimento e os dados reais de vendas/demanda
# MAGIC para comparar alocações previstas vs. realidade.

# COMMAND ----------

df_matriz_telas_metricas = (
    df_matriz_telas
    .join(
        df_pct_telas,
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

print("Dados consolidados para análise de métricas de telas:")
df_matriz_telas_metricas.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cálculo das Métricas de Avaliação
# MAGIC
# MAGIC Calculamos as métricas linha a linha e agregadas para avaliar a qualidade
# MAGIC das alocações da matriz de merecimento para telas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Métricas Linha a Linha
# MAGIC
# MAGIC Calculamos métricas para cada linha individual para análise detalhada.
# MAGIC **Nota**: Usamos percentuais da matriz (Percentual_matriz_fixa) vs. percentuais reais da demanda (pct_demanda_perc).
# MAGIC
# MAGIC **Métricas Calculadas:**
# MAGIC - **Erro Absoluto**: Diferença absoluta entre matriz e demanda real
# MAGIC - **wMAPE**: Erro ponderado pelo volume real da filial
# MAGIC - **Cross Entropy**: Divergência entre distribuições de participação

# COMMAND ----------

# APLICA FILTROS PRIMEIRO
df_filtered_telas = (
    df_matriz_telas_metricas
    .filter(F.col("Demanda_total_mes_especie") > 50)  # Filtro de demanda mínima
    # Adicione outros filtros conforme necessário
)

# DEPOIS calcula métricas sobre os dados filtrados
w_filtered_telas = Window.partitionBy(F.lit(1))  # Janela sobre dados filtrados

df_with_metrics_telas = (
    df_filtered_telas
    # Totais sobre dados FILTRADOS (não globais)
    .withColumn("total_matriz_filtrado", F.sum("Percentual_matriz_fixa").over(w_filtered_telas))
    .withColumn("total_demanda_real_filtrado", F.sum("pct_demanda_perc").over(w_filtered_telas))
    
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
               F.lit(1.5) * F.col("pct_demanda_perc"))
         .otherwise(F.col("pct_demanda_perc"))
    )
    .withColumn("w_abs", F.col("weight") * F.col("abs_err"))
    
    # KL divergence term
    .withColumn(
        "kl_term", 
        F.when(
            (F.col("p") > 0) & (F.col("phat") > 0),
            F.col("p") * F.log((F.col("p") + F.lit(1e-12)) / (F.col("phat") + F.lit(1e-12)))
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

print("Métricas linha a linha calculadas para telas:")
df_with_metrics_telas.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 Métricas Agregadas
# MAGIC
# MAGIC Calculamos as métricas agregadas para o dataframe inteiro usando a função
# MAGIC `add_allocation_metrics` que criamos no início.
# MAGIC **Nota**: Métricas calculadas comparando percentuais da matriz vs. percentuais reais da demanda.

# COMMAND ----------

# Carregamento do mapeamento filial -> CD primário
de_para_filial_cd = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .select('CdFilial', 'Cd_primario')
    .distinct()
    .dropna()
)

# Métricas agregadas por CD para telas
df_agg_metrics_telas = add_allocation_metrics(
    df=df_filtered_telas.join(de_para_filial_cd, how="left", on="CdFilial"),
    y_col="pct_demanda_perc",     
    yhat_col="Percentual_matriz_fixa",  
    group_cols=["Cd_primario"]            
).dropna(subset=["Cd_primario"])

print("Métricas agregadas calculadas para telas por CD:")
df_agg_metrics_telas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Visualização: Scatter Plot de Erro Percentual por Filial
# MAGIC
# MAGIC Criamos um scatter plot onde cada ponto representa uma filial, mostrando:
# MAGIC - **Eixo X**: Ordenação arbitrária baseada em Cd_primario (não tem significado específico)
# MAGIC - **Eixo Y**: Erro percentual agregado da filial (agregando todos os produtos)
# MAGIC
# MAGIC **Objetivo**: Identificar visualmente quais filiais têm maior discrepância entre matriz prevista e realidade.
# MAGIC
# MAGIC **Métricas Visualizadas:**
# MAGIC - **Erro com Sinal**: Positivo (subalocação) vs. Negativo (superalocação)
# MAGIC - **Erro Absoluto**: Magnitude do erro independente da direção
# MAGIC - **Escala de Cores**: RdBu (vermelho = superestimação, azul = subestimação)

# COMMAND ----------

# Cálculo do erro percentual agregado por filial para telas
df_erro_por_filial_telas = (
    df_filtered_telas
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

print("Erro percentual agregado por filial para telas:")
df_erro_por_filial_telas.display()

# COMMAND ----------

# Preparação dos dados para o scatter plot
df_scatter_plot_telas = (
    df_erro_por_filial_telas
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

print("Dados preparados para scatter plot de telas:")
df_scatter_plot_telas.display()

# COMMAND ----------

# Conversão para pandas para visualização
df_scatter_pandas_telas = df_scatter_plot_telas.toPandas()

# Criação do scatter plot
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Scatter plot principal
fig_telas = go.Figure()

# Adiciona os pontos (filiais) com cores baseadas no sinal do erro
fig_telas.add_trace(
    go.Scatter(
        x=df_scatter_pandas_telas['indice_ordenacao'],
        y=df_scatter_pandas_telas['erro_percentual_sinal'],
        mode='markers',
        marker=dict(
            size=8,
            color=df_scatter_pandas_telas['erro_percentual_sinal'],
            colorscale='RdBu',  # Vermelho para negativo, Azul para positivo
            showscale=True,
            colorbar=dict(title="Erro % com Sinal"),
            cmin=df_scatter_pandas_telas['erro_percentual_sinal'].min(),
            cmax=df_scatter_pandas_telas['erro_percentual_sinal'].max()
        ),
        text=df_scatter_pandas_telas['CdFilial'].astype(str),
        hovertemplate=(
            '<b>Filial:</b> %{text}<br>' +
            '<b>Cd_primario:</b> ' + df_scatter_pandas_telas['Cd_primario'].astype(str) + '<br>' +
            '<b>Erro % com Sinal:</b> %{y:.2f}%<br>' +
            '<b>Erro % Absoluto:</b> ' + df_scatter_pandas_telas['erro_percentual_abs'].round(2).astype(str) + '%<br>' +
            '<b>Demanda Real:</b> ' + df_scatter_pandas_telas['demanda_real_total'].round(2).astype(str) + '%<br>' +
            '<b>Matriz Prevista:</b> ' + df_scatter_pandas_telas['matriz_prevista_total'].round(2).astype(str) + '%<br>' +
            '<b>Qtd Produtos:</b> ' + df_scatter_pandas_telas['qtd_produtos'].astype(str) + '<br>' +
            '<extra></extra>'
        ),
        name="Filiais"
    )
)

# Linha de referência para erro médio com sinal
erro_medio_sinal_telas = df_scatter_pandas_telas['erro_percentual_sinal'].mean()
fig_telas.add_hline(
    y=erro_medio_sinal_telas, 
    line_dash="dash", 
    line_color="gray",
    annotation_text=f"Erro Médio: {erro_medio_sinal_telas:.2f}%",
    annotation_position="top right"
)

# Linha de referência para zero (sem erro)
fig_telas.add_hline(
    y=0, 
    line_dash="dot", 
    line_color="black",
    annotation_text="Sem Erro (0%)",
    annotation_position="bottom right"
)

# Configuração do layout
fig_telas.update_layout(
    title={
        'text': 'Erro Percentual da Matriz de Merecimento por Filial - Telas (com Sinal)',
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
fig_telas.update_xaxes(
    showgrid=True, 
    gridwidth=1, 
    gridcolor='lightgray',
    zeroline=False
)
fig_telas.update_yaxes(
    showgrid=True, 
    gridwidth=1, 
    gridcolor='lightgray',
    zeroline=True,
    zerolinecolor='black'
)

# Exibe o gráfico
fig_telas.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise do Scatter Plot para Telas
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
# MAGIC
# MAGIC **Métricas Complementares:**
# MAGIC - **Erro Absoluto**: Magnitude do erro independente da direção
# MAGIC - **wMAPE**: Erro ponderado pelo volume real da filial
# MAGIC - **Cross Entropy**: Qualidade da distribuição de participações

# COMMAND ----------

# Estatísticas resumidas por faixa de erro para telas
df_resumo_erro_telas = (
    df_scatter_pandas_telas
    .assign(
        faixa_erro=lambda x: pd.cut(
            x['erro_percentual_sinal'], 
            bins=[float('-inf'), -erro_medio_sinal_telas, -erro_medio_sinal_telas/2, 0, erro_medio_sinal_telas/2, erro_medio_sinal_telas, float('inf')],
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

df_resumo_erro_telas.columns = ['Qtd_Filiais', 'Erro_Medio_Sinal', 'Erro_Desvio', 'Erro_Medio_Abs', 'Produtos_Medio']
df_resumo_erro_telas = df_resumo_erro_telas.reset_index()

print("Resumo por faixa de erro para telas:")
display(df_resumo_erro_telas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Análise de Heterocedasticidade para Telas
# MAGIC
# MAGIC **Problema identificado**: A agregação por CD pode mascarar problemas reais ao nível de loja individual.
# MAGIC **Objetivo**: Demonstrar que embora as métricas wMAPE agregadas por CD pareçam "aceitáveis", 
# MAGIC ao nível de loja existem erros significativos em todas as direções (heterocedasticidade).
# MAGIC
# MAGIC **Hipótese**: Os erros se "cancelam" na agregação, mas individualmente são problemáticos.
# MAGIC
# MAGIC **Métricas Analisadas:**
# MAGIC - **wMAPE_share_perc**: Weighted Mean Absolute Percentage Error para shares
# MAGIC - **SE_pp**: Share Error em pontos percentuais
# MAGIC - **Cross_entropy**: Entropia cruzada entre distribuições

# COMMAND ----------

# Cálculo de métricas ao nível de loja individual (sem agregação por CD) para telas
df_metricas_loja_individual_telas = add_allocation_metrics(
    df=df_filtered_telas.join(de_para_filial_cd, how="left", on="CdFilial"),
    y_col="pct_demanda_perc",     
    yhat_col="Percentual_matriz_fixa",  
    group_cols=["CdFilial", "Cd_primario"]  # Agrupa por loja individual
).dropna(subset=["CdFilial", "Cd_primario"])

print("Métricas calculadas ao nível de loja individual para telas:")
df_metricas_loja_individual_telas.display()

# COMMAND ----------

# Comparação: Métricas agregadas por CD vs. Distribuição das métricas ao nível de loja para telas
df_comparacao_agregacao_telas = (
    df_metricas_loja_individual_telas
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

print("Comparação: Agregação por CD vs. Distribuição ao nível de loja para telas:")
df_comparacao_agregacao_telas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusões sobre a Heterocedasticidade para Telas
# MAGIC
# MAGIC **Evidências encontradas:**
# MAGIC
# MAGIC 1. **Máscara da Agregação**: As métricas wMAPE agregadas por CD não refletem a realidade das lojas individuais
# MAGIC 2. **Variabilidade Interna**: CDs com wMAPE médio baixo podem ter lojas com erros 10-100x maiores
# MAGIC 3. **Cancelamento de Erros**: Erros positivos e negativos se compensam na agregação, mascarando problemas reais
# MAGIC
# MAGIC **Recomendações:**
# MAGIC - **Análise Granular**: Sempre analisar erros ao nível de loja individual
# MAGIC - **Métricas Complementares**: Usar desvio padrão e coeficiente de variação além das médias
# MAGIC - **Alertas por Loja**: Implementar monitoramento individual, não apenas agregado
# MAGIC - **Investigações Específicas**: Focar nas lojas com maior variabilidade interna
# MAGIC
# MAGIC **Métricas de Monitoramento Recomendadas:**
# MAGIC - **wMAPE_share_perc**: Para identificar lojas com maior erro de alocação
# MAGIC - **SE_pp**: Para detectar problemas na distribuição de participações
# MAGIC - **Cross_entropy**: Para avaliar qualidade geral das distribuições
# MAGIC - **Coeficiente de Variação**: Para identificar CDs com alta variabilidade interna
