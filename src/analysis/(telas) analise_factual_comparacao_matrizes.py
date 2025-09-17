# Databricks notebook source
# MAGIC %md
# MAGIC # Análise de Factual e Comparação de Matrizes de Merecimento
# MAGIC
# MAGIC Este notebook implementa a análise de factual, cálculo de métricas de erro (sMAPE e WMAPE)
# MAGIC e comparação com a matriz DRP geral para identificar distorções.
# MAGIC
# MAGIC **Author**: Scardini  
# MAGIC **Date**: 2025  
# MAGIC **Purpose**: Análise de qualidade das matrizes de merecimento calculadas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e Configuração Inicial

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any
import plotly.graph_objects as go

# Inicialização do Spark
spark = SparkSession.builder.appName("calculo_matriz_merecimento_unificado").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))


GRUPOS_TESTE = ['Telef pp', 'TV 50 ALTO P', 'TV 55 ALTO P']
print(GRUPOS_TESTE)


data_inicio = "2025-08-29"
fim_baseline = "2025-09-05"

inicio_teste = "2025-09-05"

categorias_teste = ['DE_TELAS']


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregamento das Matrizes de Merecimento Calculadas

# COMMAND ----------

def carregar_matrizes_merecimento_calculadas() -> Dict[str, DataFrame]:
    """
    Carrega todas as matrizes de merecimento calculadas para cada categoria.
    
    Returns:
        Dicionário com DataFrames das matrizes por categoria
    """
    print("🔄 Carregando matrizes de merecimento calculadas...")
    
    categorias = [
        "DE_TELAS",
        #"TELEFONIA_CELULAR", 
        #"LINHA_BRANCA",
        #"LINHA_LEVE",
        #"INFO_GAMES"
    ]
    
    matrizes = {}
    
    for categoria in categorias:
        try:
            nome_tabela = f"databox.bcg_comum.supply_matriz_merecimento_{categoria}_teste1509"
            df_matriz = spark.table(nome_tabela)
            
            matrizes[categoria] = df_matriz
            print(f"✅ {categoria}: {df_matriz.count():,} registros carregados")
            
        except Exception as e:
            print(f"❌ {categoria}: Erro ao carregar - {str(e)}")
            matrizes[categoria] = None
    
    print(f"📊 Total de matrizes carregadas: {len([m for m in matrizes.values() if m is not None])}")
    return matrizes

df_merecimento_offline = {}
df_merecimento_offline['DE_TELAS'] = carregar_matrizes_merecimento_calculadas()['DE_TELAS']

df_merecimento_offline['DE_TELAS'].limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento da Matriz neogrid

# COMMAND ----------

df_matriz_neogrid_offline = (
    spark.createDataFrame(
        pd.read_csv(
            "/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/(DRP)_MATRIZ_20250902160333.csv",
            delimiter=";",
        )
    )
    .select(
        F.col("CODIGO").cast("int").alias("CdSku"),
        F.regexp_replace(F.col("CODIGO_FILIAL"), ".*_", "").cast("int").alias("CdFilial"),
        F.regexp_replace(F.col("PERCENTUAL_MATRIZ"), ",", ".").cast("float").alias("PercMatrizNeogrid"),
        F.col("CLUSTER").cast("string").alias("is_Cluster"),
        F.col("TIPO_ENTREGA").cast("string").alias("TipoEntrega"),
    )
    .dropDuplicates()
    #.filter(F.col('TIPO_ENTREGA') == 'SL')
    .join(
        spark.table('databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia'),
        how="inner",
        on="CdSku")
    .filter(F.col("grupo_de_necessidade").isNotNull())
)

df_matriz_neogrid_agg_offline = (
  df_matriz_neogrid_offline
  .groupBy('CdFilial', 'grupo_de_necessidade')
  .agg(
    F.round(F.mean('PercMatrizNeogrid'), 3).alias('PercMatrizNeogrid'),
    F.round(F.median('PercMatrizNeogrid'),3).alias('PercMatrizNeogrid_median')
  )
)

df_matriz_neogrid_offline.cache()
#df_matriz_neogrid_offline.display()
df_matriz_neogrid_agg_offline.limit(1).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregamento dos Dados Factuais

# COMMAND ----------

inicio_janela = "2025-09-01"
fim_janela = "2025-09-16"

print(inicio_janela, fim_janela)

# partindo do df_proporcao_factual já agregado por CdFilial × grupo_de_necessidade
w_grp = Window.partitionBy("grupo_de_necessidade")

df_proporcao_factual = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
    .filter(F.col('DtAtual') >= inicio_janela)
    .filter(F.col('DtAtual') <= fim_janela)      
    .fillna(0, subset=['deltaRuptura', 'QtMercadoria'])
    .withColumn("QtDemanda", F.col("QtMercadoria") + F.col("deltaRuptura"))
    .join(
        spark.table('databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia'),
        how="inner",
        on="CdSku")
    .filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
    .dropna(subset='grupo_de_necessidade')
    .groupBy('CdFilial', 'grupo_de_necessidade')
    .agg(
        F.round(F.sum('QtDemanda'), 0).alias('QtDemanda'),
    )
    .withColumn("Total_QtDemanda", F.round(F.sum(F.col("QtDemanda")).over(w_grp), 0))
    .withColumn(
        "Proporcao_Interna_QtDemanda",
        F.when(F.col("Total_QtDemanda") > 0, F.col("QtDemanda") / F.col("Total_QtDemanda")).otherwise(F.lit(0.0))
    )
    .withColumn("Percentual_QtDemanda", F.round(F.col("Proporcao_Interna_QtDemanda") * 100.0, 3))
    .select('grupo_de_necessidade', 'CdFilial', 'Percentual_QtDemanda', 'QtDemanda', 'Total_QtDemanda')

    )

#df_proporcao_factual.limit(1).display()

colunas = [
    "Merecimento_Final_Media90_Qt_venda_sem_ruptura",
    "Merecimento_Final_Media180_Qt_venda_sem_ruptura",
    "Merecimento_Final_Media270_Qt_venda_sem_ruptura",
    "Merecimento_Final_Media360_Qt_venda_sem_ruptura",
    "Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura",
    "Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura",
    "Merecimento_Final_MediaAparada270_Qt_venda_sem_ruptura",
    "Merecimento_Final_MediaAparada360_Qt_venda_sem_ruptura",
]

df_acuracia = {}

for categoria in categorias_teste:
  df_acuracia[categoria] = (
      df_proporcao_factual
      .join(
        df_merecimento_offline[categoria]
        .drop("CdSku", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica")
        .dropDuplicates(),
        on=['CdFilial', 'grupo_de_necessidade'],
        how='inner'
      )
      .join(df_matriz_neogrid_agg_offline,
            on=['CdFilial', 'grupo_de_necessidade'],
            how="left")

      .fillna(0.0, subset=[
            'Percentual_QtDemanda',
            'Merecimento_Final_Media90_Qt_venda_sem_ruptura',
            'Merecimento_Final_Media180_Qt_venda_sem_ruptura',
            'Merecimento_Final_Media270_Qt_venda_sem_ruptura',
            'Merecimento_Final_Media360_Qt_venda_sem_ruptura',])
      .select(
            "CdFilial",
            "grupo_de_necessidade",
            "Percentual_QtDemanda",
            "QtDemanda",
                *[F.round(F.col(c) * 100, 3).alias(c)
                for c in colunas],
            "PercMatrizNeogrid",
            "PercMatrizNeogrid_median",
      )
  )


  

  df_acuracia[categoria].limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de sMAPE e wsMAPE

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

# === Constantes ===
COL_REAL = "Percentual_QtDemanda"
COL_PESO = "QtDemanda"

# === Função utilitária: adiciona componentes sMAPE/WSMAPE para uma coluna de predição ===
def add_smape_components(df, pred_col, real_col=COL_REAL, peso_col=COL_PESO, label=None):
    label = label or pred_col
    denom = F.abs(F.col(pred_col)) + F.abs(F.col(real_col))
    smape_comp = F.when(denom == 0, F.lit(0.0)) \
                  .otherwise(200.0 * F.abs(F.col(pred_col) - F.col(real_col)) / denom)
    return (
        df
        .withColumn(f"sMAPE_comp_{label}", smape_comp)
        .withColumn(f"WSMAPE_comp_{label}", smape_comp * F.col(peso_col))
    )

# === Lista de colunas de predição alvo ===
pred_cols_base = list(colunas)  # ["Merecimento_Final_Media90_...", ...]
extras = ["PercMatrizNeogrid", "PercMatrizNeogrid_median"]
# mantém só as extras que existem no DF
def existing_pred_cols(df, base_cols, maybe_cols):
    present = [c for c in maybe_cols if c in df.columns]
    return base_cols + present

# === Calcula e agrega métricas por categoria e modelo ===
metrics_all = None  # DataFrame final com métricas

for categoria in categorias_teste:
    df_cat = df_acuracia[categoria]

    pred_cols = existing_pred_cols(df_cat, pred_cols_base, extras)

    # Adiciona componentes sMAPE/WSMAPE para todas as colunas de previsão
    df_with_comps = reduce(
        lambda acc, c: add_smape_components(acc, c, label=c),
        pred_cols,
        df_cat
    )

    # Agrega métricas por modelo: sMAPE médio e WSMAPE ponderado por QtDemanda
    # sMAPE = média dos componentes; WSMAPE = sum(WSMAPE_comp)/sum(peso)
    aggs = []
    for c in pred_cols:
        smape_col = f"sMAPE_comp_{c}"
        wsmape_col = f"WSMAPE_comp_{c}"
        aggs.append(
            F.struct(
                F.lit(categoria).alias("categoria"),
                F.lit(c).alias("modelo"),
                F.round(F.avg(F.col(smape_col)), 4).alias("sMAPE"),
                F.round(F.sum(F.col(wsmape_col)) / F.sum(F.col(COL_PESO)), 4).alias("WSMAPE")
            ).alias(c)  # nome temporário
        )

    # Converte a lista de structs em linhas
    metrics_cat = df_with_comps.select(*aggs)
    # explode para linhas: uma por modelo
    metrics_cat = metrics_cat.select(F.explode(F.array(*metrics_cat.columns)).alias("m")).select("m.*")

    metrics_all = metrics_cat if metrics_all is None else metrics_all.unionByName(metrics_cat)
# Mostrar as métricas agregadas por categoria e modelo
metrics_all.orderBy("categoria", "modelo").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plot das bolinhas

# COMMAND ----------

# === Plotly scatters ===
import plotly.express as px
from pyspark.sql import functions as F

df_filial_mean = {}
pdf = {}

for categoria in categorias_teste:
    df_base = df_acuracia[categoria]

    # checa existência das colunas opcionais
    has_neogrid = "PercMatrizNeogrid" in df_base.columns

    df_tmp = (
        df_base
        .withColumn("merecimento_percentual",
                    F.col("Merecimento_Final_Media270_Qt_venda_sem_ruptura"))
        .join(
            spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
            .select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica"),
            on="CdFilial",
            how="left"
        )
    )

    # agregações obrigatórias
    agg_exprs = [
        F.avg("Percentual_QtDemanda").alias("x_real"),
        F.avg("merecimento_percentual").alias("y_nova"),
    ]
    # agrega também neogrid se existir
    if has_neogrid:
        agg_exprs.append(F.avg("PercMatrizNeogrid_median").alias("y_neogrid"))

    df_filial_mean[categoria] = (
        df_tmp
        .groupBy("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica")
        .agg(*agg_exprs)
        # Extrai o número do porte
        .withColumn("PorteNum_raw", F.regexp_replace(F.col("NmPorteLoja"), "[^0-9]", ""))
        .withColumn("PorteNum", F.col("PorteNum_raw").cast("int"))
        # Garante valores válidos 1–6 e escala o bubble
        .withColumn("PorteNum", F.when(F.col("PorteNum").between(1,6), F.col("PorteNum")*1.5).otherwise(F.lit(1)))
    )

    pdf[categoria] = df_filial_mean[categoria].toPandas()
    print(categoria)

color_map = {
    "Sudeste": "#0d3b66",
    "Sul": "#5dade2",
    "Centro Oeste": "#ff9896",
    "Nordeste": "#1f77b4",
    "Norte": "#d62728",
}

def make_scatter(df, y_col, y_label, categoria):
    fig = px.scatter(
        df,
        x="x_real",
        y=y_col,
        size="PorteNum",
        color="NmRegiaoGeografica",
        color_discrete_map=color_map,
        size_max=12,
        opacity=0.75,
        labels={
            "x_real": "Percentual_QtDemanda médio por filial (real)",
            y_col:   y_label,
            "NmRegiaoGeografica": "Região Geográfica",
            "PorteNum": "Porte"
        },
        hover_data={
            "CdFilial": True,
            "NmFilial": True,
            "NmPorteLoja": True,
            "NmRegiaoGeografica": True,
            "x_real": ":.3f",
            y_col: ":.3f",
        }
    )
    fig.update_layout(
        title=dict(text=f"{y_label} vs Real – por filial ({categoria})", x=0.5, xanchor="center"),
        paper_bgcolor="#f2f2f2",
        plot_bgcolor="#f2f2f2",
        margin=dict(l=40, r=40, t=60, b=40),
        xaxis=dict(showgrid=True, gridwidth=0.3, gridcolor="rgba(0,0,0,0.08)", zeroline=False, range=[0,1.0]),
        yaxis=dict(showgrid=True, gridwidth=0.3, gridcolor="rgba(0,0,0,0.08)", zeroline=False, range=[0,2]),
        legend=dict(title="Região Geográfica", orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
        width=1200,
        height=400,
    )
    fig.update_traces(marker=dict(line=dict(width=0.1, color="rgba(0,0,0,0.35)")))
    fig.add_shape(type="line", x0=0, y0=0, x1=1, y1=1,
                  line=dict(color="rgba(0,0,0,0.45)", width=0.2, dash="dash"))
    return fig

for categoria in categorias_teste:
    # Matriz Nova
    fig_nova = make_scatter(
        pdf[categoria],
        "y_nova",
        "PercMatrizNova médio por filial (previsão)",
        categoria
    )
    fig_nova.show()

    # PercMatrizNeogrid (se existir)
    if "y_neogrid" in pdf[categoria].columns:
        fig_neogrid = make_scatter(
            pdf[categoria],
            "y_neogrid",
            "PercMatrizNeogrid médio por filial (baseline)",
            categoria
        )
        fig_neogrid.show()
    else:
        print(f"[{categoria}] Coluna PercMatrizNeogrid não disponível para plot.")
