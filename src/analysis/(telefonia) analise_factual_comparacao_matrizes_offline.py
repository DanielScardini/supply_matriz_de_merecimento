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


GRUPOS_TESTE = [
    "1200 a 1600",
    "1601 a 2000",
    "2001 a 2500",
    "2501 a 3000",
    "3001 a 3500",
    "<1099",
    "<799",
    ">4000"]
print(GRUPOS_TESTE)

GRUPOS_REMOVER = ['Chip', 'FORA DE LINHA', 'SEM_GN']

data_inicio = "2025-08-29"
fim_baseline = "2025-09-05"

inicio_teste = "2025-09-05"

categorias_teste = ['TELEFONIA_CELULAR']


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
        #"DE_TELAS",
        "TELEFONIA_CELULAR", 
        #"LINHA_BRANCA",
        #"LINHA_LEVE",
        #"INFO_GAMES"
    ]
    
    matrizes = {}
    
    for categoria in categorias:
        try:
            nome_tabela = f"databox.bcg_comum.supply_matriz_merecimento_{categoria}_teste2410"
            df_matriz = spark.table(nome_tabela)
            
            matrizes[categoria] = df_matriz
            print(f"✅ {categoria}: {df_matriz.count():,} registros carregados")
            
        except Exception as e:
            print(f"❌ {categoria}: Erro ao carregar - {str(e)}")
            matrizes[categoria] = None
    
    print(f"📊 Total de matrizes carregadas: {len([m for m in matrizes.values() if m is not None])}")
    return matrizes

df_merecimento_offline = {}
df_merecimento_offline['TELEFONIA_CELULAR'] = carregar_matrizes_merecimento_calculadas()['TELEFONIA_CELULAR']

df_merecimento_offline['TELEFONIA_CELULAR'].limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento da Matriz neogrid

# COMMAND ----------

df_matriz_neogrid_offline = (
    spark.createDataFrame(
        pd.read_csv(
            "/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/(DRP)_MATRIZ_20250902160333.csv",
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

from pyspark.sql import functions as F
from pyspark.sql import Window

# === Janela dinâmica: últimos 30 dias até ontem ===
fim_janela = F.date_sub(F.current_date(), 2)
inicio_janela = F.date_sub(fim_janela, 32)

# Log das datas (yyyy-MM-dd)
_row = (
    spark.range(1)
    .select(
        F.date_format(inicio_janela, "yyyy-MM-dd").alias("inicio"),
        F.date_format(fim_janela, "yyyy-MM-dd").alias("fim"),
    )
).first()
print(_row["inicio"], _row["fim"])

# partindo do df_proporcao_factual já agregado por CdFilial × grupo_de_necessidade
w_grp = Window.partitionBy("grupo_de_necessidade")

df_proporcao_factual = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
    .filter(F.col('DtAtual').between(inicio_janela, fim_janela))
    .fillna(0, subset=['deltaRuptura', 'QtMercadoria'])
    .withColumn("QtDemanda", F.col("QtMercadoria") + F.col("deltaRuptura"))
    .join(
        spark.table('databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia'),
        how="inner",
        on="CdSku"
    )
    .filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
    .filter(~F.col("grupo_de_necessidade").isin(GRUPOS_REMOVER))
    .dropna(subset='grupo_de_necessidade')
    .groupBy('CdFilial', 'grupo_de_necessidade')
    .agg(F.round(F.sum('QtDemanda'), 0).alias('QtDemanda'))
    .withColumn("Total_QtDemanda", F.round(F.sum(F.col("QtDemanda")).over(w_grp), 0))
    .withColumn(
        "Proporcao_Interna_QtDemanda",
        F.when(F.col("Total_QtDemanda") > 0,
               F.col("QtDemanda") / F.col("Total_QtDemanda")).otherwise(F.lit(0.0))
    )
    .withColumn("Percentual_QtDemanda", F.round(F.col("Proporcao_Interna_QtDemanda") * 100.0, 3))
    .select('grupo_de_necessidade', 'CdFilial', 'Percentual_QtDemanda', 'QtDemanda', 'Total_QtDemanda')
)

#df_proporcao_factual.limit(1).display()

colunas = [
    # "Merecimento_Final_Media90_Qt_venda_sem_ruptura",
    # "Merecimento_Final_Media180_Qt_venda_sem_ruptura",
    # "Merecimento_Final_Media270_Qt_venda_sem_ruptura",
    # "Merecimento_Final_Media360_Qt_venda_sem_ruptura",
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
        .join(
            df_matriz_neogrid_agg_offline,
            on=['CdFilial', 'grupo_de_necessidade'],
            how="left"
        )
        .filter(~F.col("grupo_de_necessidade").isin(GRUPOS_REMOVER))

        .fillna(0.0, subset=[
            'Percentual_QtDemanda',
            'Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura',
            'Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura',
            'Merecimento_Final_MediaAparada360_Qt_venda_sem_ruptura',
             "PercMatrizNeogrid",
            "PercMatrizNeogrid_median",
        ])
        .select(
            "CdFilial",
            "grupo_de_necessidade",
            "Percentual_QtDemanda",
            "QtDemanda",
            *[F.round(F.col(c) * 100, 3).alias(c) for c in colunas],
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

from pyspark.sql import functions as F, Window as W

# === Constantes ===
COL_REAL = "Percentual_QtDemanda"
COL_PESO = "QtDemanda"
GROUP_COL = "grupo_de_necessidade"

def existing_pred_cols(df, base_cols, maybe_cols):
    present = [c for c in maybe_cols if c in df.columns]
    return base_cols + present

def wmape_expr(pred_col, real_col=COL_REAL, peso_col=COL_PESO):
    yhat = F.coalesce(F.col(pred_col).cast("double"), F.lit(0.0))
    y    = F.coalesce(F.col(real_col).cast("double"), F.lit(0.0))
    w    = F.coalesce(F.col(peso_col).cast("double"), F.lit(0.0))
    num = F.sum(F.abs(yhat - y) * w)
    den = F.sum(F.abs(y) * w)
    return F.when(den == 0, F.lit(0.0)).otherwise(100.0 * num / den)

# pred_cols_base = list(colunas)
# extras = ["PercMatrizNeogrid", "PercMatrizNeogrid_median"]
# categorias_teste, df_acuracia: já definidos

wmape_all = None

for categoria in categorias_teste:
    df_cat = df_acuracia[categoria]
    pred_cols = existing_pred_cols(df_cat, pred_cols_base, extras)

    # Volume por grupo via Window
    w_grp = W.partitionBy(GROUP_COL)
    df_aug = df_cat.withColumn("Volume", F.sum(F.col(COL_PESO)).over(w_grp))

    # Aggregations por modelo em structs nomeados
    aggs, agg_names = [], []
    for c in pred_cols:
        name = f"agg_{c}"
        agg_names.append(name)
        aggs.append(
            F.struct(
                F.lit(categoria).alias("categoria"),
                F.col(GROUP_COL).alias("grupo"),
                F.lit(c).alias("modelo"),
                F.round(wmape_expr(c), 4).alias("WMAPE")
            ).alias(name)
        )

    wmape_cat = (
        df_aug
        .groupBy(GROUP_COL)
        .agg(*aggs, F.max("Volume").alias("Volume"))
        .select(
            F.col(GROUP_COL).alias("grupo"),
            "Volume",
            F.array(*[F.col(n) for n in agg_names]).alias("arr")
        )
        .select("grupo", "Volume", F.explode("arr").alias("m"))
        .select(
            F.lit(categoria).alias("categoria"),
            "grupo",
            F.col("m.modelo").alias("modelo"),
            F.col("m.WMAPE").alias("WMAPE"),
            "Volume"
        )
    )

    # Volume total da categoria para share (sem criar totais)
    vol_tot_cat = (
        df_cat
        .agg(F.sum(F.col(COL_PESO)).alias("Volume_total_categoria"))
        .withColumn("categoria", F.lit(categoria))
    )

    wmape_cat = wmape_cat.join(vol_tot_cat, on="categoria", how="left") \
                         .withColumn("ShareVolumeCategoria",
                                     F.round(F.col("Volume") / F.col("Volume_total_categoria"), 6))

    wmape_all = wmape_cat if wmape_all is None else wmape_all.unionByName(wmape_cat)

# Apenas grupos existentes, sem linhas de TOTAL
wmape_all.orderBy("categoria", "grupo", "modelo").display()

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
                    F.col("Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura"))
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
        agg_exprs.append(F.avg("PercMatrizNeogrid").alias("y_neogrid"))

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
        xaxis=dict(showgrid=True, gridwidth=0.3, gridcolor="rgba(0,0,0,0.08)", zeroline=False, range=[0,0.6]),
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise dos resultados em CDs

# COMMAND ----------

def criar_de_para_filial_cd() -> DataFrame:
    """
    Cria o mapeamento filial → CD usando dados da tabela base.
    """
    print("🔄 Criando de-para filial → CD...")
    
    df_base = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
        .filter(F.col("DtAtual") == "2025-08-01")
        .filter(F.col("CdSku").isNotNull())
    )
    
    de_para_filial_cd = (
        df_base
        .select("cdfilial", "cd_secundario")
        .distinct()
        .filter(F.col("cdfilial").isNotNull())
        .withColumn(
            "cd_vinculo",
            F.coalesce(F.col("cd_secundario"), F.lit("SEM_CD"))
        )
        .drop("cd_secundario")
    )
    
    print(f"✅ De-para filial → CD criado: {de_para_filial_cd.count():,} filiais")
    return de_para_filial_cd

de_para_filial_cd = criar_de_para_filial_cd()

for categoria in categorias_teste:
    df_acuracia_cd = (
        df_acuracia[categoria]
        .join(
            de_para_filial_cd,
            how="left",
            on="CdFilial"
        )
        .groupBy("cd_vinculo", "grupo_de_necessidade")
        .agg(
            F.sum("QtDemanda").alias("QtDemanda"),
            F.sum("Percentual_QtDemanda").alias("Percentual_QtDemanda"),
            # F.sum("Merecimento_Final_Media90_Qt_venda_sem_ruptura").alias("Merecimento_Final_Media90_Qt_venda_sem_ruptura"),
            # F.sum("Merecimento_Final_Media180_Qt_venda_sem_ruptura").alias("Merecimento_Final_Media180_Qt_venda_sem_ruptura"),
            # F.sum("Merecimento_Final_Media270_Qt_venda_sem_ruptura").alias("Merecimento_Final_Media270_Qt_venda_sem_ruptura"),
            # F.sum("Merecimento_Final_Media360_Qt_venda_sem_ruptura").alias("Merecimento_Final_Media360_Qt_venda_sem_ruptura"),
            F.sum("Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura").alias("Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura"),
            F.sum("Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura").alias("Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura"),
            F.sum("Merecimento_Final_MediaAparada270_Qt_venda_sem_ruptura").alias("Merecimento_Final_MediaAparada270_Qt_venda_sem_ruptura"),
            F.sum("Merecimento_Final_MediaAparada360_Qt_venda_sem_ruptura").alias("Merecimento_Final_MediaAparada360_Qt_venda_sem_ruptura"),
            F.sum("PercMatrizNeogrid").alias("PercMatrizNeogrid"),
            F.sum("PercMatrizNeogrid_median").alias("PercMatrizNeogrid_median")
        )
)

from pyspark.sql import functions as F
from functools import reduce

# ==== Config ====
COL_REAL = "Percentual_QtDemanda"
COL_PESO = "QtDemanda"

# modelos base já existentes em `colunas`
pred_cols_base = list(colunas)
extras = ["PercMatrizNeogrid", "PercMatrizNeogrid_median"]

def existing_pred_cols(df, base_cols, maybe_cols):
    return base_cols + [c for c in maybe_cols if c in df.columns]

# ==== sMAPE helpers ====
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

# ==== agregação correta para CD: média ponderada por QtDemanda ====
def aggregate_to_cd(df_cat, de_para):
    df_join = (
        df_cat.join(de_para, on="CdFilial", how="left")
              .withColumn("cd_vinculo", F.coalesce(F.col("cd_vinculo"), F.lit("SEM_CD")))
    )

    # colunas de predição presentes
    preds = existing_pred_cols(df_join, pred_cols_base, extras)

    # numeradores: sum(valor * peso) para cada coluna (inclui o REAL)
    num_exprs = [
        (F.sum(F.col(COL_PESO)).alias("peso_total")),
        (F.sum(F.col(COL_REAL) * F.col(COL_PESO)).alias(f"{COL_REAL}__num"))
    ] + [
        F.sum(F.col(c) * F.col(COL_PESO)).alias(f"{c}__num") for c in preds
    ]

    df_num = (
        df_join
        .groupBy("cd_vinculo", "grupo_de_necessidade")
        .agg(*num_exprs)
        .withColumn(COL_PESO, F.col("peso_total"))
        .drop("peso_total")
    )

    # converte numeradores em médias ponderadas
    df_cd = (
        df_num
        .withColumn(COL_REAL, F.when(F.col(COL_PESO) > 0, F.col(f"{COL_REAL}__num")/F.col(COL_PESO)).otherwise(F.lit(0.0)))
    )

    for c in preds:
        df_cd = df_cd.withColumn(
            c,
            F.when(F.col(COL_PESO) > 0, F.col(f"{c}__num")/F.col(COL_PESO)).otherwise(F.lit(0.0))
        )

    # limpa numeradores temporários
    cols_drop = [f"{COL_REAL}__num"] + [f"{c}__num" for c in preds]
    df_cd = df_cd.drop(*cols_drop)

    return df_cd, preds

# ==== pipeline por categoria: CD-level sMAPE/WSMAPE ====
metrics_cd_all = None

for categoria in categorias_teste:
    df_cat = df_acuracia[categoria]

    df_cd, pred_cols = aggregate_to_cd(df_cat, de_para_filial_cd)

    # adiciona componentes por modelo
    df_cd_comp = reduce(
        lambda acc, c: add_smape_components(acc, c, label=c),
        pred_cols,
        df_cd
    )

    # agrega sMAPE e WSMAPE na categoria inteira (sobre todas as linhas CD×grupo)
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
            ).alias(c)
        )

    metrics_cat = df_cd_comp.select(*aggs)
    metrics_cat = metrics_cat.select(F.explode(F.array(*metrics_cat.columns)).alias("m")).select("m.*")

    metrics_cd_all = metrics_cat if metrics_cd_all is None else metrics_cd_all.unionByName(metrics_cat)

# ==== saída ====
# Métricas (categoria × modelo) no nível CD×grupo agregado corretamente por peso
metrics_cd_all.orderBy("categoria", "modelo").display()

# Se quiser inspecionar o dataframe base já agregado no nível CD×grupo para uma categoria:
# df_cd_comp.select("cd_vinculo","grupo_de_necessidade", COL_PESO, COL_REAL, *pred_cols).limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Buckets de DDE

# COMMAND ----------

from pyspark.sql import functions as F

# Definir buckets de DDE (mesma estrutura do monitoramento)
buckets = ["0-15", "15-30", "30-45", "45-60", "60+", "Nulo"]

# Datas de baseline e piloto (ajustar conforme necessidade)
fim_baseline = "2025-09-05"
inicio_teste = "2025-09-05"

# Carregar dados históricos de estoque para calcular DDE
def load_estoque_historico_com_DDE(categoria: str, data_inicio: str):
    """
    Carrega dados históricos de estoque com cálculo de DDE (Dias De Estoque).
    """
    return (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
        .filter(F.col("DtAtual") >= data_inicio)
        .filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA TELEFONIA CELULAR')
        .join(
            spark.table('databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia'),
            how='inner',
            on='CdSku'
        )
        #.filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
        .dropna(subset='grupo_de_necessidade')
        .filter(~F.col("grupo_de_necessidade").isin(GRUPOS_REMOVER))
        .groupBy("CdFilial", "grupo_de_necessidade", "DtAtual")
        .agg(F.round(F.median("DDE"), 1).alias("DDE_mediano"))
    )

# Calcular buckets de DDE por filial e período
df_estoque_dde = {}
df_counts_export = {}

for categoria in categorias_teste:
    # Carregar dados históricos
    df_estoque = load_estoque_historico_com_DDE(categoria, data_inicio)
    
    # Classificar períodos
    df_estoque = df_estoque.withColumn(
        "periodo_analise",
        F.when(
            F.col("DtAtual") < fim_baseline, F.lit('baseline')
        )
        .when(F.col("DtAtual") >= inicio_teste, F.lit('piloto'))
        .otherwise(F.lit('ignorar'))
    )
    
    # Filtrar apenas baseline e piloto
    df_estoque = df_estoque.filter(F.col("periodo_analise").isin('baseline', 'piloto'))
    
    # Criar buckets de DDE
    df_buckets = (
        df_estoque
        .groupBy("CdFilial", "grupo_de_necessidade", "periodo_analise")
        .agg(
            F.round(F.mean("DDE_mediano"), 1).alias("DDE_medio"),
            F.round(F.percentile_approx("DDE_mediano", 0.5, 100), 1).alias("DDE_mediano_agregado")
        )
        # Bucket para média
        .withColumn(
            "bucket_DDE_medio",
            F.when(F.col("DDE_medio").isNull(), "Nulo")
            .when(F.col("DDE_medio") > 60, "60+")
            .when(F.col("DDE_medio") >= 45, "45-60")
            .when(F.col("DDE_medio") >= 30, "30-45")
            .when(F.col("DDE_medio") >= 15, "15-30")
            .when(F.col("DDE_medio") >= 0, "0-15")
            .otherwise("Nulo")
        )
        # Bucket para mediana
        .withColumn(
            "bucket_DDE_mediano",
            F.when(F.col("DDE_mediano_agregado").isNull(), "Nulo")
            .when(F.col("DDE_mediano_agregado") > 60, "60+")
            .when(F.col("DDE_mediano_agregado") >= 45, "45-60")
            .when(F.col("DDE_mediano_agregado") >= 30, "30-45")
            .when(F.col("DDE_mediano_agregado") >= 15, "15-30")
            .when(F.col("DDE_mediano_agregado") >= 0, "0-15")
            .otherwise("Nulo")
        )
    )
    
    df_estoque_dde[categoria] = df_buckets
    
    # Contagens por bucket (média)
    df_counts_medio = (
        df_buckets
        .groupBy("periodo_analise")
        .pivot("bucket_DDE_medio", buckets)
        .count()
        .na.fill(0)
    )
    for b in buckets:
        df_counts_medio = df_counts_medio.withColumn(b, F.col(b).cast("long"))
    df_counts_medio = (
        df_counts_medio
        .select("periodo_analise", *buckets)
        .withColumn("Metrica", F.lit("DDE_medio"))
        .withColumn("Categoria", F.lit(categoria))
    )
    
    # Contagens por bucket (mediana)
    df_counts_mediano = (
        df_buckets
        .groupBy("periodo_analise")
        .pivot("bucket_DDE_mediano", buckets)
        .count()
        .na.fill(0)
    )
    for b in buckets:
        df_counts_mediano = df_counts_mediano.withColumn(b, F.col(b).cast("long"))
    df_counts_mediano = (
        df_counts_mediano
        .select("periodo_analise", *buckets)
        .withColumn("Metrica", F.lit("DDE_mediano"))
        .withColumn("Categoria", F.lit(categoria))
    )
    
    # Union e exibição
    df_counts_export[categoria] = df_counts_medio.unionByName(df_counts_mediano)
    
    print(f"Categoria: {categoria}")
    df_counts_export[categoria].display()
