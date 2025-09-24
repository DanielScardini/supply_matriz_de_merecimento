# Databricks notebook source
# MAGIC %md
# MAGIC # Imports e configurações

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any
import plotly.graph_objects as go

# Inicialização do Spark
spark = SparkSession.builder.appName("dashboard_estoque").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))


GRUPOS_TESTE = ['TV 50 ALTO P', 'TV 55 ALTO P']
GRUPOS_TESTE = ['Telef pp']

print(GRUPOS_TESTE)

LISTA_YEAR_MONTH = [202508, 202509]
DIRETORIA = 'DIRETORIA DE TELAS'


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão e agregação de dados de estoque e lojas

# COMMAND ----------

df_estoque = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4_online')
    .filter(F.col("DsEstoqueLojaDeposito") == 'D')
    .filter(F.col("year_month").isin(LISTA_YEAR_MONTH))
    #.filter(F.col("NmAgrupamentoDiretoriaSetor") == DIRETORIA)
    .join(
        spark.table("databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia")
        .select("CdSku", "gemeos"),
        how="left",
        on="CdSku"
    )
    .withColumn("grupo_de_necessidade", F.col("gemeos"))
    .filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
    .groupBy("DtAtual")
    .agg(
        F.sum(F.col("QtMercadoria")).alias("Vendas_online"),
        F.sum("EstoqueLoja").alias("EstoqueCDs"),
        F.round(
            100*F.sum("ReceitaPerdidaRuptura")/F.sum("Receita"), 1).alias("Ruptura_receita_perc_CD"),
        F.round(
            F.median("DDE"), 1).alias("DDE_medio_CD")
        )
    
    .orderBy(F.col("DtAtual"))
)

df_lojas = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4_online')
    .filter(F.col("DsEstoqueLojaDeposito") == 'L')
    .filter(F.col("year_month").isin(LISTA_YEAR_MONTH))
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == DIRETORIA)
    .join(
        spark.table("databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia")
        .select("CdSku", "gemeos"),
        how="left",
        on="CdSku"
    )
    .withColumn("grupo_de_necessidade", F.col("gemeos"))
    .filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
    .groupBy("DtAtual")
    .agg(
        F.sum(F.col("QtMercadoria")).alias("Vendas_loja"),
        F.sum("EstoqueLoja").alias("EstoqueLojas"),
        F.round(
            100*F.sum("ReceitaPerdidaRuptura")/F.sum("Receita"), 1).alias("Ruptura_receita_perc_Loja"),
        F.round(
            F.median("DDE"), 1).alias("DDE_medio_Loja")
        )
    
    .orderBy(F.col("DtAtual"))
)

df_estoque.display()
df_lojas.display()

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go

# ---------------- 1) Bases ----------------
df_estoque = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4_online')
    .filter(F.col("DsEstoqueLojaDeposito") == 'D')
    .filter(F.col("year_month").isin(LISTA_YEAR_MONTH))
    #.filter(F.col("NmAgrupamentoDiretoriaSetor") == DIRETORIA)
    .join(
        spark.table("databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia")
        .select("CdSku", "gemeos"),
        how="left", on="CdSku"
    )
    .withColumn("grupo_de_necessidade", F.col("gemeos"))
    .filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
    .groupBy("DtAtual")
    .agg(
        F.sum("QtMercadoria").alias("Vendas_online"),
        F.sum("EstoqueLoja").alias("EstoqueCDs"),
        F.round(100*F.sum("ReceitaPerdidaRuptura")/F.sum("Receita"),2).alias("Ruptura_receita_perc_CD"),
        F.round(F.expr("percentile_approx(DDE,0.5)"),2).alias("DDE_medio_CD")
    )
)

df_lojas = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4_online')
    .filter(F.col("DsEstoqueLojaDeposito") == 'L')
    .filter(F.col("year_month").isin(LISTA_YEAR_MONTH))
    #.filter(F.col("NmAgrupamentoDiretoriaSetor") == DIRETORIA)
    .join(
        spark.table("databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia")
        .select("CdSku", "gemeos"),
        how="left", on="CdSku"
    )
    .withColumn("grupo_de_necessidade", F.col("gemeos"))
    .filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
    .groupBy("DtAtual")
    .agg(
        F.sum("QtMercadoria").alias("Vendas_loja"),
        F.sum("EstoqueLoja").alias("EstoqueLojas"),
        F.round(100*F.sum("ReceitaPerdidaRuptura")/F.sum("Receita"),2).alias("Ruptura_receita_perc_Loja"),
        F.round(F.expr("percentile_approx(DDE,0.5)"),2).alias("DDE_medio_Loja")
    )
)

# ---------------- 2) Merge Pandas ----------------
pdf = pd.merge(
    df_estoque.toPandas(), df_lojas.toPandas(), on="DtAtual", how="outer"
).sort_values("DtAtual")

pdf["DtAtual"] = pd.to_datetime(pdf["DtAtual"])

# ---------------- 3) Cores ----------------
COLORS = {
    "estoque_cd":   "#1f4e79",  # azul escuro
    "estoque_loja": "#6fa8dc",  # azul claro
    "demanda_cd":   "#1b5e20",  # verde escuro
    "demanda_loja": "#9ccc65",  # verde claro
    "ruptura_cd":   "#8b0000",  # vermelho escuro
    "ruptura_loja": "#ff7f7f",  # vermelho claro
    "dde_cd":       "#6a3d9a",  # roxo
    "dde_loja":     "#b39ddb",  # lilás
}


# --- MM3 para demanda ---
pdf = pdf.sort_values("DtAtual")
pdf["Vendas_online_mm3"] = pdf["Vendas_online"].rolling(window=3, min_periods=1).mean()
pdf["Vendas_loja_mm3"]   = pdf["Vendas_loja"].rolling(window=3, min_periods=1).mean()

# ---------------- 4) Figura com 2 linhas ----------------
fig = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    subplot_titles=("Estoques e Demandas","DDE e Ruptura (%)"),
    vertical_spacing=0.15,
    specs=[[{"secondary_y": True}], [{"secondary_y": True}]]
)

# --- Topo: Estoques (esq) + Demandas (dir)
fig.add_trace(go.Scatter(
    x=pdf["DtAtual"], y=pdf["EstoqueCDs"], name="Estoque CD",
    line=dict(color=COLORS["estoque_cd"])
), row=1, col=1, secondary_y=False)

fig.add_trace(go.Scatter(
    x=pdf["DtAtual"], y=pdf["EstoqueLojas"], name="Estoque Loja",
    line=dict(color=COLORS["estoque_loja"])
), row=1, col=1, secondary_y=False)

# Demanda CD (MM3) no eixo direito
fig.add_trace(go.Scatter(
    x=pdf["DtAtual"], y=pdf["Vendas_online_mm3"], name="Demanda CD (MM3)",
    line=dict(color=COLORS["demanda_cd"])
), row=1, col=1, secondary_y=True)

# Demanda Loja (MM3) no eixo direito
fig.add_trace(go.Scatter(
    x=pdf["DtAtual"], y=pdf["Vendas_loja_mm3"], name="Demanda Loja (MM3)",
    line=dict(color=COLORS["demanda_loja"])
), row=1, col=1, secondary_y=True)

# fig.add_trace(go.Scatter(
#     x=pdf["DtAtual"], y=pdf["Vendas_online"], name="Demanda CD",
#     line=dict(color=COLORS["demanda_cd"])
# ), row=1, col=1, secondary_y=True)

# fig.add_trace(go.Scatter(
#     x=pdf["DtAtual"], y=pdf["Vendas_loja"], name="Demanda Loja",
#     line=dict(color=COLORS["demanda_loja"])
# ), row=1, col=1, secondary_y=True)

# --- Base: DDE (esq) + Ruptura (dir)
fig.add_trace(go.Scatter(
    x=pdf["DtAtual"], y=pdf["DDE_medio_CD"], name="DDE CD (dias)",
    line=dict(color=COLORS["dde_cd"])
), row=2, col=1, secondary_y=False)

fig.add_trace(go.Scatter(
    x=pdf["DtAtual"], y=pdf["DDE_medio_Loja"], name="DDE Loja (dias)",
    line=dict(color=COLORS["dde_loja"])
), row=2, col=1, secondary_y=False)

fig.add_trace(go.Scatter(
    x=pdf["DtAtual"], y=pdf["Ruptura_receita_perc_CD"], name="Ruptura CD (%)",
    line=dict(color=COLORS["ruptura_cd"])
), row=2, col=1, secondary_y=True)

fig.add_trace(go.Scatter(
    x=pdf["DtAtual"], y=pdf["Ruptura_receita_perc_Loja"], name="Ruptura Loja (%)",
    line=dict(color=COLORS["ruptura_loja"])
), row=2, col=1, secondary_y=True)

# --- Layout ---
fig.update_layout(
    title="Indicadores CD vs Loja",
    paper_bgcolor="#f2f2f2",
    plot_bgcolor="#f2f2f2",
    hovermode="x unified",
    width=1400, height=900,
    legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
    margin=dict(l=60, r=40, t=100, b=80),
    font=dict(size=16)
)

# Rangeslider
fig.update_xaxes(rangeslider=dict(visible=True), row=2, col=1)

# Eixos
fig.update_yaxes(title_text="Estoque", row=1, col=1, secondary_y=False)
fig.update_yaxes(title_text="Demanda", row=1, col=1, secondary_y=True)
fig.update_yaxes(title_text="DDE (dias)", row=2, col=1, secondary_y=False)
fig.update_yaxes(title_text="Ruptura (%)", row=2, col=1, secondary_y=True)

fig.show()
