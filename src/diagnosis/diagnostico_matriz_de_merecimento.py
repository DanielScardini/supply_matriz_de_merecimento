# Databricks notebook source
# =========================================================
# 🗄️ CARREGAMENTO DA BASE
# =========================================================
# Lê a tabela de vendas & estoque que será usada no monitoramento (Source: Torre de Controle)
# Tabela: databox.bcg_comum.supply_gold_prod_kpi_vendas_estoque_lojas
# =========================================================

df_base = spark.table("databox.bcg_comum.supply_gold_prod_kpi_vendas_estoque_lojas")

# (Opcional) cachear para acelerar as próximas etapas
df_base = df_base.cache()

print("✅ Base carregada:")
df_base.display(5, truncate=False)


# COMMAND ----------

# =========================================================
# 📦 MONITORAMENTO DE MATRIZ DE MERECIMENTO – PYSPARK
# =========================================================
# Premissa: já existe um DataFrame Spark chamado `df_base`
# com o schema informado.
# =========================================================

from pyspark.sql import functions as F

# 🎯 PARÂMETROS GERAIS
DAYS_TO_EXPIRY_ALERT = 7          # dias para alerta de validade da matriz
THRESHOLD_PCT_DIFF = 0.01         # 1 p.p. de desvio entre matriz e vendas YTD

# ⚠️ REVISAR: quais labels indicam matriz automática na sua base
AUTOMATIC_MATRIX_LABELS = ["AUTOMATICA", "AUTO", "AUTOMÁTICA"]

# ⚠️ PONTO DE ATENÇÃO:
# - Se PctMatriz / PctVendas estiverem em escala 0–100 (e não 0–1),
#   ajustar THRESHOLD_PCT_DIFF (por exemplo, para 10 em vez de 0.10).


# COMMAND ----------

# ---------------------------------------------------------
# 🧱 2) NORMALIZAÇÃO POR CANAL (ON / OFF)
# ---------------------------------------------------------

# Dimensões principais
dim_cols = [
    "DtAtual",
    "CdFilial",
    "NmFilial",
    "CdSku",
    "NmSku",
    "NmAgrupamentoDiretoriaSetor",
    "NmSetorGerencial",
    "NmEspecieGerencial",
    "NmMarca",
]

# 🧩 Canal ON
df_on = (
    df_base
    .select(
        *dim_cols,
        F.lit("ON").alias("Canal"),
        F.col("PctMatriz_ON").alias("PctMatriz"),
        F.col("DtValidade_ON").alias("DtValidade_str"),
        F.col("TipoMatriz_ON").alias("TipoMatriz"),
        F.col("PctVendas_ON_YTD").alias("PctVendas_YTD"),
        F.col("Receita_ON_YTD").alias("Receita_YTD"),
        F.col("QtMercadoria_ON_YTD").alias("QtMercadoria_YTD"),
    )
)

# 🧩 Canal OFF
df_off = (
    df_base
    .select(
        *dim_cols,
        F.lit("OFF").alias("Canal"),
        F.col("PctMatriz_OFF").alias("PctMatriz"),
        F.col("DtValidade_OFF").alias("DtValidade_str"),
        F.col("TipoMatriz_OFF").alias("TipoMatriz"),
        # ⚠️ PONTO DE ATENÇÃO:
        # Supondo que o % OFF seja (1 - %ON). Se existir PctVendas_OFF_YTD,
        # troque a expressão abaixo por F.col("PctVendas_OFF_YTD").
        (F.lit(1.0) - F.col("PctVendas_ON_YTD")).alias("PctVendas_YTD"),
        F.col("Receita_OFF_YTD").alias("Receita_YTD"),
        F.col("QtMercadoria_OFF_YTD").alias("QtMercadoria_YTD"),
    )
)

# União dos canais em um único DataFrame normalizado
df_norm = df_on.unionByName(df_off)


# COMMAND ----------

# ---------------------------------------------------------
# 🗓️ 3) TRATAMENTO DE DATAS DE VALIDADE
# ---------------------------------------------------------

# ⚠️ PONTO DE ATENÇÃO:
# - Ajustar o formato de DtValidade_str conforme origem real.
#   Ex.: se estiver em "dd/MM/yyyy", usar "dd/MM/yyyy".
df_norm = (
    df_norm
    .withColumn(
        "DtValidade",
        F.to_date(F.col("DtValidade_str"), "yyyy-MM-dd")  # TODO: revisar formato
    )
    .withColumn(
        "DiasParaVencer",
        F.datediff(F.col("DtValidade"), F.col("DtAtual"))
    )
)

df_norm.display(5, truncate=False)


# COMMAND ----------

# ---------------------------------------------------------
# 🚨 4) ALERTA #1 – MATRIZ NÃO AUTOMÁTICA
# ---------------------------------------------------------
# Regra:
# - SKU/canal com PctMatriz informado (> 0)
# - TipoMatriz não está na lista de matrizes automáticas

cond_tem_matriz = (F.col("PctMatriz").isNotNull()) & (F.col("PctMatriz") > 0)
cond_nao_automatica = ~F.upper(F.col("TipoMatriz")).isin(
    [x.upper() for x in AUTOMATIC_MATRIX_LABELS]
)

alerta_matriz_nao_auto = (
    df_norm
    .where(cond_tem_matriz & cond_nao_automatica)
    .withColumn("TipoAlerta", F.lit("01_MATRIZ_NAO_AUTOMATICA"))
    .withColumn("DescricaoAlerta", F.lit("SKU com matriz de merecimento mas TipoMatriz ≠ automática"))
    .withColumn("Severidade", F.lit("MÉDIA"))
)

print("📋 Alerta 1 – Matriz não automática")
alerta_matriz_nao_auto.show(20, truncate=False)


# COMMAND ----------

# ---------------------------------------------------------
# ⏰ 5) ALERTA #2 – MATRIZ PRÓXIMA DE VENCER
# ---------------------------------------------------------
# Regra:
# - DtValidade não nula
# - 0 <= DiasParaVencer <= DAYS_TO_EXPIRY_ALERT

cond_dt_validade_ok = F.col("DtValidade").isNotNull()
cond_prox_vencer = (
    (F.col("DiasParaVencer") >= 0) &
    (F.col("DiasParaVencer") <= DAYS_TO_EXPIRY_ALERT)
)

alerta_validade = (
    df_norm
    .where(cond_dt_validade_ok & cond_prox_vencer)
    .withColumn("TipoAlerta", F.lit("02_MATRIZ_PROXIMA_VENCER"))
    .withColumn(
        "DescricaoAlerta",
        F.concat(
            F.lit("Matriz vence em "),
            F.col("DiasParaVencer").cast("string"),
            F.lit(" dia(s)")
        )
    )
    .withColumn(
        "Severidade",
        F.when(F.col("DiasParaVencer") <= 2, "ALTA")
         .otherwise("MÉDIA")
    )
)

print("📋 Alerta 2 – Matriz próxima de vencer")
alerta_validade.show(20, truncate=False)


# COMMAND ----------

# ---------------------------------------------------------
# 📉 6) ALERTA #3 – DESVIO % VENDAS vs MATRIZ (YTD)
# ---------------------------------------------------------
# Regra:
# - PctMatriz e PctVendas_YTD não nulos
# - QtMercadoria_YTD > 0
# - |PctVendas_YTD - PctMatriz| >= THRESHOLD_PCT_DIFF

cond_pct_ok = F.col("PctMatriz").isNotNull() & F.col("PctVendas_YTD").isNotNull()
cond_tem_venda = F.col("QtMercadoria_YTD") > 0
desvio_pct = (F.col("PctVendas_YTD") - F.col("PctMatriz"))

alerta_desvio_pct = (
    df_norm
    .where(
        cond_pct_ok &
        cond_tem_venda &
        (F.abs(desvio_pct) >= THRESHOLD_PCT_DIFF)
    )
    .withColumn("DesvioPct", desvio_pct)
    .withColumn(
        "DirecaoDesvio",
        F.when(F.col("DesvioPct") < 0, "ABAIXO_MATRIZ")
         .otherwise("ACIMA_MATRIZ")
    )
    .withColumn("TipoAlerta", F.lit("03_DESVIO_VENDAS_VS_MATRIZ_YTD"))
    .withColumn(
        "DescricaoAlerta",
        F.concat(
            F.lit("Vendas YTD "),
            F.when(F.col("DirecaoDesvio") == "ABAIXO_MATRIZ", F.lit("abaixo"))
             .otherwise(F.lit("acima")),
            F.lit(" do % sugerido pela matriz")
        )
    )
    .withColumn(
        "Severidade",
        F.when(F.abs(F.col("DesvioPct")) >= 0.20, "ALTA")     # >= 20 p.p.
         .when(F.abs(F.col("DesvioPct")) >= 0.10, "MÉDIA")    # entre 10 e 20 p.p.
         .otherwise("BAIXA")
    )
)

print("📋 Alerta 3 – Desvio de % vendas vs % matriz (YTD)")
alerta_desvio_pct.show(20, truncate=False)


# COMMAND ----------

# ---------------------------------------------------------
# 🧾 7) DATAFRAME ÚNICO DE MONITORAMENTO
# ---------------------------------------------------------

cols_monitor_base = dim_cols + [
    "Canal",
    "PctMatriz",
    "PctVendas_YTD",
    "QtMercadoria_YTD",
    "Receita_YTD",
    "DtValidade",
    "DiasParaVencer",
    "TipoMatriz",
]

def padroniza(df):
    # Garante colunas base + colunas de alerta
    return (
        df
        .select(
            *cols_monitor_base,
            F.col("TipoAlerta"),
            F.col("DescricaoAlerta"),
            F.col("Severidade"),
            # Campos específicos do alerta 3 (ficam nulos nos outros casos)
            F.col("DesvioPct"),
            F.col("DirecaoDesvio")
        )
    )

df_alerta_1 = padroniza(alerta_matriz_nao_auto)
df_alerta_2 = padroniza(alerta_validade)
df_alerta_3 = padroniza(alerta_desvio_pct)

monitor_matriz = (
    df_alerta_1
    .unionByName(df_alerta_2)
    .unionByName(df_alerta_3)
)

print("📋 Tabela consolidada de monitoramento de matriz")
monitor_matriz.show(50, truncate=False)


# COMMAND ----------

# ---------------------------------------------------------
# 📊 8) VISÕES RESUMIDAS PARA MONITORAMENTO
# ---------------------------------------------------------

# 📌 Resumo por tipo de alerta, canal e severidade
print("📊 Resumo de alertas por tipo, canal e severidade")
(
    monitor_matriz
    .groupBy("TipoAlerta", "Canal", "Severidade")
    .agg(
        F.count("*").alias("QtdLinhas"),
        F.countDistinct("CdSku").alias("QtdSkus"),
        F.countDistinct("CdFilial").alias("QtdLojas")
    )
    .orderBy("TipoAlerta", "Canal", "Severidade")
    .show(truncate=False)
)

# 🎯 Foco: top 20 SKUs mais abaixo da matriz
print("📉 Top 20 SKUs com maior desvio NEGATIVO de % vendas vs matriz (YTD)")
(
    monitor_matriz
    .where(F.col("TipoAlerta") == "03_DESVIO_VENDAS_VS_MATRIZ_YTD")
    .orderBy(F.col("DesvioPct").asc())   # mais abaixo da matriz primeiro
    .select(
        "Canal", "CdFilial", "NmFilial", "CdSku", "NmSku",
        "PctMatriz", "PctVendas_YTD", "DesvioPct", "Severidade"
    )
    .show(20, truncate=False)
)

