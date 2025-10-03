# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any
import plotly.graph_objects as go

!pip install openpyxl

# Inicialização do Spark
spark = SparkSession.builder.appName("envio_manual_lojas_telas").getOrCreate()

PROPORCAO_OFF = 0.735

# Configuração das tabelas de matriz
TABELAS_MATRIZ_MERECIMENTO = {
    "DIRETORIA DE TELAS": {
        "offline": "databox.bcg_comum.supply_matriz_merecimento_de_telas_teste0110",
        "online": "databox.bcg_comum.supply_matriz_merecimento_de_telas_online_teste0110"
    }
}

# Configuração da coluna de merecimento
COLUNAS_MERECIMENTO = {
    "DIRETORIA DE TELAS": "Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura"
}

# Configuração de filtros
FILTROS_GRUPO_NECESSIDADE_REMOCAO = {
    "DIRETORIA DE TELAS": ["FORA DE LINHA", "SEM_GN"]
}

FLAG_SELECAO_REMOCAO = {
    "DIRETORIA DE TELAS": "REMOÇÃO"
}

FILTROS_GRUPO_NECESSIDADE_SELECAO = {
    "DIRETORIA DE TELAS": [
        "TV 50 ALTO P", 
        "TV 55 ALTO P",
        "TV 43 PP", 
        "TV 75 PP",
        "TV 75 ALTO P",
        ""
    ]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura das demandas de unidade

# COMMAND ----------

df_demanda_envio = (
    spark.createDataFrame(
        pd.read_excel('/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/Black telas.xlsx', engine='openpyxl', skiprows=1, sheet_name="Planilha3")
    )
    .withColumnRenamed("Codigo", "CdSku")
    .withColumnRenamed("Abastecer", "Envio Adicional")
    .join(
        spark.table(TABELAS_MATRIZ_MERECIMENTO['DIRETORIA DE TELAS']["offline"])
                    .select("CdSku", "grupo_de_necessidade")
                    .distinct(),
        how="left",
        on="CdSku")
)

df_demanda_envio.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matrizes de merecimento on e off

# COMMAND ----------

def processar_matriz_merecimento(categoria: str, canal: str) -> DataFrame:
    """
    Processa a matriz de merecimento para uma categoria e canal específicos.
    Segue o mesmo racional do código de salvar matrizes.
    
    Args:
        categoria: Categoria da diretoria
        canal: Canal (offline ou online)
        
    Returns:
        DataFrame processado com merecimento normalizado
    """
    print(f"🔄 Processando matriz para: {categoria} - {canal}")
    
    # Configurações específicas
    tabela = TABELAS_MATRIZ_MERECIMENTO[categoria][canal]
    coluna_merecimento = COLUNAS_MERECIMENTO[categoria]
    flag_tipo = FLAG_SELECAO_REMOCAO.get(categoria, "REMOÇÃO")
    filtros_grupo_remocao = FILTROS_GRUPO_NECESSIDADE_REMOCAO[categoria]
    filtros_grupo_selecao = FILTROS_GRUPO_NECESSIDADE_SELECAO[categoria]
    
    print(f"  • Tabela: {tabela}")
    print(f"  • Coluna merecimento: {coluna_merecimento}")
    print(f"  • Tipo de filtro: {flag_tipo}")
    
    # Carregamento dos dados base
    df_base = (
        spark.table(tabela)
        .select(
            "CdFilial", "CdSku", "grupo_de_necessidade",
            (100 * F.col(coluna_merecimento)).alias(f"Merecimento_Percentual_{canal}_raw")
        )
    )
    
    # Aplicar filtro baseado no flag
    if flag_tipo == "SELEÇÃO":
        df_raw = df_base.filter(F.col("grupo_de_necessidade").isin(filtros_grupo_selecao))
        print(f"  • Aplicado filtro de SELEÇÃO: mantendo apenas {filtros_grupo_selecao}")
    else:
        df_raw = df_base.filter(~F.col("grupo_de_necessidade").isin(filtros_grupo_remocao))
        print(f"  • Aplicado filtro de REMOÇÃO: removendo {filtros_grupo_remocao}")
    
    # Join com dados de filiais
    df_com_filiais = df_raw.join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica"),
        on="CdFilial", how="left"
    )
    
    # Normalização por SKU
    window_sku = Window.partitionBy("CdSku")
    total_sku = F.sum(f"Merecimento_Percentual_{canal}_raw").over(window_sku)
    
    df_normalizado = (
        df_com_filiais
        .withColumn(
            f"Merecimento_Percentual_{canal}",
            F.round(
                F.when(total_sku > 0, F.col(f"Merecimento_Percentual_{canal}_raw") * (100.0 / total_sku))
                .otherwise(0.0), 
                3
            )
        )
        .drop(f"Merecimento_Percentual_{canal}_raw")
    )
    
    # Regra especial para canal online: sobrescrever CdFilial 1401 → 14
    if canal == "online":
        df_normalizado = (
            df_normalizado
            .withColumn("CdFilial", F.when(F.col("CdFilial") == 1401, 14).otherwise(F.col("CdFilial")))
        )
        print("  • Aplicada regra especial: CdFilial 1401 → 14")
    
    # Agregação por grupo de necessidade - mesmo racional das análises factuais
    df_agregado = (
        df_normalizado
        .groupBy("CdFilial", "grupo_de_necessidade")
        .agg(
            F.round(F.mean(f"Merecimento_Percentual_{canal}"), 3).alias(f"Merecimento_Percentual_{canal}")
        )
    )
    
    print(f"✅ Matriz processada:")
    print(f"  • Total de registros: {df_agregado.count():,}")
    print(f"  • Filiais únicas: {df_agregado.select('CdFilial').distinct().count():,}")
    print(f"  • Grupos únicos: {df_agregado.select('grupo_de_necessidade').distinct().count():,}")
    
    return df_agregado

# Processar matrizes online e offline
df_merecimento_offline = (
    processar_matriz_merecimento("DIRETORIA DE TELAS", "offline")
    .join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmFilial", "NmUF", "NmPorteLoja"),
        how="left",
        on="CdFilial")
    .orderBy(F.desc("Merecimento_Percentual_offline"))

    )
df_merecimento_online = (
    processar_matriz_merecimento("DIRETORIA DE TELAS", "online")
    .join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmFilial", "NmUF", "NmPorteLoja"),
        how="left",
        on="CdFilial")
    .orderBy(F.desc("Merecimento_Percentual_online"))
    )

print("\n📊 MATRIZES PROCESSADAS:")
print("=" * 50)
df_merecimento_offline.display()
print("\n" + "=" * 50)
df_merecimento_online.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Desdobramento da demanda entre ON e OFF

# COMMAND ----------

from pyspark.sql import functions as F, Window

# Window
w_cd = Window.partitionBy("CdSku", "CdFilialEntrega")

# Base de mapeamento (reutilizada para ambos os usos)
plano = (
    spark.table("data_engineering_prd.context_logistica.planoabastecimento")
    .select(
        F.col("CdFilialEntrega").cast("int").alias("CdFilialEntrega"),
        F.col("CdLoja").cast("int").alias("CdFilial")
    )
    .distinct()
)

# Conjunto de valores existentes em CdFilialEntrega para checagem de existência
entregas_set = (
    spark.table("data_engineering_prd.context_logistica.planoabastecimento")
    .select(F.col("CdFilialEntrega").cast("int").alias("CdFilial_ref"))
    .where(F.col("CdFilial_ref").isNotNull())
    .distinct()
)

# Pipeline principal
df_demanda_on_off = (
    df_merecimento_offline
    .join(
        df_merecimento_online,
        on=["CdFilial", "NmFilial", "NmUF", "NmPorteLoja", "grupo_de_necessidade"],
        how="outer"
    )
    .fillna(0, subset=["Merecimento_Percentual_online", "Merecimento_Percentual_offline"])
    .join(
        df_demanda_envio.select("CdSku", "grupo_de_necessidade", "ITEM", "Envio adicional"),
        how="inner",
        on="grupo_de_necessidade"
    )
    .withColumn(
        "merecimento_percentual_propocionalizado_on_off",
        F.round(
            (F.col("Merecimento_Percentual_offline") * F.lit(PROPORCAO_OFF)) +
            (F.col("Merecimento_Percentual_online") * (1 - F.lit(PROPORCAO_OFF))), 3)
    )
    .withColumn("proporção_demanda_off_percentual", F.round(F.lit(100 * PROPORCAO_OFF), 1))
    .withColumn("demanda_envio_on",  F.col("Envio adicional") * (1 - F.lit(PROPORCAO_OFF)))
    .withColumn("demanda_envio_off", F.col("Envio adicional") * (F.lit(PROPORCAO_OFF)))
    .withColumn("Qtd_pecas_on",  F.col("demanda_envio_on")  / 100 * F.col("Merecimento_Percentual_online"))
    .withColumn("Qtd_pecas_off", F.col("demanda_envio_off") / 100 * F.col("Merecimento_Percentual_offline"))
    .withColumn("Qtd_pecas_total", F.round(F.col("Qtd_pecas_on") + F.col("Qtd_pecas_off"), 0))
    # Traz CdFilialEntrega por CdFilial (loja)
    .join(plano, how="left", on="CdFilial")
    # Marca se CdFilial existe na lista de CdFilialEntrega ou é 14
    .join(F.broadcast(entregas_set), on=F.col("CdFilial") == F.col("CdFilial_ref"), how="left")
    .withColumn(
        "should_fill_entrega",
        (F.col("CdFilialEntrega").isNull()) &
        ( (F.col("CdFilial") == F.lit(14)) | F.col("CdFilial_ref").isNotNull() )
    )
    # Preenche somente quando a condição acima é verdadeira
    .withColumn(
        "CdFilialEntrega",
        F.when(F.col("should_fill_entrega"), F.col("CdFilial")).otherwise(F.col("CdFilialEntrega"))
    )
    .drop("CdFilial_ref", "should_fill_entrega")
    # Remove linhas que ainda ficaram nulas
    .dropna(subset=["CdFilialEntrega"])
    # Agrega merecimento por SKU x CD
    .withColumn("MerecimentoCD", F.round(F.sum("merecimento_percentual_propocionalizado_on_off").over(w_cd), 3))
)

df_demanda_on_off.display()

# COMMAND ----------

from pyspark.sql import functions as F

TOL = 0.1  # tolerância em pontos percentuais

# Soma por SKU
df_check = (
    df_demanda_on_off
    .groupBy("CdSku")
    .agg(
        F.round(F.sum("merecimento_percentual_propocionalizado_on_off"), 3)
         .alias("soma_percentual")
    )
    .withColumn("diff_pp", F.round(F.col("soma_percentual") - F.lit(100.0), 3))
    .withColumn("ok", F.abs(F.col("diff_pp")) <= F.lit(TOL))
)

# SKUs com problema
df_off = df_check.filter(~F.col("ok"))

# Resultados
df_check.display()      # visão geral por SKU
df_off.display()        # somente SKUs fora de 100% ± tolerância

# (Opcional) Detalhar contribuições dos SKUs com problema
df_detalhe_off = (
    df_demanda_on_off
    .join(df_off.select("CdSku"), on="CdSku", how="inner")
    .select(
        "CdSku", "CdFilialEntrega",
        "merecimento_percentual_propocionalizado_on_off"
    )
    .orderBy("CdSku", "CdFilialEntrega")
)
df_detalhe_off.display()

# COMMAND ----------

from pyspark.sql import functions as F

TOL = 0.1  # tolerância em pontos percentuais

# --- Checagem por SKU ---
df_check_sku = (
    df_demanda_on_off
    .groupBy("CdSku")
    .agg(F.round(F.sum("merecimento_percentual_propocionalizado_on_off"), 3)
         .alias("soma_percentual"))
    .withColumn("diff_pp", F.round(F.col("soma_percentual") - F.lit(100.0), 3))
    .withColumn("ok", F.abs(F.col("diff_pp")) <= F.lit(TOL))
)

# --- Checagem por SKU x CdFilialEntrega (MerecimentoCD) ---
df_check_cd = (
    df_demanda_on_off
    .groupBy("CdSku", "CdFilialEntrega")
    .agg(F.round(F.sum("merecimento_percentual_propocionalizado_on_off"), 3)
         .alias("soma_percentual"))
    .join(
        df_demanda_on_off
        .select("CdSku", "CdFilialEntrega", "MerecimentoCD")
        .distinct(),
        on=["CdSku", "CdFilialEntrega"],
        how="left"
    )
    .withColumn("diff_pp", F.round(F.col("soma_percentual") - F.col("MerecimentoCD"), 3))
    .withColumn("ok", F.abs(F.col("diff_pp")) <= F.lit(TOL))
)

# Mostra resultados
df_check_sku.display()   # soma por SKU
df_check_cd.display()    # soma vs MerecimentoCD

# COMMAND ----------

df_check_cd.groupBy('CdSku').agg(F.sum('MerecimentoCD')).display()

# COMMAND ----------

hoje_str = datetime.now().strftime("%Y-%m-%d")

(
    df_demanda_on_off.select("CdSku", "ITEM", "grupo_de_necessidade", "CdFilial", "NmFilial", 
                         "NmPorteLoja", "Merecimento_Percentual_offline",
                         "Merecimento_Percentual_online","proporção_demanda_off_percentual", 
                         F.round(
                             F.col("merecimento_percentual_propocionalizado_on_off"), 3).alias("merecimento_percentual_propocionalizado_on_off"),
                         "MerecimentoCD"
                         ).toPandas()
).to_excel(f"merecimento_proporcional_on_off_envio_manual_{hoje_str}.xlsx",
    index=False)

# COMMAND ----------

hoje_str = datetime.now().strftime("%Y-%m-%d")

(
    df_demanda_on_off.select("CdSku", "ITEM", "grupo_de_necessidade", 
                         "MerecimentoCD",
                         "CdFilialEntrega"
                         ).dropDuplicates().toPandas()
).to_excel(f"merecimento_CD_on_off_envio_manual_{hoje_str}.xlsx",
    index=False)
