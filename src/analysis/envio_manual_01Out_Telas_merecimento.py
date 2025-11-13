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
        "TV 75 ALTO P"
    ]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura das demandas de unidade

# COMMAND ----------

df_demanda_envio = (
    spark.createDataFrame(
        pd.read_excel('/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/Envio adicional Telas.xlsx', engine='openpyxl', skiprows=1)
    )
    .withColumnRenamed("CODIGO_ITEM", "CdSku")
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

spark.table("data_engineering_prd.context_logistica.planoabastecimento").display()

# COMMAND ----------

w_cd = Window.partitionBy("CdSku", "CdFilialEntrega")

df_demanda_on_off = (
    df_merecimento_offline
    .join(
        df_merecimento_online,
        on=["CdFilial", "NmFilial", "NmUF", "NmPorteLoja", "grupo_de_necessidade"],
        how="outer")
    .fillna(0, subset=["Merecimento_Percentual_online", "Merecimento_Percentual_offline"])
    .join(
        df_demanda_envio
        .select("CdSku", "grupo_de_necessidade", "ITEM", "Envio adicional"),
        how="inner",
        on="grupo_de_necessidade")
    
    .withColumn("merecimento_percentual_propocionalizado_on_off",
                (F.col("Merecimento_Percentual_offline") * PROPORCAO_OFF) + (F.col("Merecimento_Percentual_online") * (1-PROPORCAO_OFF))) 
    .withColumn("proporção_demanda_off_percentual", F.round(F.lit(100*PROPORCAO_OFF), 1))
    .withColumn("demanda_envio_on",
                F.col("Envio adicional") * (1 - PROPORCAO_OFF) )
    .withColumn("demanda_envio_off",
                F.col("Envio adicional") * (PROPORCAO_OFF) )
    .withColumn("Qtd_pecas_on",
                F.col("demanda_envio_on")/100 * F.col("Merecimento_Percentual_online"))
    .withColumn("Qtd_pecas_off",
                F.col("demanda_envio_off")/100 * F.col("Merecimento_Percentual_offline"))
    .withColumn("Qtd_pecas_total",
                F.round(F.col("Qtd_pecas_on") + F.col("Qtd_pecas_off"), 0)
    )
    .join(
        spark.table("data_engineering_prd.context_logistica.planoabastecimento")
        .select(
            F.col("CdFilialEntrega").cast("int").alias("CdFilialEntrega"),
            F.col("CdLoja").cast("int").alias("CdFilial")
        ).distinct(),
        how="left",
        on="CdFilial"
    )
    .withColumn("CdFilialEntrega",
                F.when(F.col("CdFilialEntrega").isNull(), F.col("CdFilial"))
                .otherwise(F.col("CdFilialEntrega")))
    .withColumn("MerecimentoCD",
                F.round(F.sum("merecimento_percentual_propocionalizado_on_off").over(w_cd), 3))
)

df_demanda_on_off.display()
    

# COMMAND ----------

(
    df_demanda_on_off.select("CdSku", "ITEM", "grupo_de_necessidade", "CdFilial", "NmFilial", 
                         "NmPorteLoja", "Merecimento_Percentual_offline",
                         "Merecimento_Percentual_online","proporção_demanda_off_percentual", 
                         F.round(
                             F.col("merecimento_percentual_propocionalizado_on_off"), 3).alias("merecimento_percentual_propocionalizado_on_off"),
                         "MerecimentoCD"
                         ).toPandas()
).to_excel("merecimento_proporcional_on_off_envio_manual.xlsx")

# COMMAND ----------

(
    df_demanda_on_off.select("CdSku", "ITEM", "grupo_de_necessidade", 
                         "MerecimentoCD",
                         "CdFilialEntrega"
                         ).dropDuplicates().toPandas()
).to_excel("merecimento_CD_on_off_envio_manual.xlsx")

# COMMAND ----------

df_demanda_on_off_agg = (
    df_demanda_on_off
    .groupBy("CdSku", "ITEM")
    .agg(
        F.sum("Qtd_pecas_total").alias("Qtd_pecas_total_distribuidas"),
        F.first("Envio adicional").alias("Envio adicional")
    )
    .withColumn("n_itens_nao_distribuidos",
                F.col("Envio adicional") - F.col("Qtd_pecas_total_distribuidas"))
)

df_demanda_on_off_agg.display()
