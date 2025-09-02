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
from typing import List, Optional, Dict, Any
import pandas as pd

# Inicialização do Spark
spark = SparkSession.builder.appName("analise_factual_comparacao_matrizes").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

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
            nome_tabela = f"databox.bcg_comum.supply_matriz_merecimento_{categoria}"
            df_matriz = spark.table(nome_tabela)
            
            matrizes[categoria] = df_matriz
            print(f"✅ {categoria}: {df_matriz.count():,} registros carregados")
            
        except Exception as e:
            print(f"❌ {categoria}: Erro ao carregar - {str(e)}")
            matrizes[categoria] = None
    
    print(f"📊 Total de matrizes carregadas: {len([m for m in matrizes.values() if m is not None])}")
    return matrizes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregamento dos Dados Factuais de Julho-2025

# COMMAND ----------

def carregar_dados_factual_janela_movel() -> DataFrame:
    """
    Calcula dados factuais baseados na janela móvel de 180 dias (média aparada robusta a ruptura).
    
    Returns:
        DataFrame com dados factuais por SKU e filial
    """
    print("📊 Calculando dados factuais baseados na janela móvel de 180 dias...")
    
    # Carregar dados base para cálculo da média aparada 180 dias
    df_base = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3')
        .filter(F.col('NmAgrupamentoDiretoriaSetor') == 'DIRETORIA TELEFONIA CELULAR')
        .filter(F.col("DtAtual") >= F.lit("2025-01-01"))  # Período para cálculo
        .select(
            "CdSku", 
            "CdFilial", 
            "DtAtual",
            "QtMercadoria",
            "deltaRuptura"
        )
        .withColumn("Qt_venda_sem_ruptura",
                    F.col("QtMercadoria") + F.col("deltaRuptura"))
        .filter(F.col("Qt_venda_sem_ruptura").isNotNull())
        .filter(F.col("Qt_venda_sem_ruptura") >= 0)
    )
    
    # Calcular média aparada 180 dias (robusta a ruptura)
    # Para cada SKU-filial, pegar os últimos 180 dias e calcular média aparada (remove top e bottom 5%)
    w_180_dias = Window.partitionBy("CdSku", "CdFilial").orderBy(F.desc("DtAtual")).rowsBetween(0, 179)
    
    df_com_media_aparada = (
        df_base
        .withColumn(
            "MediaAparada180_Qt_venda_sem_ruptura",
            F.expr("avg(Qt_venda_sem_ruptura)").over(w_180_dias)
        )
        .withColumn(
            "P5", F.expr("percentile_approx(Qt_venda_sem_ruptura, 0.05)").over(w_180_dias)
        )
        .withColumn(
            "P95", F.expr("percentile_approx(Qt_venda_sem_ruptura, 0.95)").over(w_180_dias)
        )
        .withColumn(
            "MediaAparada180_Qt_venda_sem_ruptura",
            F.when(
                (F.col("Qt_venda_sem_ruptura") >= F.col("P5")) & 
                (F.col("Qt_venda_sem_ruptura") <= F.col("P95")),
                F.col("Qt_venda_sem_ruptura")
            ).otherwise(F.lit(None))
        )
        .groupBy("CdSku", "CdFilial", "DtAtual")
        .agg(
            F.avg("MediaAparada180_Qt_venda_sem_ruptura").alias("MediaAparada180_Qt_venda_sem_ruptura")
        )
        .filter(F.col("DtAtual") == F.lit("2025-06-30"))  # Data de referência
        .select(
            "CdSku", 
            "CdFilial", 
            "MediaAparada180_Qt_venda_sem_ruptura"
        )
        .filter(F.col("MediaAparada180_Qt_venda_sem_ruptura").isNotNull())
        .filter(F.col("MediaAparada180_Qt_venda_sem_ruptura") > 0)
    )
    
    print(f"✅ Dados factuais (janela móvel 180 dias) calculados: {df_com_media_aparada.count():,} registros")
    print(f"📅 Base: Média aparada 180 dias robusta a ruptura (calculada)")
    
    return df_com_media_aparada

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cálculo da Proporção Factual Baseada em Julho-2025

# COMMAND ----------

def calcular_proporcao_factual_janela_movel(df_factual: DataFrame, categoria: str) -> DataFrame:
    """
    Calcula proporção factual: % que aquele SKU vendeu naquela filial vs total do SKU na empresa.
    
    Args:
        df_factual: DataFrame com dados factuais (janela móvel 180 dias)
        categoria: Nome da categoria/diretoria
        
    Returns:
        DataFrame com proporção factual calculada por SKU na filial vs total do SKU na empresa
    """
    print(f"📈 Calculando proporção factual para: {categoria}")
    print("📊 IMPORTANTE: Proporção factual = % que SKU vendeu na FILIAL vs TOTAL do SKU na EMPRESA")
    
    # Calcular total de vendas do SKU na empresa (agrupado por SKU)
    w_total_sku_empresa = Window.partitionBy("CdSku")  # Total por SKU na empresa
    
    df_proporcao_factual = (
        df_factual
        .withColumn(
            "total_sku_empresa",
            F.sum(F.col("MediaAparada180_Qt_venda_sem_ruptura")).over(w_total_sku_empresa)
        )
        .withColumn(
            "proporcao_factual_janela_movel",
            F.when(
                F.col("total_sku_empresa") > 0,
                F.col("MediaAparada180_Qt_venda_sem_ruptura") / F.col("total_sku_empresa")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "proporcao_factual_julho_percentual",
            F.round(F.col("proporcao_factual_janela_movel") * 100, 4)
        )
    )
    
    print(f"✅ Proporção factual calculada para {categoria}")
    print(f"  • Total de registros: {df_proporcao_factual.count():,}")
    print(f"  • SKUs únicos: {df_proporcao_factual.select('CdSku').distinct().count():,}")
    print(f"  • Filiais únicas: {df_proporcao_factual.select('CdFilial').distinct().count():,}")
    
    return df_proporcao_factual


def carregar_dados_factual_julho_completo() -> DataFrame:
    """
    Calcula dados factuais baseados no mês completo de julho (Qt_venda_sem_ruptura total do mês).
    
    Returns:
        DataFrame com dados factuais por SKU e filial (total de julho)
    """
    print("📊 Calculando dados factuais baseados no mês completo de julho...")
    
    # Carregar dados base para julho completo
    df_base = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3')
        .filter(F.col('NmAgrupamentoDiretoriaSetor') == 'DIRETORIA TELEFONIA CELULAR')
        .filter(F.col("year_month") == 202507)  # Julho de 2025
        .select(
            "CdSku", 
            "CdFilial", 
            "DtAtual",
            "QtMercadoria",
            "deltaRuptura"
        )
        .withColumn("Qt_venda_sem_ruptura",
                    F.col("QtMercadoria") + F.col("deltaRuptura"))
        .filter(F.col("Qt_venda_sem_ruptura").isNotNull())
        .filter(F.col("Qt_venda_sem_ruptura") >= 0)
    )
    
    # Calcular total do mês de julho por SKU e filial
    df_total_julho = (
        df_base
        .groupBy("CdSku", "CdFilial")
        .agg(
            F.sum("Qt_venda_sem_ruptura").alias("Total_Julho_Qt_venda_sem_ruptura")
        )
        .filter(F.col("Total_Julho_Qt_venda_sem_ruptura") > 0)
    )
    
    print(f"✅ Dados factuais (julho completo) calculados: {df_total_julho.count():,} registros")
    print(f"📅 Base: Total de vendas sem ruptura do mês de julho de 2025")
    
    return df_total_julho

def calcular_proporcao_factual_julho_completo(df_factual: DataFrame, categoria: str) -> DataFrame:
    """
    Calcula proporção factual: % que aquele SKU vendeu naquela filial vs total do SKU na empresa em julho.
    
    Args:
        df_factual: DataFrame com dados factuais (total de julho)
        categoria: Nome da categoria/diretoria
        
    Returns:
        DataFrame com proporção factual calculada por SKU na filial vs total do SKU na empresa
    """
    print(f"📈 Calculando proporção factual para: {categoria}")
    print("�� IMPORTANTE: Proporção factual = % que SKU vendeu na FILIAL vs TOTAL do SKU na EMPRESA (julho)")
    
    # Calcular total de vendas do SKU na empresa (agrupado por SKU)
    w_total_sku_empresa = Window.partitionBy("CdSku")  # Total por SKU na empresa
    
    df_proporcao_factual = (
        df_factual
        .withColumn(
            "total_sku_empresa_julho",
            F.sum(F.col("Total_Julho_Qt_venda_sem_ruptura")).over(w_total_sku_empresa)
        )
        .withColumn(
            "proporcao_factual_julho",
            F.when(
                F.col("total_sku_empresa_julho") > 0,
                F.col("Total_Julho_Qt_venda_sem_ruptura") / F.col("total_sku_empresa_julho")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "proporcao_factual_julho_percentual",
            F.round(F.col("proporcao_factual_julho") * 100, 4)
        )
    )
    
    print(f"✅ Proporção factual calculada para {categoria}")
    print(f"  • Total de registros: {df_proporcao_factual.count():,}")
    print(f"  • SKUs únicos: {df_proporcao_factual.select('CdSku').distinct().count():,}")
    print(f"  • Filiais únicas: {df_proporcao_factual.select('CdFilial').distinct().count():,}")
    
    return df_proporcao_factual

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cálculo de sMAPE e WMAPE vs Factual de Julho-2025

# COMMAND ----------

def calcular_smape_vs_factual_janela_movel(df_matriz: DataFrame, df_proporcao_factual: DataFrame, categoria: str) -> DataFrame:
    """
    Calcula sMAPE comparando merecimento calculado com proporção factual (janela móvel 180 dias).
    
    Args:
        df_matriz: DataFrame com matriz de merecimento calculada
        df_proporcao_factual: DataFrame com proporção factual (janela móvel 180 dias)
        categoria: Nome da categoria
        
    Returns:
        DataFrame com sMAPE calculado para todas as medidas vs factual (janela móvel 180 dias)
    """
    print(f"📊 Calculando sMAPE vs factual (janela móvel 180 dias) para: {categoria}")
    print("🔄 Merecimento calculado vs Proporção Factual (janela móvel 180 dias)...")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # Join entre matriz e proporção factual de julho-2025
    # Renomeia colunas para evitar ambiguidade
    df_matriz_renomeado = df_matriz.select(
        *[F.col(c).alias(f"matriz_{c}") for c in df_matriz.columns if c not in ["CdSku", "CdFilial"]],
        F.col("CdSku"),
        F.col("CdFilial")
    )
    
    df_comparacao = (
        df_matriz_renomeado
        .join(df_proporcao_factual, on=["CdSku", "CdFilial"], how="inner")
    )
    
    print(f"    🔍 Debug: Registros após join: {df_comparacao.count():,}")
    
    # Calcular sMAPE para cada medida vs factual de julho-2025
    df_com_smape = df_comparacao
    EPSILON = 1e-12
    
    for medida in medidas_disponiveis:
        if medida in df_matriz.columns:
            # Calcula percentual do merecimento calculado
            df_com_smape = (
                df_com_smape
                .withColumn(
                    f"merecimento_{medida}_percentual",
                    F.when(
                        F.col(f"matriz_Merecimento_Final_{medida}") > 0,
                        F.col(f"matriz_Merecimento_Final_{medida}") / F.sum(f"matriz_Merecimento_Final_{medida}").over(
                            Window.partitionBy("matriz_grupo_de_necessidade")
                        ) * 100
                    ).otherwise(F.lit(0.0))
                )
                .withColumn(
                    f"erro_absoluto_vs_factual_janela_{medida}",
                    F.abs(F.col(f"merecimento_{medida}_percentual") - F.col("proporcao_factual_julho_percentual"))
                )
                .withColumn(
                    f"smape_vs_factual_janela_{medida}",
                    F.when(
                        (F.col(f"merecimento_{medida}_percentual") + F.col("proporcao_factual_julho_percentual")) > 0,
                        F.lit(2.0) * F.col(f"erro_absoluto_vs_factual_janela_{medida}") / 
                        (F.col(f"merecimento_{medida}_percentual") + F.col("proporcao_factual_julho_percentual") + F.lit(EPSILON)) * 100
                    ).otherwise(F.lit(0.0))
                )
            )
    
    print(f"✅ sMAPE vs factual (janela móvel 180 dias) calculado para {categoria}")
    return df_com_smape

def calcular_wmape_vs_factual_janela_movel(df_com_smape: DataFrame, categoria: str) -> Dict[str, DataFrame]:
    """
    Calcula WMAPE (Weighted Mean Absolute Percentage Error) vs factual (janela móvel 180 dias) por diferentes agrupamentos.
    
    Args:
        df_com_smape: DataFrame com sMAPE calculado vs factual (janela móvel 180 dias)
        categoria: Nome da categoria
        
    Returns:
        Dicionário com DataFrames de WMAPE por agrupamento
    """
    print(f"📊 Calculando WMAPE vs factual (janela móvel 180 dias) para: {categoria}")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # 1. WMAPE POR FILIAL
    print("📊 Calculando WMAPE por filial vs factual de julho-2025...")
    
    aggs_filial = []
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            aggs_filial.extend([
                F.sum(F.col(f"erro_absoluto_vs_factual_janela_{medida}") * F.col("MediaAparada180_Qt_venda_sem_ruptura")).alias(f"soma_erro_peso_{medida}"),
                F.sum(F.col("MediaAparada180_Qt_venda_sem_ruptura")).alias(f"soma_peso_{medida}"),
                F.avg(f"smape_vs_factual_janela_{medida}").alias(f"smape_medio_{medida}"),
                F.count("*").alias("total_skus")
            ])
    
    df_wmape_filial = (
        df_com_smape
        .groupBy("CdFilial")
        .agg(*aggs_filial)
    )
    
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            df_wmape_filial = df_wmape_filial.withColumn(
                f"wmape_vs_factual_janela_{medida}",
                F.when(
                    F.col(f"soma_peso_{medida}") > 0,
                    F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
                ).otherwise(F.lit(0.0))
            )
    
    df_wmape_filial = df_wmape_filial.withColumn("tipo_agregacao", F.lit("FILIAL"))
    
    # 2. WMAPE POR GRUPO DE NECESSIDADE
    print("📊 Calculando WMAPE por grupo de necessidade vs factual de julho-2025...")
    
    aggs_grupo = []
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            aggs_grupo.extend([
                F.sum(F.col(f"erro_absoluto_vs_factual_janela_{medida}") * F.col("MediaAparada180_Qt_venda_sem_ruptura")).alias(f"soma_erro_peso_{medida}"),
                F.sum(F.col("MediaAparada180_Qt_venda_sem_ruptura")).alias(f"soma_peso_{medida}"),
                F.avg(f"smape_vs_factual_janela_{medida}").alias(f"smape_medio_{medida}"),
                F.count("*").alias("total_skus")
            ])
    
    df_wmape_grupo = (
        df_com_smape
        .groupBy("matriz_grupo_de_necessidade")
        .agg(*aggs_grupo)
    )
    
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            df_wmape_grupo = df_wmape_grupo.withColumn(
                f"wmape_vs_factual_janela_{medida}",
                F.when(
                    F.col(f"soma_peso_{medida}") > 0,
                    F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
                ).otherwise(F.lit(0.0))
            )
    
    df_wmape_grupo = df_wmape_grupo.withColumn("tipo_agregacao", F.lit("GRUPO_NECESSIDADE"))
    
    # 3. WMAPE DA CATEGORIA INTEIRA
    print("📊 Calculando WMAPE da categoria inteira vs factual de julho-2025...")
    
    aggs_categoria = []
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            aggs_categoria.extend([
                F.sum(F.col(f"erro_absoluto_vs_factual_janela_{medida}") * F.col("MediaAparada180_Qt_venda_sem_ruptura")).alias(f"soma_erro_peso_{medida}"),
                F.sum(F.col("MediaAparada180_Qt_venda_sem_ruptura")).alias(f"soma_peso_{medida}"),
                F.avg(f"smape_vs_factual_janela_{medida}").alias(f"smape_medio_{medida}"),
                F.count("*").alias("total_skus")
            ])
    
    df_wmape_categoria = df_com_smape.agg(*aggs_categoria)
    
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            df_wmape_categoria = df_wmape_categoria.withColumn(
                f"wmape_vs_factual_janela_{medida}",
                F.when(
                    F.col(f"soma_peso_{medida}") > 0,
                    F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
                ).otherwise(F.lit(0.0))
            )
    
    df_wmape_categoria = df_wmape_categoria.withColumn("tipo_agregacao", F.lit("CATEGORIA_INTEIRA"))
    
    print(f"✅ WMAPE vs factual de julho-2025 calculado para {categoria}")
    
    return {
        "filial": df_wmape_filial,
        "grupo": df_wmape_grupo,
        "categoria": df_wmape_categoria
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Carregamento da Matriz DRP Geral

# COMMAND ----------

def carregar_matriz_drp_geral() -> DataFrame:
    """
    Carrega a matriz DRP geral para comparação.
    
    Returns:
        DataFrame com a matriz geral de referência
    """
    print("📊 Carregando matriz DRP geral para comparação...")
    
    df_matriz_geral = (
        spark.createDataFrame(
            pd.read_csv(
                "/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/(DRP)_MATRIZ_20250829135142.csv",
                delimiter=";",
            )
        )
        .select(
            F.col("CODIGO").cast("int").alias("CdSku"),
            F.regexp_replace(F.col("CODIGO_FILIAL"), ".*_", "").cast("int").alias("CdFilial"),
            F.regexp_replace(F.col("PERCENTUAL_MATRIZ"), ",", ".").cast("float").alias("PercMatrizNeogrid"),
            F.col("CLUSTER").cast("string").alias("is_Cluster"),
        )
        .dropDuplicates()
    )
    
    print(f"✅ Matriz DRP geral carregada: {df_matriz_geral.count():,} registros")
    return df_matriz_geral

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cálculo de sMAPE da Matriz DRP vs Factual de Julho-2025

# COMMAND ----------

def calcular_smape_drp_vs_factual_janela_movel(df_comparacao_drp: DataFrame, df_proporcao_factual: DataFrame, categoria: str) -> DataFrame:
    """
    Calcula sMAPE comparando matriz DRP geral com proporção factual (janela móvel 180 dias).
    
    Args:
        df_comparacao_drp: DataFrame com comparação entre matriz calculada e DRP geral
        df_proporcao_factual: DataFrame com proporção factual (janela móvel 180 dias)
        categoria: Nome da categoria
        
    Returns:
        DataFrame com sMAPE da matriz DRP vs factual (janela móvel 180 dias)
    """
    print(f"📊 Calculando sMAPE da matriz DRP vs factual (janela móvel 180 dias) para: {categoria}")
    print("🔄 Matriz DRP vs Proporção Factual (janela móvel 180 dias)...")
    
    # Join com proporção factual de julho-2025
    df_comparacao_drp_vs_factual = (
        df_comparacao_drp
        .join(df_proporcao_factual, on=["CdSku", "CdFilial"], how="inner")
    )
    
    print(f"    🔍 Debug: Registros após join DRP vs factual: {df_comparacao_drp_vs_factual.count():,}")
    
    # Calcular sMAPE da matriz DRP vs factual de julho-2025
    df_com_smape_drp_vs_factual = (
        df_comparacao_drp_vs_factual
        .withColumn(
            "erro_absoluto_drp_vs_factual_janela",
            F.abs(F.col("PercMatrizNeogrid") - F.col("proporcao_factual_julho_percentual"))
        )
        .withColumn(
            "smape_drp_vs_factual_janela",
            F.when(
                (F.col("PercMatrizNeogrid") + F.col("proporcao_factual_julho_percentual")) > 0,
                F.lit(2.0) * F.col("erro_absoluto_drp_vs_factual_janela") / 
                (F.col("PercMatrizNeogrid") + F.col("proporcao_factual_julho_percentual")) * 100
            ).otherwise(F.lit(0.0))
        )
    )
    
    print(f"✅ sMAPE da matriz DRP vs factual (janela móvel 180 dias) calculado para {categoria}")
    return df_com_smape_drp_vs_factual

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Comparação com Matriz DRP Geral

# COMMAND ----------

def comparar_com_matriz_drp_geral(df_matriz: DataFrame, df_matriz_geral: DataFrame, categoria: str) -> DataFrame:
    """
    Compara matriz calculada com matriz DRP geral.
    
    Args:
        df_matriz: DataFrame com matriz calculada
        df_matriz_geral: DataFrame com matriz DRP geral
        categoria: Nome da categoria
        
    Returns:
        DataFrame com comparação e métricas de erro
    """
    print(f"📊 Comparando {categoria} com matriz DRP geral...")
    
    # Normalizar IDs para garantir compatibilidade
    df_matriz_norm = (
        df_matriz
        .withColumn("CdSku", F.col("CdSku").cast("int"))
        .withColumn("CdFilial", F.col("CdFilial").cast("int"))
    )
    
    df_geral_norm = (
        df_matriz_geral
        .withColumn("CdSku", F.col("CdSku").cast("int"))
        .withColumn("CdFilial", F.col("CdFilial").cast("int"))
    )
    
    # Renomeia colunas da matriz para evitar ambiguidade
    df_matriz_norm_renomeado = df_matriz_norm.select(
        *[F.col(c).alias(f"matriz_{c}") for c in df_matriz_norm.columns if c not in ["CdFilial", "CdSku"]],
        F.col("CdFilial"),
        F.col("CdSku")
    )
    
    # Join para comparação
    df_comparacao = (
        df_matriz_norm_renomeado
        .join(df_geral_norm, on=["CdFilial", "CdSku"], how="inner")
        .withColumn("categoria", F.lit(categoria))
    )
    
    # Calcular métricas de erro
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    print(f"    🔍 Debug: Verificando colunas disponíveis...")
    print(f"    🔍 Debug: Colunas da matriz renomeada: {[c for c in df_matriz_norm_renomeado.columns if c.startswith('matriz_')]}")
    print(f"    🔍 Debug: Medidas disponíveis: {medidas_disponiveis}")
    
    df_com_metricas = df_comparacao
    
    for medida in medidas_disponiveis:
        if f"matriz_{medida}" in df_matriz_norm_renomeado.columns:
            df_com_metricas = (
                df_com_metricas
                .withColumn(
                    f"merecimento_{medida}_percentual",
                    F.when(
                        F.col(f"matriz_Merecimento_Final_{medida}") > 0,
                        F.col(f"matriz_Merecimento_Final_{medida}") / F.sum(f"matriz_Merecimento_Final_{medida}").over(
                            Window.partitionBy("matriz_grupo_de_necessidade")
                        ) * 100
                    ).otherwise(F.lit(0.0))
                )
                .withColumn(
                    f"erro_absoluto_vs_drp_{medida}",
                    F.abs(F.col(f"merecimento_{medida}_percentual") - F.col("PercMatrizNeogrid"))
                )
                .withColumn(
                    f"smape_vs_drp_{medida}",
                    F.when(
                        (F.col(f"merecimento_{medida}_percentual") + F.col("PercMatrizNeogrid")) > 0,
                        F.lit(2.0) * F.col(f"erro_absoluto_vs_drp_{medida}") / 
                        (F.col(f"merecimento_{medida}_percentual") + F.col("PercMatrizNeogrid")) * 100
                    ).otherwise(F.lit(0.0))
                )
            )
    
    print(f"✅ Comparação com DRP concluída para {categoria}")
    return df_com_metricas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Identificação de Distorções

# COMMAND ----------

def identificar_distorcoes(df_comparacao: DataFrame, categoria: str) -> DataFrame:
    """
    Identifica distorções significativas entre matriz calculada e DRP geral.
    
    Args:
        df_comparacao: DataFrame com comparação completa
        categoria: Nome da categoria
        
    Returns:
        DataFrame com distorções identificadas
    """
    print(f"🔍 Identificando distorções para: {categoria}")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # Calcular melhor medida para cada registro (sMAPE vs factual janela móvel 180 dias)
    colunas_smape_vs_factual = [f"smape_vs_factual_janela_{medida}" for medida in medidas_disponiveis if f"smape_vs_factual_janela_{medida}" in df_comparacao.columns]
    
    print(f"    🔍 Debug: Colunas sMAPE vs factual janela móvel encontradas: {colunas_smape_vs_factual}")
    print(f"    🔍 Debug: Total de colunas sMAPE vs factual: {len(colunas_smape_vs_factual)}")
    
    # Verifica se há pelo menos 2 colunas para usar F.least
    if len(colunas_smape_vs_factual) >= 2:
        df_com_distorcao = df_comparacao.withColumn(
            "melhor_smape_vs_factual_janela",
            F.least(*[F.col(col) for col in colunas_smape_vs_factual])
        )
    elif len(colunas_smape_vs_factual) == 1:
        # Se há apenas uma coluna, usa ela diretamente
        df_com_distorcao = df_comparacao.withColumn(
            "melhor_smape_vs_factual_janela",
            F.col(colunas_smape_vs_factual[0])
        )
    else:
        # Se não há colunas, cria coluna com valor padrão
        print(f"    ⚠️  Nenhuma coluna sMAPE vs factual encontrada, criando coluna padrão")
        df_com_distorcao = df_comparacao.withColumn(
            "melhor_smape_vs_factual_janela",
            F.lit(999.0)  # Valor alto para indicar erro
        )
    
    # Categorizar qualidade vs factual de julho-2025
    df_com_distorcao = df_com_distorcao.withColumn(
        "categoria_qualidade_vs_factual_janela",
        F.when(F.col("melhor_smape_vs_factual_janela") < 10, "Excelente")
        .when(F.col("melhor_smape_vs_factual_janela") < 20, "Muito Boa")
        .when(F.col("melhor_smape_vs_factual_janela") < 30, "Boa")
        .when(F.col("melhor_smape_vs_factual_janela") < 50, "Regular")
        .otherwise("Ruim")
    )
    
    # Identificar distorções significativas (sMAPE > 50%)
    df_distorcoes = (
        df_com_distorcao
        .filter(F.col("melhor_smape_vs_factual_janela") > 50)
        .orderBy(F.col("melhor_smape_vs_factual_janela").desc())
    )
    
    print(f"✅ Distorções identificadas para {categoria}")
    print(f"  • Total de distorções: {df_distorcoes.count():,}")
    
    return df_distorcoes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Execução Principal da Análise

# COMMAND ----------

# MAGIC %md
# MAGIC ### **EXECUÇÃO COMPLETA DA ANÁLISE**

# COMMAND ----------

# EXECUTAR ANÁLISE COMPLETA PARA TODAS AS CATEGORIAS
print("🚀 INICIANDO ANÁLISE DE FACTUAL E COMPARAÇÃO DE MATRIZES...")
print("=" * 80)

# 1. Carregar matrizes calculadas
print("📊 Passo 1: Carregando matrizes de merecimento...")
matrizes_calculadas = carregar_matrizes_merecimento_calculadas()

# 2. Carregar matriz DRP geral
print("\n📊 Passo 2: Carregando matriz DRP geral...")
matriz_drp_geral = carregar_matriz_drp_geral()

# 3. Processar cada categoria
resultados_analise = {}

for categoria, df_matriz in matrizes_calculadas.items():
    if df_matriz is not None:
        print(f"\n🔄 Processando categoria: {categoria}")
        print("-" * 60)
        
        try:
        # Carregar dados factuais (janela móvel 180 dias)
        # Carregar dados de julho completo
            df_factual_julho = carregar_dados_factual_julho_completo()

            # Calcular proporções
            df_proporcao_julho = calcular_proporcao_factual_julho_completo(df_factual_julho, "DIRETORIA TELEFONIA CELULAR")


            # Resultado: DataFrame com proporção de cada filial vs total do SKU na empresa em julho
            
            # Calcular sMAPE vs factual (janela móvel 180 dias)
            df_com_smape_vs_factual = calcular_smape_vs_factual_janela_movel(df_matriz, df_proporcao_julho, categoria)
            
            # Calcular WMAPE vs factual (janela móvel 180 dias)
            dict_wmape_vs_factual = calcular_wmape_vs_factual_janela_movel(df_com_smape_vs_factual, categoria)
            
            # Comparar com matriz DRP geral (usando factual de julho-2025 como referência)
            df_comparacao_drp = comparar_com_matriz_drp_geral(df_matriz, matriz_drp_geral, categoria)
            
            # Calcular sMAPE da matriz DRP vs factual (janela móvel 180 dias)
            df_comparacao_drp_vs_factual = calcular_smape_drp_vs_factual_janela_movel(
                df_comparacao_drp, df_proporcao_julho, categoria
            )
            
            # Identificar distorções
            df_distorcoes = identificar_distorcoes(df_comparacao_drp_vs_factual, categoria)
            
            # Armazenar resultados
            resultados_analise[categoria] = {
                "proporcao_factual_janela": df_proporcao_julho,
                "smape_vs_factual_janela": df_com_smape_vs_factual,
                "wmape_vs_factual_janela": dict_wmape_vs_factual,
                "comparacao_drp": df_comparacao_drp,
                "smape_drp_vs_factual_janela": df_comparacao_drp_vs_factual,
                "distorcoes": df_distorcoes,
                "status": "SUCESSO"
            }
            
            print(f"✅ {categoria} - Análise concluída com sucesso!")
            
        except Exception as e:
            print(f"❌ {categoria} - Erro: {str(e)}")
            resultados_analise[categoria] = {
                "status": "ERRO",
                "erro": str(e)
            }

print("\n" + "=" * 80)
print("🎉 ANÁLISE COMPLETA CONCLUÍDA!")
print("=" * 80)

# Exibe resumo dos resultados
print("📊 RESUMO DOS RESULTADOS:")
for categoria, resultado in resultados_analise.items():
    if resultado["status"] == "SUCESSO":
        print(f"  ✅ {categoria}: Análise completa")
        print(f"     • Proporção factual janela móvel: {resultado['proporcao_factual_janela'].count():,} registros")
        print(f"     • sMAPE vs factual janela móvel: {resultado['smape_vs_factual_janela'].count():,} registros")
        print(f"     • sMAPE DRP vs factual janela móvel: {resultado['smape_drp_vs_factual_janela'].count():,} registros")
        print(f"     • Distorções vs factual julho-2025: {resultado['distorcoes'].count():,} registros")
    else:
        print(f"  ❌ {categoria}: {resultado['erro']}")

print("=" * 80)

# COMMAND ----------

df_com_smape_vs_factual.display()

# COMMAND ----------

df_com_smape_vs_factual.display()

# COMMAND ----------

df_com_smape_vs_factual.display()

# COMMAND ----------

df_investigacao = (
    df_com_smape_vs_factual
    .join(matriz_drp_geral,
          how="inner",
          on=['CdFilial', 'CdSku'])
    #.filter(F.col("is_Cluster") == 'OBRIGATÓRIO')
    .filter(F.col("matriz_grupo_de_necessidade") != '-')

    .select("CdFilial", "CdSku", 
            F.col("matriz_cd_primario").alias('Cd_primario'), 
            "matriz_grupo_de_necessidade",
            "matriz_year_month", 
            F.round(100*F.col("matriz_Merecimento_Final_Media180_Qt_venda_sem_ruptura"),2).alias('merecimento_calculado'), 
            F.round(F.col("proporcao_factual_julho_percentual"), 2).alias('proporcao_factual'),
            F.round(F.col("PercMatrizNeogrid"), 2).alias('PercMatrizNeogrid')
    )
    .withColumn('deltaCalculadoFactual',
                F.round(F.col('merecimento_calculado') - F.col('proporcao_factual'), 2))
    .withColumn('deltaNeogridFactual',
                F.round(F.col('PercMatrizNeogrid') - F.col('proporcao_factual'),2))
    .withColumn('deltaNeogridCalculado',
                F.round(F.col('PercMatrizNeogrid') - F.col('merecimento_calculado'),2))
)


df_investigacao.cache()

df_investigacao.display()

# COMMAND ----------

df_investigacao.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 RESUMO FINAL DO SCRIPT DE ANÁLISE
# MAGIC
# MAGIC ### **O que este script faz:**
# MAGIC 1. **Carrega matrizes calculadas** de todas as categorias
# MAGIC 2. **Carrega dados factuais** (janela móvel 180 dias - média aparada robusta a ruptura)
# MAGIC 3. **Calcula proporção factual**: % que SKU vendeu na filial vs total do SKU na empresa
# MAGIC 4. **Calcula sMAPE e WMAPE** comparando merecimento vs factual (janela móvel 180 dias)
# MAGIC 5. **Carrega matriz DRP geral** para comparação
# MAGIC 6. **Calcula sMAPE da matriz DRP vs factual** (janela móvel 180 dias)
# MAGIC 7. **Compara matrizes calculadas** com DRP geral
# MAGIC 8. **Identifica distorções** significativas vs factual (janela móvel 180 dias)
# MAGIC
# MAGIC ### **Resultados gerados:**
# MAGIC - **Proporção factual** (janela móvel 180 dias - média aparada robusta a ruptura)
# MAGIC - **sMAPE e WMAPE vs factual** para todas as medidas (médias e médias aparadas)
# MAGIC - **sMAPE da matriz DRP vs factual** (janela móvel 180 dias)
# MAGIC - **Comparação com matriz DRP geral**
# MAGIC - **Identificação de distorções** vs factual (janela móvel 180 dias)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Racional para detecção de principais distorções anedotais
# MAGIC
# MAGIC 1. Filtro das principais lojas em Receita
# MAGIC 2. Filtro dos principais SKUs em Ruptura, DDE E Aging (percentis)
# MAGIC 3. Ordenação por receita dentre esses filtrados (por SKU)
# MAGIC 4. Lista com SKUs e Lojas com distorções importantes de distribuição
# MAGIC 5. Filtrar df_distorcoes que contem as matrizes de merecimento comparativas a partir da lista de SKUs ofensores

# COMMAND ----------

df_estoque_receita = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3')
    .filter(F.col('NmAgrupamentoDiretoriaSetor') == 'DIRETORIA TELEFONIA CELULAR')
    .filter(F.col('year_month') == 202507)
)

df_lojas_principais = (
    df_estoque_receita
    .groupBy('CdFilial', 'NmLoja')
    .agg(
        F.sum('Media90_Receita_venda_estq').alias('Soma_media_receita')
    )
    .orderBy(F.desc('Soma_media_receita'))
    .limit(25)
)

df_lojas_principais.display()

df_produtos_ofensores = (
    df_estoque_receita
    .groupBy('CdSku', 'DsSku')
    .agg(
        F.round(F.sum('Media90_Receita_venda_estq'), 2).alias('Soma_media_receita'),
        F.round(F.mean('DDE'), 2).alias('DDE_medio'),
        F.round(F.sum('ReceitaPerdidaRuptura'), 2).alias('ReceitaPerdidaRuptura'),
    )
    .filter(F.col("Soma_media_receita") > 1e6)
    .withColumn('Ruptura_pct_receita',
                F.round(100*F.col('ReceitaPerdidaRuptura')/F.col('Soma_media_receita'),2))
    .withColumn('score_DDE_Ruptura',
                F.col('DDE_medio') + 
                F.col('Ruptura_pct_receita'))
    .orderBy(F.desc('score_DDE_Ruptura'),)
    .limit(50)
)

df_produtos_ofensores.display()

# COMMAND ----------

df_distorcoes_matriz_fixa = (
    df_investigacao
    .join(df_produtos_ofensores,
          how="inner",
          on=['CdSku'])
    .join(df_lojas_principais,
          how="inner",
          on=['CdFilial'])
    .filter(F.col("CdSku") == 5341230)
)

df_distorcoes_matriz_fixa.display()

# COMMAND ----------

df_oppo = (
    df_investigacao
    .filter(F.col("CdSku") == 5341230)
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmFilial", "NmPorteLoja"), 
        how="left", on='CdFilial')
)
df_oppo.display()

# COMMAND ----------


# Definição da fórmula do SMAPE
def smape_column(y_true, y_pred):
    re        2 * np.abs(y_pred - y_true) / (np.abs(y_true) + np.abs(y_pred))    )

# SMAPE entre merecimento_calculado e proporcao_factual
df_smape_merecimento = df_samsung_60.withColumn(
    "smape_merecimento", smape_column(F.col("proporcao_factual"), F.col("merecimento_calculado"))
)

smape_merecimento = df_smape_merecimento.agg(
    F.mean("smape_merecimento") * 100
).collect()[0][0]

# SMAPE entre PercMatrizNeogrid e proporcao_factual
df_smape_neogrid = df_samsung_60.withColumn(
    "smape_neogrid", smape_column(F.col("proporcao_factual"), F.col("PercMatrizNeogrid"))
)

smape_neogrid = df_smape_neogrid.agg(
    F.mean("smape_neogrid") * 100
).collect()[0][0]

print("SMAPE merecimento_calculado vs proporcao_factual:", smape_merecimento)
print("SMAPE PercMatrizNeogrid vs proporcao_factual:", smape_neogrid)

# COMMAND ----------

(
    df_estoque_receita
        .groupBy('CdSku', 'CdFilial')
        .agg(
            F.round(F.sum('Media90_Receita_venda_estq'), 2).alias('Soma_media_receita'),
            F.round(F.mean('DDE'), 2).alias('DDE_medio'),
            F.round(F.sum('ReceitaPerdidaRuptura'), 2).alias('ReceitaPerdidaRuptura'),
        )
        .filter(F.col("CdSku") == 5159393)
        .withColumn('Ruptura_pct_receita',
                    F.round(100*F.col('ReceitaPerdidaRuptura')/F.col('Soma_media_receita'),2))
        .withColumn('score_DDE_Ruptura',
                    F.col('DDE_medio') + 
                    F.col('Ruptura_pct_receita'))
        .orderBy(F.desc('score_DDE_Ruptura'),)
).display()

# COMMAND ----------

df_comparacao_drp.filter(F.col("CdSku") == 5159393).display()
