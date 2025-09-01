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
        "DE_TELAS",
        #"TELEFONIA_CELULAR", 
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
# MAGIC ## 2. Cálculo da Proporção Factual

# COMMAND ----------

def calcular_proporcao_factual_por_sku_filial(df: DataFrame, coluna_medida: str) -> DataFrame:
    """
    Calcula proporção factual por SKU na filial em relação ao TOTAL DA EMPRESA.
    
    Args:
        df: DataFrame com dados de demanda
        coluna_medida: Nome da coluna de medida (ex: Media90_Qt_venda_sem_ruptura)
        
    Returns:
        DataFrame com proporção factual calculada por SKU na filial vs total da empresa
    """
    w_total_empresa = Window.partitionBy()  # Sem partição = total geral
    
    return (
        df
        .withColumn(
            f"total_{coluna_medida}_empresa",
            F.sum(F.col(coluna_medida)).over(w_total_empresa)
        )
        .withColumn(
            f"proporcao_factual_{coluna_medida}",
            F.when(
                F.col(f"total_{coluna_medida}_empresa") > 0,
                F.col(coluna_medida) / F.col(f"total_{coluna_medida}_empresa")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            f"proporcao_factual_{coluna_medida}_percentual",
            F.round(F.col(f"proporcao_factual_{coluna_medida}") * 100, 4)
        )
    )

def calcular_proporcao_factual_completa(df_matriz: DataFrame, categoria: str) -> DataFrame:
    """
    Calcula proporção factual para todas as medidas disponíveis.
    
    Args:
        df_matriz: DataFrame com matriz de merecimento calculada
        categoria: Nome da categoria/diretoria
        
    Returns:
        DataFrame com proporção factual para todas as medidas
    """
    print(f"📈 Calculando proporção factual para: {categoria}")
    print("📊 IMPORTANTE: Proporção factual calculada por SKU na FILIAL vs TOTAL DA EMPRESA")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # Aplicar cálculo de proporção factual para todas as medidas
    df_proporcao_factual = df_matriz
    for medida in medidas_disponiveis:
        if medida in df_matriz.columns:
            df_proporcao_factual = (
                calcular_proporcao_factual_por_sku_filial(
                    df_proporcao_factual, 
                    medida
                )
            )
    
    print(f"✅ Proporção factual calculada para {categoria}")
    print(f"  • Total de registros: {df_proporcao_factual.count():,}")
    
    return df_proporcao_factual

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cálculo de sMAPE e WMAPE

# COMMAND ----------

def calcular_smape_comparacao_factual(df_matriz: DataFrame, df_proporcao_factual: DataFrame, categoria: str) -> DataFrame:
    """
    Calcula sMAPE comparando merecimento calculado com proporção factual.
    
    Args:
        df_matriz: DataFrame com matriz de merecimento calculada
        df_proporcao_factual: DataFrame com proporção factual
        categoria: Nome da categoria
        
    Returns:
        DataFrame com sMAPE calculado para todas as medidas
    """
    print(f"📊 Calculando sMAPE para: {categoria}")
    print("🔄 Merecimento vs Proporção Factual...")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # Join entre matriz e proporção factual
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
    
    # Calcular sMAPE para cada medida
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
                    f"erro_absoluto_{medida}",
                    F.abs(F.col(f"merecimento_{medida}_percentual") - F.col(f"proporcao_factual_{medida}_percentual"))
                )
                .withColumn(
                    f"smape_{medida}",
                    F.when(
                        (F.col(f"merecimento_{medida}_percentual") + F.col(f"proporcao_factual_{medida}_percentual")) > 0,
                        F.lit(2.0) * F.col(f"erro_absoluto_{medida}") / 
                        (F.col(f"merecimento_{medida}_percentual") + F.col(f"proporcao_factual_{medida}_percentual") + F.lit(EPSILON)) * 100
                    ).otherwise(F.lit(0.0))
                )
            )
    
    print(f"✅ sMAPE calculado para {categoria}")
    return df_com_smape

def calcular_wmape_por_agrupamentos(df_com_smape: DataFrame, categoria: str) -> Dict[str, DataFrame]:
    """
    Calcula WMAPE (Weighted Mean Absolute Percentage Error) por diferentes agrupamentos.
    
    Args:
        df_com_smape: DataFrame com sMAPE calculado
        categoria: Nome da categoria
        
    Returns:
        Dicionário com DataFrames de WMAPE por agrupamento
    """
    print(f"📊 Calculando WMAPE para: {categoria}")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # 1. WMAPE POR FILIAL
    print("📊 Calculando WMAPE por filial...")
    
    aggs_filial = []
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            aggs_filial.extend([
                F.sum(F.col(f"erro_absoluto_{medida}") * F.col(medida)).alias(f"soma_erro_peso_{medida}"),
                F.sum(F.col(medida)).alias(f"soma_peso_{medida}"),
                F.avg(f"smape_{medida}").alias(f"smape_medio_{medida}"),
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
                f"wmape_{medida}",
                F.when(
                    F.col(f"soma_peso_{medida}") > 0,
                    F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
                ).otherwise(F.lit(0.0))
            )
    
    df_wmape_filial = df_wmape_filial.withColumn("tipo_agregacao", F.lit("FILIAL"))
    
    # 2. WMAPE POR GRUPO DE NECESSIDADE
    print("📊 Calculando WMAPE por grupo de necessidade...")
    
    aggs_grupo = []
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            aggs_grupo.extend([
                F.sum(F.col(f"erro_absoluto_{medida}") * F.col(medida)).alias(f"soma_erro_peso_{medida}"),
                F.sum(F.col(medida)).alias(f"soma_peso_{medida}"),
                F.avg(f"smape_{medida}").alias(f"smape_medio_{medida}"),
                F.count("*").alias("total_skus")
            ])
    
    df_wmape_grupo = (
        df_com_smape
        .groupBy("grupo_de_necessidade")
        .agg(*aggs_grupo)
    )
    
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            df_wmape_grupo = df_wmape_grupo.withColumn(
                f"wmape_{medida}",
                F.when(
                    F.col(f"soma_peso_{medida}") > 0,
                    F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
                ).otherwise(F.lit(0.0))
            )
    
    df_wmape_grupo = df_wmape_grupo.withColumn("tipo_agregacao", F.lit("GRUPO_NECESSIDADE"))
    
    # 3. WMAPE DA CATEGORIA INTEIRA
    print("📊 Calculando WMAPE da categoria inteira...")
    
    aggs_categoria = []
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            aggs_categoria.extend([
                F.sum(F.col(f"erro_absoluto_{medida}") * F.col(medida)).alias(f"soma_erro_peso_{medida}"),
                F.sum(F.col(medida)).alias(f"soma_peso_{medida}"),
                F.avg(f"smape_{medida}").alias(f"smape_medio_{medida}"),
                F.count("*").alias("total_skus")
            ])
    
    df_wmape_categoria = df_com_smape.agg(*aggs_categoria)
    
    for medida in medidas_disponiveis:
        if medida in df_com_smape.columns:
            df_wmape_categoria = df_wmape_categoria.withColumn(
                f"wmape_{medida}",
                F.when(
                    F.col(f"soma_peso_{medida}") > 0,
                    F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
                ).otherwise(F.lit(0.0))
            )
    
    df_wmape_categoria = df_wmape_categoria.withColumn("tipo_agregacao", F.lit("CATEGORIA_INTEIRA"))
    
    print(f"✅ WMAPE calculado para {categoria}")
    
    return {
        "filial": df_wmape_filial,
        "grupo": df_wmape_grupo,
        "categoria": df_wmape_categoria
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carregamento da Matriz DRP Geral

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
# MAGIC ## 5. Comparação com Matriz DRP Geral

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
    
    df_com_metricas = df_comparacao
    
    for medida in medidas_disponiveis:
        if medida in df_matriz.columns:
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
# MAGIC ## 6. Identificação de Distorções

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
    
    # Calcular melhor medida para cada registro
    colunas_smape = [f"smape_vs_drp_{medida}" for medida in medidas_disponiveis if medida in df_comparacao.columns]
    
    print(f"    🔍 Debug: Colunas sMAPE encontradas: {colunas_smape}")
    print(f"    🔍 Debug: Total de colunas sMAPE: {len(colunas_smape)}")
    
    # Verifica se há pelo menos 2 colunas para usar F.least
    if len(colunas_smape) >= 2:
        df_com_distorcao = df_comparacao.withColumn(
            "melhor_smape_vs_drp",
            F.least(*[F.col(col) for col in colunas_smape])
        )
    elif len(colunas_smape) == 1:
        # Se há apenas uma coluna, usa ela diretamente
        df_com_distorcao = df_comparacao.withColumn(
            "melhor_smape_vs_drp",
            F.col(colunas_smape[0])
        )
    else:
        # Se não há colunas, cria coluna com valor padrão
        print(f"    ⚠️  Nenhuma coluna sMAPE encontrada, criando coluna padrão")
        df_com_distorcao = df_comparacao.withColumn(
            "melhor_smape_vs_drp",
            F.lit(999.0)  # Valor alto para indicar erro
        )
    
    # Categorizar qualidade
    df_com_distorcao = df_com_distorcao.withColumn(
        "categoria_qualidade_vs_drp",
        F.when(F.col("melhor_smape_vs_drp") < 10, "Excelente")
        .when(F.col("melhor_smape_vs_drp") < 20, "Muito Boa")
        .when(F.col("melhor_smape_vs_drp") < 30, "Boa")
        .when(F.col("melhor_smape_vs_drp") < 50, "Regular")
        .otherwise("Ruim")
    )
    
    # Identificar distorções significativas (sMAPE > 50%)
    df_distorcoes = (
        df_com_distorcao
        .filter(F.col("melhor_smape_vs_drp") > 50)
        .orderBy(F.col("melhor_smape_vs_drp").desc())
    )
    
    print(f"✅ Distorções identificadas para {categoria}")
    print(f"  • Total de distorções: {df_distorcoes.count():,}")
    
    return df_distorcoes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Execução Principal da Análise

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
            # Calcular proporção factual
            df_proporcao_factual = calcular_proporcao_factual_completa(df_matriz, categoria)
            
            # Calcular sMAPE
            df_com_smape = calcular_smape_comparacao_factual(df_matriz, df_proporcao_factual, categoria)
            
            # Calcular WMAPE
            dict_wmape = calcular_wmape_por_agrupamentos(df_com_smape, categoria)
            
            # Comparar com matriz DRP geral
            df_comparacao_drp = comparar_com_matriz_drp_geral(df_matriz, matriz_drp_geral, categoria)
            
            # Identificar distorções
            df_distorcoes = identificar_distorcoes(df_comparacao_drp, categoria)
            
            # Armazenar resultados
            resultados_analise[categoria] = {
                "proporcao_factual": df_proporcao_factual,
                "smape": df_com_smape,
                "wmape": dict_wmape,
                "comparacao_drp": df_comparacao_drp,
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
        print(f"     • Proporção factual: {resultado['proporcao_factual'].count():,} registros")
        print(f"     • sMAPE: {resultado['smape'].count():,} registros")
        print(f"     • Distorções: {resultado['distorcoes'].count():,} registros")
    else:
        print(f"  ❌ {categoria}: {resultado['erro']}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 RESUMO FINAL DO SCRIPT DE ANÁLISE
# MAGIC
# MAGIC ### **O que este script faz:**
# MAGIC 1. **Carrega matrizes calculadas** de todas as categorias
# MAGIC 2. **Calcula proporção factual** para cada categoria
# MAGIC 3. **Calcula sMAPE e WMAPE** comparando merecimento vs factual
# MAGIC 4. **Carrega matriz DRP geral** para comparação
# MAGIC 5. **Compara matrizes calculadas** com DRP geral
# MAGIC 6. **Identifica distorções** significativas
# MAGIC
# MAGIC ### **Resultados gerados:**
# MAGIC - Proporção factual por categoria
# MAGIC - Métricas de sMAPE e WMAPE
# MAGIC - Comparação com matriz DRP geral
# MAGIC - Identificação de distorções
# MAGIC
# MAGIC **Este script está completo e finalizado!** 🎉
