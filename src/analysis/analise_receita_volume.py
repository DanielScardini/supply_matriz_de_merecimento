# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any
import plotly.graph_objects as go

# Inicialização do Spark
spark = SparkSession.builder.appName("analise_receita_volume").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

# Configuração das regras de agrupamento por categoria (mesmo do cálculo de matriz)
REGRAS_AGRUPAMENTO = {
    "DIRETORIA DE TELAS": {
        "coluna_grupo_necessidade": "gemeos",
        "tipo_agrupamento": "gêmeos",
        "descricao": "Agrupamento por produtos similares (gêmeos)"
    },
    "DIRETORIA TELEFONIA CELULAR": {
        "coluna_grupo_necessidade": "gemeos", 
        "tipo_agrupamento": "gêmeos",
        "descricao": "Agrupamento por produtos similares (gêmeos)"
    },
    "DIRETORIA LINHA BRANCA": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial",
        "descricao": "Agrupamento por espécie gerencial + voltagem (DsVoltagem)"
    },
    "DIRETORIA LINHA LEVE": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial", 
        "descricao": "Agrupamento por espécie gerencial + voltagem (DsVoltagem)"
    },
    "DIRETORIA INFO/GAMES": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial",
        "descricao": "Agrupamento por espécie gerencial"
    }
}

dt_inicio = "2024-07-01"
dt_fim = hoje_str

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Função para Determinar Grupo de Necessidade

# COMMAND ----------

def determinar_grupo_necessidade(categoria: str, df: DataFrame) -> DataFrame:
    """
    Determina o grupo de necessidade baseado na categoria e aplica a regra correspondente.
    Mesmo racional usado no cálculo de matriz de merecimento.
    """
    if categoria not in REGRAS_AGRUPAMENTO:
        raise ValueError(f"Categoria '{categoria}' não suportada. Categorias válidas: {list(REGRAS_AGRUPAMENTO.keys())}")
    
    regra = REGRAS_AGRUPAMENTO[categoria]
    coluna_origem = regra["coluna_grupo_necessidade"]

    colunas_df = df.columns
    if coluna_origem not in colunas_df:
        raise ValueError(f"Coluna '{coluna_origem}' não encontrada no DataFrame. Colunas disponíveis: {colunas_df}")
    
    # Verifica se é LINHA BRANCA ou LINHA LEVE para aplicar agrupamento especial
    if categoria in ["DIRETORIA DE LINHA BRANCA", "DIRETORIA LINHA LEVE"]:
        # Verifica se DsVoltagem existe no DataFrame
        if "DsVoltagem" not in colunas_df:
            raise ValueError(f"Coluna 'DsVoltagem' não encontrada no DataFrame para categoria '{categoria}'. Colunas disponíveis: {colunas_df}")
        
        # Cria grupo de necessidade combinando NmEspecieGerencial + "_" + DsVoltagem (nulls preenchidos com "")
        df_com_grupo = df.withColumn(
            "DsVoltagem_filled",
            F.substring(F.coalesce(F.col("DsVoltagem"), F.lit("")), 1, 3)
        ).withColumn(
            "grupo_de_necessidade",
            F.concat(
                F.coalesce(F.col(coluna_origem), F.lit("SEM_GN")),
                F.lit("_"),
                F.col("DsVoltagem_filled")
            )
        ).withColumn(
            "tipo_agrupamento",
            F.lit(regra["tipo_agrupamento"])
        ).drop("DsVoltagem_filled")
        
        print(f"✅ Grupo de necessidade definido para '{categoria}' (com DsVoltagem):")
        print(f"  • Coluna origem: {coluna_origem} + DsVoltagem")
        print(f"  • Valores copiados: {df_com_grupo.select('grupo_de_necessidade').distinct().count()} grupos únicos")
        
    else:
        # Para outras categorias, mantém o comportamento original
        df_com_grupo = df.withColumn(
            "grupo_de_necessidade",
            F.coalesce(F.col(coluna_origem), F.lit("SEM_GN"))
        ).withColumn(
            "tipo_agrupamento",
            F.lit(regra["tipo_agrupamento"])
        )
        
        print(f"✅ Grupo de necessidade definido para '{categoria}':")
        print(f"  • Coluna origem: {coluna_origem}")
        print(f"  • Valores copiados: {df_com_grupo.select('grupo_de_necessidade').distinct().count()} grupos únicos")
    
    return df_com_grupo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Análise de Receita por Grupo de Necessidade

# COMMAND ----------

def analisar_receita_por_grupo_necessidade(categoria: str) -> DataFrame:
    """
    Analisa % de receita por grupo de necessidade vs companhia inteira.
    Usa o mesmo racional de definição de grupo de necessidade do cálculo de matriz.
    """
    print(f"🔍 Analisando receita por grupo de necessidade: {categoria}")
    print("=" * 80)
    
    # Carregar dados base
    df_base = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
        .filter(F.col("DtAtual") >= dt_inicio)
        .filter(F.col("DtAtual") < dt_fim)
    )
    
    print(f"📦 Dados base carregados: {df_base.count():,} registros")
    
    # Determinar grupo de necessidade
    df_com_grupo = determinar_grupo_necessidade(categoria, df_base)
    
    # Agregar por grupo de necessidade
    df_agregado = (
        df_com_grupo
        .groupBy("grupo_de_necessidade", "tipo_agrupamento")
        .agg(
            F.sum("QtMercadoria").alias("QtDemanda"),
            F.sum("Receita").alias("Receita"),
            F.countDistinct("CdSku").alias("QtdSKUs"),
            F.countDistinct("CdFilial").alias("QtdFiliais")
        )
    )
    
    # Calcular totais da companhia
    totais = df_agregado.agg(
        F.sum("QtDemanda").alias("TotalDemanda"),
        F.sum("Receita").alias("TotalReceita")
    ).collect()[0]
    
    total_demanda = totais["TotalDemanda"]
    total_receita = totais["TotalReceita"]
    
    print(f"📊 Totais da companhia:")
    print(f"  • Demanda total: {total_demanda:,.0f}")
    print(f"  • Receita total: R$ {total_receita:,.2f}")
    
    # Calcular percentuais
    df_percentuais = (
        df_agregado
        .withColumn("PercDemanda", F.round((F.col("QtDemanda") / total_demanda) * 100, 2))
        .withColumn("PercReceita", F.round((F.col("Receita") / total_receita) * 100, 2))
        .withColumn("ReceitaPorSKU", F.round(F.col("Receita") / F.col("QtdSKUs"), 2))
        .withColumn("ReceitaPorFilial", F.round(F.col("Receita") / F.col("QtdFiliais"), 2))
        .orderBy(F.desc("Receita"))
    )
    
    print(f"\n📈 Análise por grupo de necessidade:")
    print(f"  • Total de grupos: {df_percentuais.count()}")
    
    # Mostrar top 10 grupos por receita
    print(f"\n🏆 TOP 10 GRUPOS POR RECEITA:")
    df_percentuais.select(
        "grupo_de_necessidade",
        "tipo_agrupamento", 
        "QtdSKUs",
        "QtdFiliais",
        "QtDemanda",
        "Receita",
        "PercDemanda",
        "PercReceita",
        "ReceitaPorSKU",
        "ReceitaPorFilial"
    ).show(10, truncate=False)
    
    # Validação dos percentuais
    validacao = df_percentuais.agg(
        F.sum("PercDemanda").alias("TotalPercDemanda"),
        F.sum("PercReceita").alias("TotalPercReceita")
    ).collect()[0]
    
    print(f"\n✅ VALIDAÇÃO DOS PERCENTUAIS:")
    print(f"  • Soma % Demanda: {validacao['TotalPercDemanda']:.2f}%")
    print(f"  • Soma % Receita: {validacao['TotalPercReceita']:.2f}%")
    
    return df_percentuais

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Executar Análise para Todas as Categorias

# COMMAND ----------

# Executar análise para todas as categorias
categorias = list(REGRAS_AGRUPAMENTO.keys())

print("🚀 INICIANDO ANÁLISE DE RECEITA POR GRUPO DE NECESSIDADE")
print("=" * 100)

resultados = {}

for categoria in categorias:
    print(f"\n{'='*20} {categoria} {'='*20}")
    
    try:
        df_resultado = analisar_receita_por_grupo_necessidade(categoria)
        resultados[categoria] = df_resultado
        
        # Salvar resultado em CSV
        df_resultado.coalesce(1).write.mode("overwrite").option("header", True).csv(f"/tmp/analise_receita_{categoria.replace(' ', '_').lower()}")
        print(f"💾 Resultado salvo em: /tmp/analise_receita_{categoria.replace(' ', '_').lower()}")
        
    except Exception as e:
        print(f"❌ Erro na categoria {categoria}: {str(e)}")
        continue

print(f"\n✅ ANÁLISE CONCLUÍDA!")
print(f"📊 Categorias processadas: {len(resultados)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Análise Detalhada de Categoria Específica

# COMMAND ----------

# Análise detalhada de uma categoria específica
categoria_detalhada = "DIRETORIA LINHA LEVE"  # Pode ser alterada

if categoria_detalhada in resultados:
    print(f"🔍 ANÁLISE DETALHADA: {categoria_detalhada}")
    print("=" * 80)
    
    df_detalhado = resultados[categoria_detalhada]
    
    # Top 20 grupos por receita
    print(f"\n🏆 TOP 20 GRUPOS POR RECEITA ({categoria_detalhada}):")
    df_detalhado.select(
        "grupo_de_necessidade",
        "tipo_agrupamento", 
        "QtdSKUs",
        "QtdFiliais",
        "QtDemanda",
        "Receita",
        "PercDemanda",
        "PercReceita",
        "ReceitaPorSKU",
        "ReceitaPorFilial"
    ).show(20, truncate=False)
    
    # Estatísticas gerais
    print(f"\n📊 ESTATÍSTICAS GERAIS ({categoria_detalhada}):")
    stats = df_detalhado.agg(
        F.count("grupo_de_necessidade").alias("TotalGrupos"),
        F.sum("QtdSKUs").alias("TotalSKUs"),
        F.sum("QtdFiliais").alias("TotalFiliais"),
        F.sum("QtDemanda").alias("TotalDemanda"),
        F.sum("Receita").alias("TotalReceita"),
        F.avg("PercReceita").alias("MediaPercReceita"),
        F.max("PercReceita").alias("MaxPercReceita"),
        F.min("PercReceita").alias("MinPercReceita")
    ).collect()[0]
    
    print(f"  • Total de grupos: {stats['TotalGrupos']}")
    print(f"  • Total de SKUs: {stats['TotalSKUs']}")
    print(f"  • Total de filiais: {stats['TotalFiliais']}")
    print(f"  • Demanda total: {stats['TotalDemanda']:,.0f}")
    print(f"  • Receita total: R$ {stats['TotalReceita']:,.2f}")
    print(f"  • % Receita médio por grupo: {stats['MediaPercReceita']:.2f}%")
    print(f"  • % Receita máximo: {stats['MaxPercReceita']:.2f}%")
    print(f"  • % Receita mínimo: {stats['MinPercReceita']:.2f}%")
    
    # Grupos com maior concentração de receita
    print(f"\n🎯 CONCENTRAÇÃO DE RECEITA ({categoria_detalhada}):")
    df_concentracao = (
        df_detalhado
        .withColumn("PercReceitaCumulativo", F.sum("PercReceita").over(W.orderBy(F.desc("Receita")).rowsBetween(W.unboundedPreceding, 0)))
        .select("grupo_de_necessidade", "PercReceita", "PercReceitaCumulativo")
        .filter(F.col("PercReceitaCumulativo") <= 80)
    )
    
    print(f"  • Grupos que representam 80% da receita: {df_concentracao.count()}")
    df_concentracao.show(10, truncate=False)
    
else:
    print(f"❌ Categoria '{categoria_detalhada}' não foi processada com sucesso.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo Executivo

# COMMAND ----------

print("📋 RESUMO EXECUTIVO - ANÁLISE DE RECEITA POR GRUPO DE NECESSIDADE")
print("=" * 100)

for categoria, df_resultado in resultados.items():
    if df_resultado is not None:
        # Top 3 grupos por receita
        top3 = df_resultado.limit(3).collect()
        
        print(f"\n🏆 {categoria}:")
        print(f"  • Total de grupos: {df_resultado.count()}")
        
        if top3:
            print(f"  • Top 3 grupos por receita:")
            for i, row in enumerate(top3, 1):
                print(f"    {i}. {row['grupo_de_necessidade']}: {row['PercReceita']:.2f}% (R$ {row['Receita']:,.2f})")
        
        # Concentração de receita
        total_receita = sum([row['PercReceita'] for row in top3])
        print(f"  • Top 3 representam: {total_receita:.2f}% da receita total")

print(f"\n✅ Análise concluída para {len(resultados)} categorias!")
print(f"📁 Arquivos salvos em: /tmp/analise_receita_*")

# COMMAND ----------


