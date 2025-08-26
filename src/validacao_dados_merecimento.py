# Databricks notebook source
# MAGIC %md
# MAGIC # Validação dos Dados - Preparação de Tabelas
# MAGIC 
# MAGIC Este notebook valida os resultados da preparação das tabelas de matriz de merecimento,
# MAGIC gerando relatórios de qualidade dos dados para análise.
# MAGIC 
# MAGIC **Objetivo**: Verificar se os números fazem sentido e identificar possíveis problemas nos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e Configuração

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento dos Dados

# MAGIC %md
# MAGIC Carregamos a tabela de merecimento para validação
# MAGIC e análise de qualidade dos dados.

# COMMAND ----------

# Carregar dados da tabela de merecimento
df_merecimento = spark.table('databox.bcg_comum.supply_base_merecimento_diario')

print(f"✅ Dados carregados com sucesso!")
print(f"📊 Total de registros: {df_merecimento.count():,}")
print(f"📋 Total de colunas: {len(df_merecimento.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Informações Básicas do Dataset

# MAGIC %md
# MAGIC Analisamos as informações básicas do dataset para entender
# MAGIC a estrutura e período dos dados.

# COMMAND ----------

# Informações básicas
total_rows = df_merecimento.count()
total_columns = len(df_merecimento.columns)

# Período dos dados
date_range = df_merecimento.agg(
    F.min("DtAtual").alias("data_min"),
    F.max("DtAtual").alias("data_max")
).collect()[0]

print("🔍 INFORMAÇÕES BÁSICAS DO DATASET")
print("=" * 50)
print(f"📈 Total de registros: {total_rows:,}")
print(f"📋 Total de colunas: {total_columns}")
print(f"📅 Período: {date_range['data_min']} a {date_range['data_max']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Contagem de Entidades Distintas

# MAGIC %md
# MAGIC Contamos as entidades distintas para verificar a cobertura
# MAGIC dos dados por filial, SKU e classificações.

# COMMAND ----------

# Contagem de entidades distintas
distinct_counts = df_merecimento.agg(
    F.countDistinct("CdFilial").alias("filiais_distintas"),
    F.countDistinct("CdSku").alias("skus_distintos"),
    F.countDistinct("DsSetor").alias("setores_distintos"),
    F.countDistinct("DsCurva").alias("curvas_distintas"),
    F.countDistinct("DsCurvaAbcLoja").alias("curvas_abc_distintas")
).collect()[0]

print("🏢 CONTAGEM DE ENTIDADES DISTINTAS")
print("=" * 50)
print(f"🏪 Filiais distintas: {distinct_counts['filiais_distintas']}")
print(f"📦 SKUs distintos: {distinct_counts['skus_distintos']}")
print(f"🏭 Setores distintos: {distinct_counts['setores_distintas']}")
print(f"📊 Curvas distintas: {distinct_counts['curvas_distintas']}")
print(f"📊 Curvas ABC distintas: {distinct_counts['curvas_abc_distintas']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Análise de Campos Nulos

# MAGIC %md
# MAGIC Verificamos a presença de campos nulos para identificar
# MAGIC possíveis problemas de qualidade dos dados.

# COMMAND ----------

print("❌ ANÁLISE DE CAMPOS NULOS")
print("=" * 50)

null_counts = {}
for col in df_merecimento.columns:
    null_count = df_merecimento.filter(F.col(col).isNull()).count()
    null_percentage = (null_count / total_rows) * 100
    null_counts[col] = {"count": null_count, "percentage": null_percentage}
    
    if null_count > 0:
        print(f"⚠️ {col}: {null_count:,} nulos ({null_percentage:.2f}%)")
    else:
        print(f"✅ {col}: Sem nulos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Distribuição de Dias por Filial e SKU

# MAGIC %md
# MAGIC Analisamos a distribuição de dias por filial e SKU para verificar
# MAGIC a consistência temporal dos dados.

# COMMAND ----------

# Distribuição de dias por filial e SKU
days_per_filial_sku = df_merecimento.groupBy("CdFilial", "CdSku").agg(
    F.count("DtAtual").alias("dias_com_dados")
).orderBy("dias_com_dados", ascending=False)

# Estatísticas da distribuição
days_stats = days_per_filial_sku.agg(
    F.min("dias_com_dados").alias("min_dias"),
    F.max("dias_com_dados").alias("max_dias"),
    F.avg("dias_com_dados").alias("avg_dias"),
    F.stddev("dias_com_dados").alias("std_dias")
).collect()[0]

print("📅 DISTRIBUIÇÃO DE DIAS POR FILIAL E SKU")
print("=" * 50)
print(f"📊 Mínimo de dias por filial/SKU: {days_stats['min_dias']}")
print(f"📊 Máximo de dias por filial/SKU: {days_stats['max_dias']}")
print(f"📊 Média de dias por filial/SKU: {days_stats['avg_dias']:.2f}")
print(f"📊 Desvio padrão: {days_stats['std_dias']:.2f}" if days_stats['std_dias'] else "📊 Desvio padrão: N/A")

# Mostrar top 10 combinações filial/SKU com mais dias
print("\n🏆 TOP 10 - Filiais/SKUs com mais dias de dados:")
display(days_per_filial_sku.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Quantidade de SKUs por Filial

# MAGIC %md
# MAGIC Analisamos a quantidade de SKUs por filial para verificar
# MAGIC a distribuição de produtos entre as lojas.

# COMMAND ----------

# Quantidade de SKUs por filial
skus_per_filial = df_merecimento.groupBy("CdFilial").agg(
    F.countDistinct("CdSku").alias("skus_por_filial")
).orderBy("skus_por_filial", ascending=False)

# Estatísticas da distribuição
skus_stats = skus_per_filial.agg(
    F.min("skus_por_filial").alias("min_skus"),
    F.max("skus_por_filial").alias("max_skus"),
    F.avg("skus_por_filial").alias("avg_skus"),
    F.stddev("skus_por_filial").alias("std_skus")
).collect()[0]

print("🏪 QUANTIDADE DE SKUs POR FILIAL")
print("=" * 50)
print(f"📦 Mínimo de SKUs por filial: {skus_stats['min_skus']}")
print(f"📦 Máximo de SKUs por filial: {skus_stats['max_skus']}")
print(f"📦 Média de SKUs por filial: {skus_stats['avg_skus']:.2f}")
print(f"📦 Desvio padrão: {skus_stats['std_skus']:.2f}" if skus_stats['std_skus'] else "📦 Desvio padrão: N/A")

# Mostrar top 10 filiais com mais SKUs
print("\n🏆 TOP 10 - Filiais com mais SKUs:")
display(skus_per_filial.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Análise de Métricas de Negócio

# MAGIC %md
# MAGIC Analisamos as métricas de negócio para verificar
# MAGIC a consistência dos valores calculados.

# COMMAND ----------

# Métricas de negócio
business_metrics = df_merecimento.agg(
    F.sum("EstoqueLoja").alias("total_estoque"),
    F.sum("Receita").alias("total_receita"),
    F.sum("QtMercadoria").alias("total_vendas"),
    F.sum("FlagRuptura").alias("total_rupturas"),
    F.sum("ReceitaPerdidaRuptura").alias("receita_perdida_total")
).collect()[0]

print("💰 ANÁLISE DE MÉTRICAS DE NEGÓCIO")
print("=" * 50)
print(f"📦 Total de estoque: {business_metrics['total_estoque']:,}")
print(f"💰 Total de receita: R$ {business_metrics['total_receita']:,.2f}")
print(f"🛒 Total de vendas: {business_metrics['total_vendas']:,}")
print(f"⚠️ Total de rupturas: {business_metrics['total_rupturas']:,}")
print(f"💸 Receita perdida por ruptura: R$ {business_metrics['receita_perdida_total']:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificações de Qualidade

# MAGIC %md
# MAGIC Realizamos verificações específicas de qualidade para identificar
# MAGIC possíveis inconsistências nos dados.

# COMMAND ----------

print("🔍 VERIFICAÇÕES DE QUALIDADE")
print("=" * 50)

# Verificar valores negativos
negative_estoque = df_merecimento.filter(F.col("EstoqueLoja") < 0).count()
negative_receita = df_merecimento.filter(F.col("Receita") < 0).count()
negative_vendas = df_merecimento.filter(F.col("QtMercadoria") < 0).count()

print(f"📦 Estoque negativo: {negative_estoque} registros")
print(f"💰 Receita negativa: {negative_receita} registros")
print(f"🛒 Vendas negativas: {negative_vendas} registros")

# Verificar datas inválidas
invalid_dates = df_merecimento.filter(
    (F.col("DtAtual").isNull()) |
    (F.col("DtAtual") < "2020-01-01") |
    (F.col("DtAtual") > "2030-12-31")
).count()

print(f"📅 Datas inválidas: {invalid_dates} registros")

# Verificar médias móveis inválidas
invalid_media = df_merecimento.filter(
    (F.col("Media90_Qt_venda_estq") < 0)
).count()

print(f"✅ Médias móveis inválidas: {invalid_media} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Análise de Rupturas

# MAGIC %md
# MAGIC Analisamos detalhadamente as rupturas para entender
# MAGIC o impacto no negócio e identificar padrões.

# COMMAND ----------

# Análise detalhada de rupturas
ruptura_analysis = df_merecimento.filter(F.col("FlagRuptura") == 1).agg(
    F.count("*").alias("total_rupturas"),
    F.sum("deltaRuptura").alias("total_delta_ruptura"),
    F.sum("ReceitaPerdidaRuptura").alias("total_receita_perdida"),
    F.avg("deltaRuptura").alias("media_delta_ruptura"),
    F.avg("ReceitaPerdidaRuptura").alias("media_receita_perdida")
).collect()[0]

print("⚠️ ANÁLISE DE RUPTURAS")
print("=" * 50)
print(f"📊 Total de rupturas: {ruptura_analysis['total_rupturas']:,}")
print(f"📊 Total de delta de ruptura: {ruptura_analysis['total_delta_ruptura']:,.0f}")
print(f"💰 Total de receita perdida: R$ {ruptura_analysis['total_receita_perdida']:,.2f}")
print(f"📊 Média de delta de ruptura: {ruptura_analysis['media_delta_ruptura']:,.2f}")
print(f"💰 Média de receita perdida: R$ {ruptura_analysis['media_receita_perdida']:,.2f}")

# Top 10 rupturas por receita perdida
print("\n🏆 TOP 10 - Rupturas com maior receita perdida:")
top_rupturas = df_merecimento.filter(F.col("FlagRuptura") == 1) \
    .select("DtAtual", "CdFilial", "CdSku", "DsSku", "EstoqueLoja", 
            "Media90_Qt_venda_estq", "deltaRuptura", "ReceitaPerdidaRuptura") \
    .orderBy(F.col("ReceitaPerdidaRuptura").desc()) \
    .limit(10)

display(top_rupturas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Distribuição por Setor e Curva

# MAGIC %md
# MAGIC Analisamos a distribuição dos dados por setor e curva
# MAGIC para verificar a representatividade das classificações.

# COMMAND ----------

# Distribuição por setor
setor_distribution = df_merecimento.groupBy("DsSetor").agg(
    F.count("*").alias("total_registros"),
    F.countDistinct("CdFilial").alias("filiais_distintas"),
    F.countDistinct("CdSku").alias("skus_distintos"),
    F.sum("EstoqueLoja").alias("total_estoque"),
    F.sum("Receita").alias("total_receita")
).orderBy("total_registros", ascending=False)

print("🏭 DISTRIBUIÇÃO POR SETOR")
print("=" * 50)
display(setor_distribution)

# Distribuição por curva
curva_distribution = df_merecimento.groupBy("DsCurva").agg(
    F.count("*").alias("total_registros"),
    F.countDistinct("CdFilial").alias("filiais_distintas"),
    F.countDistinct("CdSku").alias("skus_distintos"),
    F.sum("EstoqueLoja").alias("total_estoque"),
    F.sum("Receita").alias("total_receita")
).orderBy("total_registros", ascending=False)

print("📊 DISTRIBUIÇÃO POR CURVA")
print("=" * 50)
display(curva_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Resumo Executivo

# MAGIC %md
# MAGIC Apresentamos um resumo executivo da validação com
# MAGIC score de qualidade e recomendações.

# COMMAND ----------

# Calcular score de qualidade (0-100)
quality_score = 100

# Penalizar por campos nulos
total_null_fields = sum(1 for col_data in null_counts.values() if col_data["count"] > 0)
if total_null_fields > 0:
    quality_score -= min(20, total_null_fields * 2)

# Penalizar por valores negativos
total_negative = negative_estoque + negative_receita + negative_vendas
if total_negative > 0:
    quality_score -= min(15, total_negative * 3)

# Penalizar por inconsistências de data
if invalid_dates > 0:
    quality_score -= min(10, invalid_dates * 2)

print("📋 RESUMO EXECUTIVO")
print("=" * 50)

if quality_score >= 90:
    print("🟢 QUALIDADE EXCELENTE")
elif quality_score >= 80:
    print("🟡 QUALIDADE BOA")
elif quality_score >= 70:
    print("🟠 QUALIDADE REGULAR")
else:
    print("🔴 QUALIDADE CRÍTICA")

print(f"📊 Score de qualidade: {quality_score}/100")

print(f"\n📈 Total de registros: {total_rows:,}")
print(f"🏪 Total de filiais: {distinct_counts['filiais_distintas']}")
print(f"📦 Total de SKUs: {distinct_counts['skus_distintos']}")
print(f"⚠️ Total de rupturas: {business_metrics['total_rupturas']:,}")

print(f"\n🔍 PROBLEMAS IDENTIFICADOS:")
if total_null_fields > 0:
    print(f"  • {total_null_fields} campos com valores nulos")
if total_negative > 0:
    print(f"  • {total_negative} registros com valores negativos")
if invalid_dates > 0:
    print(f"  • {invalid_dates} registros com datas inválidas")

print(f"\n✅ RECOMENDAÇÕES:")
if quality_score >= 90:
    print("  • Dados prontos para uso em análises")
elif quality_score >= 80:
    print("  • Pequenos ajustes recomendados antes do uso")
elif quality_score >= 70:
    print("  • Revisão moderada necessária")
else:
    print("  • Revisão urgente necessária antes do uso")

print(f"\n🎯 Próximos passos: Executar notebook de análise da matriz de merecimento")
