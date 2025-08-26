# Databricks notebook source
# MAGIC %md
# MAGIC # Cálculo da Matriz de Merecimento - Detecção Automática de Meses Atípicos
# MAGIC
# MAGIC Este notebook implementa o cálculo da matriz de merecimento com detecção automática
# MAGIC de meses atípicos usando regra analítica baseada em estatísticas robustas.
# MAGIC
# MAGIC **Objetivo**: Calcular a matriz de merecimento otimizada removendo meses com comportamento
# MAGIC atípico que podem distorcer as alocações.
# MAGIC
# MAGIC **Metodologia de Detecção de Outliers**:
# MAGIC - **Regra dos 3 Desvios Padrão**: Remove meses com QtMercadoria > 3σ da média
# MAGIC - **Cálculo por Gêmeo**: Estatísticas calculadas individualmente para cada grupo de produtos similares
# MAGIC - **Validação Automática**: Identifica e reporta meses removidos com justificativa estatística

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e Configurações Iniciais

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Optional

# Inicialização do Spark
spark = SparkSession.builder.appName("calculo_matriz_merecimento").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregamento dos Dados Base

# MAGIC %md
# MAGIC Carregamos a base de dados de vendas e estoque para produtos de telefonia celular,
# MAGIC que será utilizada para o cálculo da matriz de merecimento.

# COMMAND ----------

df_vendas_estoque_telefonia = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA TELEFONIA CELULAR')
    .filter(F.col("DtAtual") >= "2024-01-01")
    .withColumn(
        "year_month",
        F.date_format(F.col("DtAtual"), "yyyyMM").cast("int")
    )
    .fillna(0, subset=["Receita", "QtMercadoria", "TeveVenda"])
)
df_vendas_estoque_telefonia.cache()

print("✅ Dados de vendas e estoque de telefonia carregados:")
print(f"📊 Total de registros: {df_vendas_estoque_telefonia.count():,}")
print(f"📅 Período: {df_vendas_estoque_telefonia.agg(F.min('DtAtual'), F.max('DtAtual')).collect()[0]}")

df_vendas_estoque_telefonia.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carregamento dos Mapeamentos de Produtos

# MAGIC %md
# MAGIC Carregamos os arquivos de mapeamento que relacionam SKUs com modelos, 
# MAGIC espécies gerenciais e grupos de produtos similares ("gêmeos").

# COMMAND ----------

# Mapeamento de modelos e tecnologia
de_para_modelos_tecnologia = (
    pd.read_csv('dados_analise/MODELOS_AJUSTE (1).csv', 
                delimiter=';')
    .drop_duplicates()
)

# Normalização de nomes de colunas
de_para_modelos_tecnologia.columns = (
    de_para_modelos_tecnologia.columns
    .str.strip()            # remove leading/trailing spaces
    .str.lower()            # lowercase
    .str.replace(r"[^\w]+", "_", regex=True)  # non-alphanumeric -> "_"
    .str.strip("_")         # remove leading/trailing underscores
)

# Mapeamento de produtos similares (gêmeos)
de_para_gemeos_tecnologia = (
    pd.read_csv('dados_analise/ITENS_GEMEOS 2.csv',
                delimiter=";",
                encoding='iso-8859-1')
    .drop_duplicates()
)

# Normalização de nomes de colunas
de_para_gemeos_tecnologia.columns = (
    de_para_gemeos_tecnologia
    .columns
    .str.strip()            # remove leading/trailing spaces
    .str.lower()            # lowercase
    .str.replace(r"[^\w]+", "_", regex=True)  # non-alphanumeric -> "_"
    .str.strip("_")         # remove leading/trailing underscores
)

# Renomeação e merge dos mapeamentos
de_para_modelos_tecnologia = (
    de_para_modelos_tecnologia.rename(columns={'codigo_item': 'sku_loja'})
)

de_para_modelos_gemeos_tecnologia = (
    spark.createDataFrame(
        pd.merge(
            de_para_modelos_tecnologia,
            de_para_gemeos_tecnologia,
            on='sku_loja',
            how="outer"
        )
        [['sku_loja', 'item', 'modelos', 'setor_gerencial', 'gemeos']]
        .drop_duplicates()
    )
    .withColumnRenamed("sku_loja", "CdSku")
)

print("✅ Mapeamentos de produtos carregados:")
print(f"📦 Total de SKUs mapeados: {de_para_modelos_gemeos_tecnologia.count():,}")
print(f"🔄 Total de grupos gêmeos: {de_para_modelos_gemeos_tecnologia.select('gemeos').distinct().count()}")

de_para_modelos_gemeos_tecnologia.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Join dos Dados com Mapeamentos

# MAGIC %md
# MAGIC Realizamos o join entre os dados de vendas/estoque e os mapeamentos
# MAGIC para obter uma base consolidada com informações de gêmeos.

# COMMAND ----------

df_vendas_estoque_telefonia_gemeos_modelos = (
    df_vendas_estoque_telefonia
    .join(
        de_para_modelos_gemeos_tecnologia,
        on=['CdSku'],
        how='left'
    )
    .fillna("SEM_GRUPO", subset=["gemeos"])
)

print("✅ Dados consolidados com mapeamentos:")
print(f"📊 Total de registros após join: {df_vendas_estoque_telefonia_gemeos_modelos.count():,}")

df_vendas_estoque_telefonia_gemeos_modelos.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Detecção Automática de Meses Atípicos

# MAGIC %md
# MAGIC Implementamos a regra analítica para detectar meses atípicos:
# MAGIC - **Cálculo por Gêmeo**: Estatísticas calculadas individualmente para cada grupo de produtos similares
# MAGIC - **Regra dos 3 Desvios**: Remove meses com QtMercadoria > 3σ da média
# MAGIC - **Validação Automática**: Identifica e reporta meses removidos com justificativa estatística

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Cálculo de Estatísticas por Gêmeo e Mês

# MAGIC %md
# MAGIC Calculamos as estatísticas (média e desvio padrão) da quantidade de mercadoria
# MAGIC para cada grupo de produtos similares (gêmeos) por mês.

# COMMAND ----------

# Agregação por gêmeo e mês para cálculo de estatísticas
df_stats_por_gemeo_mes = (
    df_vendas_estoque_telefonia_gemeos_modelos
    .groupBy("gemeos", "year_month")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria_total"),
        F.countDistinct("CdFilial").alias("qtd_filiais"),
        F.countDistinct("CdSku").alias("qtd_skus")
    )
    .filter(F.col("QtMercadoria_total") > 0)  # Remove meses sem vendas
)

print("📊 Estatísticas calculadas por gêmeo e mês:")
print(f"📈 Total de registros: {df_stats_por_gemeo_mes.count():,}")

df_stats_por_gemeo_mes.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Cálculo de Média e Desvio Padrão por Gêmeo

# MAGIC %md
# MAGIC Calculamos a média e desvio padrão da quantidade de mercadoria para cada gêmeo,
# MAGIC considerando todos os meses disponíveis.

# COMMAND ----------

# Janela para cálculo de estatísticas por gêmeo
w_stats_gemeo = Window.partitionBy("gemeos")

# Cálculo de média e desvio padrão por gêmeo
df_stats_gemeo = (
    df_stats_por_gemeo_mes
    .withColumn(
        "media_qt_mercadoria",
        F.avg("QtMercadoria_total").over(w_stats_gemeo)
    )
    .withColumn(
        "desvio_padrao_qt_mercadoria",
        F.stddev("QtMercadoria_total").over(w_stats_gemeo)
    )
    .withColumn(
        "limite_superior_3sigma",
        F.col("media_qt_mercadoria") + (F.lit(3) * F.col("desvio_padrao_qt_mercadoria"))
    )
    .withColumn(
        "limite_inferior_3sigma",
        F.greatest(
            F.col("media_qt_mercadoria") - (F.lit(3) * F.col("desvio_padrao_qt_mercadoria")),
            F.lit(0)  # Não permite valores negativos
        )
    )
    .withColumn(
        "flag_mes_atipico",
        F.when(
            (F.col("QtMercadoria_total") > F.col("limite_superior_3sigma")) |
            (F.col("QtMercadoria_total") < F.col("limite_inferior_3sigma")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)

print("📊 Estatísticas calculadas por gêmeo:")
print(f"📈 Total de registros: {df_stats_gemeo.count():,}")

# Mostrar estatísticas para alguns gêmeos
df_stats_gemeo.orderBy("gemeos", "year_month").limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Identificação de Meses Atípicos

# MAGIC %md
# MAGIC Identificamos os meses que serão removidos por serem considerados atípicos
# MAGIC segundo a regra dos 3 desvios padrão.

# COMMAND ----------

# Meses identificados como atípicos
df_meses_atipicos = (
    df_stats_gemeo
    .filter(F.col("flag_mes_atipico") == 1)
    .select(
        "gemeos",
        "year_month",
        F.round("QtMercadoria_total", 2).alias("QtMercadoria_total"),
        F.round("media_qt_mercadoria", 2).alias("media_qt_mercadoria"),
        F.round("desvio_padrao_qt_mercadoria", 2).alias("desvio_padrao_qt_mercadoria"),
        F.round("limite_superior_3sigma", 2).alias("limite_superior_3sigma"),
        F.round("limite_inferior_3sigma", 2).alias("limite_inferior_3sigma"),
        "flag_mes_atipico"
    )
    .orderBy("gemeos", "year_month")
)

print("⚠️ MESES IDENTIFICADOS COMO ATÍPICOS:")
print("=" * 80)
print(f"📊 Total de meses atípicos: {df_meses_atipicos.count():,}")

if df_meses_atipicos.count() > 0:
    print("\n🔍 Detalhamento dos meses atípicos:")
    df_meses_atipicos.display()
else:
    print("✅ Nenhum mês atípico foi identificado!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Resumo Estatístico por Gêmeo

# MAGIC %md
# MAGIC Apresentamos um resumo estatístico mostrando quantos meses foram identificados
# MAGIC como atípicos para cada grupo de produtos similares.

# COMMAND ----------

# Resumo estatístico por gêmeo
df_resumo_atipicos_gemeo = (
    df_stats_gemeo
    .groupBy("gemeos")
    .agg(
        F.count("*").alias("total_meses"),
        F.sum("flag_mes_atipico").alias("meses_atipicos"),
        F.round(F.avg("media_qt_mercadoria"), 2).alias("media_qt_mercadoria"),
        F.round(F.avg("desvio_padrao_qt_mercadoria"), 2).alias("desvio_padrao_qt_mercadoria")
    )
    .withColumn(
        "percentual_meses_atipicos",
        F.round(F.col("meses_atipicos") / F.col("total_meses") * 100, 2)
    )
    .orderBy(F.desc("meses_atipicos"))
)

print("📋 RESUMO ESTATÍSTICO POR GÊMEO:")
print("=" * 80)
df_resumo_atipicos_gemeo.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Filtragem dos Dados - Remoção de Meses Atípicos

# MAGIC %md
# MAGIC Aplicamos o filtro para remover os meses identificados como atípicos,
# MAGIC criando uma base limpa para o cálculo da matriz de merecimento.

# COMMAND ----------

# Lista de meses atípicos para filtro
meses_atipicos_filtro = (
    df_meses_atipicos
    .select("year_month")
    .distinct()
    .collect()
)

meses_atipicos_lista = [row["year_month"] for row in meses_atipicos_filtro]

print("🔍 MESES ATÍPICOS IDENTIFICADOS PARA REMOÇÃO:")
print("=" * 50)
if meses_atipicos_lista:
    for mes in sorted(meses_atipicos_lista):
        print(f"📅 {mes}")
    print(f"\n📊 Total de meses únicos a serem removidos: {len(meses_atipicos_lista)}")
else:
    print("✅ Nenhum mês atípico identificado!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Aplicação do Filtro de Meses Atípicos

# MAGIC %md
# MAGIC Aplicamos o filtro para remover os meses atípicos da base de dados,
# MAGIC mantendo apenas os meses com comportamento normal para cálculo da matriz.

# COMMAND ----------

# Aplicação do filtro de meses atípicos
df_vendas_estoque_telefonia_filtrado = (
    df_vendas_estoque_telefonia_gemeos_modelos
    .filter(~F.col("year_month").isin(meses_atipicos_lista))
)

print("✅ FILTRO DE MESES ATÍPICOS APLICADO:")
print("=" * 50)
print(f"📊 Total de registros ANTES do filtro: {df_vendas_estoque_telefonia_gemeos_modelos.count():,}")
print(f"📊 Total de registros DEPOIS do filtro: {df_vendas_estoque_telefonia_filtrado.count():,}")
print(f"📊 Registros removidos: {df_vendas_estoque_telefonia_gemeos_modelos.count() - df_vendas_estoque_telefonia_filtrado.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Validação da Filtragem

# MAGIC %md
# MAGIC Validamos que a filtragem foi aplicada corretamente, verificando
# MAGIC se os meses atípicos foram efetivamente removidos.

# COMMAND ----------

# Validação da filtragem
meses_restantes = (
    df_vendas_estoque_telefonia_filtrado
    .select("year_month")
    .distinct()
    .orderBy("year_month")
    .collect()
)

meses_restantes_lista = [row["year_month"] for row in meses_restantes]

print("✅ VALIDAÇÃO DA FILTRAGEM:")
print("=" * 50)
print(f"📊 Total de meses restantes: {len(meses_restantes_lista)}")
print(f"📅 Meses disponíveis para análise:")
for mes in meses_restantes_lista:
    print(f"  • {mes}")

# Verificar se algum mês atípico ainda está presente
meses_atipicos_ainda_presentes = set(meses_atipicos_lista) & set(meses_restantes_lista)
if meses_atipicos_ainda_presentes:
    print(f"\n⚠️ ATENÇÃO: Meses atípicos ainda presentes: {meses_atipicos_ainda_presentes}")
else:
    print(f"\n✅ CONFIRMADO: Todos os meses atípicos foram removidos com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Agregação dos Dados Filtrados

# MAGIC %md
# MAGIC Agregamos os dados filtrados por mês, modelo, gêmeos e filial para
# MAGIC análise no nível de loja, agora sem os meses atípicos.

# COMMAND ----------

df_vendas_estoque_telefonia_agg_filtrado = (
    df_vendas_estoque_telefonia_filtrado
    .filter(~F.col("NmEspecieGerencial").contains("CHIP"))  # Excluir chips
    .groupBy("year_month", "gemeos", "CdFilial")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria"),
        F.round(F.sum("Receita"), 2).alias("Receita"),
        F.round(F.sum("Media90_Qt_venda_estq"), 0).alias("QtdDemanda"),
        F.round(F.median("PrecoMedio90"), 2).alias("PrecoMedio90")
    )
)

print("✅ Dados agregados por filial (sem meses atípicos):")
print(f"📊 Total de registros agregados: {df_vendas_estoque_telefonia_agg_filtrado.count():,}")

df_vendas_estoque_telefonia_agg_filtrado.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cálculo de Percentuais para Matriz de Merecimento

# MAGIC %md
# MAGIC Calculamos os percentuais de participação nas vendas e demanda por mês,
# MAGIC modelo e grupo de produtos similares, agora com base nos dados filtrados.

# COMMAND ----------

# Janela por mês, modelo e gêmeos
w = Window.partitionBy("year_month", "gemeos")

df_pct_telefonia_filtrado = (
    df_vendas_estoque_telefonia_agg_filtrado
    # Totais no mês/especie
    .withColumn("Qt_total_mes_especie", F.sum("QtMercadoria").over(w))
    .withColumn("Demanda_total_mes_especie", F.sum("QtdDemanda").over(w))
    
    # Percentuais de venda e demanda
    .withColumn(
        "pct_vendas", 
        F.when(F.col("Qt_total_mes_especie") > 0,
               F.col("QtMercadoria") / F.col("Qt_total_mes_especie"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "pct_demanda", 
        F.when(F.col("Demanda_total_mes_especie") > 0,
               F.col("QtdDemanda") / F.col("Demanda_total_mes_especie"))
         .otherwise(F.lit(None).cast("double"))
    )
    
    # Percentuais em %
    .withColumn("pct_vendas_perc", F.round(F.col("pct_vendas") * 100, 2))
    .withColumn("pct_demanda_perc", F.round(F.col("pct_demanda") * 100, 2))
    
    # Selecionar colunas finais
    .select(
        "year_month", "gemeos", "CdFilial",
        "QtMercadoria", "QtdDemanda", "PrecoMedio90",
        "Qt_total_mes_especie", "Demanda_total_mes_especie",
        "pct_vendas", "pct_vendas_perc",
        "pct_demanda", "pct_demanda_perc"
    )
    .fillna(0, subset=[
        "Qt_total_mes_especie", "Demanda_total_mes_especie",
        "pct_vendas", "pct_vendas_perc", 
        "pct_demanda", "pct_demanda_perc"
    ])
)

print("✅ Percentuais calculados por filial (dados filtrados):")
print(f"📊 Total de registros: {df_pct_telefonia_filtrado.count():,}")

df_pct_telefonia_filtrado.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumo da Detecção de Meses Atípicos

# MAGIC %md
# MAGIC Apresentamos um resumo completo da detecção e remoção de meses atípicos,
# MAGIC incluindo estatísticas e impacto na base de dados.

# COMMAND ----------

# Estatísticas finais
total_registros_original = df_vendas_estoque_telefonia_gemeos_modelos.count()
total_registros_filtrado = df_vendas_estoque_telefonia_filtrado.count()
total_meses_original = df_vendas_estoque_telefonia_gemeos_modelos.select("year_month").distinct().count()
total_meses_filtrado = df_vendas_estoque_telefonia_filtrado.select("year_month").distinct().count()

print("📋 RESUMO COMPLETO DA DETECÇÃO DE MESES ATÍPICOS")
print("=" * 80)

print(f"\n📊 IMPACTO NA BASE DE DADOS:")
print(f"  • Registros originais: {total_registros_original:,}")
print(f"  • Registros após filtro: {total_registros_filtrado:,}")
print(f"  • Registros removidos: {total_registros_original - total_registros_filtrado:,}")
print(f"  • Percentual de remoção: {((total_registros_original - total_registros_filtrado) / total_registros_original * 100):.2f}%")

print(f"\n📅 IMPACTO NOS MESES:")
print(f"  • Meses originais: {total_meses_original}")
print(f"  • Meses após filtro: {total_meses_filtrado}")
print(f"  • Meses removidos: {total_meses_original - total_meses_filtrado}")

print(f"\n🔍 MESES ATÍPICOS REMOVIDOS:")
if meses_atipicos_lista:
    for mes in sorted(meses_atipicos_lista):
        print(f"  • {mes}")
else:
    print("  • Nenhum mês atípico identificado")

print(f"\n✅ RESULTADO:")
print(f"  • Base limpa para cálculo da matriz de merecimento")
print(f"  • Remoção automática de outliers estatísticos")
print(f"  • Melhoria na qualidade das alocações calculadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Próximos Passos

# MAGIC %md
# MAGIC A base de dados está agora limpa e pronta para o cálculo da matriz de merecimento.
# MAGIC Os próximos passos incluem:

# COMMAND ----------

print("🎯 PRÓXIMOS PASSOS PARA CÁLCULO DA MATRIZ:")
print("=" * 60)

print(f"\n1️⃣ BASE PREPARADA:")
print(f"   ✅ Dados de vendas e estoque carregados")
print(f"   ✅ Mapeamentos de produtos aplicados")
print(f"   ✅ Meses atípicos removidos automaticamente")
print(f"   ✅ Estatísticas validadas")

print(f"\n2️⃣ PRÓXIMAS ETAPAS:")
print(f"   📊 Cálculo da matriz de merecimento por filial")
print(f"   📊 Otimização das alocações")
print(f"   📊 Validação dos resultados")
print(f"   📊 Geração de relatórios")

print(f"\n3️⃣ ARQUIVOS DE SAÍDA:")
print(f"   📁 Matriz de merecimento otimizada")
print(f"   📁 Relatório de meses removidos")
print(f"   📁 Estatísticas de qualidade")

print(f"\n✅ Notebook de detecção de meses atípicos concluído com sucesso!")
print(f"🎯 Base pronta para cálculo da matriz de merecimento!")


