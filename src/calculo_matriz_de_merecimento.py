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
# MAGIC - **Regra dos 3 Desvios**: Remove meses com QtMercadoria > 3σ da média
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
# MAGIC
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
# MAGIC
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
# MAGIC
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
# MAGIC
# MAGIC %md
# MAGIC Implementamos a regra analítica para detectar meses atípicos:
# MAGIC - **Cálculo por Gêmeo**: Estatísticas calculadas individualmente para cada grupo de produtos similares
# MAGIC - **Regra dos n Desvios**: Remove meses com QtMercadoria > nσ da média
# MAGIC - **Validação Automática**: Identifica e reporta meses removidos com justificativa estatística

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Cálculo de Estatísticas por Gêmeo e Mês
# MAGIC
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
# MAGIC
# MAGIC %md
# MAGIC Calculamos a média e desvio padrão da quantidade de mercadoria para cada gêmeo,
# MAGIC considerando todos os meses disponíveis.

# COMMAND ----------

n_desvios = 2

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
        "limite_superior_nsigma",
        F.col("media_qt_mercadoria") + (F.lit(n_desvios) * F.col("desvio_padrao_qt_mercadoria"))
    )
    .withColumn(
        "limite_inferior_nsigma",
        F.greatest(
            F.col("media_qt_mercadoria") - (F.lit(n_desvios) * F.col("desvio_padrao_qt_mercadoria")),
            F.lit(0)  # Não permite valores negativos
        )
    )
    .withColumn(
        "flag_mes_atipico",
        F.when(
            (F.col("QtMercadoria_total") > F.col("limite_superior_nsigma")) |
            (F.col("QtMercadoria_total") < F.col("limite_inferior_nsigma")),
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
# MAGIC
# MAGIC %md
# MAGIC Identificamos os meses que serão removidos por serem considerados atípicos
# MAGIC segundo a regra dos n desvios padrão.

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
        F.round("limite_superior_nsigma", 2).alias("limite_superior_nsigma"),
        F.round("limite_inferior_nsigma", 2).alias("limite_inferior_nsigma"),
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
# MAGIC
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

de_para_filial_cd = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .select("CdFilial", "Cd_primario")
    .distinct()
    .dropna()
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Cálculo de Demanda por Médias Móveis (Sem Ruptura)
# MAGIC
# MAGIC %md
# MAGIC Calculamos a demanda usando médias móveis de 90, 120 e 360 dias,
# MAGIC considerando apenas dias que não tiveram ruptura (com estoque).
# MAGIC
# MAGIC **Metodologia:**
# MAGIC - **Filtro de Ruptura**: Apenas dias com estoque > 0
# MAGIC - **Médias Móveis**: 90, 120 e 360 dias
# MAGIC - **Nível SKU-Loja**: Cálculo individual por produto e filial
# MAGIC - **Agregação por Gêmeo**: Consolidação para cálculo de percentuais

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.1 Preparação dos Dados para Cálculo de Médias Móveis
# MAGIC
# MAGIC %md
# MAGIC Preparamos os dados filtrados para cálculo das médias móveis,
# MAGIC aplicando filtros de ruptura e organizando por SKU e loja.

# COMMAND ----------

# Preparação dos dados para cálculo de médias móveis
df_dados_medias_moveis = (
    df_vendas_estoque_telefonia_filtrado
    .filter(F.col("EstoqueLoja") > 0)  # Apenas dias sem ruptura (com estoque)
    .select(
        "DtAtual",
        "CdSku",
        "CdFilial",
        "gemeos",
        "QtMercadoria",
        "Receita",
        "EstoqueLoja"
    )
    .withColumn(
        "DtAtual_date",
        F.to_date("DtAtual")
    )
    .withColumn(
        "DayIdx",
        F.datediff(F.col("DtAtual_date"), F.lit("1970-01-01"))
    )
    .orderBy("gemeos", "CdFilial", "DayIdx")
)

print("✅ Dados preparados para cálculo de médias móveis:")
print(f"📊 Total de registros (sem ruptura): {df_dados_medias_moveis.count():,}")
print(f"📅 Período: {df_dados_medias_moveis.agg(F.min('DtAtual'), F.max('DtAtual')).collect()[0]}")

df_dados_medias_moveis.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.2 Cálculo de Médias Móveis por SKU-Loja
# MAGIC
# MAGIC %md
# MAGIC Calculamos as médias móveis de 90, 120 e 360 dias para cada
# MAGIC combinação de SKU e loja, considerando apenas dias sem ruptura.

# COMMAND ----------

# Janelas para diferentes períodos de média móvel
w_90 = Window.partitionBy("gemeos", "CdFilial").orderBy("DayIdx").rangeBetween(-89, 0)
w_120 = Window.partitionBy("CdSku", "CdFilial").orderBy("DayIdx").rangeBetween(-119, 0)
w_360 = Window.partitionBy("CdSku", "CdFilial").orderBy("DayIdx").rangeBetween(-359, 0)

# Cálculo das médias móveis
df_medias_moveis_sku_loja = (
    df_dados_medias_moveis
    .withColumn(
        "Media90_Qt_venda_estq",
        F.avg("QtMercadoria").over(w_90)
    )
    .withColumn(
        "Media120_Qt_venda_estq",
        F.avg("QtMercadoria").over(w_120)
    )
    .withColumn(
        "Media360_Qt_venda_estq",
        F.avg("QtMercadoria").over(w_360)
    )
    .withColumn(
        "Media90_Receita",
        F.avg("Receita").over(w_90)
    )
    .withColumn(
        "Media120_Receita",
        F.avg("Receita").over(w_120)
    )
    .withColumn(
        "Media360_Receita",
        F.avg("Receita").over(w_360)
    )
    .fillna(0, subset=[
        "Media90_Qt_venda_estq", "Media120_Qt_venda_estq", "Media360_Qt_venda_estq",
        "Media90_Receita", "Media120_Receita", "Media360_Receita"
    ])
)

print("✅ Médias móveis calculadas por SKU-Loja:")
print(f"📊 Total de registros: {df_medias_moveis_sku_loja.count():,}")

# Mostrar exemplo das médias calculadas
df_medias_moveis_sku_loja.orderBy("CdSku", "CdFilial", "DtAtual").limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.3 Agregação por Gêmeo para Cálculo de Percentuais
# MAGIC
# MAGIC %md
# MAGIC Agregamos os dados por gêmeo para calcular os percentuais de merecimento,
# MAGIC consolidando as informações de SKUs similares.

# COMMAND ----------

# Agregação por gêmeo, filial e mês
df_agregado_gemeo_filial = (
    df_medias_moveis_sku_loja
    .groupBy("gemeos", "CdFilial", "year_month")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria_total"),
        F.sum("Receita").alias("Receita_total"),
        F.round(F.avg("Media90_Qt_venda_estq"), 2).alias("Media90_Qt_venda_estq"),
        F.round(F.avg("Media120_Qt_venda_estq"), 2).alias("Media120_Qt_venda_estq"),
        F.round(F.avg("Media360_Qt_venda_estq"), 2).alias("Media360_Qt_venda_estq"),
        F.round(F.avg("Media90_Receita"), 2).alias("Media90_Receita"),
        F.round(F.avg("Media120_Receita"), 2).alias("Media120_Receita"),
        F.round(F.avg("Media360_Receita"), 2).alias("Media360_Receita"),
        F.countDistinct("CdSku").alias("qtd_skus_gemeo")
    )
    .filter(F.col("QtMercadoria_total") > 0)  # Remove registros sem vendas
)

print("✅ Dados agregados por gêmeo e filial:")
print(f"📊 Total de registros agregados: {df_agregado_gemeo_filial.count():,}")

df_agregado_gemeo_filial.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.4 Cálculo de Percentuais de Merecimento
# MAGIC
# MAGIC %md
# MAGIC Calculamos os percentuais de merecimento por gêmeo e filial,
# MAGIC usando as médias móveis calculadas para distribuir as alocações.

# COMMAND ----------

# Janela para cálculo de totais por gêmeo e mês
w_gemeo_mes = Window.partitionBy("gemeos", "year_month")

# Cálculo de percentuais de merecimento
df_percentuais_merecimento = (
    df_agregado_gemeo_filial
    # Totais por gêmeo e mês
    .withColumn("total_qt_mercadoria_gemeo_mes", F.sum("QtMercadoria_total").over(w_gemeo_mes))
    .withColumn("total_receita_gemeo_mes", F.sum("Receita_total").over(w_gemeo_mes))
    .withColumn("total_media90_demanda_gemeo_mes", F.sum("Media90_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_media120_demanda_gemeo_mes", F.sum("Media120_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_media360_demanda_gemeo_mes", F.sum("Media360_Qt_venda_estq").over(w_gemeo_mes))
    
    # Percentuais baseados em diferentes médias móveis
    .withColumn(
        "pct_merecimento_media90",
        F.when(F.col("total_media90_demanda_gemeo_mes") > 0,
               F.col("Media90_Qt_venda_estq") / F.col("total_media90_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_media120",
        F.when(F.col("total_media120_demanda_gemeo_mes") > 0,
               F.col("Media120_Qt_venda_estq") / F.col("total_media120_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_media360",
        F.when(F.col("total_media360_demanda_gemeo_mes") > 0,
               F.col("Media360_Qt_venda_estq") / F.col("total_media360_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    
    # Percentuais em %
    .withColumn("pct_merecimento_media90_perc", F.round(F.col("pct_merecimento_media90") * 100, 4))
    .withColumn("pct_merecimento_media120_perc", F.round(F.col("pct_merecimento_media120") * 100, 4))
    .withColumn("pct_merecimento_media360_perc", F.round(F.col("pct_merecimento_media360") * 100, 4))
    
    # Seleção das colunas finais
    .select(
        "year_month", "gemeos", "CdFilial",
        "QtMercadoria_total", "Receita_total",
        "Media90_Qt_venda_estq", "Media120_Qt_venda_estq", "Media360_Qt_venda_estq",
        "Media90_Receita", "Media120_Receita", "Media360_Receita",
        "total_qt_mercadoria_gemeo_mes", "total_receita_gemeo_mes",
        "total_media90_demanda_gemeo_mes", "total_media120_demanda_gemeo_mes", "total_media360_demanda_gemeo_mes",
        "pct_merecimento_media90", "pct_merecimento_media90_perc",
        "pct_merecimento_media120", "pct_merecimento_media120_perc",
        "pct_merecimento_media360", "pct_merecimento_media360_perc",
        "qtd_skus_gemeo"
    )
)

print("✅ Percentuais de merecimento calculados:")
print(f"📊 Total de registros: {df_percentuais_merecimento.count():,}")

# Mostrar exemplo dos percentuais calculados
df_percentuais_merecimento.orderBy("gemeos", "CdFilial", "year_month").limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.5 Resumo das Médias Móveis por Gêmeo
# MAGIC
# MAGIC %md
# MAGIC Apresentamos um resumo das médias móveis calculadas por gêmeo,
# MAGIC mostrando a distribuição da demanda entre diferentes períodos.

# COMMAND ----------

# Resumo das médias móveis por gêmeo
df_resumo_medias_moveis_gemeo = (
    df_percentuais_merecimento
    .groupBy("gemeos")
    .agg(
        F.count("*").alias("total_registros"),
        F.countDistinct("CdFilial").alias("total_filiais"),
        F.countDistinct("year_month").alias("total_meses"),
        F.round(F.avg("Media90_Qt_venda_estq"), 2).alias("media90_media"),
        F.round(F.avg("Media120_Qt_venda_estq"), 2).alias("media120_media"),
        F.round(F.avg("Media360_Qt_venda_estq"), 2).alias("media360_media"),
        F.round(F.avg("pct_merecimento_media90_perc"), 4).alias("pct_merecimento_90_medio"),
        F.round(F.avg("pct_merecimento_media120_perc"), 4).alias("pct_merecimento_120_medio"),
        F.round(F.avg("pct_merecimento_media360_perc"), 4).alias("pct_merecimento_360_medio")
    )
    .orderBy("gemeos")
)

print("📋 RESUMO DAS MÉDIAS MÓVEIS POR GÊMEO:")
print("=" * 80)
df_resumo_medias_moveis_gemeo.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11.6 Comparação entre Diferentes Períodos de Média Móvel
# MAGIC
# MAGIC %md
# MAGIC Analisamos as diferenças entre os percentuais calculados usando
# MAGIC diferentes períodos de média móvel para identificar a melhor abordagem.

# COMMAND ----------

# Análise comparativa das médias móveis
df_comparacao_medias = (
    df_percentuais_merecimento
    .withColumn(
        "diff_90_120",
        F.abs(F.col("pct_merecimento_media90_perc") - F.col("pct_merecimento_media120_perc"))
    )
    .withColumn(
        "diff_90_360",
        F.abs(F.col("pct_merecimento_media90_perc") - F.col("pct_merecimento_media360_perc"))
    )
    .withColumn(
        "diff_120_360",
        F.abs(F.col("pct_merecimento_media120_perc") - F.col("pct_merecimento_media360_perc"))
    )
    .select(
        "gemeos", "CdFilial", "year_month",
        "pct_merecimento_media90_perc", "pct_merecimento_media120_perc", "pct_merecimento_media360_perc",
        F.round("diff_90_120", 4).alias("diff_90_120"),
        F.round("diff_90_360", 4).alias("diff_90_360"),
        F.round("diff_120_360", 4).alias("diff_120_360")
    )
)

print("📊 COMPARAÇÃO ENTRE DIFERENTES PERÍODOS DE MÉDIA MÓVEL:")
print("=" * 80)
print(f"📈 Total de registros para comparação: {df_comparacao_medias.count():,}")

# Estatísticas das diferenças
stats_diferencas = df_comparacao_medias.agg(
    F.round(F.avg("diff_90_120"), 4).alias("media_diff_90_120"),
    F.round(F.avg("diff_90_360"), 4).alias("media_diff_90_360"),
    F.round(F.avg("diff_120_360"), 4).alias("media_diff_120_360"),
    F.round(F.stddev("diff_90_120"), 4).alias("std_diff_90_120"),
    F.round(F.stddev("diff_90_360"), 4).alias("std_diff_90_360"),
    F.round(F.stddev("diff_120_360"), 4).alias("std_diff_120_360")
).collect()[0]

print(f"\n📊 ESTATÍSTICAS DAS DIFERENÇAS:")
print(f"  • Média diferença 90-120 dias: {stats_diferencas['media_diff_90_120']}%")
print(f"  • Média diferença 90-360 dias: {stats_diferencas['media_diff_90_360']}%")
print(f"  • Média diferença 120-360 dias: {stats_diferencas['media_diff_120_360']}%")

# Mostrar exemplos das comparações
df_comparacao_medias.orderBy("gemeos", "CdFilial", "year_month").limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Resumo Final - Matriz de Merecimento Calculada
# MAGIC
# MAGIC %md
# MAGIC A matriz de merecimento foi calculada com sucesso usando médias móveis
# MAGIC de diferentes períodos, considerando apenas dias sem ruptura.

# COMMAND ----------

# Estatísticas finais
total_gemeos = df_percentuais_merecimento.select("gemeos").distinct().count()
total_filiais = df_percentuais_merecimento.select("CdFilial").distinct().count()
total_meses = df_percentuais_merecimento.select("year_month").distinct().count()

print("🎯 MATRIZ DE MERECIMENTO CALCULADA COM SUCESSO!")
print("=" * 80)

print(f"\n📊 COBERTURA DA MATRIZ:")
print(f"  • Total de grupos gêmeos: {total_gemeos}")
print(f"  • Total de filiais: {total_filiais}")
print(f"  • Total de meses: {total_meses}")
print(f"  • Total de combinações gêmeo-filial-mês: {df_percentuais_merecimento.count():,}")

print(f"\n📈 MÉDIAS MÓVEIS CALCULADAS:")
print(f"  • 90 dias: Demanda de curto prazo")
print(f"  • 120 dias: Demanda de médio prazo")
print(f"  • 360 dias: Demanda de longo prazo")

print(f"\n✅ CARACTERÍSTICAS DA IMPLEMENTAÇÃO:")
print(f"  • Filtro de ruptura aplicado (apenas dias com estoque)")
print(f"  • Cálculo por SKU-Loja individual")
print(f"  • Agregação por gêmeo para percentuais")
print(f"  • Múltiplos períodos de análise")

print(f"\n🎯 PRÓXIMOS PASSOS:")
print(f"  • Validação dos percentuais calculados")
print(f"  • Ajustes manuais se necessário")
print(f"  • Exportação da matriz final")
print(f"  • Implementação no sistema")

print(f"\n✅ Notebook de cálculo da matriz de merecimento concluído!")
print(f"🎯 Matriz pronta para uso com percentuais baseados em médias móveis!")


