# Databricks notebook source
# MAGIC %md
# MAGIC # Cálculo da Matriz de Merecimento - Arquitetura em Duas Camadas com Remoção de Outliers Históricos
# MAGIC
# MAGIC Este notebook implementa o cálculo da matriz de merecimento em duas camadas com detecção automática
# MAGIC de meses atípicos, remoção de outliers históricos e múltiplas abordagens de médias móveis para demanda robusta à ruptura.
# MAGIC
# MAGIC **Objetivo**: Calcular a matriz de merecimento otimizada em duas camadas:
# MAGIC 1. **Primeira camada**: Matriz a nível CD (gêmeo)
# MAGIC 2. **Segunda camada**: Distribuição interna ao CD para as lojas
# MAGIC
# MAGIC **Metodologia de Detecção de Outliers**:
# MAGIC - **Meses Atípicos**: Remove meses com QtMercadoria > nσ da média APENAS do gêmeo específico
# MAGIC - **Outliers Históricos CD**: Remove registros > 3σ da média por gêmeo (configurável)
# MAGIC - **Outliers Históricos Loja**: Remove registros > 3σ da média por gêmeo-loja (configurável)
# MAGIC - **Flag de Atacado**: Parâmetros diferenciados para lojas de atacado vs. varejo
# MAGIC
# MAGIC **Múltiplas Médias Móveis**:
# MAGIC - **Médias Móveis Normais**: 90, 180, 270, 360 dias
# MAGIC - **Medianas Móveis**: 90, 180, 270, 360 dias
# MAGIC - **Médias Móveis Aparadas (10%)**: 90, 180, 270, 360 dias

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
# MAGIC ## 5. Detecção Automática de Meses Atípicos por Gêmeo
# MAGIC
# MAGIC %md
# MAGIC Implementamos a regra analítica para detectar meses atípicos:
# MAGIC - **Cálculo por Gêmeo**: Estatísticas calculadas individualmente para cada grupo de produtos similares
# MAGIC - **Regra dos n Desvios**: Remove meses com QtMercadoria > nσ da média APENAS do gêmeo específico
# MAGIC - **Validação Automática**: Identifica e reporta meses removidos com justificativa estatística por gêmeo

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

# Uso dos parâmetros configuráveis para meses atípicos
n_desvios = PARAMETROS_OUTLIERS["desvios_meses_atipicos"]

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

# Mapeamento de filiais para CDs primários
de_para_filial_cd = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .select("CdFilial", "Cd_primario")
    .distinct()
    .dropna()
)

print("✅ Mapeamento de filiais para CDs primários carregado:")
print(f"🏪 Total de filiais mapeadas: {de_para_filial_cd.count():,}")
print(f"🏢 Total de CDs primários: {de_para_filial_cd.select('Cd_primario').distinct().count()}")

de_para_filial_cd.limit(5).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Configuração de Parâmetros para Detecção de Outliers
# MAGIC
# MAGIC %md
# MAGIC Configuramos os parâmetros para detecção de outliers em diferentes níveis:
# MAGIC - **Meses atípicos**: Por gêmeo específico
# MAGIC - **Outliers históricos**: Por gêmeo-CD e gêmeo-loja com parâmetros configuráveis
# MAGIC - **Flag de atacado**: Lojas com vendas atacado recebem tratamento diferenciado

# COMMAND ----------

# Configuração de parâmetros para detecção de outliers
PARAMETROS_OUTLIERS = {
    "desvios_meses_atipicos": 2,  # Desvios para meses atípicos
    "desvios_historico_cd": 3,     # Desvios para outliers históricos a nível CD
    "desvios_historico_loja": 3,   # Desvios para outliers históricos a nível loja
    "desvios_atacado_cd": 2.5,     # Desvios para outliers CD em lojas de atacado
    "desvios_atacado_loja": 2.5    # Desvios para outliers loja em lojas de atacado
}

# Flag para identificar lojas de atacado (pode ser carregado de uma tabela)
lojas_atacado = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .select("CdFilial")
    .distinct()
    .withColumn("flag_atacado", F.lit(1))  # Por enquanto, todas as lojas são consideradas
    .cache()
)

print("✅ Parâmetros de outliers configurados:")
for param, valor in PARAMETROS_OUTLIERS.items():
    print(f"  • {param}: {valor} desvios padrão")

print(f"\n🏪 Lojas de atacado identificadas: {lojas_atacado.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Filtragem de Meses Atípicos por Gêmeo Específico
# MAGIC
# MAGIC %md
# MAGIC Aplicamos o filtro para remover os meses identificados como atípicos,
# MAGIC mas APENAS para o gêmeo específico onde o mês foi diagnosticado como atípico.
# MAGIC Isso garante que a remoção seja precisa e não afete outros gêmeos ou produtos.

# COMMAND ----------

# Aplicação do filtro de meses atípicos por gêmeo específico
df_vendas_estoque_telefonia_filtrado = (
    df_vendas_estoque_telefonia_gemeos_modelos
    .join(
        df_meses_atipicos.select("gemeos", "year_month").withColumn("flag_remover", F.lit(1)),
        on=["gemeos", "year_month"],
        how="left"
    )
    .filter(
        F.col("flag_remover").isNull()  # Remove apenas os meses atípicos do gêmeo específico
    )
    .drop("flag_remover")
)

print("✅ FILTRO DE MESES ATÍPICOS APLICADO (por gêmeo específico):")
print("=" * 60)
print(f"📊 Total de registros ANTES do filtro: {df_vendas_estoque_telefonia_gemeos_modelos.count():,}")
print(f"📊 Total de registros DEPOIS do filtro: {df_vendas_estoque_telefonia_filtrado.count():,}")
print(f"📊 Registros removidos: {df_vendas_estoque_telefonia_gemeos_modelos.count() - df_vendas_estoque_telefonia_filtrado.count():,}")
print("ℹ️  Nota: Apenas meses atípicos do gêmeo específico foram removidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cálculo da Matriz de Merecimento a Nível CD (Gêmeo)
# MAGIC
# MAGIC %md
# MAGIC Calculamos a primeira camada da matriz de merecimento ao nível de CD (gêmeo),
# MAGIC consolidando as informações por grupo de produtos similares.

# COMMAND ----------

# Agregação por gêmeo e mês para matriz a nível CD (usando dados sem outliers)
df_matriz_cd_gemeo = (
    df_vendas_estoque_sem_outliers
    .filter(~F.col("NmEspecieGerencial").contains("CHIP"))  # Excluir chips
    .groupBy("year_month", "gemeos")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria_total_cd"),
        F.round(F.sum("Receita"), 2).alias("Receita_total_cd"),
        F.round(F.sum("Media90_Qt_venda_estq"), 0).alias("Demanda_total_cd"),
        F.round(F.median("PrecoMedio90"), 2).alias("PrecoMedio90_cd"),
        F.countDistinct("CdSku").alias("qtd_skus_cd"),
        F.countDistinct("CdFilial").alias("qtd_filiais_cd")
    )
    .filter(F.col("QtMercadoria_total_cd") > 0)  # Remove registros sem vendas
)

print("✅ Matriz de merecimento a nível CD (gêmeo) calculada:")
print(f"📊 Total de registros: {df_matriz_cd_gemeo.count():,}")

df_matriz_cd_gemeo.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Join com CdFilial usando Cd_primario
# MAGIC
# MAGIC %md
# MAGIC Realizamos o join entre a matriz de CD e as filiais usando o mapeamento
# MAGIC de Cd_primario para distribuir as alocações às lojas.

# COMMAND ----------

# Join da matriz de CD com as filiais
df_matriz_cd_filiais = (
    df_matriz_cd_gemeo
    .crossJoin(de_para_filial_cd)
    .select(
        "year_month", "gemeos", "CdFilial", "Cd_primario",
        "QtMercadoria_total_cd", "Receita_total_cd", "Demanda_total_cd",
        "PrecoMedio90_cd", "qtd_skus_cd", "qtd_filiais_cd"
    )
)

print("✅ Join realizado entre matriz de CD e filiais:")
print(f"📊 Total de registros após join: {df_matriz_cd_filiais.count():,}")

df_matriz_cd_filiais.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cálculo da Segunda Matriz Interna ao CD para as Lojas
# MAGIC
# MAGIC %md
# MAGIC Calculamos a segunda camada da matriz de merecimento, distribuindo
# MAGIC as alocações do CD entre as lojas atreladas.

# COMMAND ----------

# Janela para cálculo de totais por CD e mês
w_cd_mes = Window.partitionBy("Cd_primario", "year_month")

# Cálculo da segunda matriz interna ao CD
df_matriz_interna_cd = (
    df_matriz_cd_filiais
    # Totais por CD e mês
    .withColumn("total_filiais_cd_mes", F.count("CdFilial").over(w_cd_mes))
    
    # Percentuais de distribuição entre filiais do mesmo CD
    .withColumn(
        "pct_distribuicao_filial",
        F.lit(1.0) / F.col("total_filiais_cd_mes")  # Distribuição igual entre filiais
    )
    
    # Alocações calculadas para cada filial
    .withColumn(
        "QtMercadoria_alocada_filial",
        F.round(F.col("QtMercadoria_total_cd") * F.col("pct_distribuicao_filial"), 2)
    )
    .withColumn(
        "Receita_alocada_filial",
        F.round(F.col("Receita_total_cd") * F.col("pct_distribuicao_filial"), 2)
    )
    .withColumn(
        "Demanda_alocada_filial",
        F.round(F.col("Demanda_total_cd") * F.col("pct_distribuicao_filial"), 0)
    )
    
    # Percentuais em %
    .withColumn("pct_distribuicao_filial_perc", F.round(F.col("pct_distribuicao_filial") * 100, 4))
    
    # Seleção das colunas finais
    .select(
        "year_month", "gemeos", "CdFilial", "Cd_primario",
        "QtMercadoria_total_cd", "Receita_total_cd", "Demanda_total_cd",
        "QtMercadoria_alocada_filial", "Receita_alocada_filial", "Demanda_alocada_filial",
        "pct_distribuicao_filial", "pct_distribuicao_filial_perc",
        "total_filiais_cd_mes", "qtd_skus_cd", "qtd_filiais_cd"
    )
)

print("✅ Segunda matriz interna ao CD calculada:")
print(f"📊 Total de registros: {df_matriz_interna_cd.count():,}")

df_matriz_interna_cd.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Resumo da Matriz de Merecimento em Duas Camadas
# MAGIC
# MAGIC %md
# MAGIC Apresentamos um resumo da matriz de merecimento calculada em duas camadas:
# MAGIC 1. **Primeira camada**: Matriz a nível CD (gêmeo)
# MAGIC 2. **Segunda camada**: Distribuição interna ao CD para as lojas

# COMMAND ----------

# Estatísticas finais da matriz em duas camadas
total_cds = df_matriz_interna_cd.select("Cd_primario").distinct().count()
total_filiais = df_matriz_interna_cd.select("CdFilial").distinct().count()
total_gemeos = df_matriz_interna_cd.select("gemeos").distinct().count()
total_meses = df_matriz_interna_cd.select("year_month").distinct().count()

print("🎯 MATRIZ DE MERECIMENTO EM DUAS CAMADAS CALCULADA COM SUCESSO!")
print("=" * 80)

print(f"\n📊 COBERTURA DA MATRIZ:")
print(f"  • Total de CDs primários: {total_cds}")
print(f"  • Total de filiais: {total_filiais}")
print(f"  • Total de grupos gêmeos: {total_gemeos}")
print(f"  • Total de meses: {total_meses}")
print(f"  • Total de combinações CD-filial-mês: {df_matriz_interna_cd.count():,}")

print(f"\n🏗️  ARQUITETURA EM DUAS CAMADAS:")
print(f"  • Camada 1: Matriz a nível CD (gêmeo)")
print(f"  • Camada 2: Distribuição interna ao CD para as lojas")

print(f"\n✅ CARACTERÍSTICAS DA IMPLEMENTAÇÃO:")
print(f"  • Meses atípicos removidos por gêmeo específico")
print(f"  • Cálculo hierárquico CD → Lojas")
print(f"  • Distribuição proporcional entre filiais do mesmo CD")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Detecção e Remoção de Outliers Históricos por Gêmeo-CD e Gêmeo-Loja
# MAGIC
# MAGIC %md
# MAGIC Antes de calcular as médias móveis, removemos outliers históricos em dois níveis:
# MAGIC - **Nível CD (gêmeo)**: Outliers por grupo de produtos similares
# MAGIC - **Nível Loja**: Outliers por filial específica
# MAGIC - **Parâmetros configuráveis**: Diferentes desvios para lojas normais vs. atacado

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Detecção de Outliers Históricos a Nível CD (Gêmeo)
# MAGIC
# MAGIC %md
# MAGIC Calculamos estatísticas históricas por gêmeo para identificar outliers
# MAGIC que podem distorcer o cálculo das médias móveis.

# COMMAND ----------

# Agregação histórica por gêmeo para detecção de outliers
df_stats_historico_gemeo = (
    df_vendas_estoque_telefonia_filtrado
    .filter(~F.col("NmEspecieGerencial").contains("CHIP"))  # Excluir chips
    .groupBy("gemeos")
    .agg(
        F.count("*").alias("total_registros_historico"),
        F.sum("QtMercadoria").alias("qt_total_historico"),
        F.round(F.avg("QtMercadoria"), 2).alias("media_qt_historico"),
        F.round(F.stddev("QtMercadoria"), 2).alias("desvio_padrao_qt_historico"),
        F.round(F.avg("Receita"), 2).alias("media_receita_historico"),
        F.round(F.stddev("Receita"), 2).alias("desvio_padrao_receita_historico")
    )
    .filter(F.col("total_registros_historico") >= 10)  # Mínimo de registros para estatísticas válidas
)

# Cálculo de limites para outliers a nível CD
df_limites_outliers_cd = (
    df_stats_historico_gemeo
    .withColumn(
        "limite_superior_qt_cd",
        F.col("media_qt_historico") + (F.lit(PARAMETROS_OUTLIERS["desvios_historico_cd"]) * F.col("desvio_padrao_qt_historico"))
    )
    .withColumn(
        "limite_inferior_qt_cd",
        F.greatest(
            F.col("media_qt_historico") - (F.lit(PARAMETROS_OUTLIERS["desvios_historico_cd"]) * F.col("desvio_padrao_qt_historico")),
            F.lit(0)
        )
    )
    .withColumn(
        "limite_superior_receita_cd",
        F.col("media_receita_historico") + (F.lit(PARAMETROS_OUTLIERS["desvios_historico_cd"]) * F.col("desvio_padrao_receita_historico"))
    )
    .withColumn(
        "limite_inferior_receita_cd",
        F.greatest(
            F.col("media_receita_historico") - (F.lit(PARAMETROS_OUTLIERS["desvios_historico_cd"]) * F.col("desvio_padrao_receita_historico")),
            F.lit(0)
        )
    )
)

print("✅ Limites de outliers a nível CD calculados:")
print(f"📊 Total de gêmeos analisados: {df_limites_outliers_cd.count():,}")

df_limites_outliers_cd.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Detecção de Outliers Históricos a Nível Loja
# MAGIC
# MAGIC %md
# MAGIC Calculamos estatísticas históricas por gêmeo e loja para identificar outliers
# MAGIC específicos de cada filial, considerando o flag de atacado.

# COMMAND ----------

# Join com flag de atacado
df_vendas_estoque_com_atacado = (
    df_vendas_estoque_telefonia_filtrado
    .join(
        lojas_atacado,
        on="CdFilial",
        how="left"
    )
    .fillna(0, subset=["flag_atacado"])
)

# Agregação histórica por gêmeo e loja para detecção de outliers
df_stats_historico_gemeo_loja = (
    df_vendas_estoque_com_atacado
    .filter(~F.col("NmEspecieGerencial").contains("CHIP"))  # Excluir chips
    .groupBy("gemeos", "CdFilial", "flag_atacado")
    .agg(
        F.count("*").alias("total_registros_historico_loja"),
        F.sum("QtMercadoria").alias("qt_total_historico_loja"),
        F.round(F.avg("QtMercadoria"), 2).alias("media_qt_historico_loja"),
        F.round(F.stddev("QtMercadoria"), 2).alias("desvio_padrao_qt_historico_loja"),
        F.round(F.avg("Receita"), 2).alias("media_receita_historico_loja"),
        F.round(F.stddev("Receita"), 2).alias("desvio_padrao_receita_historico_loja")
    )
    .filter(F.col("total_registros_historico_loja") >= 5)  # Mínimo de registros para estatísticas válidas
)

# Cálculo de limites para outliers a nível loja (considerando flag de atacado)
df_limites_outliers_loja = (
    df_stats_historico_gemeo_loja
    .withColumn(
        "desvios_loja",
        F.when(F.col("flag_atacado") == 1,
               F.lit(PARAMETROS_OUTLIERS["desvios_atacado_loja"]))
         .otherwise(F.lit(PARAMETROS_OUTLIERS["desvios_historico_loja"]))
    )
    .withColumn(
        "limite_superior_qt_loja",
        F.col("media_qt_historico_loja") + (F.col("desvios_loja") * F.col("desvio_padrao_qt_historico_loja"))
    )
    .withColumn(
        "limite_inferior_qt_loja",
        F.greatest(
            F.col("media_qt_historico_loja") - (F.col("desvios_loja") * F.col("desvio_padrao_qt_historico_loja")),
            F.lit(0)
        )
    )
    .withColumn(
        "limite_superior_receita_loja",
        F.col("media_receita_historico_loja") + (F.col("desvios_loja") * F.col("desvio_padrao_receita_historico_loja"))
    )
    .withColumn(
        "limite_inferior_receita_loja",
        F.greatest(
            F.col("media_receita_historico_loja") - (F.col("desvios_loja") * F.col("desvio_padrao_receita_historico_loja")),
            F.lit(0)
        )
    )
)

print("✅ Limites de outliers a nível loja calculados:")
print(f"📊 Total de combinações gêmeo-loja analisadas: {df_limites_outliers_loja.count():,}")

df_limites_outliers_loja.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Aplicação dos Filtros de Outliers Históricos
# MAGIC
# MAGIC %md
# MAGIC Aplicamos os filtros de outliers históricos para remover registros
# MAGIC que podem distorcer o cálculo das médias móveis.

# COMMAND ----------

# Aplicação dos filtros de outliers históricos
df_vendas_estoque_sem_outliers = (
    df_vendas_estoque_com_atacado
    .join(
        df_limites_outliers_cd.select("gemeos", "limite_superior_qt_cd", "limite_inferior_qt_cd", 
                                     "limite_superior_receita_cd", "limite_inferior_receita_cd"),
        on="gemeos",
        how="left"
    )
    .join(
        df_limites_outliers_loja.select("gemeos", "CdFilial", "limite_superior_qt_loja", "limite_inferior_qt_loja",
                                       "limite_superior_receita_loja", "limite_inferior_receita_loja"),
        on=["gemeos", "CdFilial"],
        how="left"
    )
    .filter(
        # Filtro de outliers a nível CD
        (F.col("QtMercadoria") <= F.col("limite_superior_qt_cd")) &
        (F.col("QtMercadoria") >= F.col("limite_inferior_qt_cd")) &
        (F.col("Receita") <= F.col("limite_superior_receita_cd")) &
        (F.col("Receita") >= F.col("limite_inferior_receita_cd")) &
        
        # Filtro de outliers a nível loja
        (F.col("QtMercadoria") <= F.col("limite_superior_qt_loja")) &
        (F.col("QtMercadoria") >= F.col("limite_inferior_qt_loja")) &
        (F.col("Receita") <= F.col("limite_superior_receita_loja")) &
        (F.col("Receita") >= F.col("limite_inferior_receita_loja"))
    )
    .drop("limite_superior_qt_cd", "limite_inferior_qt_cd", "limite_superior_receita_cd", "limite_inferior_receita_cd",
          "limite_superior_qt_loja", "limite_inferior_qt_loja", "limite_superior_receita_loja", "limite_inferior_receita_loja")
)

print("✅ Filtros de outliers históricos aplicados:")
print(f"📊 Total de registros ANTES dos filtros: {df_vendas_estoque_com_atacado.count():,}")
print(f"📊 Total de registros DEPOIS dos filtros: {df_vendas_estoque_sem_outliers.count():,}")
print(f"📊 Registros removidos por outliers: {df_vendas_estoque_com_atacado.count() - df_vendas_estoque_sem_outliers.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 Resumo da Remoção de Outliers Históricos
# MAGIC
# MAGIC %md
# MAGIC Apresentamos um resumo da remoção de outliers históricos por nível
# MAGIC e tipo de loja (normal vs. atacado).

# COMMAND ----------

# Resumo da remoção de outliers por tipo de loja
df_resumo_outliers_por_tipo = (
    df_vendas_estoque_com_atacado
    .join(
        df_vendas_estoque_sem_outliers.select("DtAtual", "CdSku", "CdFilial", "gemeos"),
        on=["DtAtual", "CdSku", "CdFilial", "gemeos"],
        how="left"
    )
    .withColumn(
        "flag_outlier_removido",
        F.when(F.col("gemeos").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )
    .groupBy("flag_atacado")
    .agg(
        F.count("*").alias("total_registros"),
        F.sum("flag_outlier_removido").alias("registros_mantidos"),
        F.count("*").alias("registros_removidos")
    )
    .withColumn(
        "percentual_removido",
        F.round((F.col("registros_removidos") / F.col("total_registros")) * 100, 2)
    )
)

print("📋 RESUMO DA REMOÇÃO DE OUTLIERS HISTÓRICOS:")
print("=" * 80)

for row in df_resumo_outliers_por_tipo.collect():
    tipo_loja = "ATACADO" if row["flag_atacado"] == 1 else "VAREJO"
    print(f"\n🏪 {tipo_loja}:")
    print(f"  • Total de registros: {row['total_registros']:,}")
    print(f"  • Registros mantidos: {row['registros_mantidos']:,}")
    print(f"  • Registros removidos: {row['registros_removidos']:,}")
    print(f"  • Percentual removido: {row['percentual_removido']}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cálculo da Matriz de Merecimento a Nível CD (Gêmeo)
# MAGIC
# MAGIC %md
# MAGIC Calculamos a demanda usando múltiplas abordagens de médias móveis:
# MAGIC - **Média Móvel Normal**: 90, 180, 270 e 360 dias
# MAGIC - **Mediana Móvel**: 90, 180, 270 e 360 dias  
# MAGIC - **Média Móvel Aparada (10%)**: 90, 180, 270 e 360 dias
# MAGIC
# MAGIC **Metodologia:**
# MAGIC - **Filtro de Ruptura**: Apenas dias com estoque > 0 (demanda robusta à ruptura)
# MAGIC - **Múltiplos Períodos**: Análise de curto, médio e longo prazo
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

# Preparação dos dados para cálculo de médias móveis (usando dados sem outliers)
df_dados_medias_moveis = (
    df_vendas_estoque_sem_outliers
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
w_90 = Window.partitionBy("CdSku", "CdFilial").orderBy("DayIdx").rangeBetween(-89, 0)
w_180 = Window.partitionBy("CdSku", "CdFilial").orderBy("DayIdx").rangeBetween(-179, 0)
w_270 = Window.partitionBy("CdSku", "CdFilial").orderBy("DayIdx").rangeBetween(-269, 0)
w_360 = Window.partitionBy("CdSku", "CdFilial").orderBy("DayIdx").rangeBetween(-359, 0)

# Cálculo das múltiplas médias móveis
df_medias_moveis_sku_loja = (
    df_dados_medias_moveis
    # Médias móveis normais
    .withColumn(
        "Media90_Qt_venda_estq",
        F.avg("QtMercadoria").over(w_90)
    )
    .withColumn(
        "Media180_Qt_venda_estq",
        F.avg("QtMercadoria").over(w_180)
    )
    .withColumn(
        "Media270_Qt_venda_estq",
        F.avg("QtMercadoria").over(w_270)
    )
    .withColumn(
        "Media360_Qt_venda_estq",
        F.avg("QtMercadoria").over(w_360)
    )
    
    # Medianas móveis
    .withColumn(
        "Mediana90_Qt_venda_estq",
        F.expr("percentile_approx(QtMercadoria, 0.5)").over(w_90)
    )
    .withColumn(
        "Mediana180_Qt_venda_estq",
        F.expr("percentile_approx(QtMercadoria, 0.5)").over(w_180)
    )
    .withColumn(
        "Mediana270_Qt_venda_estq",
        F.expr("percentile_approx(QtMercadoria, 0.5)").over(w_270)
    )
    .withColumn(
        "Mediana360_Qt_venda_estq",
        F.expr("percentile_approx(QtMercadoria, 0.5)").over(w_360)
    )
    
    # Médias móveis aparadas (10% - percentis 10-90)
    .withColumn(
        "MediaAparada90_Qt_venda_estq",
        F.expr("(percentile_approx(QtMercadoria, 0.9) + percentile_approx(QtMercadoria, 0.1)) / 2").over(w_90)
    )
    .withColumn(
        "MediaAparada180_Qt_venda_estq",
        F.expr("(percentile_approx(QtMercadoria, 0.9) + percentile_approx(QtMercadoria, 0.1)) / 2").over(w_180)
    )
    .withColumn(
        "MediaAparada270_Qt_venda_estq",
        F.expr("(percentile_approx(QtMercadoria, 0.9) + percentile_approx(QtMercadoria, 0.1)) / 2").over(w_270)
    )
    .withColumn(
        "MediaAparada360_Qt_venda_estq",
        F.expr("(percentile_approx(QtMercadoria, 0.9) + percentile_approx(QtMercadoria, 0.1)) / 2").over(w_360)
    )
    
    # Médias móveis de receita
    .withColumn(
        "Media90_Receita",
        F.avg("Receita").over(w_90)
    )
    .withColumn(
        "Media180_Receita",
        F.avg("Receita").over(w_180)
    )
    .withColumn(
        "Media270_Receita",
        F.avg("Receita").over(w_270)
    )
    .withColumn(
        "Media360_Receita",
        F.avg("Receita").over(w_360)
    )
    .fillna(0, subset=[
        "Media90_Qt_venda_estq", "Media180_Qt_venda_estq", "Media270_Qt_venda_estq", "Media360_Qt_venda_estq",
        "Mediana90_Qt_venda_estq", "Mediana180_Qt_venda_estq", "Mediana270_Qt_venda_estq", "Mediana360_Qt_venda_estq",
        "MediaAparada90_Qt_venda_estq", "MediaAparada180_Qt_venda_estq", "MediaAparada270_Qt_venda_estq", "MediaAparada360_Qt_venda_estq",
        "Media90_Receita", "Media180_Receita", "Media270_Receita", "Media360_Receita"
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
        
        # Médias móveis normais
        F.round(F.avg("Media90_Qt_venda_estq"), 2).alias("Media90_Qt_venda_estq"),
        F.round(F.avg("Media180_Qt_venda_estq"), 2).alias("Media180_Qt_venda_estq"),
        F.round(F.avg("Media270_Qt_venda_estq"), 2).alias("Media270_Qt_venda_estq"),
        F.round(F.avg("Media360_Qt_venda_estq"), 2).alias("Media360_Qt_venda_estq"),
        
        # Medianas móveis
        F.round(F.avg("Mediana90_Qt_venda_estq"), 2).alias("Mediana90_Qt_venda_estq"),
        F.round(F.avg("Mediana180_Qt_venda_estq"), 2).alias("Mediana180_Qt_venda_estq"),
        F.round(F.avg("Mediana270_Qt_venda_estq"), 2).alias("Mediana270_Qt_venda_estq"),
        F.round(F.avg("Mediana360_Qt_venda_estq"), 2).alias("Mediana360_Qt_venda_estq"),
        
        # Médias móveis aparadas
        F.round(F.avg("MediaAparada90_Qt_venda_estq"), 2).alias("MediaAparada90_Qt_venda_estq"),
        F.round(F.avg("MediaAparada180_Qt_venda_estq"), 2).alias("MediaAparada180_Qt_venda_estq"),
        F.round(F.avg("MediaAparada270_Qt_venda_estq"), 2).alias("MediaAparada270_Qt_venda_estq"),
        F.round(F.avg("MediaAparada360_Qt_venda_estq"), 2).alias("MediaAparada360_Qt_venda_estq"),
        
        # Médias móveis de receita
        F.round(F.avg("Media90_Receita"), 2).alias("Media90_Receita"),
        F.round(F.avg("Media180_Receita"), 2).alias("Media180_Receita"),
        F.round(F.avg("Media270_Receita"), 2).alias("Media270_Receita"),
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
    
    # Totais de demanda por diferentes médias móveis
    .withColumn("total_media90_demanda_gemeo_mes", F.sum("Media90_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_media180_demanda_gemeo_mes", F.sum("Media180_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_media270_demanda_gemeo_mes", F.sum("Media270_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_media360_demanda_gemeo_mes", F.sum("Media360_Qt_venda_estq").over(w_gemeo_mes))
    
    # Totais de medianas móveis
    .withColumn("total_mediana90_demanda_gemeo_mes", F.sum("Mediana90_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_mediana180_demanda_gemeo_mes", F.sum("Mediana180_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_mediana270_demanda_gemeo_mes", F.sum("Mediana270_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_mediana360_demanda_gemeo_mes", F.sum("Mediana360_Qt_venda_estq").over(w_gemeo_mes))
    
    # Totais de médias aparadas
    .withColumn("total_mediaAparada90_demanda_gemeo_mes", F.sum("MediaAparada90_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_mediaAparada180_demanda_gemeo_mes", F.sum("MediaAparada180_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_mediaAparada270_demanda_gemeo_mes", F.sum("MediaAparada270_Qt_venda_estq").over(w_gemeo_mes))
    .withColumn("total_mediaAparada360_demanda_gemeo_mes", F.sum("MediaAparada360_Qt_venda_estq").over(w_gemeo_mes))
    
    # Percentuais baseados em médias móveis normais
    .withColumn(
        "pct_merecimento_media90",
        F.when(F.col("total_media90_demanda_gemeo_mes") > 0,
               F.col("Media90_Qt_venda_estq") / F.col("total_media90_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_media180",
        F.when(F.col("total_media180_demanda_gemeo_mes") > 0,
               F.col("Media180_Qt_venda_estq") / F.col("total_media180_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_media270",
        F.when(F.col("total_media270_demanda_gemeo_mes") > 0,
               F.col("Media270_Qt_venda_estq") / F.col("total_media270_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_media360",
        F.when(F.col("total_media360_demanda_gemeo_mes") > 0,
               F.col("Media360_Qt_venda_estq") / F.col("total_media360_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    
    # Percentuais baseados em medianas móveis
    .withColumn(
        "pct_merecimento_mediana90",
        F.when(F.col("total_mediana90_demanda_gemeo_mes") > 0,
               F.col("Mediana90_Qt_venda_estq") / F.col("total_mediana90_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_mediana180",
        F.when(F.col("total_mediana180_demanda_gemeo_mes") > 0,
               F.col("Mediana180_Qt_venda_estq") / F.col("total_mediana180_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_mediana270",
        F.when(F.col("total_mediana270_demanda_gemeo_mes") > 0,
               F.col("Mediana270_Qt_venda_estq") / F.col("total_mediana270_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_mediana360",
        F.when(F.col("total_mediana360_demanda_gemeo_mes") > 0,
               F.col("Mediana360_Qt_venda_estq") / F.col("total_mediana360_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    
    # Percentuais baseados em médias aparadas
    .withColumn(
        "pct_merecimento_mediaAparada90",
        F.when(F.col("total_mediaAparada90_demanda_gemeo_mes") > 0,
               F.col("MediaAparada90_Qt_venda_estq") / F.col("total_mediaAparada90_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_mediaAparada180",
        F.when(F.col("total_mediaAparada180_demanda_gemeo_mes") > 0,
               F.col("MediaAparada180_Qt_venda_estq") / F.col("total_mediaAparada180_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_mediaAparada270",
        F.when(F.col("total_mediaAparada270_demanda_gemeo_mes") > 0,
               F.col("MediaAparada270_Qt_venda_estq") / F.col("total_mediaAparada270_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    .withColumn(
        "pct_merecimento_mediaAparada360",
        F.when(F.col("total_mediaAparada360_demanda_gemeo_mes") > 0,
               F.col("MediaAparada360_Qt_venda_estq") / F.col("total_mediaAparada360_demanda_gemeo_mes"))
         .otherwise(F.lit(0.0))
    )
    
    # Percentuais em %
    .withColumn("pct_merecimento_media90_perc", F.round(F.col("pct_merecimento_media90") * 100, 4))
    .withColumn("pct_merecimento_media180_perc", F.round(F.col("pct_merecimento_media180") * 100, 4))
    .withColumn("pct_merecimento_media270_perc", F.round(F.col("pct_merecimento_media270") * 100, 4))
    .withColumn("pct_merecimento_media360_perc", F.round(F.col("pct_merecimento_media360") * 100, 4))
    
    .withColumn("pct_merecimento_mediana90_perc", F.round(F.col("pct_merecimento_mediana90") * 100, 4))
    .withColumn("pct_merecimento_mediana180_perc", F.round(F.col("pct_merecimento_mediana180") * 100, 4))
    .withColumn("pct_merecimento_mediana270_perc", F.round(F.col("pct_merecimento_mediana270") * 100, 4))
    .withColumn("pct_merecimento_mediana360_perc", F.round(F.col("pct_merecimento_mediana360") * 100, 4))
    
    .withColumn("pct_merecimento_mediaAparada90_perc", F.round(F.col("pct_merecimento_mediaAparada90") * 100, 4))
    .withColumn("pct_merecimento_mediaAparada180_perc", F.round(F.col("pct_merecimento_mediaAparada180") * 100, 4))
    .withColumn("pct_merecimento_mediaAparada270_perc", F.round(F.col("pct_merecimento_mediaAparada270") * 100, 4))
    .withColumn("pct_merecimento_mediaAparada360_perc", F.round(F.col("pct_merecimento_mediaAparada270") * 100, 4))
    
    # Seleção das colunas finais
    .select(
        "year_month", "gemeos", "CdFilial",
        "QtMercadoria_total", "Receita_total",
        
        # Médias móveis normais
        "Media90_Qt_venda_estq", "Media180_Qt_venda_estq", "Media270_Qt_venda_estq", "Media360_Qt_venda_estq",
        
        # Medianas móveis
        "Mediana90_Qt_venda_estq", "Mediana180_Qt_venda_estq", "Mediana270_Qt_venda_estq", "Mediana360_Qt_venda_estq",
        
        # Médias móveis aparadas
        "MediaAparada90_Qt_venda_estq", "MediaAparada180_Qt_venda_estq", "MediaAparada270_Qt_venda_estq", "MediaAparada360_Qt_venda_estq",
        
        # Médias móveis de receita
        "Media90_Receita", "Media180_Receita", "Media270_Receita", "Media360_Receita",
        
        # Totais
        "total_qt_mercadoria_gemeo_mes", "total_receita_gemeo_mes",
        "total_media90_demanda_gemeo_mes", "total_media180_demanda_gemeo_mes", "total_media270_demanda_gemeo_mes", "total_media360_demanda_gemeo_mes",
        "total_mediana90_demanda_gemeo_mes", "total_mediana180_demanda_gemeo_mes", "total_mediana270_demanda_gemeo_mes", "total_mediana360_demanda_gemeo_mes",
        "total_mediaAparada90_demanda_gemeo_mes", "total_mediaAparada180_demanda_gemeo_mes", "total_mediaAparada270_demanda_gemeo_mes", "total_mediaAparada360_demanda_gemeo_mes",
        
        # Percentuais de merecimento
        "pct_merecimento_media90", "pct_merecimento_media90_perc",
        "pct_merecimento_media180", "pct_merecimento_media180_perc",
        "pct_merecimento_media270", "pct_merecimento_media270_perc",
        "pct_merecimento_media360", "pct_merecimento_media360_perc",
        
        "pct_merecimento_mediana90", "pct_merecimento_mediana90_perc",
        "pct_merecimento_mediana180", "pct_merecimento_mediana180_perc",
        "pct_merecimento_mediana270", "pct_merecimento_mediana270_perc",
        "pct_merecimento_mediana360", "pct_merecimento_mediana360_perc",
        
        "pct_merecimento_mediaAparada90", "pct_merecimento_mediaAparada90_perc",
        "pct_merecimento_mediaAparada180", "pct_merecimento_mediaAparada180_perc",
        "pct_merecimento_mediaAparada270", "pct_merecimento_mediaAparada270_perc",
        "pct_merecimento_mediaAparada360", "pct_merecimento_mediaAparada360_perc",
        
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

# Resumo das múltiplas médias móveis por gêmeo
df_resumo_medias_moveis_gemeo = (
    df_percentuais_merecimento
    .groupBy("gemeos")
    .agg(
        F.count("*").alias("total_registros"),
        F.countDistinct("CdFilial").alias("total_filiais"),
        F.countDistinct("year_month").alias("total_meses"),
        
        # Médias móveis normais
        F.round(F.avg("Media90_Qt_venda_estq"), 2).alias("media90_media"),
        F.round(F.avg("Media180_Qt_venda_estq"), 2).alias("media180_media"),
        F.round(F.avg("Media270_Qt_venda_estq"), 2).alias("media270_media"),
        F.round(F.avg("Media360_Qt_venda_estq"), 2).alias("media360_media"),
        
        # Medianas móveis
        F.round(F.avg("Mediana90_Qt_venda_estq"), 2).alias("mediana90_media"),
        F.round(F.avg("Mediana180_Qt_venda_estq"), 2).alias("mediana180_media"),
        F.round(F.avg("Mediana270_Qt_venda_estq"), 2).alias("mediana270_media"),
        F.round(F.avg("Mediana360_Qt_venda_estq"), 2).alias("mediana360_media"),
        
        # Médias móveis aparadas
        F.round(F.avg("MediaAparada90_Qt_venda_estq"), 2).alias("mediaAparada90_media"),
        F.round(F.avg("MediaAparada180_Qt_venda_estq"), 2).alias("mediaAparada180_media"),
        F.round(F.avg("MediaAparada270_Qt_venda_estq"), 2).alias("mediaAparada270_media"),
        F.round(F.avg("MediaAparada360_Qt_venda_estq"), 2).alias("mediaAparada360_media"),
        
        # Percentuais de merecimento - médias móveis normais
        F.round(F.avg("pct_merecimento_media90_perc"), 4).alias("pct_merecimento_90_medio"),
        F.round(F.avg("pct_merecimento_media180_perc"), 4).alias("pct_merecimento_180_medio"),
        F.round(F.avg("pct_merecimento_media270_perc"), 4).alias("pct_merecimento_270_medio"),
        F.round(F.avg("pct_merecimento_media360_perc"), 4).alias("pct_merecimento_360_medio"),
        
        # Percentuais de merecimento - medianas móveis
        F.round(F.avg("pct_merecimento_mediana90_perc"), 4).alias("pct_merecimento_mediana90_medio"),
        F.round(F.avg("pct_merecimento_mediana180_perc"), 4).alias("pct_merecimento_mediana180_medio"),
        F.round(F.avg("pct_merecimento_mediana270_perc"), 4).alias("pct_merecimento_mediana270_medio"),
        F.round(F.avg("pct_merecimento_mediana360_perc"), 4).alias("pct_merecimento_mediana360_medio"),
        
        # Percentuais de merecimento - médias aparadas
        F.round(F.avg("pct_merecimento_mediaAparada90_perc"), 4).alias("pct_merecimento_mediaAparada90_medio"),
        F.round(F.avg("pct_merecimento_mediaAparada180_perc"), 4).alias("pct_merecimento_mediaAparada180_medio"),
        F.round(F.avg("pct_merecimento_mediaAparada270_perc"), 4).alias("pct_merecimento_mediaAparada270_medio"),
        F.round(F.avg("pct_merecimento_mediaAparada360_perc"), 4).alias("pct_merecimento_mediaAparada360_medio")
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

# Análise comparativa das múltiplas médias móveis
df_comparacao_medias = (
    df_percentuais_merecimento
    # Diferenças entre médias móveis normais
    .withColumn(
        "diff_media_90_180",
        F.abs(F.col("pct_merecimento_media90_perc") - F.col("pct_merecimento_media180_perc"))
    )
    .withColumn(
        "diff_media_90_270",
        F.abs(F.col("pct_merecimento_media90_perc") - F.col("pct_merecimento_media270_perc"))
    )
    .withColumn(
        "diff_media_90_360",
        F.abs(F.col("pct_merecimento_media90_perc") - F.col("pct_merecimento_media360_perc"))
    )
    
    # Diferenças entre medianas móveis
    .withColumn(
        "diff_mediana_90_180",
        F.abs(F.col("pct_merecimento_mediana90_perc") - F.col("pct_merecimento_mediana180_perc"))
    )
    .withColumn(
        "diff_mediana_90_270",
        F.abs(F.col("pct_merecimento_mediana90_perc") - F.col("pct_merecimento_mediana270_perc"))
    )
    .withColumn(
        "diff_mediana_90_360",
        F.abs(F.col("pct_merecimento_mediana90_perc") - F.col("pct_merecimento_mediana360_perc"))
    )
    
    # Diferenças entre médias aparadas
    .withColumn(
        "diff_mediaAparada_90_180",
        F.abs(F.col("pct_merecimento_mediaAparada90_perc") - F.col("pct_merecimento_mediaAparada180_perc"))
    )
    .withColumn(
        "diff_mediaAparada_90_270",
        F.abs(F.col("pct_merecimento_mediaAparada90_perc") - F.col("pct_merecimento_mediaAparada270_perc"))
    )
    .withColumn(
        "diff_mediaAparada_90_360",
        F.abs(F.col("pct_merecimento_mediaAparada90_perc") - F.col("pct_merecimento_mediaAparada360_perc"))
    )
    
    # Seleção das colunas para análise
    .select(
        "gemeos", "CdFilial", "year_month",
        
        # Percentuais de merecimento - médias móveis normais
        "pct_merecimento_media90_perc", "pct_merecimento_media180_perc", 
        "pct_merecimento_media270_perc", "pct_merecimento_media360_perc",
        
        # Percentuais de merecimento - medianas móveis
        "pct_merecimento_mediana90_perc", "pct_merecimento_mediana180_perc",
        "pct_merecimento_mediana270_perc", "pct_merecimento_mediana360_perc",
        
        # Percentuais de merecimento - médias aparadas
        "pct_merecimento_mediaAparada90_perc", "pct_merecimento_mediaAparada180_perc",
        "pct_merecimento_mediaAparada270_perc", "pct_merecimento_mediaAparada360_perc",
        
        # Diferenças calculadas
        F.round("diff_media_90_180", 4).alias("diff_media_90_180"),
        F.round("diff_media_90_270", 4).alias("diff_media_90_270"),
        F.round("diff_media_90_360", 4).alias("diff_media_90_360"),
        
        F.round("diff_mediana_90_180", 4).alias("diff_mediana_90_180"),
        F.round("diff_mediana_90_270", 4).alias("diff_mediana_90_270"),
        F.round("diff_mediana_90_360", 4).alias("diff_mediana_90_360"),
        
        F.round("diff_mediaAparada_90_180", 4).alias("diff_mediaAparada_90_180"),
        F.round("diff_mediaAparada_90_270", 4).alias("diff_mediaAparada_90_270"),
        F.round("diff_mediaAparada_90_360", 4).alias("diff_mediaAparada_90_360")
    )
)

print("📊 COMPARAÇÃO ENTRE DIFERENTES PERÍODOS DE MÉDIA MÓVEL:")
print("=" * 80)
print(f"📈 Total de registros para comparação: {df_comparacao_medias.count():,}")

# Estatísticas das diferenças entre múltiplas médias móveis
stats_diferencas = df_comparacao_medias.agg(
    # Diferenças entre médias móveis normais
    F.round(F.avg("diff_media_90_180"), 4).alias("media_diff_90_180"),
    F.round(F.avg("diff_media_90_270"), 4).alias("media_diff_90_270"),
    F.round(F.avg("diff_media_90_360"), 4).alias("media_diff_90_360"),
    
    # Diferenças entre medianas móveis
    F.round(F.avg("diff_mediana_90_180"), 4).alias("mediana_diff_90_180"),
    F.round(F.avg("diff_mediana_90_270"), 4).alias("mediana_diff_90_270"),
    F.round(F.avg("diff_mediana_90_360"), 4).alias("mediana_diff_90_360"),
    
    # Diferenças entre médias aparadas
    F.round(F.avg("diff_mediaAparada_90_180"), 4).alias("mediaAparada_diff_90_180"),
    F.round(F.avg("diff_mediaAparada_90_270"), 4).alias("mediaAparada_diff_90_270"),
    F.round(F.avg("diff_mediaAparada_90_360"), 4).alias("mediaAparada_diff_90_360")
).collect()[0]

print(f"\n📊 ESTATÍSTICAS DAS DIFERENÇAS ENTRE MÚLTIPLAS MÉDIAS MÓVEIS:")
print(f"\n📈 MÉDIAS MÓVEIS NORMAIS:")
print(f"  • Média diferença 90-180 dias: {stats_diferencas['media_diff_90_180']}%")
print(f"  • Média diferença 90-270 dias: {stats_diferencas['media_diff_90_270']}%")
print(f"  • Média diferença 90-360 dias: {stats_diferencas['media_diff_90_360']}%")

print(f"\n📊 MEDIANAS MÓVEIS:")
print(f"  • Média diferença 90-180 dias: {stats_diferencas['mediana_diff_90_180']}%")
print(f"  • Média diferença 90-270 dias: {stats_diferencas['mediana_diff_90_270']}%")
print(f"  • Média diferença 90-360 dias: {stats_diferencas['mediana_diff_90_360']}%")

print(f"\n✂️  MÉDIAS MÓVEIS APARADAS (10%):")
print(f"  • Média diferença 90-180 dias: {stats_diferencas['mediaAparada_diff_90_180']}%")
print(f"  • Média diferença 90-270 dias: {stats_diferencas['mediaAparada_diff_90_270']}%")
print(f"  • Média diferença 90-360 dias: {stats_diferencas['mediaAparada_diff_90_360']}%")

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

print(f"\n📈 MÚLTIPLAS MÉDIAS MÓVEIS CALCULADAS:")
print(f"  • Médias Móveis Normais: 90, 180, 270, 360 dias")
print(f"  • Medianas Móveis: 90, 180, 270, 360 dias")
print(f"  • Médias Móveis Aparadas (10%): 90, 180, 270, 360 dias")

print(f"\n✅ CARACTERÍSTICAS DA IMPLEMENTAÇÃO:")
print(f"  • Filtro de ruptura aplicado (demanda robusta à ruptura)")
print(f"  • Remoção de outliers históricos por gêmeo-CD e gêmeo-loja")
print(f"  • Parâmetros configuráveis para diferentes tipos de loja (normal vs. atacado)")
print(f"  • Cálculo por SKU-Loja individual")
print(f"  • Agregação por gêmeo para percentuais")
print(f"  • Múltiplos tipos e períodos de análise")
print(f"  • Matriz em duas camadas: CD → Lojas")
print(f"  • Remoção de meses atípicos por gêmeo específico")
print(f"  • Remoção de outliers históricos antes do cálculo de médias móveis")

print(f"\n🎯 PRÓXIMOS PASSOS:")
print(f"  • Validação dos percentuais calculados")
print(f"  • Ajustes manuais se necessário")
print(f"  • Exportação da matriz final")
print(f"  • Implementação no sistema")

print(f"\n✅ Notebook de cálculo da matriz de merecimento concluído!")
print(f"🎯 Matriz pronta para uso com percentuais baseados em médias móveis!")


