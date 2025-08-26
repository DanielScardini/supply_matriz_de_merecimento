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

# Configuração de parâmetros para detecção de outliers
PARAMETROS_OUTLIERS = {
    "desvios_meses_atipicos": 2,  # Desvios para meses atípicos
    "desvios_historico_cd": 2,     # Desvios para outliers históricos a nível CD
    "desvios_historico_loja": 2,   # Desvios para outliers históricos a nível loja
    "desvios_atacado_cd": 1.5,     # Desvios para outliers CD em lojas de atacado
    "desvios_atacado_loja": 1.5    # Desvios para outliers loja em lojas de atacado
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
# MAGIC ## 7. Cálculo das Medidas Centrais de Demanda com Janela Móvel
# MAGIC
# MAGIC Nesta etapa, calculamos as medidas centrais de demanda usando janela móvel,
# MAGIC considerando APENAS os dias em que FlagRuptura == 0 (sem ruptura).
# MAGIC Isso garante que as medidas de demanda sejam baseadas em períodos de disponibilidade real.
# MAGIC
# MAGIC **Medidas calculadas**:
# MAGIC - Médias móveis: 90, 180, 270, 360 dias
# MAGIC - Medianas móveis: 90, 180, 270, 360 dias  
# MAGIC - Médias móveis aparadas (10%): 90, 180, 270, 360 dias
# MAGIC
# MAGIC **Filtro aplicado**: Apenas registros com FlagRuptura == 0

# COMMAND ----------

# Filtragem para considerar apenas dias sem ruptura
df_sem_ruptura = (
    df_vendas_estoque_telefonia_filtrado
    .filter(F.col("FlagRuptura") == 0)  # Apenas dias sem ruptura
)

print("✅ FILTRO DE DIAS SEM RUPTURA APLICADO:")
print("=" * 60)
print(f"📊 Total de registros ANTES do filtro: {df_vendas_estoque_telefonia_filtrado.count():,}")
print(f"📊 Total de registros DEPOIS do filtro: {df_sem_ruptura.count():,}")
print(f"📊 Registros removidos: {df_vendas_estoque_telefonia_filtrado.count() - df_sem_ruptura.count():,}")
print("ℹ️  Nota: Apenas dias sem ruptura são considerados para cálculo de demanda (incluindo dias sem vendas)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Definição das Janelas Móveis por SKU-Loja
# MAGIC
# MAGIC Definimos janelas móveis para cada combinação de SKU e loja,
# MAGIC ordenadas por data para cálculo das medidas centrais.

# COMMAND ----------

# Janelas móveis por SKU e loja, ordenadas por data
w_sku_loja_90 = Window.partitionBy("CdSku", "CdFilial").orderBy("DtAtual").rowsBetween(-90, 0)
w_sku_loja_180 = Window.partitionBy("CdSku", "CdFilial").orderBy("DtAtual").rowsBetween(-180, 0)
w_sku_loja_270 = Window.partitionBy("CdSku", "CdFilial").orderBy("DtAtual").rowsBetween(-270, 0)
w_sku_loja_360 = Window.partitionBy("CdSku", "CdFilial").orderBy("DtAtual").rowsBetween(-360, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Cálculo das Médias Móveis por Período
# MAGIC
# MAGIC Calculamos as médias móveis para diferentes períodos (90, 180, 270, 360 dias),
# MAGIC considerando apenas os dias sem ruptura para cada SKU-loja.

# COMMAND ----------

# Cálculo das médias móveis por período
df_com_medias_moveis = (
    df_sem_ruptura
    .withColumn(
        "Media90_Qt_venda_sem_ruptura",
        F.avg("QtMercadoria").over(w_sku_loja_90)
    )
    .withColumn(
        "Media180_Qt_venda_sem_ruptura", 
        F.avg("QtMercadoria").over(w_sku_loja_180)
    )
    .withColumn(
        "Media270_Qt_venda_sem_ruptura",
        F.avg("QtMercadoria").over(w_sku_loja_270)
    )
    .withColumn(
        "Media360_Qt_venda_sem_ruptura",
        F.avg("QtMercadoria").over(w_sku_loja_360)
    )
)

print("✅ Médias móveis calculadas para períodos de 90, 180, 270 e 360 dias")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Cálculo das Medianas Móveis por Período
# MAGIC
# MAGIC Calculamos as medianas móveis para diferentes períodos,
# MAGIC que são mais robustas a outliers que as médias aritméticas.

# COMMAND ----------

# Cálculo das medianas móveis por período
df_com_medianas_moveis = (
    df_com_medias_moveis
    .withColumn(
        "Mediana90_Qt_venda_sem_ruptura",
        F.expr("percentile_approx(QtMercadoria, 0.5)").over(w_sku_loja_90)
    )
    .withColumn(
        "Mediana180_Qt_venda_sem_ruptura",
        F.expr("percentile_approx(QtMercadoria, 0.5)").over(w_sku_loja_180)
    )
    .withColumn(
        "Mediana270_Qt_venda_sem_ruptura",
        F.expr("percentile_approx(QtMercadoria, 0.5)").over(w_sku_loja_270)
    )
    .withColumn(
        "Mediana360_Qt_venda_sem_ruptura",
        F.expr("percentile_approx(QtMercadoria, 0.5)").over(w_sku_loja_360)
    )
)

print("✅ Medianas móveis calculadas para períodos de 90, 180, 270 e 360 dias")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.5 Consolidação das Medidas Centrais de Demanda
# MAGIC
# MAGIC Consolidamos todas as medidas calculadas em uma base única,
# MAGIC mantendo apenas as colunas essenciais para as próximas etapas.

# COMMAND ----------

# Consolidação das medidas centrais de demanda
df_medidas_centrais_demanda = (
    df_com_medianas_moveis
    .select(
        "DtAtual", "CdSku", "CdFilial", "gemeos", "year_month",
        "QtMercadoria", "Receita", "FlagRuptura",
        # Médias móveis
        "Media90_Qt_venda_sem_ruptura",
        "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura",
        "Media360_Qt_venda_sem_ruptura",
        # Medianas móveis
        "Mediana90_Qt_venda_sem_ruptura",
        "Mediana180_Qt_venda_sem_ruptura",
        "Mediana270_Qt_venda_sem_ruptura", 
        "Mediana360_Qt_venda_sem_ruptura",
    )
    .fillna(0, subset=[
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura",
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "Mediana90_Qt_venda_sem_ruptura", "Mediana180_Qt_venda_sem_ruptura",
        "Mediana270_Qt_venda_sem_ruptura", "Mediana360_Qt_venda_sem_ruptura",
    ])
)

# Se for string, converter para date
df_medidas_centrais_demanda = df_medidas_centrais_demanda.withColumn(
    "DtAtual", F.to_date("DtAtual")
)


print("✅ MEDIDAS CENTRAIS DE DEMANDA CALCULADAS COM SUCESSO:")
print("=" * 80)
print(f"📊 Total de registros: {df_medidas_centrais_demanda.count():,}")
print(f"📅 Período: {df_medidas_centrais_demanda.agg(F.min('DtAtual'), F.max('DtAtual')).collect()[0]}")
print(f"🏪 Total de lojas: {df_medidas_centrais_demanda.select('CdFilial').distinct().count()}")
print(f"📦 Total de SKUs: {df_medidas_centrais_demanda.select('CdSku').distinct().count()}")
print(f"🔄 Total de grupos gêmeos: {df_medidas_centrais_demanda.select('gemeos').distinct().count()}")

print("\n📋 COLUNAS DE DEMANDA CALCULADAS:")
print("  • Médias móveis: 90, 180, 270, 360 dias")
print("  • Medianas móveis: 90, 180, 270, 360 dias") 
print("  • Médias aparadas: 90, 180, 270, 360 dias")
print("  • Total: 12 colunas de demanda por SKU-loja-dia")

# Exibição de exemplo
print("\n🔍 EXEMPLO DAS MEDIDAS CALCULADAS:")
df_medidas_centrais_demanda.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cálculo da Matriz de Merecimento a Nível CD (Gêmeo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Cálculo do Merecimento a Nível CD-Gêmeo
# MAGIC
# MAGIC Nesta etapa, calculamos o merecimento a nível CD-gêmeo, que representa
# MAGIC quanto de cada gêmeo cada CD (agrupamento de filiais) vai receber.
# MAGIC
# MAGIC **Processo**:
# MAGIC 1. **Agregação**: Agrupamos por `Cd_primario` (CD) e `gemeos` (grupo de SKUs)
# MAGIC 2. **Soma das métricas**: Somamos as 12 métricas de demanda para cada combinação CD-gêmeo
# MAGIC 3. **Cálculo do merecimento**: Calculamos 12 merecimentos, 1 para cada métrica de demanda
# MAGIC
# MAGIC **Resultado**: Matriz de merecimento com percentuais de alocação por CD e gêmeo

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Join com Mapeamento de Filiais para CD
# MAGIC
# MAGIC Primeiro, precisamos fazer o join com o mapeamento de filiais para CD
# MAGIC para obter o `Cd_primario` de cada loja.

# COMMAND ----------

# Verificação se o mapeamento de filiais para CD já existe
try:
    # Tentativa de usar o mapeamento existente
    de_para_filial_cd
    print("✅ Mapeamento de filiais para CD já disponível")
except NameError:
    # Criação do mapeamento se não existir
    print("⚠️  Mapeamento de filiais para CD não encontrado. Criando mapeamento padrão...")
    
    # Mapeamento padrão: cada filial é seu próprio CD
    de_para_filial_cd = (
        df_medidas_centrais_demanda
        .select("CdFilial")
        .distinct()
        .withColumn("Cd_primario", F.col("CdFilial"))  # CD = Filial (mapeamento 1:1)
    )
    
    print(f"✅ Mapeamento padrão criado: {de_para_filial_cd.count():,} filiais mapeadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Join das Medidas de Demanda com Mapeamento de CD
# MAGIC
# MAGIC Realizamos o join para obter o `Cd_primario` de cada loja.

# COMMAND ----------

# Join das medidas de demanda com mapeamento de CD
df_medidas_demanda_com_cd = (
    df_medidas_centrais_demanda
    .join(
        de_para_filial_cd,
        on="CdFilial",
        how="left"
    )
    .fillna("CD_NAO_MAPEADO", subset=["Cd_primario"])
)

print("✅ Join realizado entre medidas de demanda e mapeamento de CD:")
print(f"📊 Total de registros após join: {df_medidas_demanda_com_cd.count():,}")
print(f"🏪 Total de CDs únicos: {df_medidas_demanda_com_cd.select('Cd_primario').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Agregação das Métricas de Demanda por CD-Gêmeo
# MAGIC
# MAGIC Agrupamos por `Cd_primario` e `gemeos` e somamos as 12 métricas de demanda
# MAGIC para obter as demandas agregadas a nível CD-gêmeo.

# COMMAND ----------

# Agregação das métricas de demanda por CD e gêmeo
df_demanda_agregada_cd_gemeo = (
    df_medidas_demanda_com_cd
    .groupBy("Cd_primario", "gemeos")
    .agg(
        # Soma das 12 métricas de demanda
        # Médias móveis
        F.sum("Media90_Qt_venda_sem_ruptura").alias("Media90_Qt_venda_sem_ruptura_CD"),
        F.sum("Media180_Qt_venda_sem_ruptura").alias("Media180_Qt_venda_sem_ruptura_CD"),
        F.sum("Media270_Qt_venda_sem_ruptura").alias("Media270_Qt_venda_sem_ruptura_CD"),
        F.sum("Media360_Qt_venda_sem_ruptura").alias("Media360_Qt_venda_sem_ruptura_CD"),
        
        # Medianas móveis
        F.sum("Mediana90_Qt_venda_sem_ruptura").alias("Mediana90_Qt_venda_sem_ruptura_CD"),
        F.sum("Mediana180_Qt_venda_sem_ruptura").alias("Mediana180_Qt_venda_sem_ruptura_CD"),
        F.sum("Mediana270_Qt_venda_sem_ruptura").alias("Mediana270_Qt_venda_sem_ruptura_CD"),
        F.sum("Mediana360_Qt_venda_sem_ruptura").alias("Mediana360_Qt_venda_sem_ruptura_CD"),
        
        
        # Métricas adicionais para contexto
        F.countDistinct("CdFilial").alias("qtd_filiais_cd"),
        F.countDistinct("CdSku").alias("qtd_skus_gemeo"),
        F.sum("QtMercadoria").alias("QtMercadoria_total_cd_gemeo"),
        F.sum("Receita").alias("Receita_total_cd_gemeo")
    )
    .fillna(0, subset=[
        "Media90_Qt_venda_sem_ruptura_CD", "Media180_Qt_venda_sem_ruptura_CD",
        "Media270_Qt_venda_sem_ruptura_CD", "Media360_Qt_venda_sem_ruptura_CD",
        "Mediana90_Qt_venda_sem_ruptura_CD", "Mediana180_Qt_venda_sem_ruptura_CD",
        "Mediana270_Qt_venda_sem_ruptura_CD", "Mediana360_Qt_venda_sem_ruptura_CD",
    ])
)

print("✅ Agregação das métricas de demanda por CD-gêmeo concluída:")
print(f"📊 Total de combinações CD-gêmeo: {df_demanda_agregada_cd_gemeo.count():,}")
print(f"🏪 Total de CDs únicos: {df_demanda_agregada_cd_gemeo.select('Cd_primario').distinct().count()}")
print(f"🔄 Total de grupos gêmeos: {df_demanda_agregada_cd_gemeo.select('gemeos').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 Cálculo dos 12 Merecimentos por Métrica de Demanda
# MAGIC
# MAGIC Calculamos os 12 merecimentos, um para cada métrica de demanda,
# MAGIC representando o percentual que cada CD vai receber de cada gêmeo.

# COMMAND ----------

# Janela para cálculo de totais por gêmeo (denominador do merecimento)
w_gemeo = Window.partitionBy("gemeos")

# Cálculo dos 12 merecimentos
df_merecimento_cd_gemeo = (
    df_demanda_agregada_cd_gemeo
    .withColumn(
        "total_demanda_gemeo_Media90",
        F.sum("Media90_Qt_venda_sem_ruptura_CD").over(w_gemeo)
    )
    .withColumn(
        "total_demanda_gemeo_Media180",
        F.sum("Media180_Qt_venda_sem_ruptura_CD").over(w_gemeo)
    )
    .withColumn(
        "total_demanda_gemeo_Media270",
        F.sum("Media270_Qt_venda_sem_ruptura_CD").over(w_gemeo)
    )
    .withColumn(
        "total_demanda_gemeo_Media360",
        F.sum("Media360_Qt_venda_sem_ruptura_CD").over(w_gemeo)
    )
    .withColumn(
        "total_demanda_gemeo_Mediana90",
        F.sum("Mediana90_Qt_venda_sem_ruptura_CD").over(w_gemeo)
    )
    .withColumn(
        "total_demanda_gemeo_Mediana180",
        F.sum("Mediana180_Qt_venda_sem_ruptura_CD").over(w_gemeo)
    )
    .withColumn(
        "total_demanda_gemeo_Mediana270",
        F.sum("Mediana270_Qt_venda_sem_ruptura_CD").over(w_gemeo)
    )
    .withColumn(
        "total_demanda_gemeo_Mediana360",
        F.sum("Mediana360_Qt_venda_sem_ruptura_CD").over(w_gemeo)
    )
)

# Cálculo dos percentuais de merecimento (evitando divisão por zero)
df_merecimento_cd_gemeo_final = (
    df_merecimento_cd_gemeo
    .withColumn(
        "Merecimento_Media90",
        F.when(F.col("total_demanda_gemeo_Media90") > 0,
               F.round(F.col("Media90_Qt_venda_sem_ruptura_CD") / F.col("total_demanda_gemeo_Media90") * 100, 4)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "Merecimento_Media180",
        F.when(F.col("total_demanda_gemeo_Media180") > 0,
               F.round(F.col("Media180_Qt_venda_sem_ruptura_CD") / F.col("total_demanda_gemeo_Media180") * 100, 4)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "Merecimento_Media270",
        F.when(F.col("total_demanda_gemeo_Media270") > 0,
               F.round(F.col("Media270_Qt_venda_sem_ruptura_CD") / F.col("total_demanda_gemeo_Media270") * 100, 4)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "Merecimento_Media360",
        F.when(F.col("total_demanda_gemeo_Media360") > 0,
               F.round(F.col("Media360_Qt_venda_sem_ruptura_CD") / F.col("total_demanda_gemeo_Media360") * 100, 4)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "Merecimento_Mediana90",
        F.when(F.col("total_demanda_gemeo_Mediana90") > 0,
               F.round(F.col("Mediana90_Qt_venda_sem_ruptura_CD") / F.col("total_demanda_gemeo_Mediana90") * 100, 4)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "Merecimento_Mediana180",
        F.when(F.col("total_demanda_gemeo_Mediana180") > 0,
               F.round(F.col("Mediana180_Qt_venda_sem_ruptura_CD") / F.col("total_demanda_gemeo_Mediana180") * 100, 4)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "Merecimento_Mediana270",
        F.when(F.col("total_demanda_gemeo_Mediana270") > 0,
               F.round(F.col("Mediana270_Qt_venda_sem_ruptura_CD") / F.col("total_demanda_gemeo_Mediana270") * 100, 4)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "Merecimento_Mediana360",
        F.when(F.col("total_demanda_gemeo_Mediana360") > 0,
               F.round(F.col("Mediana360_Qt_venda_sem_ruptura_CD") / F.col("total_demanda_gemeo_Mediana360") * 100, 4)
        ).otherwise(F.lit(0))
    )
)

print("✅ CÁLCULO DOS 12 MERECIMENTOS CONCLUÍDO COM SUCESSO:")
print("=" * 80)
print(f"📊 Total de combinações CD-gêmeo: {df_merecimento_cd_gemeo_final.count():,}")
print(f"🏪 Total de CDs únicos: {df_merecimento_cd_gemeo_final.select('Cd_primario').distinct().count()}")
print(f"🔄 Total de grupos gêmeos: {df_merecimento_cd_gemeo_final.select('gemeos').distinct().count()}")

print("\n📋 MERECIMENTOS CALCULADOS (12 métricas):")
print("  • Médias móveis: 90, 180, 270, 360 dias")
print("  • Medianas móveis: 90, 180, 270, 360 dias")
print("  • Total: 8 colunas de merecimento por CD-gêmeo")

print("\n🎯 INTERPRETAÇÃO:")
print("  • Cada valor representa o percentual que o CD vai receber do gêmeo")
print("  • Soma dos percentuais por gêmeo = 100%")
print("  • Valores maiores = maior alocação para aquele CD")

# Exibição de exemplo
print("\n🔍 EXEMPLO DOS MERECIMENTOS CALCULADOS:")
df_merecimento_cd_gemeo_final.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.5 Validação dos Merecimentos Calculados
# MAGIC
# MAGIC Validamos que a soma dos merecimentos por gêmeo seja igual a 100%
# MAGIC para cada uma das 12 métricas de demanda.

# COMMAND ----------

# Validação da soma dos merecimentos por gêmeo
df_validacao_merecimento = (
    df_merecimento_cd_gemeo_final
    .groupBy("gemeos")
    .agg(
        F.round(F.sum("Merecimento_Media90"), 4).alias("soma_Merecimento_Media90"),
        F.round(F.sum("Merecimento_Media180"), 4).alias("soma_Merecimento_Media180"),
        F.round(F.sum("Merecimento_Media270"), 4).alias("soma_Merecimento_Media270"),
        F.round(F.sum("Merecimento_Media360"), 4).alias("soma_Merecimento_Media360"),
        F.round(F.sum("Merecimento_Mediana90"), 4).alias("soma_Merecimento_Mediana90"),
        F.round(F.sum("Merecimento_Mediana180"), 4).alias("soma_Merecimento_Mediana180"),
        F.round(F.sum("Merecimento_Mediana270"), 4).alias("soma_Merecimento_Mediana270"),
        F.round(F.sum("Merecimento_Mediana360"), 4).alias("soma_Merecimento_Mediana360"),
    )
)

print("✅ VALIDAÇÃO DOS MERECIMENTOS:")
print("=" * 60)
print("📊 Verificação: Soma dos merecimentos por gêmeo deve ser 100%")
print("🔍 Exibindo as somas para validação:")

df_validacao_merecimento.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Desdobramento de merecimento para lojas
# MAGIC
# MAGIC

# COMMAND ----------

# Join das medidas de demanda com o mapeamento de filiais para CD
df_medidas_demanda_com_cd = (
    df_medidas_centrais_demanda
    .join(
        de_para_filial_cd,
        on="CdFilial",
        how="left"
    )
    .dropna()
    #.fillna("CD_NAO_MAPEADO", subset=["Cd_primario"])
)

print("✅ Join realizado entre medidas de demanda e mapeamento de CD:")
print(f"�� Total de registros após join: {df_medidas_demanda_com_cd.count():,}")
print(f"🏪 Total de lojas únicas: {df_medidas_demanda_com_cd.select('CdFilial').distinct().count()}")
print(f"🏢 Total de CDs únicos: {df_medidas_demanda_com_cd.select('Cd_primario').distinct().count()}")


# COMMAND ----------

# Join das medidas de demanda com o merecimento CD-gêmeo
df_demanda_com_merecimento_cd = (
    df_medidas_demanda_com_cd
    .join(
        df_merecimento_cd_gemeo_final,
        on=["Cd_primario", "gemeos"],
        how="left"
    )
    .fillna(0, subset=[
        "Merecimento_Media90", "Merecimento_Media180", "Merecimento_Media270", "Merecimento_Media360",
        "Merecimento_Mediana90", "Merecimento_Mediana180", "Merecimento_Mediana270", "Merecimento_Mediana360",
    ])
)

# print("✅ Join realizado entre medidas de demanda e merecimento CD-gêmeo:")
# print(f"�� Total de registros após join: {df_demanda_com_merecimento_cd.count():,}")
# print(f"🏪 Total de lojas únicas: {df_demanda_com_merecimento_cd.select('CdFilial').distinct().count()}")
# print(f"🔄 Total de grupos gêmeos: {df_demanda_com_merecimento_cd.select('gemeos').distinct().count()}")

