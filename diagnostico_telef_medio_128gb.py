# Databricks notebook source
# MAGIC %md
# MAGIC # Diagnóstico: Merecimento 0 para "Telef Medio 128GB"
# MAGIC
# MAGIC Este notebook investiga por que o grupo "Telef Medio 128GB" está tendo merecimento 0.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date

# Inicialização do Spark
spark = SparkSession.builder.appName("diagnostico_telef_medio_128gb").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificação de Dados Base

# COMMAND ----------

# Carrega dados base para telefonia
df_base = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == "DIRETORIA TELEFONIA CELULAR")
    .filter(F.col("DtAtual") >= "2024-01-01")
)

print(f"📊 Total de registros na base: {df_base.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificação do Grupo "Telef Medio 128GB"

# COMMAND ----------

# Carrega mapeamento de gêmeos
try:
    de_para_gemeos = (
        pd.read_csv('dados_analise/ITENS_GEMEOS 2.csv',
                    delimiter=";",
                    encoding='iso-8859-1')
        .drop_duplicates()
    )
    
    # Normalização de nomes de colunas
    de_para_gemeos.columns = (
        de_para_gemeos.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
        .str.strip("_")
    )
    
    print("✅ Mapeamento de gêmeos carregado")
    print(f"📊 Total de registros no mapeamento: {len(de_para_gemeos):,}")
    
    # Verifica se existe o grupo "Telef Medio 128GB"
    grupo_telef_medio = de_para_gemeos[de_para_gemeos['gemeos'] == 'Telef Medio 128GB']
    print(f"🔍 Registros com grupo 'Telef Medio 128GB': {len(grupo_telef_medio):,}")
    
    if len(grupo_telef_medio) > 0:
        print("✅ Grupo encontrado no mapeamento!")
        print(f"📋 SKUs do grupo: {grupo_telef_medio['sku_loja'].tolist()[:10]}...")  # Primeiros 10
    else:
        print("❌ Grupo NÃO encontrado no mapeamento!")
        
except Exception as e:
    print(f"❌ Erro ao carregar mapeamento: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aplicação do Mapeamento e Verificação

# COMMAND ----------

# Aplica mapeamento de gêmeos
df_gemeos_spark = spark.createDataFrame(de_para_gemeos.rename(columns={"sku_loja": "CdSku"})[['CdSku', 'gemeos']])

df_com_grupo = df_base.join(
    df_gemeos_spark,
    on="CdSku",
    how="left"
).withColumn(
    "grupo_de_necessidade",
    F.coalesce(F.col("gemeos"), F.lit("SEM_GN"))
)

print(f"📊 Registros após mapeamento: {df_com_grupo.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Análise do Grupo "Telef Medio 128GB"

# COMMAND ----------

# Filtra apenas o grupo de interesse
df_telef_medio = df_com_grupo.filter(F.col("grupo_de_necessidade") == "Telef Medio 128GB")

print(f"🔍 Registros do grupo 'Telef Medio 128GB': {df_telef_medio.count():,}")

if df_telef_medio.count() > 0:
    print("✅ Grupo tem dados na base!")
    
    # Análise temporal
    print("\n📅 Análise temporal:")
    df_telef_medio.groupBy("DtAtual").agg(
        F.count("*").alias("total_registros"),
        F.sum("QtMercadoria").alias("total_vendas"),
        F.sum("Receita").alias("total_receita")
    ).orderBy("DtAtual").show(20)
    
    # Análise por loja
    print("\n🏪 Análise por loja:")
    df_telef_medio.groupBy("CdFilial").agg(
        F.count("*").alias("total_registros"),
        F.sum("QtMercadoria").alias("total_vendas"),
        F.sum("Receita").alias("total_receita")
    ).orderBy(F.desc("total_vendas")).show(20)
    
    # Análise de ruptura
    print("\n🚫 Análise de ruptura:")
    df_telef_medio.groupBy("FlagRuptura").agg(
        F.count("*").alias("total_registros"),
        F.sum("QtMercadoria").alias("total_vendas")
    ).show()
    
else:
    print("❌ Grupo NÃO tem dados na base!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificação na Data de Cálculo (2025-06-30)

# COMMAND ----------

data_calculo = "2025-06-30"
df_data_calculo = df_telef_medio.filter(F.col("DtAtual") == data_calculo)

print(f"📅 Registros na data de cálculo ({data_calculo}): {df_data_calculo.count():,}")

if df_data_calculo.count() > 0:
    print("✅ Há dados na data de cálculo!")
    
    # Mostra os dados
    df_data_calculo.select(
        "CdSku", "CdFilial", "QtMercadoria", "Receita", "FlagRuptura", "EstoqueLoja"
    ).show(20)
    
else:
    print("❌ NÃO há dados na data de cálculo!")
    print("🔍 Verificando datas próximas...")
    
    # Verifica datas próximas
    df_telef_medio.groupBy("DtAtual").agg(
        F.count("*").alias("total_registros")
    ).orderBy(F.desc("DtAtual")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificação de Médias Móveis

# COMMAND ----------

# Calcula médias móveis para o grupo
df_sem_ruptura = df_telef_medio.filter(F.col("FlagRuptura") == 0)

print(f"📊 Registros sem ruptura: {df_sem_ruptura.count():,}")

if df_sem_ruptura.count() > 0:
    print("✅ Há registros sem ruptura!")
    
    # Calcula médias móveis de 90 dias
    w90 = Window.partitionBy("CdSku", "CdFilial").orderBy("DtAtual").rowsBetween(-89, 0)
    
    df_com_medias = df_sem_ruptura.withColumn(
        "Media90_Qt_venda_sem_ruptura",
        F.avg("QtMercadoria").over(w90)
    )
    
    # Verifica as médias na data de cálculo
    df_medias_calculo = df_com_medias.filter(F.col("DtAtual") == data_calculo)
    
    if df_medias_calculo.count() > 0:
        print("✅ Há médias calculadas na data de cálculo!")
        df_medias_calculo.select(
            "CdSku", "CdFilial", "QtMercadoria", "Media90_Qt_venda_sem_ruptura"
        ).show(20)
    else:
        print("❌ NÃO há médias calculadas na data de cálculo!")
        
else:
    print("❌ NÃO há registros sem ruptura!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificação de Mapeamento CD

# COMMAND ----------

# Carrega mapeamento filial → CD
df_base_cd = spark.table('databox.bcg_comum.supply_base_merecimento_diario_v2')

de_para_filial_cd = (
    df_base_cd
    .select("cdfilial", "cd_primario")
    .distinct()
    .filter(F.col("cdfilial").isNotNull())
    .withColumn(
        "cd_primario",
        F.coalesce(F.col("cd_primario"), F.lit("SEM_CD"))
    )
)

# Verifica mapeamento para as filiais do grupo
filiais_grupo = df_telef_medio.select("CdFilial").distinct()
df_mapeamento_cd = filiais_grupo.join(de_para_filial_cd, filiais_grupo.CdFilial == de_para_filial_cd.cdfilial, "left")

print("🏪 Mapeamento filial → CD para o grupo:")
df_mapeamento_cd.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumo do Diagnóstico

# COMMAND ----------

print("=" * 80)
print("📋 RESUMO DO DIAGNÓSTICO - GRUPO 'Telef Medio 128GB'")
print("=" * 80)

# 1. Dados na base
total_base = df_base.count()
total_grupo = df_telef_medio.count()
print(f"1. Dados na base: {total_base:,} registros")
print(f"   Dados do grupo: {total_grupo:,} registros ({total_grupo/total_base*100:.2f}%)")

# 2. Dados na data de cálculo
total_data_calculo = df_data_calculo.count()
print(f"2. Dados na data de cálculo ({data_calculo}): {total_data_calculo:,} registros")

# 3. Dados sem ruptura
total_sem_ruptura = df_sem_ruptura.count()
print(f"3. Dados sem ruptura: {total_sem_ruptura:,} registros")

# 4. Análise de ruptura
if total_grupo > 0:
    ruptura_stats = df_telef_medio.groupBy("FlagRuptura").agg(
        F.count("*").alias("count")
    ).collect()
    
    ruptura_0 = next((row.count for row in ruptura_stats if row.FlagRuptura == 0), 0)
    ruptura_1 = next((row.count for row in ruptura_stats if row.FlagRuptura == 1), 0)
    
    print(f"4. Análise de ruptura:")
    print(f"   Sem ruptura (FlagRuptura=0): {ruptura_0:,} registros")
    print(f"   Com ruptura (FlagRuptura=1): {ruptura_1:,} registros")
    print(f"   Percentual com ruptura: {ruptura_1/(ruptura_0+ruptura_1)*100:.2f}%")

# 5. Conclusão
print("\n" + "=" * 80)
print("🎯 POSSÍVEIS CAUSAS DO MERECIMENTO 0:")
print("=" * 80)

if total_grupo == 0:
    print("❌ CAUSA 1: Grupo não encontrado no mapeamento de gêmeos")
elif total_data_calculo == 0:
    print("❌ CAUSA 2: Nenhum dado na data de cálculo (2025-06-30)")
elif total_sem_ruptura == 0:
    print("❌ CAUSA 3: Todos os registros têm FlagRuptura=1 (são filtrados)")
    print("   → O grupo tem vendas, mas sempre com ruptura")
else:
    print("✅ Dados disponíveis - verificar cálculo das médias móveis")

print("\n💡 PRÓXIMOS PASSOS:")
print("1. Verificar se o grupo está corretamente mapeado")
print("2. Verificar se há dados na data de cálculo")
print("3. Verificar se há registros sem ruptura")
print("4. Verificar se as médias móveis estão sendo calculadas")
print("5. Verificar se o mapeamento CD está correto")
