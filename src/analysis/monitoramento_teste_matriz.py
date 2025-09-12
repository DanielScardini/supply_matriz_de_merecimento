# Databricks notebook source
# MAGIC %md
# MAGIC # Monitoramento do teste

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e constants

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any

# Inicialização do Spark
spark = SparkSession.builder.appName("calculo_matriz_merecimento_unificado").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

GRUPOS_TESTE = ['Telef pp', 'TV 50 ALTO P', 'TV 55 ALTO P']
print(GRUPOS_TESTE)

data_inicio = "2025-08-15"
inicio_teste = "2025-09-05"

categorias_teste = ['TELAS', 'TELEFONIA']

dict_diretorias = {
  'TELAS': 'TVS',
  'TELEFONIA': 'TELEFONIA CELULAR'
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos merecimentos

# COMMAND ----------

df_merecimento_offline = {}
df_merecimento_online = {}


df_merecimento_offline['TELAS'] = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas_teste1009')
    .select('CdFilial', 'grupo_de_necessidade', 'CdSku',
            F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('merecimento_percentual')
    ).dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade','CdSku',])
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmRegiaoGeografica", "NmPorteLoja").distinct(),
          how="left",
          on="CdFilial")
    .filter(F.col('grupo_de_necessidade').isin(GRUPOS_TESTE))
)

df_merecimento_online['TELAS'] = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas_online_teste0809')
    .select('CdFilial', 'grupo_de_necessidade', 'CdSku',
            F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('merecimento_percentual')
    ).dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade', 'CdSku',])
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmRegiaoGeografica", "NmPorteLoja").distinct(),
          how="left",
          on="CdFilial")
    .filter(F.col('grupo_de_necessidade').isin(GRUPOS_TESTE))
)


df_merecimento_offline['TELEFONIA'] = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_teste1009')
    .select('CdFilial', 'grupo_de_necessidade', 'CdSku',
            F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('merecimento_percentual')
    ).dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade', 'CdSku',])
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmRegiaoGeografica", "NmPorteLoja").distinct(),
          how="left",
          on="CdFilial")       
    .filter(F.col('grupo_de_necessidade').isin(GRUPOS_TESTE))
)

df_merecimento_online['TELEFONIA'] = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_online_teste0809')
    .select('CdFilial', 'grupo_de_necessidade', 'CdSku',
            F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('merecimento_percentual')
    ).dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade', 'CdSku',])
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmRegiaoGeografica", "NmPorteLoja").distinct(),
          how="left",
          on="CdFilial")       
    .filter(F.col('grupo_de_necessidade').isin(GRUPOS_TESTE))
)

df_merecimento_offline['TELAS']#.display()
df_merecimento_offline['TELEFONIA']#.display()
df_merecimento_online['TELAS']#.display()
df_merecimento_online['TELEFONIA']#.display()


produtos_do_teste = {}
produtos_do_teste['TELAS'] = df_merecimento_offline['TELAS'].select("CdSku", "grupo_de_necessidade").distinct()
produtos_do_teste['TELEFONIA'] = df_merecimento_offline['TELEFONIA'].select("CdSku", "grupo_de_necessidade").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de matriz Neogrid

# COMMAND ----------

df_matriz_neogrid = (
    spark.createDataFrame(
        pd.read_csv(
            "/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/(DRP)_MATRIZ_20250902160333.csv",
            delimiter=";",
        )
    )
    .select(
        F.col("CODIGO").cast("int").alias("CdSku"),
        F.regexp_replace(F.col("CODIGO_FILIAL"), ".*_", "").cast("int").alias("CdFilial"),
        F.regexp_replace(F.col("PERCENTUAL_MATRIZ"), ",", ".").cast("float").alias("PercMatrizNeogrid"),
        F.col("CLUSTER").cast("string").alias("is_Cluster"),
        F.col("TIPO_ENTREGA").cast("string").alias("TipoEntrega"),
    )
    .dropDuplicates()
    #.filter(F.col('TIPO_ENTREGA') == 'SL')
    .join(
        spark.table('databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia'),
        how="inner",
        on="CdSku")
)

df_matriz_neogrid_agg = (
  df_matriz_neogrid
  .groupBy('CdFilial', 'grupo_de_necessidade')
  .agg(
    F.round(F.mean('PercMatrizNeogrid'), 3).alias('PercMatrizNeogrid'),
    F.round(F.median('PercMatrizNeogrid'),3).alias('PercMatrizNeogrid_median')
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos estoques

# COMMAND ----------

def load_estoque_loja_data(spark: SparkSession, categoria: str) -> DataFrame:
    """
    Carrega dados de estoque das lojas ativas.
    
    Args:
        spark: Sessão do Spark
        current_year: Ano atual para filtro de partição
        
    Returns:
        DataFrame com dados de estoque das lojas, incluindo:
        - Informações da filial e SKU
        - Dados de estoque e classificação
        - Métricas de DDE e faixas
    """
    return (
        spark.read.table("databox.bcg_comum.supply_base_merecimento_diario_v4")
        .filter(F.col("DtAtual") >= data_inicio)
        .filter(F.col('DsSetor') == dict_diretorias[categoria])
        .join(
            produtos_do_teste[categoria],
            how="left",
            on="CdSku"
        )

        .withColumn(
            "grupo",
            F.when(
                F.col("grupo_de_necessidade").isNotNull(), F.lit("teste")
            )
            .otherwise(F.lit("controle"))
        
            
        )
        .dropDuplicates(["DtAtual", "CdSku", "CdFilial"])
        .withColumn("periodo_analise",
                    F.when(
                        F.col("DtAtual") <= inicio_teste, F.lit('baseline')
                    )
                    .otherwise(F.lit('piloto'))
                    )
        .withColumn("DtAtual", F.to_date(F.col("DtAtual")))
    )

df_estoque_loja = {}

df_estoque_loja['TELAS'] = load_estoque_loja_data(spark, 'TELAS')
df_estoque_loja['TELAS'].cache()
df_estoque_loja['TELAS'].limit(1).display()

df_estoque_loja['TELEFONIA'] = load_estoque_loja_data(spark, 'TELEFONIA')
df_estoque_loja['TELEFONIA'].cache()
df_estoque_loja['TELEFONIA'].limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de DDExRuptura

# COMMAND ----------

df_analise = {}

for categoria in categorias_teste:
    df_analise[categoria] = (
        df_estoque_loja[categoria]
        .groupBy("periodo_analise", "grupo")
        .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio"),
            F.round(100 * F.avg(F.when(F.col("FlagRuptura") == 1, 1).otherwise(0)), 1).alias("PctRupturaBinario"),
            F.round(100 * (F.sum("ReceitaPerdidaRuptura") / F.sum("Receita")), 1).alias("PctRupturaReceita")
        )
        .orderBy(F.desc("grupo"), F.desc("periodo_analise"))
    )

    df_analise[categoria].display()

    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise por região

# COMMAND ----------

df_estoque_loja_porte_regiao = {}

for categoria in categorias_teste:
    df_estoque_loja_porte_regiao[categoria] = (
        df_estoque_loja[categoria]
        .join(
            spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
            .select(
                "CdFilial",
                "NmFilial",
                #"NmPorteLoja",
                "NmUF",
                "NmRegiaoGeografica",
            )
            .distinct(),
            on="CdFilial",
            how="left"
        )
        
    )

df_estoque_loja_porte_regiao['TELAS'].limit(1).display()
df_estoque_loja_porte_regiao['TELEFONIA'].limit(1).display()


# COMMAND ----------

df_analise_regiao = {}

for categoria in categorias_teste:
    df_analise_regiao[categoria] = (
        df_estoque_loja_porte_regiao[categoria]
        .groupBy('grupo', 'periodo_analise', 'NmRegiaoGeografica')
       .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio"),
            F.round(100 * F.avg(F.when(F.col("FlagRuptura") == 1, 1).otherwise(0)), 1).alias("PctRupturaBinario"),
            F.round(100 * (F.sum("ReceitaPerdidaRuptura") / F.sum("Receita")), 1).alias("PctRupturaReceita")
        )
            .orderBy(F.desc("grupo"), F.desc("periodo_analise"))
            .dropna(subset=["NmRegiaoGeografica"])
            .fillna(0.0, subset=["DDE_medio", "PctRupturaReceita"])
    )

    df_analise_regiao[categoria].display()

# COMMAND ----------

from pyspark.sql import functions as F

df_analise_regiao = {}

for categoria in categorias_teste:
    # 1) agrega métricas por grupo, período e região
    df_base = (
        df_estoque_loja_porte_regiao[categoria]
        .groupBy("grupo", "periodo_analise", "NmRegiaoGeografica")
        .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio"),
            F.round(100 * F.avg(F.when(F.col("FlagRuptura") == 1, 1).otherwise(0)), 1).alias("PctRupturaBinario"),
            # calcula % de receita perdida de forma robusta
            F.sum("ReceitaPerdidaRuptura").alias("sum_perda"),
            F.sum("Receita").alias("sum_receita"),
        )
        .withColumn(
            "PctRupturaReceita",
            F.round(100 * F.when(F.col("sum_receita") > 0, F.col("sum_perda") / F.col("sum_receita")).otherwise(0.0), 1)
        )
        .select("grupo","periodo_analise","NmRegiaoGeografica","DDE_medio","PctRupturaBinario","PctRupturaReceita")
        .dropna(subset=["NmRegiaoGeografica"])
    )

    # 2) baseline e piloto via WHEN (sem pivot)
    df_wide = (
        df_base.groupBy("NmRegiaoGeografica", "grupo")
        .agg(
            F.max(F.when(F.col("periodo_analise")=="baseline", F.col("DDE_medio"))).alias("baseline_DDE_medio"),
            F.max(F.when(F.col("periodo_analise")=="piloto",   F.col("DDE_medio"))).alias("piloto_DDE_medio"),

            F.max(F.when(F.col("periodo_analise")=="baseline", F.col("PctRupturaBinario"))).alias("baseline_PctRupturaBinario"),
            F.max(F.when(F.col("periodo_analise")=="piloto",   F.col("PctRupturaBinario"))).alias("piloto_PctRupturaBinario"),

            F.max(F.when(F.col("periodo_analise")=="baseline", F.col("PctRupturaReceita"))).alias("baseline_PctRupturaReceita"),
            F.max(F.when(F.col("periodo_analise")=="piloto",   F.col("PctRupturaReceita"))).alias("piloto_PctRupturaReceita"),
        )
        .fillna(0.0)  # evita nulos se faltar baseline/piloto em alguma combinação
    )

    # 3) deltas piloto - baseline
    df_delta = (
        df_wide
        .withColumn("DDE_delta", F.round(F.col("piloto_DDE_medio") - F.col("baseline_DDE_medio"), 1))
        .withColumn("PctRupturaBinario_delta", F.round(F.col("piloto_PctRupturaBinario") - F.col("baseline_PctRupturaBinario"), 1))
        .withColumn("PctRupturaReceita_delta", F.round(F.col("piloto_PctRupturaReceita") - F.col("baseline_PctRupturaReceita"), 1))
    )

    # 4) separar teste e controle
    df_teste = (
        df_delta.filter(F.col("grupo") == "teste")
        .select(
            "NmRegiaoGeografica",
            F.col("DDE_delta").alias("DDE_delta_teste"),
            F.col("PctRupturaBinario_delta").alias("PctRupturaBinario_delta_teste"),
            F.col("PctRupturaReceita_delta").alias("PctRupturaReceita_delta_teste"),
        )
    )

    df_controle = (
        df_delta.filter(F.col("grupo") == "controle")
        .select(
            "NmRegiaoGeografica",
            F.col("DDE_delta").alias("DDE_delta_controle"),
            F.col("PctRupturaBinario_delta").alias("PctRupturaBinario_delta_controle"),
            F.col("PctRupturaReceita_delta").alias("PctRupturaReceita_delta_controle"),
        )
    )

    # 5) Diff-in-Diff = delta_teste - delta_controle
    df_diff = (
        df_teste.join(df_controle, on="NmRegiaoGeografica", how="inner")
        .withColumn("DDE_diff_in_diff", F.round(F.col("DDE_delta_teste") - F.col("DDE_delta_controle"), 1))
        .withColumn("PctRupturaBinario_diff_in_diff", F.round(F.col("PctRupturaBinario_delta_teste") - F.col("PctRupturaBinario_delta_controle"), 1))
        .withColumn("PctRupturaReceita_diff_in_diff", F.round(F.col("PctRupturaReceita_delta_teste") - F.col("PctRupturaReceita_delta_controle"), 1))
    )

    df_analise_regiao[categoria] = df_diff
    df_diff.display()

# COMMAND ----------

df_wide.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise por porte de loja

# COMMAND ----------

from pyspark.sql import functions as F

df_analise_porte = {}

for categoria in categorias_teste:
    # normaliza periodo_analise para evitar variações (trim/lower)
    df_norm = (
        df_estoque_loja_porte_regiao[categoria]
        .withColumn("periodo_norm", F.lower(F.trim(F.col("periodo_analise"))))
    )

    # 1) agrega métricas por grupo, período e porte
    df_base = (
        df_norm
        .groupBy("grupo", "periodo_norm", "NmPorteLoja")
        .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio"),
            F.round(100 * F.avg(F.when(F.col("FlagRuptura") == 1, 1).otherwise(0)), 1).alias("PctRupturaBinario"),
            F.sum("ReceitaPerdidaRuptura").alias("sum_perda"),
            F.sum("Receita").alias("sum_receita"),
        )
        .withColumn(
            "PctRupturaReceita",
            F.round(100 * F.when(F.col("sum_receita") > 0, F.col("sum_perda")/F.col("sum_receita")).otherwise(0.0), 1)
        )
        .select("grupo","periodo_norm","NmPorteLoja","DDE_medio","PctRupturaBinario","PctRupturaReceita")
        .dropna(subset=["NmPorteLoja"])
    )

    # 2) baseline/piloto via WHEN (sem pivot)
    df_wide = (
        df_base.groupBy("NmPorteLoja", "grupo")
        .agg(
            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("DDE_medio"))).alias("baseline_DDE_medio"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("DDE_medio"))).alias("piloto_DDE_medio"),

            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("PctRupturaBinario"))).alias("baseline_PctRupturaBinario"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("PctRupturaBinario"))).alias("piloto_PctRupturaBinario"),

            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("PctRupturaReceita"))).alias("baseline_PctRupturaReceita"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("PctRupturaReceita"))).alias("piloto_PctRupturaReceita"),
        )
        .fillna(0.0)
    )

    # 3) deltas piloto - baseline
    df_delta = (
        df_wide
        .withColumn("DDE_delta", F.round(F.col("piloto_DDE_medio") - F.col("baseline_DDE_medio"), 1))
        .withColumn("PctRupturaBinario_delta", F.round(F.col("piloto_PctRupturaBinario") - F.col("baseline_PctRupturaBinario"), 1))
        .withColumn("PctRupturaReceita_delta", F.round(F.col("piloto_PctRupturaReceita") - F.col("baseline_PctRupturaReceita"), 1))
    )

    # 4) separar teste e controle com chave única para evitar ambiguidade
    df_teste = (
        df_delta.filter(F.col("grupo") == "teste")
        .select(
            F.col("NmPorteLoja").alias("NmPorteLoja_key"),
            F.col("DDE_delta").alias("DDE_delta_teste"),
            F.col("PctRupturaBinario_delta").alias("PctRupturaBinario_delta_teste"),
            F.col("PctRupturaReceita_delta").alias("PctRupturaReceita_delta_teste"),
        )
    )

    df_controle = (
        df_delta.filter(F.col("grupo") == "controle")
        .select(
            F.col("NmPorteLoja").alias("NmPorteLoja_key"),
            F.col("DDE_delta").alias("DDE_delta_controle"),
            F.col("PctRupturaBinario_delta").alias("PctRupturaBinario_delta_controle"),
            F.col("PctRupturaReceita_delta").alias("PctRupturaReceita_delta_controle"),
        )
    )

    # 5) Diff-in-Diff
    df_diff = (
        df_teste.join(df_controle, on="NmPorteLoja_key", how="inner")
        .withColumnRenamed("NmPorteLoja_key", "NmPorteLoja")
        .withColumn("DDE_diff_in_diff", F.round(F.col("DDE_delta_teste") - F.col("DDE_delta_controle"), 1))
        .withColumn("PctRupturaBinario_diff_in_diff", F.round(F.col("PctRupturaBinario_delta_teste") - F.col("PctRupturaBinario_delta_controle"), 1))
        .withColumn("PctRupturaReceita_diff_in_diff", F.round(F.col("PctRupturaReceita_delta_teste") - F.col("PctRupturaReceita_delta_controle"), 1))
    )

    df_analise_porte[categoria] = df_diff
    df_diff.filter(F.col("NmPorteLoja") != '-').orderBy("NmPorteLoja").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise dos maiores deltas de merecimento

# COMMAND ----------

df_comparacao = {}

for categoria in categorias_teste:
  df_comparacao[categoria] = (
    df_merecimento_offline[categoria]
    .select("CdFilial", "NmFilial", "grupo_de_necessidade", "merecimento_percentual")
    .filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
    .distinct()
    .join(
      df_matriz_neogrid_agg,
      on=["CdFilial", "grupo_de_necessidade"],
      how="inner"
    )
    .withColumn("delta_merecimento", 
                F.col("merecimento_percentual") - F.col("PercMatrizNeogrid_median")
    )
    .withColumn("delta_merecimento_pct", 
                F.round(100*F.col("delta_merecimento")/F.col("PercMatrizNeogrid_median"), 0)
    )
    .withColumn("bucket_delta",
                F.when(F.col("delta_merecimento_pct") > 20, "acima")
                .when(F.col("delta_merecimento_pct") < -20, "abaixo")
                .otherwise("manteve"))
  )
  
  
  df_comparacao[categoria]#.display()
  df_comparacao[categoria].groupBy("bucket_delta").agg(F.count("*").alias("count")).orderBy("bucket_delta")#.display()

  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de maiores deltas

# COMMAND ----------

df_analise_deltas = {}

for categoria in categorias_teste:
    df_analise_deltas[categoria] = (
        df_estoque_loja_porte_regiao[categoria]
        .join(
            df_comparacao[categoria]
            .select("grupo_de_necessidade", "CdFilial", "bucket_delta")
            .distinct(),
            on=["CdFilial", "grupo_de_necessidade"],
            how="inner"
            )
        .groupBy("grupo_de_necessidade", "bucket_delta", "periodo_analise")
        .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio"),
            F.round(100 * F.avg(F.when(F.col("FlagRuptura") == 1, 1).otherwise(0)), 1).alias("PctRupturaBinario"),
            F.round(100 * (F.sum("ReceitaPerdidaRuptura") / F.sum("Receita")), 1).alias("PctRupturaReceita")
        )
        .orderBy('grupo_de_necessidade', 'bucket_delta')
    )

    df_analise_deltas[categoria].display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: análises de acompanhamento

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Consequência dos deltas mais relevantes vs Matriz antiga - DDE ou ruptura 
# MAGIC 2. Buckets de DDE - delta versus categoria
# MAGIC 3. DDE x Ruptura  - delta versus categoria
# MAGIC 4. Proporção prevista vs real - janela desde inicio do teste
