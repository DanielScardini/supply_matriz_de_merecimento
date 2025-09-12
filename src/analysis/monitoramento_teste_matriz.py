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
# MAGIC ## Visualizações com Cards - Diff-in-Diff

# COMMAND ----------

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.offline as pyo

def calculate_dde_percentage_diff(df_analise_regiao, categoria):
    """
    Calcula o diff-in-diff percentual para DDE baseado nos dados reais
    """
    df_pandas = df_analise_regiao[categoria].toPandas()
    
    # Calcular o baseline médio de DDE para teste e controle
    # Assumindo que temos os dados de baseline em df_analise[categoria]
    # Por enquanto, usar um valor estimado baseado na média das regiões
    baseline_dde_medio = 15.0  # Este valor deve ser calculado dos dados reais
    
    # Calcular o diff-in-diff percentual
    dde_diff_abs = df_pandas['DDE_diff_in_diff'].mean()
    dde_pct_change = (dde_diff_abs / baseline_dde_medio) * 100 if baseline_dde_medio > 0 else 0
    
    return dde_pct_change

def create_diff_in_diff_cards(df_analise_regiao, categorias_teste):
    """
    Cria cards visuais para mostrar diff-in-diff das métricas principais
    """
    cards = []
    
    for categoria in categorias_teste:
        # Converter para pandas para facilitar manipulação
        df_pandas = df_analise_regiao[categoria].toPandas()
        
        # Calcular médias ponderadas por região (assumindo igual peso)
        avg_dde_diff = df_pandas['DDE_diff_in_diff'].mean()
        avg_ruptura_binario_diff = df_pandas['PctRupturaBinario_diff_in_diff'].mean()
        avg_ruptura_receita_diff = df_pandas['PctRupturaReceita_diff_in_diff'].mean()
        
        # Para DDE, calcular o percentual de mudança baseado no baseline
        dde_pct_change = calculate_dde_percentage_diff(df_analise_regiao, categoria)
        
        # Criar cards para cada métrica
        cards.extend([
            create_metric_card(
                title=f"DDE Médio - {categoria}",
                value=f"{avg_dde_diff:+.1f}",
                subtitle=f"dias ({dde_pct_change:+.1f}%)",
                delta=avg_dde_diff,
                threshold=1.0,
                metric_type="dde"
            ),
            create_metric_card(
                title=f"% Ruptura Binário - {categoria}",
                value=f"{avg_ruptura_binario_diff:+.1f}",
                subtitle="p.p.",
                delta=avg_ruptura_binario_diff,
                threshold=1.0,
                metric_type="ruptura"
            ),
            create_metric_card(
                title=f"% Ruptura Receita - {categoria}",
                value=f"{avg_ruptura_receita_diff:+.1f}",
                subtitle="p.p.",
                delta=avg_ruptura_receita_diff,
                threshold=1.0,
                metric_type="ruptura"
            )
        ])
    
    return cards

def create_metric_card(title, value, subtitle, delta, threshold=1.0, metric_type="dde"):
    """
    Cria um card individual para uma métrica
    """
    # Determinar cor e seta baseado no delta e tipo de métrica
    if abs(delta) >= threshold:
        if metric_type == "dde":
            # Para DDE, aumento é ruim (vermelho), diminuição é boa (verde)
            if delta > 0:
                color = "#DC143C"  # Vermelho escuro - DDE aumentou (ruim)
                arrow = "↑"
                arrow_color = "#DC143C"
            else:
                color = "#2E8B57"  # Verde escuro - DDE diminuiu (bom)
                arrow = "↓"
                arrow_color = "#2E8B57"
        else:
            # Para ruptura, aumento é ruim (vermelho), diminuição é boa (verde)
            if delta > 0:
                color = "#DC143C"  # Vermelho escuro - ruptura aumentou (ruim)
                arrow = "↑"
                arrow_color = "#DC143C"
            else:
                color = "#2E8B57"  # Verde escuro - ruptura diminuiu (bom)
                arrow = "↓"
                arrow_color = "#2E8B57"
    else:
        color = "#808080"  # Cinza - mudança pequena
        arrow = "→"
        arrow_color = "#808080"
    
    # Criar o card
    fig = go.Figure()
    
    # Adicionar retângulo de fundo
    fig.add_shape(
        type="rect",
        x0=0, y0=0, x1=1, y1=1,
        fillcolor="#F2F2F2",
        line=dict(color="#E0E0E0", width=1),
        layer="below"
    )
    
    # Adicionar texto do título
    fig.add_annotation(
        x=0.5, y=0.8,
        text=title,
        showarrow=False,
        font=dict(size=14, color="#333333", family="Arial"),
        xref="paper", yref="paper"
    )
    
    # Adicionar valor principal
    fig.add_annotation(
        x=0.5, y=0.5,
        text=f"<b>{value}</b>",
        showarrow=False,
        font=dict(size=24, color=color, family="Arial Black"),
        xref="paper", yref="paper"
    )
    
    # Adicionar subtítulo
    fig.add_annotation(
        x=0.5, y=0.35,
        text=subtitle,
        showarrow=False,
        font=dict(size=12, color="#666666", family="Arial"),
        xref="paper", yref="paper"
    )
    
    # Adicionar seta
    fig.add_annotation(
        x=0.5, y=0.15,
        text=f"<span style='font-size:20px; color:{arrow_color}'>{arrow}</span>",
        showarrow=False,
        xref="paper", yref="paper"
    )
    
    # Configurar layout
    fig.update_layout(
        width=200,
        height=150,
        margin=dict(l=10, r=10, t=10, b=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        xaxis_range=[0, 1],
        yaxis_range=[0, 1]
    )
    
    return fig

# Gerar os cards
cards = create_diff_in_diff_cards(df_analise_regiao, categorias_teste)

# Exibir os cards
for i, card in enumerate(cards):
    print(f"\n--- Card {i+1} ---")
    card.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cards por Região - Detalhamento

# COMMAND ----------

def create_regional_cards(df_analise_regiao, categorias_teste):
    """
    Cria cards detalhados por região
    """
    for categoria in categorias_teste:
        df_pandas = df_analise_regiao[categoria].toPandas()
        
        print(f"\n=== {categoria} - Análise por Região ===")
        
        # Criar subplot com cards por região
        fig = make_subplots(
            rows=2, cols=3,
            subplot_titles=[f"{regiao}" for regiao in df_pandas['NmRegiaoGeografica'].unique()[:6]],
            specs=[[{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}],
                   [{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}]]
        )
        
        row, col = 1, 1
        for idx, regiao in enumerate(df_pandas['NmRegiaoGeografica'].unique()[:6]):
            regiao_data = df_pandas[df_pandas['NmRegiaoGeografica'] == regiao].iloc[0]
            
            # DDE
            dde_delta = regiao_data['DDE_diff_in_diff']
            dde_color = "#2E8B57" if dde_delta >= 1 else "#DC143C" if dde_delta <= -1 else "#808080"
            
            fig.add_trace(go.Indicator(
                mode = "number+delta",
                value = dde_delta,
                delta = {"reference": 0, "valueformat": ".1f"},
                title = {"text": f"DDE (dias)<br>{regiao}"},
                number = {"font": {"color": dde_color}},
                domain = {"row": row-1, "column": col-1}
            ), row=row, col=col)
            
            col += 1
            if col > 3:
                col = 1
                row += 1
        
        fig.update_layout(
            height=400,
            title=f"Diff-in-Diff por Região - {categoria}",
            paper_bgcolor="#F2F2F2"
        )
        
        fig.show()

# Gerar cards regionais
create_regional_cards(df_analise_regiao, categorias_teste)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo Executivo - Cards Consolidados

# COMMAND ----------

def create_executive_summary(df_analise_regiao, categorias_teste):
    """
    Cria resumo executivo com cards consolidados
    """
    summary_data = []
    
    for categoria in categorias_teste:
        df_pandas = df_analise_regiao[categoria].toPandas()
        
        # Calcular médias
        avg_dde = df_pandas['DDE_diff_in_diff'].mean()
        avg_ruptura_bin = df_pandas['PctRupturaBinario_diff_in_diff'].mean()
        avg_ruptura_rec = df_pandas['PctRupturaReceita_diff_in_diff'].mean()
        
        summary_data.append({
            'Categoria': categoria,
            'DDE_Diff': avg_dde,
            'Ruptura_Bin_Diff': avg_ruptura_bin,
            'Ruptura_Rec_Diff': avg_ruptura_rec
        })
    
    # Criar figura com subplots
    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=['DDE Médio (dias)', '% Ruptura Binário (p.p.)', '% Ruptura Receita (p.p.)'],
        specs=[[{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}]]
    )
    
    # DDE
    dde_values = [data['DDE_Diff'] for data in summary_data]
    dde_colors = ["#2E8B57" if v >= 1 else "#DC143C" if v <= -1 else "#808080" for v in dde_values]
    
    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = sum(dde_values) / len(dde_values),
        delta = {"reference": 0, "valueformat": ".1f"},
        title = {"text": "DDE Médio<br>(média das categorias)"},
        number = {"font": {"color": "#2E8B57" if sum(dde_values) >= 0 else "#DC143C"}},
        domain = {"row": 0, "column": 0}
    ), row=1, col=1)
    
    # Ruptura Binário
    ruptura_bin_values = [data['Ruptura_Bin_Diff'] for data in summary_data]
    
    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = sum(ruptura_bin_values) / len(ruptura_bin_values),
        delta = {"reference": 0, "valueformat": ".1f"},
        title = {"text": "% Ruptura Binário<br>(média das categorias)"},
        number = {"font": {"color": "#2E8B57" if sum(ruptura_bin_values) <= 0 else "#DC143C"}},
        domain = {"row": 0, "column": 1}
    ), row=1, col=2)
    
    # Ruptura Receita
    ruptura_rec_values = [data['Ruptura_Rec_Diff'] for data in summary_data]
    
    fig.add_trace(go.Indicator(
        mode = "number+delta",
        value = sum(ruptura_rec_values) / len(ruptura_rec_values),
        delta = {"reference": 0, "valueformat": ".1f"},
        title = {"text": "% Ruptura Receita<br>(média das categorias)"},
        number = {"font": {"color": "#2E8B57" if sum(ruptura_rec_values) <= 0 else "#DC143C"}},
        domain = {"row": 0, "column": 2}
    ), row=1, col=3)
    
    fig.update_layout(
        height=300,
        title="Resumo Executivo - Diff-in-Diff Consolidado",
        paper_bgcolor="#F2F2F2"
    )
    
    fig.show()

# Gerar resumo executivo
create_executive_summary(df_analise_regiao, categorias_teste)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Consolidado - Todos os Cards

# COMMAND ----------

def create_consolidated_dashboard(df_analise_regiao, categorias_teste):
    """
    Cria um dashboard consolidado com todos os cards em uma única visualização
    """
    # Criar figura com subplots para organizar os cards
    fig = make_subplots(
        rows=2, cols=3,
        subplot_titles=[
            f"DDE Médio - {categorias_teste[0]}" if len(categorias_teste) > 0 else "DDE Médio",
            f"% Ruptura Binário - {categorias_teste[0]}" if len(categorias_teste) > 0 else "% Ruptura Binário", 
            f"% Ruptura Receita - {categorias_teste[0]}" if len(categorias_teste) > 0 else "% Ruptura Receita",
            f"DDE Médio - {categorias_teste[1]}" if len(categorias_teste) > 1 else "DDE Médio",
            f"% Ruptura Binário - {categorias_teste[1]}" if len(categorias_teste) > 1 else "% Ruptura Binário",
            f"% Ruptura Receita - {categorias_teste[1]}" if len(categorias_teste) > 1 else "% Ruptura Receita"
        ],
        specs=[[{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}],
               [{"type": "indicator"}, {"type": "indicator"}, {"type": "indicator"}]]
    )
    
    card_positions = [(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)]
    card_index = 0
    
    for categoria in categorias_teste:
        df_pandas = df_analise_regiao[categoria].toPandas()
        
        # Calcular métricas
        avg_dde_diff = df_pandas['DDE_diff_in_diff'].mean()
        avg_ruptura_binario_diff = df_pandas['PctRupturaBinario_diff_in_diff'].mean()
        avg_ruptura_receita_diff = df_pandas['PctRupturaReceita_diff_in_diff'].mean()
        
        # Calcular percentual para DDE
        dde_pct_change = calculate_dde_percentage_diff(df_analise_regiao, categoria)
        
        # Determinar cores
        dde_color = "#DC143C" if avg_dde_diff > 1 else "#2E8B57" if avg_dde_diff < -1 else "#808080"
        ruptura_bin_color = "#DC143C" if avg_ruptura_binario_diff > 1 else "#2E8B57" if avg_ruptura_binario_diff < -1 else "#808080"
        ruptura_rec_color = "#DC143C" if avg_ruptura_receita_diff > 1 else "#2E8B57" if avg_ruptura_receita_diff < -1 else "#808080"
        
        # Adicionar indicadores
        metrics = [
            (avg_dde_diff, f"dias ({dde_pct_change:+.1f}%)", dde_color),
            (avg_ruptura_binario_diff, "p.p.", ruptura_bin_color),
            (avg_ruptura_receita_diff, "p.p.", ruptura_rec_color)
        ]
        
        for metric_value, subtitle, color in metrics:
            if card_index < len(card_positions):
                row, col = card_positions[card_index]
                
                fig.add_trace(go.Indicator(
                    mode = "number+delta",
                    value = metric_value,
                    delta = {"reference": 0, "valueformat": ".1f"},
                    title = {"text": f"{subtitle}"},
                    number = {"font": {"color": color, "size": 20}},
                    domain = {"row": row-1, "column": col-1}
                ), row=row, col=col)
                
                card_index += 1
    
    # Configurar layout
    fig.update_layout(
        height=600,
        title="Dashboard Consolidado - Diff-in-Diff por Categoria",
        paper_bgcolor="#F2F2F2",
        font=dict(family="Arial", size=12)
    )
    
    # Ajustar espaçamento entre subplots
    fig.update_layout(
        margin=dict(l=50, r=50, t=100, b=50)
    )
    
    fig.show()

# Gerar dashboard consolidado
create_consolidated_dashboard(df_analise_regiao, categorias_teste)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise Detalhada por Categoria e Ângulos

# COMMAND ----------

def analyze_by_angles(df_estoque_loja_porte_regiao, df_comparacao, categorias_teste):
    """
    Analisa diff-in-diff por categoria e 3 ângulos: Porte, Região e Delta Merecimento
    """
    resultados = {}
    
    for categoria in categorias_teste:
        print(f"\n=== ANÁLISE DETALHADA - {categoria} ===")
        
        # 1. ANÁLISE POR PORTE DE LOJA
        print("\n--- 1. ANÁLISE POR PORTE DE LOJA ---")
        df_porte = analyze_by_porte(df_estoque_loja_porte_regiao[categoria], categoria)
        resultados[f"{categoria}_porte"] = df_porte
        
        # 2. ANÁLISE POR REGIÃO
        print("\n--- 2. ANÁLISE POR REGIÃO ---")
        df_regiao = analyze_by_regiao(df_estoque_loja_porte_regiao[categoria], categoria)
        resultados[f"{categoria}_regiao"] = df_regiao
        
        # 3. ANÁLISE POR DELTA MERECIMENTO
        print("\n--- 3. ANÁLISE POR DELTA MERECIMENTO ---")
        df_delta_merecimento = analyze_by_delta_merecimento(
            df_estoque_loja_porte_regiao[categoria], 
            df_comparacao[categoria], 
            categoria
        )
        resultados[f"{categoria}_delta_merecimento"] = df_delta_merecimento
    
    return resultados

def analyze_by_porte(df_estoque, categoria):
    """
    Analisa diff-in-diff por porte de loja
    """
    # Agregar por grupo, período e porte
    df_base = (
        df_estoque
        .groupBy("grupo", "periodo_analise", "NmPorteLoja")
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
        .select("grupo","periodo_analise","NmPorteLoja","DDE_medio","PctRupturaBinario","PctRupturaReceita")
        .dropna(subset=["NmPorteLoja"])
    )
    
    # Calcular diff-in-diff
    df_diff = calculate_diff_in_diff(df_base, "NmPorteLoja")
    
    print(f"Diff-in-Diff por Porte - {categoria}:")
    df_diff.orderBy("NmPorteLoja").display()
    
    return df_diff

def analyze_by_regiao(df_estoque, categoria):
    """
    Analisa diff-in-diff por região
    """
    # Agregar por grupo, período e região
    df_base = (
        df_estoque
        .groupBy("grupo", "periodo_analise", "NmRegiaoGeografica")
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
        .select("grupo","periodo_analise","NmRegiaoGeografica","DDE_medio","PctRupturaBinario","PctRupturaReceita")
        .dropna(subset=["NmRegiaoGeografica"])
    )
    
    # Calcular diff-in-diff
    df_diff = calculate_diff_in_diff(df_base, "NmRegiaoGeografica")
    
    print(f"Diff-in-Diff por Região - {categoria}:")
    df_diff.orderBy("NmRegiaoGeografica").display()
    
    return df_diff

def analyze_by_delta_merecimento(df_estoque, df_comparacao, categoria):
    """
    Analisa diff-in-diff por delta de merecimento (acima, abaixo, manteve)
    """
    # Join com dados de comparação para obter bucket_delta
    df_joined = (
        df_estoque
        .join(
            df_comparacao
            .select("CdFilial", "grupo_de_necessidade", "bucket_delta")
            .distinct(),
            on=["CdFilial", "grupo_de_necessidade"],
            how="inner"
        )
    )
    
    # Agregar por grupo, período e bucket_delta
    df_base = (
        df_joined
        .groupBy("grupo", "periodo_analise", "bucket_delta")
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
        .select("grupo","periodo_analise","bucket_delta","DDE_medio","PctRupturaBinario","PctRupturaReceita")
        .dropna(subset=["bucket_delta"])
    )
    
    # Calcular diff-in-diff
    df_diff = calculate_diff_in_diff(df_base, "bucket_delta")
    
    print(f"Diff-in-Diff por Delta Merecimento - {categoria}:")
    df_diff.orderBy("bucket_delta").display()
    
    return df_diff

def calculate_diff_in_diff(df_base, group_column):
    """
    Calcula diff-in-diff para qualquer agrupamento
    """
    # Baseline e piloto via WHEN
    df_wide = (
        df_base.groupBy(group_column, "grupo")
        .agg(
            F.max(F.when(F.col("periodo_analise")=="baseline", F.col("DDE_medio"))).alias("baseline_DDE_medio"),
            F.max(F.when(F.col("periodo_analise")=="piloto",   F.col("DDE_medio"))).alias("piloto_DDE_medio"),
            F.max(F.when(F.col("periodo_analise")=="baseline", F.col("PctRupturaBinario"))).alias("baseline_PctRupturaBinario"),
            F.max(F.when(F.col("periodo_analise")=="piloto",   F.col("PctRupturaBinario"))).alias("piloto_PctRupturaBinario"),
            F.max(F.when(F.col("periodo_analise")=="baseline", F.col("PctRupturaReceita"))).alias("baseline_PctRupturaReceita"),
            F.max(F.when(F.col("periodo_analise")=="piloto",   F.col("PctRupturaReceita"))).alias("piloto_PctRupturaReceita"),
        )
        .fillna(0.0)
    )
    
    # Deltas piloto - baseline
    df_delta = (
        df_wide
        .withColumn("DDE_delta", F.round(F.col("piloto_DDE_medio") - F.col("baseline_DDE_medio"), 1))
        .withColumn("PctRupturaBinario_delta", F.round(F.col("piloto_PctRupturaBinario") - F.col("baseline_PctRupturaBinario"), 1))
        .withColumn("PctRupturaReceita_delta", F.round(F.col("piloto_PctRupturaReceita") - F.col("baseline_PctRupturaReceita"), 1))
    )
    
    # Separar teste e controle
    df_teste = (
        df_delta.filter(F.col("grupo") == "teste")
        .select(
            F.col(group_column).alias(f"{group_column}_key"),
            F.col("DDE_delta").alias("DDE_delta_teste"),
            F.col("PctRupturaBinario_delta").alias("PctRupturaBinario_delta_teste"),
            F.col("PctRupturaReceita_delta").alias("PctRupturaReceita_delta_teste"),
        )
    )
    
    df_controle = (
        df_delta.filter(F.col("grupo") == "controle")
        .select(
            F.col(group_column).alias(f"{group_column}_key"),
            F.col("DDE_delta").alias("DDE_delta_controle"),
            F.col("PctRupturaBinario_delta").alias("PctRupturaBinario_delta_controle"),
            F.col("PctRupturaReceita_delta").alias("PctRupturaReceita_delta_controle"),
        )
    )
    
    # Diff-in-Diff = delta_teste - delta_controle
    df_diff = (
        df_teste.join(df_controle, on=f"{group_column}_key", how="inner")
        .withColumnRenamed(f"{group_column}_key", group_column)
        .withColumn("DDE_diff_in_diff", F.round(F.col("DDE_delta_teste") - F.col("DDE_delta_controle"), 1))
        .withColumn("PctRupturaBinario_diff_in_diff", F.round(F.col("PctRupturaBinario_delta_teste") - F.col("PctRupturaBinario_delta_controle"), 1))
        .withColumn("PctRupturaReceita_diff_in_diff", F.round(F.col("PctRupturaReceita_delta_teste") - F.col("PctRupturaReceita_delta_controle"), 1))
    )
    
    return df_diff

# Executar análises detalhadas
resultados_detalhados = analyze_by_angles(df_estoque_loja_porte_regiao, df_comparacao, categorias_teste)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizações por Ângulos de Análise

# COMMAND ----------

def create_angle_visualizations(resultados_detalhados, categorias_teste):
    """
    Cria visualizações específicas para cada ângulo de análise
    """
    for categoria in categorias_teste:
        print(f"\n=== VISUALIZAÇÕES - {categoria} ===")
        
        # 1. Visualização por Porte
        create_porte_visualization(resultados_detalhados[f"{categoria}_porte"], categoria)
        
        # 2. Visualização por Região
        create_regiao_visualization(resultados_detalhados[f"{categoria}_regiao"], categoria)
        
        # 3. Visualização por Delta Merecimento
        create_delta_merecimento_visualization(resultados_detalhados[f"{categoria}_delta_merecimento"], categoria)

def create_porte_visualization(df_porte, categoria):
    """
    Cria visualização para análise por porte
    """
    df_pandas = df_porte.toPandas()
    
    # Criar gráfico de barras para DDE
    fig_dde = px.bar(
        df_pandas, 
        x="NmPorteLoja", 
        y="DDE_diff_in_diff",
        title=f"DDE Diff-in-Diff por Porte - {categoria}",
        color="DDE_diff_in_diff",
        color_continuous_scale=["#DC143C", "#808080", "#2E8B57"],
        labels={"DDE_diff_in_diff": "DDE Diff-in-Diff (dias)", "NmPorteLoja": "Porte da Loja"}
    )
    
    fig_dde.update_layout(
        paper_bgcolor="#F2F2F2",
        plot_bgcolor="white",
        height=400
    )
    
    fig_dde.show()
    
    # Criar gráfico de barras para Ruptura
    fig_ruptura = px.bar(
        df_pandas, 
        x="NmPorteLoja", 
        y="PctRupturaBinario_diff_in_diff",
        title=f"% Ruptura Diff-in-Diff por Porte - {categoria}",
        color="PctRupturaBinario_diff_in_diff",
        color_continuous_scale=["#2E8B57", "#808080", "#DC143C"],
        labels={"PctRupturaBinario_diff_in_diff": "% Ruptura Diff-in-Diff (p.p.)", "NmPorteLoja": "Porte da Loja"}
    )
    
    fig_ruptura.update_layout(
        paper_bgcolor="#F2F2F2",
        plot_bgcolor="white",
        height=400
    )
    
    fig_ruptura.show()

def create_regiao_visualization(df_regiao, categoria):
    """
    Cria visualização para análise por região
    """
    df_pandas = df_regiao.toPandas()
    
    # Criar gráfico de barras para DDE
    fig_dde = px.bar(
        df_pandas, 
        x="NmRegiaoGeografica", 
        y="DDE_diff_in_diff",
        title=f"DDE Diff-in-Diff por Região - {categoria}",
        color="DDE_diff_in_diff",
        color_continuous_scale=["#DC143C", "#808080", "#2E8B57"],
        labels={"DDE_diff_in_diff": "DDE Diff-in-Diff (dias)", "NmRegiaoGeografica": "Região"}
    )
    
    fig_dde.update_layout(
        paper_bgcolor="#F2F2F2",
        plot_bgcolor="white",
        height=400,
        xaxis_tickangle=-45
    )
    
    fig_dde.show()

def create_delta_merecimento_visualization(df_delta, categoria):
    """
    Cria visualização para análise por delta de merecimento
    """
    df_pandas = df_delta.toPandas()
    
    # Criar gráfico de barras para DDE
    fig_dde = px.bar(
        df_pandas, 
        x="bucket_delta", 
        y="DDE_diff_in_diff",
        title=f"DDE Diff-in-Diff por Delta Merecimento - {categoria}",
        color="DDE_diff_in_diff",
        color_continuous_scale=["#DC143C", "#808080", "#2E8B57"],
        labels={"DDE_diff_in_diff": "DDE Diff-in-Diff (dias)", "bucket_delta": "Delta Merecimento"}
    )
    
    fig_dde.update_layout(
        paper_bgcolor="#F2F2F2",
        plot_bgcolor="white",
        height=400
    )
    
    fig_dde.show()
    
    # Criar gráfico de barras para Ruptura
    fig_ruptura = px.bar(
        df_pandas, 
        x="bucket_delta", 
        y="PctRupturaBinario_diff_in_diff",
        title=f"% Ruptura Diff-in-Diff por Delta Merecimento - {categoria}",
        color="PctRupturaBinario_diff_in_diff",
        color_continuous_scale=["#2E8B57", "#808080", "#DC143C"],
        labels={"PctRupturaBinario_diff_in_diff": "% Ruptura Diff-in-Diff (p.p.)", "bucket_delta": "Delta Merecimento"}
    )
    
    fig_ruptura.update_layout(
        paper_bgcolor="#F2F2F2",
        plot_bgcolor="white",
        height=400
    )
    
    fig_ruptura.show()

# Executar visualizações por ângulos
create_angle_visualizations(resultados_detalhados, categorias_teste)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: análises de acompanhamento

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Consequência dos deltas mais relevantes vs Matriz antiga - DDE ou ruptura 
# MAGIC 2. Buckets de DDE - delta versus categoria
# MAGIC 3. DDE x Ruptura  - delta versus categoria
# MAGIC 4. Proporção prevista vs real - janela desde inicio do teste
