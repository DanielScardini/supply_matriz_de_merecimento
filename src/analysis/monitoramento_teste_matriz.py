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
import plotly.graph_objects as go

# Inicialização do Spark
spark = SparkSession.builder.appName("calculo_matriz_merecimento_unificado").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

GRUPOS_TESTE = ['Telef pp', 'TV 50 ALTO P', 'TV 55 ALTO P']
print(GRUPOS_TESTE)

data_inicio = "2025-08-29"
fim_baseline = "2025-09-05"

inicio_teste = "2025-09-05"

categorias_teste = ['TELAS', 'TELEFONIA']

dict_diretorias = {
  'TELAS': 'TVS',
  'TELEFONIA': 'TELEFONIA CELULAR'
}



# COMMAND ----------

def get_janela(inicio_teste: str, days_back: int = 7) -> str:
    """
    Retorna a data inicial da janela.
    - Por padrão: 3 dias antes de ontem.
    - Limitado para não ultrapassar inicio_teste.
    """
    dt_inicio_teste = datetime.strptime(inicio_teste, "%Y-%m-%d").date()
    ontem = datetime.today().date() - timedelta(days=1)
    inicio_calc = ontem - timedelta(days=days_back)

    # se a data calculada for antes do inicio_teste, usa inicio_teste
    if inicio_calc < dt_inicio_teste:
        inicio_calc = dt_inicio_teste

    return inicio_calc.strftime("%Y-%m-%d")

janela_teste = get_janela(inicio_teste, days_back=7)
print(janela_teste)

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

df_merecimento_offline['TELAS'].cache()#.display()
df_merecimento_offline['TELAS']#.display()
df_merecimento_offline['TELEFONIA']#.display()
df_merecimento_offline['TELEFONIA'].cache()
df_merecimento_online['TELAS']#.display()
df_merecimento_online['TELAS'].cache()
df_merecimento_online['TELEFONIA']#.display()
df_merecimento_online['TELEFONIA'].cache()

produtos_do_teste_offline = {}
produtos_do_teste_offline['TELAS'] = df_merecimento_offline['TELAS'].select("CdSku", "grupo_de_necessidade").distinct()
produtos_do_teste_offline['TELEFONIA'] = df_merecimento_offline['TELEFONIA'].select("CdSku", "grupo_de_necessidade").distinct()

produtos_do_teste_online = {}
produtos_do_teste_online['TELAS'] = df_merecimento_online['TELAS'].select("CdSku", "grupo_de_necessidade").distinct()
produtos_do_teste_online['TELEFONIA'] = df_merecimento_online['TELEFONIA'].select("CdSku", "grupo_de_necessidade").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de matriz Neogrid

# COMMAND ----------

df_matriz_neogrid_offline = (
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

df_matriz_neogrid_agg_offline = (
  df_matriz_neogrid_offline
  .groupBy('CdFilial', 'grupo_de_necessidade')
  .agg(
    F.round(F.mean('PercMatrizNeogrid'), 3).alias('PercMatrizNeogrid'),
    F.round(F.median('PercMatrizNeogrid'),3).alias('PercMatrizNeogrid_median')
  )
)

df_matriz_neogrid_offline.cache()

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
            produtos_do_teste_offline[categoria],
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
                        F.col("DtAtual") < fim_baseline, F.lit('baseline')
                    )
                    .when(F.col("DtAtual") >= janela_teste, F.lit('piloto'))
                    .otherwise(F.lit('ignorar'))
                    )
        .filter(F.col("periodo_analise") != 'ignorar')
                    
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
# MAGIC ## Análise de DDExRuptura - Geral

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

    df_analise[categoria].withColumn("categoria", F.lit(categoria)).display()

    

# COMMAND ----------

from pyspark.sql import functions as F

# Saídas
df_did = {}           # por categoria
dfs_consolidados = [] # para união final

for categoria in categorias_teste:
    # 1) Base agregada com normalização de período
    base = (
        df_estoque_loja[categoria]
        .withColumn(
            "periodo_norm",
            F.when(F.lower(F.trim(F.col("periodo_analise"))).isin("baseline","piloto"),
                   F.lower(F.trim(F.col("periodo_analise"))))
             .when(F.lower(F.col("periodo_analise")).like("%base%"), F.lit("baseline"))
             .when(F.lower(F.col("periodo_analise")).like("%pil%"),  F.lit("piloto"))
             .otherwise(F.lower(F.trim(F.col("periodo_analise"))))
        )
        .groupBy("grupo", "periodo_norm")
        .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio"),
            F.round(100 * F.avg(F.when(F.col("FlagRuptura")==1, 1).otherwise(0)), 1).alias("PctRupturaBinario"),
            (100 * (F.sum("ReceitaPerdidaRuptura") / F.sum("Receita"))).alias("PctRupturaReceita_raw")
        )
        .withColumn("PctRupturaReceita", F.round(F.col("PctRupturaReceita_raw"), 1))
        .select("grupo","periodo_norm","DDE_medio","PctRupturaBinario","PctRupturaReceita")
    )

    # 2) baseline/piloto por grupo (sem pivot de linhas)
    wide = (
        base.groupBy("grupo")
        .agg(
            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("DDE_medio"))).alias("baseline_DDE"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("DDE_medio"))).alias("piloto_DDE"),

            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("PctRupturaBinario"))).alias("baseline_PctBin"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("PctRupturaBinario"))).alias("piloto_PctBin"),

            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("PctRupturaReceita"))).alias("baseline_PctRec"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("PctRupturaReceita"))).alias("piloto_PctRec"),
        )
        .fillna(0.0)
    )

    # 3) deltas piloto - baseline
    delta = (
        wide
        .withColumn("DDE_delta",       F.round(F.col("piloto_DDE")    - F.col("baseline_DDE"),    1))
        .withColumn("PctBin_delta",    F.round(F.col("piloto_PctBin") - F.col("baseline_PctBin"), 1))
        .withColumn("PctRec_delta",    F.round(F.col("piloto_PctRec") - F.col("baseline_PctRec"), 1))
        .withColumn("k", F.lit(1))  # chave para join teste x controle
    )

    # 4) separar teste e controle com todos os valores baseline/piloto
    teste = (
        delta.filter(F.col("grupo")=="teste")
        .select(
            "k",
            "baseline_DDE","piloto_DDE","DDE_delta",
            "baseline_PctBin","piloto_PctBin","PctBin_delta",
            "baseline_PctRec","piloto_PctRec","PctRec_delta"
        )
        .withColumnRenamed("baseline_DDE","baseline_DDE_teste")
        .withColumnRenamed("piloto_DDE","piloto_DDE_teste")
        .withColumnRenamed("DDE_delta","DDE_delta_teste")
        .withColumnRenamed("baseline_PctBin","baseline_PctBin_teste")
        .withColumnRenamed("piloto_PctBin","piloto_PctBin_teste")
        .withColumnRenamed("PctBin_delta","PctRupturaBinario_delta_teste")
        .withColumnRenamed("baseline_PctRec","baseline_PctRec_teste")
        .withColumnRenamed("piloto_PctRec","piloto_PctRec_teste")
        .withColumnRenamed("PctRec_delta","PctRupturaReceita_delta_teste")
    )

    controle = (
        delta.filter(F.col("grupo")=="controle")
        .select(
            "k",
            "baseline_DDE","piloto_DDE","DDE_delta",
            "baseline_PctBin","piloto_PctBin","PctBin_delta",
            "baseline_PctRec","piloto_PctRec","PctRec_delta"
        )
        .withColumnRenamed("baseline_DDE","baseline_DDE_controle")
        .withColumnRenamed("piloto_DDE","piloto_DDE_controle")
        .withColumnRenamed("DDE_delta","DDE_delta_controle")
        .withColumnRenamed("baseline_PctBin","baseline_PctBin_controle")
        .withColumnRenamed("piloto_PctBin","piloto_PctBin_controle")
        .withColumnRenamed("PctBin_delta","PctRupturaBinario_delta_controle")
        .withColumnRenamed("baseline_PctRec","baseline_PctRec_controle")
        .withColumnRenamed("piloto_PctRec","piloto_PctRec_controle")
        .withColumnRenamed("PctRec_delta","PctRupturaReceita_delta_controle")
    )

    # 5) join + Diff-in-Diff
    did = (
        teste.join(controle, on="k", how="inner").drop("k")
        .withColumn("DDE_diff_in_diff",               F.round(F.col("DDE_delta_teste") - F.col("DDE_delta_controle"), 1))
        .withColumn("PctRupturaBinario_diff_in_diff", F.round(F.col("PctRupturaBinario_delta_teste") - F.col("PctRupturaBinario_delta_controle"), 1))
        .withColumn("PctRupturaReceita_diff_in_diff", F.round(F.col("PctRupturaReceita_delta_teste") - F.col("PctRupturaReceita_delta_controle"), 1))
    )

    # 6) Pivot por métrica: linhas = métricas, colunas = diff_in_diff | delta_teste | delta_controle | baseline/piloto por grupo
    df_pivot_metricas = (
        did.selectExpr(
            "stack(3, "
            # metrica, diff, delta_teste, delta_controle, baseline_teste, piloto_teste, baseline_controle, piloto_controle
            " 'DDE',               DDE_diff_in_diff,               DDE_delta_teste,               DDE_delta_controle, "
            " baseline_DDE_teste,  piloto_DDE_teste,               baseline_DDE_controle,         piloto_DDE_controle, "
            " 'PctRupturaBinario', PctRupturaBinario_diff_in_diff, PctRupturaBinario_delta_teste, PctRupturaBinario_delta_controle, "
            " baseline_PctBin_teste, piloto_PctBin_teste,          baseline_PctBin_controle,      piloto_PctBin_controle, "
            " 'PctRupturaReceita', PctRupturaReceita_diff_in_diff, PctRupturaReceita_delta_teste, PctRupturaReceita_delta_controle, "
            " baseline_PctRec_teste, piloto_PctRec_teste,          baseline_PctRec_controle,      piloto_PctRec_controle "
            ") as (metrica, diff_in_diff, delta_teste, delta_controle, "
            "baseline_teste, piloto_teste, baseline_controle, piloto_controle)"
        )
        .withColumn("categoria", F.lit(categoria))
        .select(
            "categoria","metrica",
            "diff_in_diff","delta_teste","delta_controle",
            "baseline_teste","piloto_teste","baseline_controle","piloto_controle"
        )
        .orderBy("metrica", "categoria")
    )

    # Guarda resultados
    df_did[categoria] = df_pivot_metricas
    dfs_consolidados.append(df_pivot_metricas)

# 7) DataFrame final consolidado (todas as categorias)
if dfs_consolidados:
    df_did_final = dfs_consolidados[0]
    for d in dfs_consolidados[1:]:
        df_did_final = df_did_final.unionByName(d, allowMissingColumns=True)
    # Mostra consolidado
    df_did_final.display()

# Também pode inspecionar cada categoria individual:
# for cat, df in df_did.items():
#     print(cat)
#     df.display()

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

    df_analise_regiao[categoria].limit(1).display()

# COMMAND ----------

from pyspark.sql import functions as F

df_did_regiao = {}
dfs_regiao_all = []

for categoria in categorias_teste:
    # Usa o agregado pronto do seu dict
    df_base0 = df_analise_regiao[categoria].dropna(subset=["NmRegiaoGeografica"])

    # Normaliza rótulos de período para evitar NULL por variações de texto
    df_base = (
        df_base0
        .withColumn(
            "periodo_norm",
            F.when(F.lower(F.trim(F.col("periodo_analise"))).isin("baseline","piloto"),
                   F.lower(F.trim(F.col("periodo_analise"))))
             .when(F.lower(F.col("periodo_analise")).like("%base%"), F.lit("baseline"))
             .when(F.lower(F.col("periodo_analise")).like("%pil%"),  F.lit("piloto"))
             .otherwise(F.lower(F.trim(F.col("periodo_analise"))))
        )
        .select(
            "NmRegiaoGeografica","grupo","periodo_norm",
            "DDE_medio","PctRupturaBinario","PctRupturaReceita"
        )
    )

    # baseline/piloto por Região x Grupo (sem pivot)
    wide = (
        df_base.groupBy("NmRegiaoGeografica","grupo")
        .agg(
            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("DDE_medio"))).alias("baseline_DDE"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("DDE_medio"))).alias("piloto_DDE"),

            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("PctRupturaBinario"))).alias("baseline_PctBin"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("PctRupturaBinario"))).alias("piloto_PctBin"),

            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("PctRupturaReceita"))).alias("baseline_PctRec"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("PctRupturaReceita"))).alias("piloto_PctRec"),
        )
        .fillna(0.0)
    )

    # deltas piloto - baseline
    delta = (
        wide
        .withColumn("DDE_delta",    F.round(F.col("piloto_DDE")    - F.col("baseline_DDE"), 1))
        .withColumn("PctBin_delta", F.round(F.col("piloto_PctBin") - F.col("baseline_PctBin"), 1))
        .withColumn("PctRec_delta", F.round(F.col("piloto_PctRec") - F.col("baseline_PctRec"), 1))
    )

    # separa teste e controle por região
    t = (
        delta.filter(F.col("grupo")=="teste")
        .select(
            "NmRegiaoGeografica",
            "baseline_DDE","piloto_DDE","DDE_delta",
            "baseline_PctBin","piloto_PctBin","PctBin_delta",
            "baseline_PctRec","piloto_PctRec","PctRec_delta"
        )
        .withColumnRenamed("baseline_DDE","baseline_DDE_teste")
        .withColumnRenamed("piloto_DDE","piloto_DDE_teste")
        .withColumnRenamed("DDE_delta","DDE_delta_teste")
        .withColumnRenamed("baseline_PctBin","baseline_PctBin_teste")
        .withColumnRenamed("piloto_PctBin","piloto_PctBin_teste")
        .withColumnRenamed("PctBin_delta","PctRupturaBinario_delta_teste")
        .withColumnRenamed("baseline_PctRec","baseline_PctRec_teste")
        .withColumnRenamed("piloto_PctRec","piloto_PctRec_teste")
        .withColumnRenamed("PctRec_delta","PctRupturaReceita_delta_teste")
    )

    c = (
        delta.filter(F.col("grupo")=="controle")
        .select(
            "NmRegiaoGeografica",
            "baseline_DDE","piloto_DDE","DDE_delta",
            "baseline_PctBin","piloto_PctBin","PctBin_delta",
            "baseline_PctRec","piloto_PctRec","PctRec_delta"
        )
        .withColumnRenamed("baseline_DDE","baseline_DDE_controle")
        .withColumnRenamed("piloto_DDE","piloto_DDE_controle")
        .withColumnRenamed("DDE_delta","DDE_delta_controle")
        .withColumnRenamed("baseline_PctBin","baseline_PctBin_controle")
        .withColumnRenamed("piloto_PctBin","piloto_PctBin_controle")
        .withColumnRenamed("PctBin_delta","PctRupturaBinario_delta_controle")
        .withColumnRenamed("baseline_PctRec","baseline_PctRec_controle")
        .withColumnRenamed("piloto_PctRec","piloto_PctRec_controle")
        .withColumnRenamed("PctRec_delta","PctRupturaReceita_delta_controle")
    )

    did = (
        t.join(c, on="NmRegiaoGeografica", how="inner")
         .withColumn("DDE_diff_in_diff",               F.round(F.col("DDE_delta_teste") - F.col("DDE_delta_controle"), 1))
         .withColumn("PctRupturaBinario_diff_in_diff", F.round(F.col("PctRupturaBinario_delta_teste") - F.col("PctRupturaBinario_delta_controle"), 1))
         .withColumn("PctRupturaReceita_diff_in_diff", F.round(F.col("PctRupturaReceita_delta_teste") - F.col("PctRupturaReceita_delta_controle"), 1))
    )

    # pivot por métrica para visualização
    df_pivot_regiao = (
        did.selectExpr(
            "NmRegiaoGeografica",
            "stack(3, "
            " 'DDE',               DDE_diff_in_diff,               DDE_delta_teste,               DDE_delta_controle, "
            " baseline_DDE_teste,  piloto_DDE_teste,               baseline_DDE_controle,         piloto_DDE_controle, "
            " 'PctRupturaBinario', PctRupturaBinario_diff_in_diff, PctRupturaBinario_delta_teste, PctRupturaBinario_delta_controle, "
            " baseline_PctBin_teste, piloto_PctBin_teste,          baseline_PctBin_controle,      piloto_PctBin_controle, "
            " 'PctRupturaReceita', PctRupturaReceita_diff_in_diff, PctRupturaReceita_delta_teste, PctRupturaReceita_delta_controle, "
            " baseline_PctRec_teste, piloto_PctRec_teste,          baseline_PctRec_controle,      piloto_PctRec_controle "
            ") as (metrica, diff_in_diff, delta_teste, delta_controle, "
            "baseline_teste, piloto_teste, baseline_controle, piloto_controle)"
        )
        .withColumn("categoria", F.lit(categoria))
        .select(
            "categoria","NmRegiaoGeografica","metrica",
            "diff_in_diff","delta_teste","delta_controle",
            "baseline_teste","piloto_teste","baseline_controle","piloto_controle"
        )
        .orderBy("categoria","metrica" ,"NmRegiaoGeografica")
    )

    df_did_regiao[categoria] = df_pivot_regiao
    dfs_regiao_all.append(df_pivot_regiao)
    #df_pivot_regiao.display()

# Consolidado de todas as categorias
if dfs_regiao_all:
    df_did_regiao_final = dfs_regiao_all[0]
    for d in dfs_regiao_all[1:]:
        df_did_regiao_final = df_did_regiao_final.unionByName(d, allowMissingColumns=True)
    df_did_regiao_final.display()

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


# COMMAND ----------

from pyspark.sql import functions as F

df_did_porte = {}
dfs_porte_all = []

for categoria in categorias_teste:
    # normaliza período
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
        .filter(F.col("NmPorteLoja") != "-")  # remove porte inválido
    )

    # 2) baseline/piloto por porte x grupo
    wide = (
        df_base.groupBy("NmPorteLoja","grupo")
        .agg(
            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("DDE_medio"))).alias("baseline_DDE"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("DDE_medio"))).alias("piloto_DDE"),

            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("PctRupturaBinario"))).alias("baseline_PctBin"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("PctRupturaBinario"))).alias("piloto_PctBin"),

            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("PctRupturaReceita"))).alias("baseline_PctRec"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("PctRupturaReceita"))).alias("piloto_PctRec"),
        )
        .fillna(0.0)
    )

    # 3) deltas
    delta = (
        wide
        .withColumn("DDE_delta",    F.round(F.col("piloto_DDE")    - F.col("baseline_DDE"), 1))
        .withColumn("PctBin_delta", F.round(F.col("piloto_PctBin") - F.col("baseline_PctBin"), 1))
        .withColumn("PctRec_delta", F.round(F.col("piloto_PctRec") - F.col("baseline_PctRec"), 1))
    )

    # 4) separa teste e controle
    t = (
        delta.filter(F.col("grupo")=="teste")
        .select(
            "NmPorteLoja",
            "baseline_DDE","piloto_DDE","DDE_delta",
            "baseline_PctBin","piloto_PctBin","PctBin_delta",
            "baseline_PctRec","piloto_PctRec","PctRec_delta"
        )
        .withColumnRenamed("baseline_DDE","baseline_DDE_teste")
        .withColumnRenamed("piloto_DDE","piloto_DDE_teste")
        .withColumnRenamed("DDE_delta","DDE_delta_teste")
        .withColumnRenamed("baseline_PctBin","baseline_PctBin_teste")
        .withColumnRenamed("piloto_PctBin","piloto_PctBin_teste")
        .withColumnRenamed("PctBin_delta","PctRupturaBinario_delta_teste")
        .withColumnRenamed("baseline_PctRec","baseline_PctRec_teste")
        .withColumnRenamed("piloto_PctRec","piloto_PctRec_teste")
        .withColumnRenamed("PctRec_delta","PctRupturaReceita_delta_teste")
    )

    c = (
        delta.filter(F.col("grupo")=="controle")
        .select(
            "NmPorteLoja",
            "baseline_DDE","piloto_DDE","DDE_delta",
            "baseline_PctBin","piloto_PctBin","PctBin_delta",
            "baseline_PctRec","piloto_PctRec","PctRec_delta"
        )
        .withColumnRenamed("baseline_DDE","baseline_DDE_controle")
        .withColumnRenamed("piloto_DDE","piloto_DDE_controle")
        .withColumnRenamed("DDE_delta","DDE_delta_controle")
        .withColumnRenamed("baseline_PctBin","baseline_PctBin_controle")
        .withColumnRenamed("piloto_PctBin","piloto_PctBin_controle")
        .withColumnRenamed("PctBin_delta","PctRupturaBinario_delta_controle")
        .withColumnRenamed("baseline_PctRec","baseline_PctRec_controle")
        .withColumnRenamed("piloto_PctRec","piloto_PctRec_controle")
        .withColumnRenamed("PctRec_delta","PctRupturaReceita_delta_controle")
    )

    did = (
        t.join(c, on="NmPorteLoja", how="inner")
         .withColumn("DDE_diff_in_diff",               F.round(F.col("DDE_delta_teste") - F.col("DDE_delta_controle"), 1))
         .withColumn("PctRupturaBinario_diff_in_diff", F.round(F.col("PctRupturaBinario_delta_teste") - F.col("PctRupturaBinario_delta_controle"), 1))
         .withColumn("PctRupturaReceita_diff_in_diff", F.round(F.col("PctRupturaReceita_delta_teste") - F.col("PctRupturaReceita_delta_controle"), 1))
    )

    # 5) pivot por métrica
    df_pivot_porte = (
        did.selectExpr(
            "NmPorteLoja",
            "stack(3, "
            " 'DDE',               DDE_diff_in_diff,               DDE_delta_teste,               DDE_delta_controle, "
            " baseline_DDE_teste,  piloto_DDE_teste,               baseline_DDE_controle,         piloto_DDE_controle, "
            " 'PctRupturaBinario', PctRupturaBinario_diff_in_diff, PctRupturaBinario_delta_teste, PctRupturaBinario_delta_controle, "
            " baseline_PctBin_teste, piloto_PctBin_teste,          baseline_PctBin_controle,      piloto_PctBin_controle, "
            " 'PctRupturaReceita', PctRupturaReceita_diff_in_diff, PctRupturaReceita_delta_teste, PctRupturaReceita_delta_controle, "
            " baseline_PctRec_teste, piloto_PctRec_teste,          baseline_PctRec_controle,      piloto_PctRec_controle "
            ") as (metrica, diff_in_diff, delta_teste, delta_controle, "
            "baseline_teste, piloto_teste, baseline_controle, piloto_controle)"
        )
        .withColumn("categoria", F.lit(categoria))
        .select(
            "categoria","NmPorteLoja","metrica",
            "diff_in_diff","delta_teste","delta_controle",
            "baseline_teste","piloto_teste","baseline_controle","piloto_controle"
        )
        .orderBy("categoria","metrica", "NmPorteLoja",)
    )

    df_did_porte[categoria] = df_pivot_porte
    dfs_porte_all.append(df_pivot_porte)
    #df_pivot_porte.display()

# Consolidado de todas as categorias
if dfs_porte_all:
    df_did_porte_final = dfs_porte_all[0]
    for d in dfs_porte_all[1:]:
        df_did_porte_final = df_did_porte_final.unionByName(d, allowMissingColumns=True)
    df_did_porte_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo dos delta merecimentos

# COMMAND ----------

df_comparacao = {}

for categoria in categorias_teste:
  df_comparacao[categoria] = (
    df_merecimento_offline[categoria]
    .select("CdFilial", "NmFilial", "grupo_de_necessidade", "merecimento_percentual")
    .filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
    .distinct()
    .join(
      df_matriz_neogrid_agg_offline,
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
            .select("CdFilial", "bucket_delta")
            .distinct(),
            on=["CdFilial"],
            how="inner"
            )
        .groupBy("bucket_delta", "periodo_analise")
        .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio"),
            F.round(100 * F.avg(F.when(F.col("FlagRuptura") == 1, 1).otherwise(0)), 1).alias("PctRupturaBinario"),
            F.round(100 * (F.sum("ReceitaPerdidaRuptura") / F.sum("Receita")), 1).alias("PctRupturaReceita")
        )
        .orderBy('bucket_delta', 'periodo_analise')
    )

    df_analise_deltas[categoria].display()

# COMMAND ----------

from pyspark.sql import functions as F

df_did_bucket = {}
dfs_all = []

for categoria in categorias_teste:
    # base do usuário + grupo incluído e período normalizado
    base = (
        df_estoque_loja_porte_regiao[categoria]
        .join(
            df_comparacao[categoria].select("CdFilial","bucket_delta").distinct(),
            on=["CdFilial"], how="inner"
        )
        .withColumn("periodo_norm", F.lower(F.trim(F.col("periodo_analise"))))
        .groupBy("bucket_delta", "grupo", "periodo_norm")
        .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio"),
            F.round(100 * F.avg(F.when(F.col("FlagRuptura")==1, 1).otherwise(0)), 1).alias("PctRupturaBinario"),
            F.round(100 * (F.sum("ReceitaPerdidaRuptura") / F.sum("Receita")), 1).alias("PctRupturaReceita")
        )
    )

    # baseline/piloto por bucket x grupo (sem pivot)
    wide = (
        base.groupBy("bucket_delta","grupo")
        .agg(
            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("DDE_medio"))).alias("baseline_DDE"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("DDE_medio"))).alias("piloto_DDE"),

            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("PctRupturaBinario"))).alias("baseline_PctBin"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("PctRupturaBinario"))).alias("piloto_PctBin"),

            F.max(F.when(F.col("periodo_norm")=="baseline", F.col("PctRupturaReceita"))).alias("baseline_PctRec"),
            F.max(F.when(F.col("periodo_norm")=="piloto",   F.col("PctRupturaReceita"))).alias("piloto_PctRec"),
        )
        .fillna(0.0)
    )

    # deltas piloto - baseline
    delta = (
        wide
        .withColumn("DDE_delta",    F.round(F.col("piloto_DDE")    - F.col("baseline_DDE"), 1))
        .withColumn("PctBin_delta", F.round(F.col("piloto_PctBin") - F.col("baseline_PctBin"), 1))
        .withColumn("PctRec_delta", F.round(F.col("piloto_PctRec") - F.col("baseline_PctRec"), 1))
    )

    # separa teste e controle
    t = (
        delta.filter(F.col("grupo")=="teste")
        .select(
            "bucket_delta",
            "baseline_DDE","piloto_DDE","DDE_delta",
            "baseline_PctBin","piloto_PctBin","PctBin_delta",
            "baseline_PctRec","piloto_PctRec","PctRec_delta"
        )
        .withColumnRenamed("baseline_DDE","baseline_DDE_teste")
        .withColumnRenamed("piloto_DDE","piloto_DDE_teste")
        .withColumnRenamed("DDE_delta","DDE_delta_teste")
        .withColumnRenamed("baseline_PctBin","baseline_PctBin_teste")
        .withColumnRenamed("piloto_PctBin","piloto_PctBin_teste")
        .withColumnRenamed("PctBin_delta","PctRupturaBinario_delta_teste")
        .withColumnRenamed("baseline_PctRec","baseline_PctRec_teste")
        .withColumnRenamed("piloto_PctRec","piloto_PctRec_teste")
        .withColumnRenamed("PctRec_delta","PctRupturaReceita_delta_teste")
    )

    c = (
        delta.filter(F.col("grupo")=="controle")
        .select(
            "bucket_delta",
            "baseline_DDE","piloto_DDE","DDE_delta",
            "baseline_PctBin","piloto_PctBin","PctBin_delta",
            "baseline_PctRec","piloto_PctRec","PctRec_delta"
        )
        .withColumnRenamed("baseline_DDE","baseline_DDE_controle")
        .withColumnRenamed("piloto_DDE","piloto_DDE_controle")
        .withColumnRenamed("DDE_delta","DDE_delta_controle")
        .withColumnRenamed("baseline_PctBin","baseline_PctBin_controle")
        .withColumnRenamed("piloto_PctBin","piloto_PctBin_controle")
        .withColumnRenamed("PctBin_delta","PctRupturaBinario_delta_controle")
        .withColumnRenamed("baseline_PctRec","baseline_PctRec_controle")
        .withColumnRenamed("piloto_PctRec","piloto_PctRec_controle")
        .withColumnRenamed("PctRec_delta","PctRupturaReceita_delta_controle")
    )

    did = (
        t.join(c, on="bucket_delta", how="inner")
         .withColumn("DDE_diff_in_diff",               F.round(F.col("DDE_delta_teste") - F.col("DDE_delta_controle"), 1))
         .withColumn("PctRupturaBinario_diff_in_diff", F.round(F.col("PctRupturaBinario_delta_teste") - F.col("PctRupturaBinario_delta_controle"), 1))
         .withColumn("PctRupturaReceita_diff_in_diff", F.round(F.col("PctRupturaReceita_delta_teste") - F.col("PctRupturaReceita_delta_controle"), 1))
    )

    # pivot por métrica; categoria primeiro
    df_pivot_bucket = (
        did.selectExpr(
            "bucket_delta",
            "stack(3, "
            " 'DDE',               DDE_diff_in_diff,               DDE_delta_teste,               DDE_delta_controle, "
            " baseline_DDE_teste,  piloto_DDE_teste,               baseline_DDE_controle,         piloto_DDE_controle, "
            " 'PctRupturaBinario', PctRupturaBinario_diff_in_diff, PctRupturaBinario_delta_teste, PctRupturaBinario_delta_controle, "
            " baseline_PctBin_teste, piloto_PctBin_teste,          baseline_PctBin_controle,      piloto_PctBin_controle, "
            " 'PctRupturaReceita', PctRupturaReceita_diff_in_diff, PctRupturaReceita_delta_teste, PctRupturaReceita_delta_controle, "
            " baseline_PctRec_teste, piloto_PctRec_teste,          baseline_PctRec_controle,      piloto_PctRec_controle "
            ") as (metrica, diff_in_diff, delta_teste, delta_controle, "
            "baseline_teste, piloto_teste, baseline_controle, piloto_controle)"
        )
        .withColumn("categoria", F.lit(categoria))
        .select(
            "categoria","bucket_delta","metrica",
            "diff_in_diff","delta_teste","delta_controle",
            "baseline_teste","piloto_teste","baseline_controle","piloto_controle"
        )
        .orderBy("categoria","metrica", "bucket_delta")
    )

    df_did_bucket[categoria] = df_pivot_bucket
    dfs_all.append(df_pivot_bucket)
    #df_pivot_bucket.display()

# consolidado opcional
if dfs_all:
    df_did_bucket_final = dfs_all[0]
    for d in dfs_all[1:]:
        df_did_bucket_final = df_did_bucket_final.unionByName(d, allowMissingColumns=True)
    df_did_bucket_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Buckets de DDE

# COMMAND ----------

# dicionário de saída
df_estoque_loja_porte_regiao_bucket = {}
df_counts_export = {}

for categoria in categorias_teste:
    df_estoque_loja_porte_regiao_bucket[categoria] = (
        df_estoque_loja_porte_regiao[categoria]
        .groupBy("CdFilial", "grupo", "periodo_analise")
        .agg(
            F.round(F.mean("DDE"), 1).alias("DDE_medio"),
            # mediana robusta em Spark
            F.round(F.percentile_approx("DDE", 0.5, 100), 1).alias("DDE_mediano")
        )
        # bucket para média
        .withColumn(
            "bucket_DDE_medio",
            F.when(F.col("DDE_medio").isNull(), F.lit("Nulo"))
             .when(F.col("DDE_medio") > 90, F.lit("90+"))
             .when(F.col("DDE_medio") >= 60, F.lit("60-90"))
             .when(F.col("DDE_medio") >= 30, F.lit("30-60"))
             .when(F.col("DDE_medio") >= 15, F.lit("15-30"))
             .when(F.col("DDE_medio") >= 0,  F.lit("0-15"))
             .otherwise(F.lit("Nulo"))  # negativos ou casos fora do esperado
        )
        # bucket para mediana
        .withColumn(
            "bucket_DDE_mediano",
            F.when(F.col("DDE_mediano").isNull(), F.lit("Nulo"))
             .when(F.col("DDE_mediano") > 90, F.lit("90+"))
             .when(F.col("DDE_mediano") >= 60, F.lit("60-90"))
             .when(F.col("DDE_mediano") >= 30, F.lit("30-60"))
             .when(F.col("DDE_mediano") >= 15, F.lit("15-30"))
             .when(F.col("DDE_mediano") >= 0,  F.lit("0-15"))
             .otherwise(F.lit("Nulo"))
        )
    )

    df_buckets = df_estoque_loja_porte_regiao_bucket[categoria]

    # --- Count por grupo e período (DDE_medio) ---
    df_counts_medio = (
        df_buckets
        .groupBy("grupo", "periodo_analise")
        .pivot("bucket_DDE_medio", ["0-15","15-30","30-60","60-90","90+","Nulo"])
        .count()
        .withColumn("Metrica", F.lit("DDE_medio"))
        .withColumn("Categoria", F.lit(categoria))
    )

    # --- Count por grupo e período (DDE_mediano) ---
    df_counts_mediano = (
        df_buckets
        .groupBy("grupo", "periodo_analise")
        .pivot("bucket_DDE_mediano", ["0-15","15-30","30-60","60-90","90+","Nulo"])
        .count()
        .withColumn("Metrica", F.lit("DDE_mediano"))
        .withColumn("Categoria", F.lit(categoria))
    )

    # --- Union dos dois ---
    df_counts_export[categoria] = df_counts_medio.unionByName(df_counts_mediano)

    # Exibir para inspeção
    df_counts_export[categoria].fillna(0).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação Visual - DDE Médio por Cortes

# COMMAND ----------

# -*- coding: utf-8 -*-
from pyspark.sql import functions as F
import plotly.express as px
import plotly.graph_objects as go

def create_dde_comparison_visualizations(df_estoque_loja_porte_regiao, df_comparacao, categorias_teste):
    """
    Cria visualizações comparativas de DDE Médio para os 4 cortes.
    Ajustes:
      - Barras bem cheias (bargap/bargroupgap pequenos)
      - Porte: cores por porte (mesma cor para teste/controle)
      - Região: cores por região (mesma cor para teste/controle)
      - Delta Merecimento: apenas TESTE, com palette por bucket
    """
    for categoria in categorias_teste:
        print(f"\n=== COMPARAÇÃO VISUAL DDE MÉDIO - {categoria} ===")
        create_tudo_cut_visualization(df_estoque_loja_porte_regiao[categoria], categoria)
        create_porte_cut_visualization(df_estoque_loja_porte_regiao[categoria], categoria)
        create_regiao_cut_visualization(df_estoque_loja_porte_regiao[categoria], categoria)
        create_delta_merecimento_cut_visualization(
            df_estoque_loja_porte_regiao[categoria],
            df_comparacao[categoria],
            categoria
        )

def create_tudo_cut_visualization(df_estoque, categoria):
    # Agregar por grupo e período
    df_base = (
        df_estoque
        .groupBy("grupo", "periodo_analise")
        .agg(F.round(F.median("DDE"), 1).alias("DDE_medio"))
    )
    # Pivot
    df_pivot = (
        df_base.groupBy("grupo")
        .pivot("periodo_analise").agg(F.first("DDE_medio")).fillna(0.0)
    )
    df_pandas = df_pivot.toPandas()

    # Valores com fallback 0.0
    def get_val(g, col):
        try:
            return float(df_pandas[df_pandas["grupo"]==g][col].iloc[0]) if col in df_pandas.columns else 0.0
        except Exception:
            return 0.0

    teste_baseline   = get_val("teste", "baseline")
    teste_piloto     = get_val("teste", "piloto")
    controle_baseline= get_val("controle", "baseline")
    controle_piloto  = get_val("controle", "piloto")

    delta_teste    = round(teste_piloto - teste_baseline, 1)
    delta_controle = round(controle_piloto - controle_baseline, 1)
    diff_in_diff   = round(delta_teste - delta_controle, 1)

    # Gráfico
    fig = go.Figure()
    colors = {'teste': ['#6BAED6', '#2171B5'], 'controle': ['#FDAE6B', '#E6550D']}

    fig.add_trace(go.Bar(name='Teste - Baseline',   x=['Teste'],    y=[teste_baseline],
                         marker_color=colors['teste'][0], opacity=0.85,
                         text=[f"{teste_baseline:.1f}"], textposition='outside'))
    fig.add_trace(go.Bar(name='Teste - Piloto',     x=['Teste'],    y=[teste_piloto],
                         marker_color=colors['teste'][1], opacity=1.0,
                         text=[f"{teste_piloto:.1f}"], textposition='outside'))
    fig.add_trace(go.Bar(name='Controle - Baseline',x=['Controle'], y=[controle_baseline],
                         marker_color=colors['controle'][0], opacity=0.85,
                         text=[f"{controle_baseline:.1f}"], textposition='outside'))
    fig.add_trace(go.Bar(name='Controle - Piloto',  x=['Controle'], y=[controle_piloto],
                         marker_color=colors['controle'][1], opacity=1.0,
                         text=[f"{controle_piloto:.1f}"], textposition='outside'))

    fig.update_layout(
        title=f"DDE Médio - Corte Tudo - {categoria}<br><sub>Δ Teste: {delta_teste:+.1f} | Δ Controle: {delta_controle:+.1f} | DiD: {diff_in_diff:+.1f}</sub>",
        xaxis_title="Grupo", yaxis_title="DDE Médio (dias)",
        barmode='group', paper_bgcolor="#F7F7F7", plot_bgcolor="#F7F7F7",
        height=480, font=dict(size=12),
        bargap=0.02, bargroupgap=0.01
    )
    fig.show()

def create_porte_cut_visualization(df_estoque, categoria):
    # Agregar
    df_base = (
        df_estoque
        .groupBy("grupo", "periodo_analise", "NmPorteLoja")
        .agg(F.round(F.median("DDE"), 1).alias("DDE_medio"))
        .dropna(subset=["NmPorteLoja"])
        .filter(F.col("NmPorteLoja") != '-')
    )
    df_pivot = (
        df_base.groupBy("grupo","NmPorteLoja")
        .pivot("periodo_analise").agg(F.first("DDE_medio")).fillna(0.0)
    )
    df_pandas = df_pivot.toPandas()

    fig = go.Figure()
    portes = sorted(df_pandas['NmPorteLoja'].unique())
    palette = px.colors.qualitative.Plotly
    color_map = {p: palette[i % len(palette)] for i, p in enumerate(portes)}

    def safe_val(df, g, col):
        try:
            return float(df[df["grupo"]==g][col].iloc[0]) if col in df.columns else 0.0
        except Exception:
            return 0.0

    for p in portes:
        d = df_pandas[df_pandas['NmPorteLoja']==p]
        tb = safe_val(d,"teste","baseline"); tp = safe_val(d,"teste","piloto")
        cb = safe_val(d,"controle","baseline"); cp = safe_val(d,"controle","piloto")

        # Teste
        fig.add_trace(go.Bar(name=f'{p} - Teste B', x=[f'{p} - Teste'], y=[tb],
                             marker_color=color_map[p], opacity=0.75,
                             text=[f"{tb:.1f}"], textposition='outside', showlegend=False))
        fig.add_trace(go.Bar(name=f'{p} - Teste P', x=[f'{p} - Teste'], y=[tp],
                             marker_color=color_map[p], opacity=1.0,
                             text=[f"{tp:.1f}"], textposition='outside', showlegend=False))
        # Controle
        fig.add_trace(go.Bar(name=f'{p} - Controle B', x=[f'{p} - Controle'], y=[cb],
                             marker_color=color_map[p], opacity=0.75,
                             text=[f"{cb:.1f}"], textposition='outside', showlegend=False))
        fig.add_trace(go.Bar(name=f'{p} - Controle P', x=[f'{p} - Controle'], y=[cp],
                             marker_color=color_map[p], opacity=1.0,
                             text=[f"{cp:.1f}"], textposition='outside', showlegend=False))

    fig.update_layout(
        title=f"DDE Médio - Corte Porte - {categoria}",
        xaxis_title="Porte da Loja", yaxis_title="DDE Médio (dias)",
        barmode='group', paper_bgcolor="#F7F7F7", plot_bgcolor="#F7F7F7",
        height=520, font=dict(size=12), xaxis_tickangle=-45,
        bargap=0.02, bargroupgap=0.01
    )
    fig.show()

def create_regiao_cut_visualization(df_estoque, categoria):
    # Agregar
    df_base = (
        df_estoque
        .groupBy("grupo", "periodo_analise", "NmRegiaoGeografica")
        .agg(F.round(F.median("DDE"), 1).alias("DDE_medio"))
        .dropna(subset=["NmRegiaoGeografica"])
    )
    df_pivot = (
        df_base.groupBy("grupo","NmRegiaoGeografica")
        .pivot("periodo_analise").agg(F.first("DDE_medio")).fillna(0.0)
    )
    df_pandas = df_pivot.toPandas()

    fig = go.Figure()
    regioes = sorted(df_pandas['NmRegiaoGeografica'].unique())
    palette = px.colors.qualitative.Plotly
    color_map = {r: palette[i % len(palette)] for i, r in enumerate(regioes)}

    def safe_val(df, g, col):
        try:
            return float(df[df["grupo"]==g][col].iloc[0]) if col in df.columns else 0.0
        except Exception:
            return 0.0

    for r in regioes:
        d = df_pandas[df_pandas['NmRegiaoGeografica']==r]
        tb = safe_val(d,"teste","baseline"); tp = safe_val(d,"teste","piloto")
        cb = safe_val(d,"controle","baseline"); cp = safe_val(d,"controle","piloto")

        # Teste
        fig.add_trace(go.Bar(name=f'{r} - Teste B', x=[f'{r} - Teste'], y=[tb],
                             marker_color=color_map[r], opacity=0.75,
                             text=[f"{tb:.1f}"], textposition='outside', showlegend=False))
        fig.add_trace(go.Bar(name=f'{r} - Teste P', x=[f'{r} - Teste'], y=[tp],
                             marker_color=color_map[r], opacity=1.0,
                             text=[f"{tp:.1f}"], textposition='outside', showlegend=False))
        # Controle
        fig.add_trace(go.Bar(name=f'{r} - Controle B', x=[f'{r} - Controle'], y=[cb],
                             marker_color=color_map[r], opacity=0.75,
                             text=[f"{cb:.1f}"], textposition='outside', showlegend=False))
        fig.add_trace(go.Bar(name=f'{r} - Controle P', x=[f'{r} - Controle'], y=[cp],
                             marker_color=color_map[r], opacity=1.0,
                             text=[f"{cp:.1f}"], textposition='outside', showlegend=False))

    fig.update_layout(
        title=f"DDE Médio - Corte Região - {categoria}",
        xaxis_title="Região", yaxis_title="DDE Médio (dias)",
        barmode='group', paper_bgcolor="#F7F7F7", plot_bgcolor="#F7F7F7",
        height=520, font=dict(size=12), xaxis_tickangle=-45,
        bargap=0.0, bargroupgap=0.005
    )
    fig.show()

def create_delta_merecimento_cut_visualization(df_estoque, df_comparacao, categoria):
    """
    Apenas TESTE, com palette por bucket_delta.
    """
    df_joined = (
        df_estoque
        .join(
            df_comparacao.select("CdFilial","grupo_de_necessidade","bucket_delta").distinct(),
            on=["CdFilial","grupo_de_necessidade"], how="inner"
        )
    )
    df_base = (
        df_joined
        .groupBy("grupo", "periodo_analise", "bucket_delta")
        .agg(F.round(F.median("DDE"), 1).alias("DDE_medio"))
        .dropna(subset=["bucket_delta"])
    )
    df_pivot = (
        df_base.groupBy("grupo","bucket_delta")
        .pivot("periodo_analise").agg(F.first("DDE_medio")).fillna(0.0)
    )
    df_pandas = df_pivot.toPandas()

    fig = go.Figure()
    # Apenas TESTE
    buckets = sorted(df_pandas['bucket_delta'].unique())
    palette = px.colors.qualitative.Plotly
    color_map = {b: palette[i % len(palette)] for i, b in enumerate(buckets)}

    def safe_val_bucket(df, col):
        try:
            return float(df[col].iloc[0]) if col in df.columns else 0.0
        except Exception:
            return 0.0

    for b in buckets:
        d_all = df_pandas[(df_pandas['bucket_delta']==b) & (df_pandas['grupo']=='teste')]
        tb = safe_val_bucket(d_all, 'baseline')
        tp = safe_val_bucket(d_all, 'piloto')

        # Barras só do teste
        fig.add_trace(go.Bar(name=f'{b} - Teste B', x=[f'{b} - Teste'], y=[tb],
                             marker_color=color_map[b], opacity=0.75,
                             text=[f"{tb:.1f}"], textposition='outside', showlegend=False))
        fig.add_trace(go.Bar(name=f'{b} - Teste P', x=[f'{b} - Teste'], y=[tp],
                             marker_color=color_map[b], opacity=1.0,
                             text=[f"{tp:.1f}"], textposition='outside', showlegend=False))

    fig.update_layout(
        title=f"DDE Médio - Corte Delta Merecimento (Teste) - {categoria}",
        xaxis_title="Delta Merecimento", yaxis_title="DDE Médio (dias)",
        barmode='group', paper_bgcolor="#F7F7F7", plot_bgcolor="#F7F7F7",
        height=520, font=dict(size=12), xaxis_tickangle=-45,
        bargap=0.0, bargroupgap=0.005
    )
    fig.show()

# Execução
# create_dde_comparison_visualizations(df_estoque_loja_porte_regiao, df_comparacao, categorias_teste)

# Executar visualizações comparativas
create_dde_comparison_visualizations(df_estoque_loja_porte_regiao, df_comparacao, categorias_teste)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de acurácia realizada

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resultado factual dos últimos 7 dias

# COMMAND ----------

janela_factual = (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d")

print(janela_factual)

# partindo do df_proporcao_factual já agregado por CdFilial × grupo_de_necessidade
w_grp = Window.partitionBy("grupo_de_necessidade")

df_proporcao_factual = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
    .filter(F.col('DtAtual') >= janela_factual)    
    .filter(F.col("NmAgrupamentoDiretoriaSetor").isin('DIRETORIA TELEFONIA CELULAR', 'DIRETORIA DE TELAS'))
    .fillna(0, subset=['deltaRuptura', 'QtMercadoria'])
    .withColumn("QtDemanda", F.col("QtMercadoria") + F.col("deltaRuptura"))
    .join(
        spark.table('databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia'),
          how='inner',
          on='CdSku')
    .dropna(subset='grupo_de_necessidade')
    .withColumn('grupo_de_necessidade', F.col("gemeos"))
    .groupBy('CdFilial', 'grupo_de_necessidade')
    .agg(
        F.round(F.sum('QtDemanda'), 0).alias('QtDemanda'),
    )
    .withColumn("Total_QtDemanda", F.round(F.sum(F.col("QtDemanda")).over(w_grp), 0))
    .withColumn(
        "Proporcao_Interna_QtDemanda",
        F.when(F.col("Total_QtDemanda") > 0, F.col("QtDemanda") / F.col("Total_QtDemanda")).otherwise(F.lit(0.0))
    )
    .withColumn("Percentual_QtDemanda", F.round(F.col("Proporcao_Interna_QtDemanda") * 100.0, 2))
    .select('grupo_de_necessidade', 'CdFilial', 'Percentual_QtDemanda', 'QtDemanda', 'Total_QtDemanda')
    .filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
)

df_proporcao_factual.limit(1).display()

df_acuracia = {}

for categoria in categorias_teste:
  df_acuracia[categoria] = (
      df_proporcao_factual
      .join(
        df_merecimento_offline[categoria]
        .drop("CdSku", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica")
        .dropDuplicates(),
        on=['CdFilial', 'grupo_de_necessidade'],
        how='inner'
      )
      .fillna(0.0, subset=['Percentual_QtDemanda', 'merecimento_percentual'])
  )

  df_acuracia[categoria].limit(1).display()

# COMMAND ----------

from pyspark.sql import functions as F

dim_loja = (
    spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
    .select("CdFilial", "NmPorteLoja", "NmRegiaoGeografica")
    .dropDuplicates(["CdFilial"])
)

for categoria in categorias_teste:
    df_base = (
        df_acuracia[categoria]
        .join(dim_loja, on="CdFilial", how="left")
    )

    denom = F.abs(F.col("merecimento_percentual")) + F.abs(F.col("Percentual_QtDemanda"))
    smape_comp = F.when(denom == 0, F.lit(0.0))\
                  .otherwise(200.0 * F.abs(F.col("merecimento_percentual") - F.col("Percentual_QtDemanda")) / denom)

    df_ws = (
        df_base
        .withColumn("WSMAPE_comp", smape_comp * F.col("QtDemanda"))
    )

    # Overall
    df_result_overall = df_ws.agg(
        (F.sum("WSMAPE_comp") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNova")
    ).withColumn("categoria", F.lit(categoria))

    # Por porte
    df_result_por_porte = (
        df_ws.groupBy("NmPorteLoja")
        .agg(
            (F.sum("WSMAPE_comp") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNova")
        )
        .withColumn("categoria", F.lit(categoria))
        .select("categoria", "NmPorteLoja", "WSMAPE_MatrizNova")
        .filter(F.col("NmPorteLoja").isin('PORTE 1', 'PORTE 2', 'PORTE 3', 'PORTE 4', 'PORTE 5', 'PORTE 6'))
        .orderBy("NmPorteLoja")
    )

    # Por região
    df_result_por_regiao = (
        df_ws.groupBy("NmRegiaoGeografica")
        .agg(
            (F.sum("WSMAPE_comp") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNova")
        )
        .withColumn("categoria", F.lit(categoria))
        .select("categoria", "NmRegiaoGeografica", "WSMAPE_MatrizNova")
    )

    # Exibir
    df_result_overall.display()
    df_result_por_porte.display()
    #df_result_por_regiao.display()

# COMMAND ----------

from pyspark.sql import functions as F

df_porte_percentual = {}

for categoria in categorias_teste:
    df_base = df_estoque_loja_porte_regiao[categoria]

    # total por porte
    df_por_porte = (
        df_base.groupBy("NmPorteLoja")
        .agg(F.sum("QtMercadoria").alias("QtMercadoria_total"))
    )

    # total geral
    total_mercadoria = df_por_porte.agg(F.sum("QtMercadoria_total")).collect()[0][0]

    # percentual
    df_por_porte = (
        df_por_porte
        .withColumn("percentual", (F.col("QtMercadoria_total") / F.lit(total_mercadoria)) * 100)
        .withColumn("categoria", F.lit(categoria))
    )

    # somatório portes 3–6
    df_soma_3a6 = (
        df_por_porte
        .filter(F.col("NmPorteLoja").rlike("3|4|5|6"))
        .agg(F.sum("QtMercadoria_total").alias("QtMercadoria_total_3a6"))
        .withColumn("percentual_3a6", (F.col("QtMercadoria_total_3a6") / F.lit(total_mercadoria)) * 100)
        .withColumn("categoria", F.lit(categoria))
    )

    df_porte_percentual[categoria] = {
        "por_porte": df_por_porte,
        "soma_3a6": df_soma_3a6
    }

    # exibir
    df_por_porte.display()
    df_soma_3a6.display()

# COMMAND ----------

# === Plotly scatters ===
import plotly.express as px

df_filial_mean = {}
pdf = {}

for categoria in categorias_teste:
    df_filial_mean[categoria] = (
        df_acuracia[categoria]
        .join(
            spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
            .select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica"),
            on="CdFilial",
            how="left"
        )
        .groupBy("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica")
        .agg(
            F.avg("Percentual_QtDemanda").alias("x_real"),
            F.avg("merecimento_percentual").alias("y_nova"),
        )
        # Extrai o número do porte
        .withColumn("PorteNum_raw", F.regexp_replace(F.col("NmPorteLoja"), "[^0-9]", ""))
        .withColumn("PorteNum", F.col("PorteNum_raw").cast("int"))
        # Garante valores válidos 1–6
        .withColumn("PorteNum", F.when(F.col("PorteNum").between(1,6), F.col("PorteNum")*1.5).otherwise(F.lit(1)))
    )


    pdf[categoria] = df_filial_mean[categoria].toPandas()
    print(categoria)


color_map = {
    "Sudeste": "#0d3b66",       # azul mais escuro
    "Sul": "#5dade2",           # azul mais claro
    "Centro Oeste": "#ff9896",  # vermelho claro
    "Nordeste": "#1f77b4",      # azul intermediário
    "Norte": "#d62728",         # vermelho
}

def make_scatter(df, y_col, y_label, categoria):
    fig = px.scatter(
        df,
        x="x_real",
        y=y_col,
        size="PorteNum",                 
        color="NmRegiaoGeografica",      
        color_discrete_map=color_map,
        size_max=12,
        opacity=0.75,
        labels={
            "x_real": "Percentual_QtDemanda médio por filial (real)",
            y_col:   y_label,
            "NmRegiaoGeografica": "Região Geográfica",
            "PorteNum": "Porte"
        },
        hover_data={
            "CdFilial": True,
            "NmFilial": True,
            "NmPorteLoja": True,
            "NmRegiaoGeografica": True,
            "x_real": ":.3f",
            y_col: ":.3f",
        }
    )
    fig.update_layout(
        title=dict(
            text=f"Real vs Matriz Nova – por filial ({categoria})",  # categoria no título
            x=0.5, xanchor="center"
        ),
        paper_bgcolor="#f2f2f2",
        plot_bgcolor="#f2f2f2",
        margin=dict(l=40, r=40, t=60, b=40),
        xaxis=dict(
            showgrid=True, gridwidth=0.3, gridcolor="rgba(0,0,0,0.08)",
            zeroline=False, range=[0,0.6]
        ),
        yaxis=dict(
            showgrid=True, gridwidth=0.3, gridcolor="rgba(0,0,0,0.08)",
            zeroline=False, range=[0,2]
        ),
        legend=dict(
            title="Região Geográfica",
            orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5
        ),
        width=1200,
        height=400,
    )

    fig.update_traces(marker=dict(line=dict(width=0.6, color="rgba(0,0,0,0.35)")))
    fig.add_shape(
        type="line", x0=0, y0=0, x1=1, y1=1,
        line=dict(color="rgba(0,0,0,0.45)", width=0.2, dash="dash")
    )
    return fig

for categoria in categorias_teste:

    fig_nova = make_scatter(pdf[categoria], "y_nova",
                            "PercMatrizNova médio por filial (previsão)",
                            "Real vs Matriz Nova – por filial")

    fig_nova.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: análises de acompanhamento

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Consequência dos deltas mais relevantes vs Matriz antiga - DDE ou ruptura 
# MAGIC 2. Buckets de DDE - delta versus categoria
# MAGIC 3. DDE x Ruptura  - delta versus categoria
# MAGIC 4. Proporção prevista vs real - janela desde inicio do teste

# COMMAND ----------

# -*- coding: utf-8 -*-
from pyspark.sql import functions as F
import plotly.express as px
import plotly.graph_objects as go

def create_dde_comparison_visualizations_teste_only(df_estoque_loja_porte_regiao, df_comparacao, categorias_teste):
    """
    Visualizações de DDE Médio usando apenas GRUPO = 'teste'
    Cortes: Tudo, Porte, Região, Delta Merecimento.
    Barras largas. Paletas por porte/região/bucket.
    """
    for categoria in categorias_teste:
        print(f"\n=== COMPARAÇÃO VISUAL DDE MÉDIO - TESTE APENAS - {categoria} ===")
        create_tudo_cut_visualization_teste_only(df_estoque_loja_porte_regiao[categoria], categoria)
        create_porte_cut_visualization_teste_only(df_estoque_loja_porte_regiao[categoria], categoria)
        create_regiao_cut_visualization_teste_only(df_estoque_loja_porte_regiao[categoria], categoria)
        create_delta_merecimento_cut_visualization_teste_only(
            df_estoque_loja_porte_regiao[categoria],
            df_comparacao[categoria],
            categoria
        )

def _safe_get(df_pd, col):
    try:
        return float(df_pd[col].iloc[0]) if col in df_pd.columns else 0.0
    except Exception:
        return 0.0

# ==== CORTE TUDO (apenas teste) ====
def create_tudo_cut_visualization_teste_only(df_estoque, categoria):
    df_base = (
        df_estoque
        .filter(F.col("grupo") == "teste")
        .groupBy("periodo_analise")
        .agg(F.round(F.median("DDE"), 1).alias("DDE_medio"))
    )

    df_pivot = (
        df_base.groupBy()
        .pivot("periodo_analise")
        .agg(F.first("DDE_medio"))
        .fillna(0.0)
    )

    df_pd = df_pivot.toPandas()
    base = _safe_get(df_pd, "baseline")
    pilo = _safe_get(df_pd, "piloto")
    delta = round(pilo - base, 1)

    fig = go.Figure()
    # uma única cor para teste
    color_test = "#2171B5"

    fig.add_trace(go.Bar(name='Teste - Baseline', x=['Teste'], y=[base],
                         marker_color=color_test, opacity=0.80,
                         text=[f"{base:.1f}"], textposition='outside'))
    fig.add_trace(go.Bar(name='Teste - Piloto',   x=['Teste'], y=[pilo],
                         marker_color=color_test, opacity=1.00,
                         text=[f"{pilo:.1f}"], textposition='outside'))

    fig.update_layout(
        title=f"DDE Médio - Corte Tudo (Teste) - {categoria}<br><sub>Δ Teste: {delta:+.1f}</sub>",
        xaxis_title="Grupo", yaxis_title="DDE Médio (dias)",
        barmode='group', paper_bgcolor="#F7F7F7", plot_bgcolor="#F7F7F7",
        height=420, font=dict(size=12),
        bargap=0.02, bargroupgap=0.01
    )
    fig.show()

# ==== CORTE PORTE (apenas teste, cores por porte) ====
def create_porte_cut_visualization_teste_only(df_estoque, categoria):
    df_base = (
        df_estoque
        .filter(F.col("grupo") == "teste")
        .groupBy("periodo_analise", "NmPorteLoja")
        .agg(F.round(F.median("DDE"), 1).alias("DDE_medio"))
        .dropna(subset=["NmPorteLoja"])
        .filter(F.col("NmPorteLoja") != "-")
    )

    df_pivot = (
        df_base.groupBy("NmPorteLoja")
        .pivot("periodo_analise")
        .agg(F.first("DDE_medio"))
        .fillna(0.0)
    )

    df_pd = df_pivot.toPandas()
    portes = sorted(df_pd['NmPorteLoja'].unique())
    palette = px.colors.qualitative.Plotly
    color_map = {p: palette[i % len(palette)] for i, p in enumerate(portes)}

    fig = go.Figure()
    for p in portes:
        d = df_pd[df_pd['NmPorteLoja'] == p]
        base = _safe_get(d, "baseline")
        pilo = _safe_get(d, "piloto")

        fig.add_trace(go.Bar(name=f'{p} - Baseline', x=[p], y=[base],
                             marker_color=color_map[p], opacity=0.80,
                             text=[f"{base:.1f}"], textposition='outside', showlegend=False))
        fig.add_trace(go.Bar(name=f'{p} - Piloto',   x=[p], y=[pilo],
                             marker_color=color_map[p], opacity=1.00,
                             text=[f"{pilo:.1f}"], textposition='outside', showlegend=False))

    fig.update_layout(
        title=f"DDE Médio - Corte Porte (Teste) - {categoria}",
        xaxis_title="Porte da Loja", yaxis_title="DDE Médio (dias)",
        barmode='group', paper_bgcolor="#F7F7F7", plot_bgcolor="#F7F7F7",
        height=520, font=dict(size=12), xaxis_tickangle=-45,
        bargap=0.02, bargroupgap=0.01
    )
    fig.show()

# ==== CORTE REGIÃO (apenas teste, cores por região) ====
def create_regiao_cut_visualization_teste_only(df_estoque, categoria):
    df_base = (
        df_estoque
        .filter(F.col("grupo") == "teste")
        .groupBy("periodo_analise", "NmRegiaoGeografica")
        .agg(F.round(F.median("DDE"), 1).alias("DDE_medio"))
        .dropna(subset=["NmRegiaoGeografica"])
    )

    df_pivot = (
        df_base.groupBy("NmRegiaoGeografica")
        .pivot("periodo_analise")
        .agg(F.first("DDE_medio"))
        .fillna(0.0)
    )

    df_pd = df_pivot.toPandas()
    regioes = sorted(df_pd['NmRegiaoGeografica'].unique())
    palette = px.colors.qualitative.Plotly
    color_map = {r: palette[i % len(palette)] for i, r in enumerate(regioes)}

    fig = go.Figure()
    for r in regioes:
        d = df_pd[df_pd['NmRegiaoGeografica'] == r]
        base = _safe_get(d, "baseline")
        pilo = _safe_get(d, "piloto")

        fig.add_trace(go.Bar(name=f'{r} - Baseline', x=[r], y=[base],
                             marker_color=color_map[r], opacity=0.80,
                             text=[f"{base:.1f}"], textposition='outside', showlegend=False))
        fig.add_trace(go.Bar(name=f'{r} - Piloto',   x=[r], y=[pilo],
                             marker_color=color_map[r], opacity=1.00,
                             text=[f"{pilo:.1f}"], textposition='outside', showlegend=False))

    fig.update_layout(
        title=f"DDE Médio - Corte Região (Teste) - {categoria}",
        xaxis_title="Região", yaxis_title="DDE Médio (dias)",
        barmode='group', paper_bgcolor="#F7F7F7", plot_bgcolor="#F7F7F7",
        height=520, font=dict(size=12), xaxis_tickangle=-45,
        bargap=0.02, bargroupgap=0.01
    )
    fig.show()

# ==== CORTE DELTA MERECIMENTO (apenas teste, paleta por bucket) ====
def create_delta_merecimento_cut_visualization_teste_only(df_estoque, df_comparacao, categoria):
    df_joined = (
        df_estoque
        .join(
            df_comparacao.select("CdFilial","grupo_de_necessidade","bucket_delta").distinct(),
            on=["CdFilial","grupo_de_necessidade"], how="inner"
        )
        .filter(F.col("grupo") == "teste")
    )

    df_base = (
        df_joined
        .groupBy("periodo_analise", "bucket_delta")
        .agg(F.round(F.median("DDE"), 1).alias("DDE_medio"))
        .dropna(subset=["bucket_delta"])
    )

    df_pivot = (
        df_base.groupBy("bucket_delta")
        .pivot("periodo_analise")
        .agg(F.first("DDE_medio"))
        .fillna(0.0)
    )

    df_pd = df_pivot.toPandas()
    buckets = sorted(df_pd['bucket_delta'].unique())
    palette = px.colors.qualitative.Plotly
    color_map = {b: palette[i % len(palette)] for i, b in enumerate(buckets)}

    fig = go.Figure()
    for b in buckets:
        d = df_pd[df_pd['bucket_delta'] == b]
        base = _safe_get(d, "baseline")
        pilo = _safe_get(d, "piloto")

        fig.add_trace(go.Bar(name=f'{b} - Baseline', x=[b], y=[base],
                             marker_color=color_map[b], opacity=0.80,
                             text=[f"{base:.1f}"], textposition='outside', showlegend=False))
        fig.add_trace(go.Bar(name=f'{b} - Piloto',   x=[b], y=[pilo],
                             marker_color=color_map[b], opacity=1.00,
                             text=[f"{pilo:.1f}"], textposition='outside', showlegend=False))

    fig.update_layout(
        title=f"DDE Médio - Corte Delta Merecimento (Teste) - {categoria}",
        xaxis_title="Delta Merecimento", yaxis_title="DDE Médio (dias)",
        barmode='group', paper_bgcolor="#F7F7F7", plot_bgcolor="#F7F7F7",
        height=520, font=dict(size=12), xaxis_tickangle=-45,
        bargap=0.02, bargroupgap=0.01
    )
    fig.show()

# Execução
create_dde_comparison_visualizations_teste_only(df_estoque_loja_porte_regiao, df_comparacao, categorias_teste)

# COMMAND ----------


