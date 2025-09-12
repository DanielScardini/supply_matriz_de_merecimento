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

data_inicio = "2025-09-01"
fim_baseline = "2025-09-05"

inicio_teste = "2025-09-05"

categorias_teste = ['TELAS', 'TELEFONIA']

dict_diretorias = {
  'TELAS': 'TVS',
  'TELEFONIA': 'TELEFONIA CELULAR'
}



# COMMAND ----------

def get_janela(inicio_teste: str, days_back: int = 3) -> str:
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

janela_teste = get_janela(inicio_teste, days_back=3)
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
        .orderBy('bucket_delta')
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
# MAGIC ## Comparação Visual - DDE Médio por Cortes

# COMMAND ----------

def create_dde_comparison_visualizations(df_estoque_loja_porte_regiao, df_comparacao, categorias_teste):
    """
    Cria visualizações comparativas de DDE Médio para os 4 cortes
    """
    for categoria in categorias_teste:
        print(f"\n=== COMPARAÇÃO VISUAL DDE MÉDIO - {categoria} ===")
        
        # 1. CORTE "TUDO" - Agregado geral
        create_tudo_cut_visualization(df_estoque_loja_porte_regiao[categoria], categoria)
        
        # 2. CORTE "PORTE" - Por porte de loja
        create_porte_cut_visualization(df_estoque_loja_porte_regiao[categoria], categoria)
        
        # 3. CORTE "REGIÃO" - Por região geográfica
        create_regiao_cut_visualization(df_estoque_loja_porte_regiao[categoria], categoria)
        
        # 4. CORTE "DELTA MERECIMENTO" - Por delta merecimento
        create_delta_merecimento_cut_visualization(
            df_estoque_loja_porte_regiao[categoria], 
            df_comparacao[categoria], 
            categoria
        )

def create_tudo_cut_visualization(df_estoque, categoria):
    """
    Cria visualização para corte "Tudo" - agregado geral
    """
    # Agregar por grupo e período
    df_base = (
        df_estoque
        .groupBy("grupo", "periodo_analise")
        .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio")
        )
    )
    
    # Pivotar para ter baseline e piloto em colunas
    df_pivot = (
        df_base
        .groupBy("grupo")
        .pivot("periodo_analise")
        .agg(F.first("DDE_medio"))
        .fillna(0.0)
    )
    
    # Converter para pandas
    df_pandas = df_pivot.toPandas()
    
    # Criar gráfico
    fig = go.Figure()
    
    # Cores harmoniosas
    colors = {
        'teste': ['#1f77b4', '#4a90e2'],  # Azul - baseline mais claro, piloto mais escuro
        'controle': ['#ff7f0e', '#ffa500']  # Laranja - baseline mais claro, piloto mais escuro
    }
    
    # Adicionar barras para Teste
    fig.add_trace(go.Bar(
        name='Teste - Baseline',
        x=['Teste'],
        y=[df_pandas[df_pandas['grupo'] == 'teste']['baseline'].iloc[0] if 'baseline' in df_pandas.columns else 0],
        marker_color=colors['teste'][0],
        opacity=0.6,
        text=[f"{df_pandas[df_pandas['grupo'] == 'teste']['baseline'].iloc[0]:.1f}" if 'baseline' in df_pandas.columns else "0.0"],
        textposition='outside'
    ))
    
    fig.add_trace(go.Bar(
        name='Teste - Piloto',
        x=['Teste'],
        y=[df_pandas[df_pandas['grupo'] == 'teste']['piloto'].iloc[0] if 'piloto' in df_pandas.columns else 0],
        marker_color=colors['teste'][1],
        opacity=1.0,
        text=[f"{df_pandas[df_pandas['grupo'] == 'teste']['piloto'].iloc[0]:.1f}" if 'piloto' in df_pandas.columns else "0.0"],
        textposition='outside'
    ))
    
    # Adicionar barras para Controle
    fig.add_trace(go.Bar(
        name='Controle - Baseline',
        x=['Controle'],
        y=[df_pandas[df_pandas['grupo'] == 'controle']['baseline'].iloc[0] if 'baseline' in df_pandas.columns else 0],
        marker_color=colors['controle'][0],
        opacity=0.6,
        text=[f"{df_pandas[df_pandas['grupo'] == 'controle']['baseline'].iloc[0]:.1f}" if 'baseline' in df_pandas.columns else "0.0"],
        textposition='outside'
    ))
    
    fig.add_trace(go.Bar(
        name='Controle - Piloto',
        x=['Controle'],
        y=[df_pandas[df_pandas['grupo'] == 'controle']['piloto'].iloc[0] if 'piloto' in df_pandas.columns else 0],
        marker_color=colors['controle'][1],
        opacity=1.0,
        text=[f"{df_pandas[df_pandas['grupo'] == 'controle']['piloto'].iloc[0]:.1f}" if 'piloto' in df_pandas.columns else "0.0"],
        textposition='outside'
    ))
    
    # Calcular e mostrar diffs
    teste_baseline = df_pandas[df_pandas['grupo'] == 'teste']['baseline'].iloc[0] if 'baseline' in df_pandas.columns else 0
    teste_piloto = df_pandas[df_pandas['grupo'] == 'teste']['piloto'].iloc[0] if 'piloto' in df_pandas.columns else 0
    controle_baseline = df_pandas[df_pandas['grupo'] == 'controle']['baseline'].iloc[0] if 'baseline' in df_pandas.columns else 0
    controle_piloto = df_pandas[df_pandas['grupo'] == 'controle']['piloto'].iloc[0] if 'piloto' in df_pandas.columns else 0
    
    delta_teste = teste_piloto - teste_baseline
    delta_controle = controle_piloto - controle_baseline
    diff_in_diff = delta_teste - delta_controle
    
    # Configurar layout
    fig.update_layout(
        title=f"DDE Médio - Corte Tudo - {categoria}<br><sub>Delta Teste: {delta_teste:+.1f} | Delta Controle: {delta_controle:+.1f} | Diff-in-Diff: {diff_in_diff:+.1f}</sub>",
        xaxis_title="Grupo",
        yaxis_title="DDE Médio (dias)",
        barmode='group',
        paper_bgcolor="#F2F2F2",
        plot_bgcolor="white",
        height=500,
        font=dict(size=12),
        bargap=0.3,  # Espaçamento entre grupos de barras
        bargroupgap=0.1  # Espaçamento entre barras do mesmo grupo
    )
    
    fig.show()

def create_porte_cut_visualization(df_estoque, categoria):
    """
    Cria visualização para corte "Porte" - por porte de loja
    """
    # Agregar por grupo, período e porte
    df_base = (
        df_estoque
        .groupBy("grupo", "periodo_analise", "NmPorteLoja")
        .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio")
        )
        .dropna(subset=["NmPorteLoja"])
    )
    
    # Pivotar para ter baseline e piloto em colunas
    df_pivot = (
        df_base
        .groupBy("grupo", "NmPorteLoja")
        .pivot("periodo_analise")
        .agg(F.first("DDE_medio"))
        .fillna(0.0)
    )
    
    # Converter para pandas
    df_pandas = df_pivot.toPandas()
    
    # Criar gráfico
    fig = go.Figure()
    
    # Cores harmoniosas
    colors = {
        'teste': ['#1f77b4', '#4a90e2'],  # Azul
        'controle': ['#ff7f0e', '#ffa500']  # Laranja
    }
    
    # Obter portes únicos
    portes = sorted(df_pandas['NmPorteLoja'].unique())
    
    for porte in portes:
        porte_data = df_pandas[df_pandas['NmPorteLoja'] == porte]
        
        # Teste
        teste_baseline = porte_data[porte_data['grupo'] == 'teste']['baseline'].iloc[0] if 'baseline' in porte_data.columns else 0
        teste_piloto = porte_data[porte_data['grupo'] == 'teste']['piloto'].iloc[0] if 'piloto' in porte_data.columns else 0
        
        # Controle
        controle_baseline = porte_data[porte_data['grupo'] == 'controle']['baseline'].iloc[0] if 'baseline' in porte_data.columns else 0
        controle_piloto = porte_data[porte_data['grupo'] == 'controle']['piloto'].iloc[0] if 'piloto' in porte_data.columns else 0
        
        # Adicionar barras
        fig.add_trace(go.Bar(
            name=f'{porte} - Teste B',
            x=[f'{porte} - Teste'],
            y=[teste_baseline],
            marker_color=colors['teste'][0],
            opacity=0.6,
            text=[f"{teste_baseline:.1f}"],
            textposition='outside',
            showlegend=False
        ))
        
        fig.add_trace(go.Bar(
            name=f'{porte} - Teste P',
            x=[f'{porte} - Teste'],
            y=[teste_piloto],
            marker_color=colors['teste'][1],
            opacity=1.0,
            text=[f"{teste_piloto:.1f}"],
            textposition='outside',
            showlegend=False
        ))
        
        fig.add_trace(go.Bar(
            name=f'{porte} - Controle B',
            x=[f'{porte} - Controle'],
            y=[controle_baseline],
            marker_color=colors['controle'][0],
            opacity=0.6,
            text=[f"{controle_baseline:.1f}"],
            textposition='outside',
            showlegend=False
        ))
        
        fig.add_trace(go.Bar(
            name=f'{porte} - Controle P',
            x=[f'{porte} - Controle'],
            y=[controle_piloto],
            marker_color=colors['controle'][1],
            opacity=1.0,
            text=[f"{controle_piloto:.1f}"],
            textposition='outside',
            showlegend=False
        ))
    
    # Configurar layout
    fig.update_layout(
        title=f"DDE Médio - Corte Porte - {categoria}",
        xaxis_title="Porte da Loja",
        yaxis_title="DDE Médio (dias)",
        barmode='group',
        paper_bgcolor="#F2F2F2",
        plot_bgcolor="white",
        height=500,
        font=dict(size=12),
        xaxis_tickangle=-45,
        bargap=0.2,  # Espaçamento entre grupos de barras
        bargroupgap=0.1  # Espaçamento entre barras do mesmo grupo
    )
    
    fig.show()

def create_regiao_cut_visualization(df_estoque, categoria):
    """
    Cria visualização para corte "Região" - por região geográfica
    """
    # Agregar por grupo, período e região
    df_base = (
        df_estoque
        .groupBy("grupo", "periodo_analise", "NmRegiaoGeografica")
        .agg(
            F.round(F.median("DDE"), 1).alias("DDE_medio")
        )
        .dropna(subset=["NmRegiaoGeografica"])
    )
    
    # Pivotar para ter baseline e piloto em colunas
    df_pivot = (
        df_base
        .groupBy("grupo", "NmRegiaoGeografica")
        .pivot("periodo_analise")
        .agg(F.first("DDE_medio"))
        .fillna(0.0)
    )
    
    # Converter para pandas
    df_pandas = df_pivot.toPandas()
    
    # Criar gráfico
    fig = go.Figure()
    
    # Cores harmoniosas
    colors = {
        'teste': ['#1f77b4', '#4a90e2'],  # Azul
        'controle': ['#ff7f0e', '#ffa500']  # Laranja
    }
    
    # Obter regiões únicas
    regioes = sorted(df_pandas['NmRegiaoGeografica'].unique())
    
    for regiao in regioes:
        regiao_data = df_pandas[df_pandas['NmRegiaoGeografica'] == regiao]
        
        # Teste
        teste_baseline = regiao_data[regiao_data['grupo'] == 'teste']['baseline'].iloc[0] if 'baseline' in regiao_data.columns else 0
        teste_piloto = regiao_data[regiao_data['grupo'] == 'teste']['piloto'].iloc[0] if 'piloto' in regiao_data.columns else 0
        
        # Controle
        controle_baseline = regiao_data[regiao_data['grupo'] == 'controle']['baseline'].iloc[0] if 'baseline' in regiao_data.columns else 0
        controle_piloto = regiao_data[regiao_data['grupo'] == 'controle']['piloto'].iloc[0] if 'piloto' in regiao_data.columns else 0
        
        # Adicionar barras
        fig.add_trace(go.Bar(
            name=f'{regiao} - Teste B',
            x=[f'{regiao} - Teste'],
            y=[teste_baseline],
            marker_color=colors['teste'][0],
            opacity=0.6,
            text=[f"{teste_baseline:.1f}"],
            textposition='outside',
            showlegend=False
        ))
        
        fig.add_trace(go.Bar(
            name=f'{regiao} - Teste P',
            x=[f'{regiao} - Teste'],
            y=[teste_piloto],
            marker_color=colors['teste'][1],
            opacity=1.0,
            text=[f"{teste_piloto:.1f}"],
            textposition='outside',
            showlegend=False
        ))
        
        fig.add_trace(go.Bar(
            name=f'{regiao} - Controle B',
            x=[f'{regiao} - Controle'],
            y=[controle_baseline],
            marker_color=colors['controle'][0],
            opacity=0.6,
            text=[f"{controle_baseline:.1f}"],
            textposition='outside',
            showlegend=False
        ))
        
        fig.add_trace(go.Bar(
            name=f'{regiao} - Controle P',
            x=[f'{regiao} - Controle'],
            y=[controle_piloto],
            marker_color=colors['controle'][1],
            opacity=1.0,
            text=[f"{controle_piloto:.1f}"],
            textposition='outside',
            showlegend=False
        ))
    
    # Configurar layout
    fig.update_layout(
        title=f"DDE Médio - Corte Região - {categoria}",
        xaxis_title="Região",
        yaxis_title="DDE Médio (dias)",
        barmode='group',
        paper_bgcolor="#F2F2F2",
        plot_bgcolor="white",
        height=500,
        font=dict(size=12),
        xaxis_tickangle=-45,
        bargap=0.2,  # Espaçamento entre grupos de barras
        bargroupgap=0.1  # Espaçamento entre barras do mesmo grupo
    )
    
    fig.show()

def create_delta_merecimento_cut_visualization(df_estoque, df_comparacao, categoria):
    """
    Cria visualização para corte "Delta Merecimento" - por delta merecimento
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
            F.round(F.median("DDE"), 1).alias("DDE_medio")
        )
        .dropna(subset=["bucket_delta"])
    )
    
    # Pivotar para ter baseline e piloto em colunas
    df_pivot = (
        df_base
        .groupBy("grupo", "bucket_delta")
        .pivot("periodo_analise")
        .agg(F.first("DDE_medio"))
        .fillna(0.0)
    )
    
    # Converter para pandas
    df_pandas = df_pivot.toPandas()
    
    # Criar gráfico
    fig = go.Figure()
    
    # Cores harmoniosas
    colors = {
        'teste': ['#1f77b4', '#4a90e2'],  # Azul
        'controle': ['#ff7f0e', '#ffa500']  # Laranja
    }
    
    # Obter buckets únicos
    buckets = sorted(df_pandas['bucket_delta'].unique())
    
    for bucket in buckets:
        bucket_data = df_pandas[df_pandas['bucket_delta'] == bucket]
        
        # Verificar se existem dados para este bucket
        if bucket_data.empty:
            continue
            
        # Teste - com verificação de segurança
        teste_data = bucket_data[bucket_data['grupo'] == 'teste']
        if not teste_data.empty and 'baseline' in teste_data.columns:
            teste_baseline = teste_data['baseline'].iloc[0] if not teste_data['baseline'].isna().iloc[0] else 0
        else:
            teste_baseline = 0
            
        if not teste_data.empty and 'piloto' in teste_data.columns:
            teste_piloto = teste_data['piloto'].iloc[0] if not teste_data['piloto'].isna().iloc[0] else 0
        else:
            teste_piloto = 0
        
        # Controle - com verificação de segurança
        controle_data = bucket_data[bucket_data['grupo'] == 'controle']
        if not controle_data.empty and 'baseline' in controle_data.columns:
            controle_baseline = controle_data['baseline'].iloc[0] if not controle_data['baseline'].isna().iloc[0] else 0
        else:
            controle_baseline = 0
            
        if not controle_data.empty and 'piloto' in controle_data.columns:
            controle_piloto = controle_data['piloto'].iloc[0] if not controle_data['piloto'].isna().iloc[0] else 0
        else:
            controle_piloto = 0
        
        # Adicionar barras
        fig.add_trace(go.Bar(
            name=f'{bucket} - Teste B',
            x=[f'{bucket} - Teste'],
            y=[teste_baseline],
            marker_color=colors['teste'][0],
            opacity=0.6,
            text=[f"{teste_baseline:.1f}"],
            textposition='outside',
            showlegend=False
        ))
        
        fig.add_trace(go.Bar(
            name=f'{bucket} - Teste P',
            x=[f'{bucket} - Teste'],
            y=[teste_piloto],
            marker_color=colors['teste'][1],
            opacity=1.0,
            text=[f"{teste_piloto:.1f}"],
            textposition='outside',
            showlegend=False
        ))
        
        fig.add_trace(go.Bar(
            name=f'{bucket} - Controle B',
            x=[f'{bucket} - Controle'],
            y=[controle_baseline],
            marker_color=colors['controle'][0],
            opacity=0.6,
            text=[f"{controle_baseline:.1f}"],
            textposition='outside',
            showlegend=False
        ))
        
        fig.add_trace(go.Bar(
            name=f'{bucket} - Controle P',
            x=[f'{bucket} - Controle'],
            y=[controle_piloto],
            marker_color=colors['controle'][1],
            opacity=1.0,
            text=[f"{controle_piloto:.1f}"],
            textposition='outside',
            showlegend=False
        ))
    
    # Configurar layout
    fig.update_layout(
        title=f"DDE Médio - Corte Delta Merecimento - {categoria}",
        xaxis_title="Delta Merecimento",
        yaxis_title="DDE Médio (dias)",
        barmode='group',
        paper_bgcolor="#F2F2F2",
        plot_bgcolor="white",
        height=500,
        font=dict(size=12),
        xaxis_tickangle=-45,
        bargap=0.2,  # Espaçamento entre grupos de barras
        bargroupgap=0.1  # Espaçamento entre barras do mesmo grupo
    )
    
    fig.show()

# Executar visualizações comparativas
create_dde_comparison_visualizations(df_estoque_loja_porte_regiao, df_comparacao, categorias_teste)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TODO: análises de acompanhamento

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Consequência dos deltas mais relevantes vs Matriz antiga - DDE ou ruptura 
# MAGIC 2. Buckets de DDE - delta versus categoria
# MAGIC 3. DDE x Ruptura  - delta versus categoria
# MAGIC 4. Proporção prevista vs real - janela desde inicio do teste
