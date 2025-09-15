# Databricks notebook source
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

# COMMAND ----------

def carregar_mapeamentos_produtos(categoria: str) -> tuple:
    """
    Carrega os arquivos de mapeamento de produtos para a categoria específica.
    """
    print("🔄 Carregando mapeamentos de produtos...")
    
    de_para_modelos_tecnologia = (
        pd.read_csv('/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/MODELOS_AJUSTE (1).csv', 
                    delimiter=';',
                    #encoding='iso-8859-1')
        )
        .drop_duplicates()
    )
    
    de_para_modelos_tecnologia.columns = (
        de_para_modelos_tecnologia.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
        .str.strip("_")
    )
    
    try:
        de_para_gemeos_tecnologia = (
            pd.read_csv('/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/ITENS_GEMEOS 2.csv',
                        delimiter=";",
                        encoding='iso-8859-1')
            .drop_duplicates()
        )
        
        de_para_gemeos_tecnologia.columns = (
            de_para_gemeos_tecnologia.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[^\w]+", "_", regex=True)
            .str.strip("_")
        )
        
        print("✅ Mapeamento de gêmeos carregado")
    except FileNotFoundError:
        print("⚠️  Arquivo de mapeamento de gêmeos não encontrado")
        de_para_gemeos_tecnologia = None
    
    return (
        spark.createDataFrame(
            de_para_modelos_tecnologia.rename(columns={"codigo_item": "CdSku"})[['CdSku', 'modelos']]), 
        spark.createDataFrame(
            de_para_gemeos_tecnologia.rename(columns={"sku_loja": "CdSku"})[['CdSku', 'gemeos']]) if de_para_gemeos_tecnologia is not None else None
    )

de_para_modelos_gemeos = (
    carregar_mapeamentos_produtos("")[0]
    .join(carregar_mapeamentos_produtos("")[1],
          how="outer",
          on="CdSku"
    ) 
    .withColumn("gemeos",
                F.when(F.col("gemeos") == '-', F.col("modelos"))
                .otherwise(F.col("gemeos"))
    )
    .withColumn("grupo_de_necessidade", F.col("gemeos"))
)

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
    .filter(F.col('TIPO_ENTREGA') == 'SL')
    .join(de_para_modelos_gemeos,
      how="inner",
      on="CdSku")
)

df_matriz_neogrid = (
    df_matriz_neogrid
    .groupBy('CdFilial', 'grupo_de_necessidade')
    .agg(
        F.median(F.col('PercMatrizNeogrid')).alias("PercMatrizNeogrid")
    )

)

df_matriz_neogrid.display()

# COMMAND ----------

df_merecimento = {}

df_merecimento['TELAS'] = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas')
    .select('CdFilial', 'grupo_de_necessidade', 
            F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('merecimento_percentual')
    ).dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade'])
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmRegiaoGeografica", "NmPorteLoja").distinct(),
          how="left",
          on="CdFilial")
    .join(
        df_matriz_neogrid,
          how="left",
          on=["grupo_de_necessidade", 'CdFilial']
    )            
)


df_merecimento['TELEFONIA'] = (
    spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular')
    .select('CdFilial', 'grupo_de_necessidade', 
            F.round(100*F.col('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('merecimento_percentual')
    ).dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade'])
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmRegiaoGeografica", "NmPorteLoja").distinct(),
          how="left",
          on="CdFilial")    
    .join(
        df_matriz_neogrid,
          how="left",
          on=["grupo_de_necessidade",  'CdFilial']   
    )       
)

display(df_merecimento['TELAS'])


display(df_merecimento['TELEFONIA'])

# COMMAND ----------

df_merecimento_lojas = {}

df_merecimento_lojas['TELAS'] = (
    df_merecimento['TELAS']
    .filter(F.col("grupo_de_necessidade").isin("TV 50 ALTO P", "TV 55 ALTO P"))
)

df_merecimento_lojas['TELEFONIA'] = (
    df_merecimento['TELEFONIA']
    .filter(F.col("grupo_de_necessidade").isin("Telef pp"))
)

display(df_merecimento_lojas['TELAS'])
display(df_merecimento_lojas['TELEFONIA'])

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_merecimento_lojas_agg = {}

w_porte  = Window.partitionBy("grupo_de_necessidade", "NmPorteLoja")
w_regiao = Window.partitionBy("grupo_de_necessidade", "NmRegiaoGeografica")

for key in ["TELAS", "TELEFONIA"]:
    df = df_merecimento_lojas[key]
    df_merecimento_lojas_agg[key] = (
        df
        .withColumn(
            "Merecimento_Porte_Nova",
            F.sum(F.coalesce(F.col("merecimento_percentual"), F.lit(0))).over(w_porte)
        )
        .withColumn(
            "Merecimento_Porte_Atual",
            F.sum(F.coalesce(F.col("PercMatrizNeogrid"), F.lit(0))).over(w_porte)
        )
        .withColumn(
            "Merecimento_Regiao_Nova",
            F.sum(F.coalesce(F.col("merecimento_percentual"), F.lit(0))).over(w_regiao)
        )
        .withColumn(
            "Merecimento_Regiao_Atual",
            F.sum(F.coalesce(F.col("PercMatrizNeogrid"), F.lit(0))).over(w_regiao)
        )
    )

display(df_merecimento_lojas_agg['TELAS'])
display(df_merecimento_lojas_agg['TELEFONIA'])

# COMMAND ----------

from pyspark.sql import functions as F, Window

df = df_merecimento_lojas_agg['TELEFONIA'] \
    .select(
        'grupo_de_necessidade',
        'NmPorteLoja',
        F.round(F.col('Merecimento_Porte_Nova'), 2).alias('Merecimento_Porte_Nova'),
        F.round(F.col('Merecimento_Porte_Atual'), 2).alias('Merecimento_Porte_Atual')
    ) \
    .dropDuplicates(['grupo_de_necessidade', 'NmPorteLoja']) \
    .filter(F.col("NmPorteLoja").isNotNull()) \
    .filter(F.col("NmPorteLoja") != '-') \
    .orderBy('grupo_de_necessidade', 'NmPorteLoja')

# janela por grupo
w = Window.partitionBy("grupo_de_necessidade")

df = df \
    .withColumn("total_nova", F.sum("Merecimento_Porte_Nova").over(w)) \
    .withColumn("total_atual", F.sum("Merecimento_Porte_Atual").over(w)) \
    .withColumn("Merecimento_Porte_Nova",
                F.round(100 * F.col("Merecimento_Porte_Nova") / F.col("total_nova"), 2)) \
    .withColumn("Merecimento_Porte_Atual",
                F.round(100 * F.col("Merecimento_Porte_Atual") / F.col("total_atual"), 2)) \
    .withColumn("deltaMatriz", 
                F.round(F.col("Merecimento_Porte_Nova") - F.col("Merecimento_Porte_Atual"), 2)) \
    .withColumn("deltaMatriz_percentual",
                F.round(100 * F.col("deltaMatriz") / F.col("Merecimento_Porte_Nova"), 1)) \
    .drop("total_nova", "total_atual")

df.display()

# COMMAND ----------

from pyspark.sql import functions as F, Window

df = df_merecimento_lojas_agg['TELAS'] \
    .select(
        'grupo_de_necessidade',
        'NmPorteLoja',
        F.round(F.col('Merecimento_Porte_Nova'), 2).alias('Merecimento_Porte_Nova'),
        F.round(F.col('Merecimento_Porte_Atual'), 2).alias('Merecimento_Porte_Atual')
    ) \
    .dropDuplicates(['grupo_de_necessidade', 'NmPorteLoja']) \
    .filter(F.col("NmPorteLoja").isNotNull()) \
    .filter(F.col("NmPorteLoja") != '-') \
    .orderBy('grupo_de_necessidade', 'NmPorteLoja')

# janela por grupo
w = Window.partitionBy("grupo_de_necessidade")

df = df \
    .withColumn("total_nova", F.sum("Merecimento_Porte_Nova").over(w)) \
    .withColumn("total_atual", F.sum("Merecimento_Porte_Atual").over(w)) \
    .withColumn("Merecimento_Porte_Nova",
                F.round(100 * F.col("Merecimento_Porte_Nova") / F.col("total_nova"), 2)) \
    .withColumn("Merecimento_Porte_Atual",
                F.round(100 * F.col("Merecimento_Porte_Atual") / F.col("total_atual"), 2)) \
    .withColumn("deltaMatriz", 
                F.round(F.col("Merecimento_Porte_Nova") - F.col("Merecimento_Porte_Atual"), 2)) \
    .withColumn("deltaMatriz_percentual",
                F.round(100 * F.col("deltaMatriz") / F.col("Merecimento_Porte_Nova"), 1)) \
    .drop("total_nova", "total_atual")

df.display()

# COMMAND ----------

from pyspark.sql import functions as F, Window

df_merecimento_lojas_agg_regiao = {}

# base
base_regiao = (
    df_merecimento_lojas_agg['TELAS']
    .select(
        'grupo_de_necessidade',
        'NmRegiaoGeografica',
        F.round(F.col('Merecimento_Regiao_Nova'), 2).alias('Merecimento_Regiao_Nova'),
        F.round(F.col('Merecimento_Regiao_Atual'), 2).alias('Merecimento_Regiao_Atual')
    )
    .dropDuplicates(['grupo_de_necessidade', 'NmRegiaoGeografica'])
    .filter(F.col("NmRegiaoGeografica").isNotNull())
    .filter(F.col("NmRegiaoGeografica") != '-')
)

w = Window.partitionBy("grupo_de_necessidade")

df_merecimento_lojas_agg_regiao['TELAS'] = (
    base_regiao
    .withColumn("total_nova",  F.sum("Merecimento_Regiao_Nova").over(w))
    .withColumn("total_atual", F.sum("Merecimento_Regiao_Atual").over(w))
    .withColumn(
        "Merecimento_Regiao_Nova",
        F.round(
            F.when(F.col("total_nova") != 0, 100 * F.col("Merecimento_Regiao_Nova") / F.col("total_nova"))
             .otherwise(F.lit(0.0)),
            2
        )
    )
    .withColumn(
        "Merecimento_Regiao_Atual",
        F.round(
            F.when(F.col("total_atual") != 0, 100 * F.col("Merecimento_Regiao_Atual") / F.col("total_atual"))
             .otherwise(F.lit(0.0)),
            2
        )
    )
    .withColumn("deltaMatriz",
        F.round(F.col("Merecimento_Regiao_Nova") - F.col("Merecimento_Regiao_Atual"), 2)
    )
    .withColumn(
        "deltaMatriz_percentual",
        F.round(
            100 * F.col("deltaMatriz") /
            F.when(F.col("Merecimento_Regiao_Nova") != 0, F.col("Merecimento_Regiao_Nova")).otherwise(F.lit(1.0)),
            1
        )
    )
    .drop("total_nova", "total_atual")
    .orderBy('grupo_de_necessidade', 'NmRegiaoGeografica')
)

df_merecimento_lojas_agg_regiao['TELAS'].display()

# COMMAND ----------

def carregar_mapeamentos_produtos(categoria: str) -> tuple:
    """
    Carrega os arquivos de mapeamento de produtos para a categoria específica.
    """
    print("🔄 Carregando mapeamentos de produtos...")
    
    de_para_modelos_tecnologia = (
        pd.read_csv('/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/MODELOS_AJUSTE (1).csv', 
                    delimiter=';')
        .drop_duplicates()
    )
    
    de_para_modelos_tecnologia.columns = (
        de_para_modelos_tecnologia.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
        .str.strip("_")
    )
    
    try:
        de_para_gemeos_tecnologia = (
            pd.read_csv('/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/ITENS_GEMEOS 2.csv',
                        delimiter=";",
                        encoding='iso-8859-1')
            .drop_duplicates()
        )
        
        de_para_gemeos_tecnologia.columns = (
            de_para_gemeos_tecnologia.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[^\w]+", "_", regex=True)
            .str.strip("_")
        )
        
        print("✅ Mapeamento de gêmeos carregado")
    except FileNotFoundError:
        print("⚠️  Arquivo de mapeamento de gêmeos não encontrado")
        de_para_gemeos_tecnologia = None
    
    return (
        spark.createDataFrame(
            de_para_modelos_tecnologia.rename(columns={"codigo_item": "CdSku"})[['CdSku', 'modelos']]), 
        spark.createDataFrame(
            de_para_gemeos_tecnologia.rename(columns={"sku_loja": "CdSku"})[['CdSku', 'gemeos']]) if de_para_gemeos_tecnologia is not None else None
    )

de_para_modelos_gemeos = (
    carregar_mapeamentos_produtos("")[0]
    .join(carregar_mapeamentos_produtos("")[1],
          how="outer",
          on="CdSku"
    ) 
    .withColumn("gemeos",
                F.when(F.col("gemeos") == '-', F.col("modelos"))
                .otherwise(F.col("gemeos"))
    )
    .withColumn("grupo_de_necessidade", F.col("gemeos"))
)

# COMMAND ----------

de_para_modelos_gemeos.display()

# COMMAND ----------

df_base_calculo_factual = (
  spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3')
  .filter(F.col('DtAtual') >= '2025-06-01')
  .filter(F.col("NmAgrupamentoDiretoriaSetor").isin('DIRETORIA DE TELAS', 'DIRETORIA TELEFONIA CELULAR')
  )
  .fillna(0, ['QtMercadoria', 'deltaRuptura'])
  .fillna(0.1, ['PrecoMedio90'])

  .join(de_para_modelos_gemeos, on='CdSku', how='left')
  .groupBy('NmAgrupamentoDiretoriaSetor', 'grupo_de_necessidade', 'modelos')
  .agg(
      F.sum('QtMercadoria').alias('Total_vendas'),
      F.sum('deltaRuptura').alias('demanda_suprimida'),
      F.mean('PrecoMedio90').alias('PrecoMedio')
      )
  .withColumn('demanda_irrestrita', 
              F.round(F.col('Total_vendas') + F.col('demanda_suprimida'), 0)
              )
  .withColumn('receita_irrestrita',
              F.round(F.col('demanda_irrestrita') * F.col('PrecoMedio'), 0)
  )
  
  .filter(F.col('grupo_de_necessidade') != 'Chip')  
)

from pyspark.sql import functions as F, Window

# total por diretoria (janela) e % por grupo_de_necessidade
w_dir = Window.partitionBy("NmAgrupamentoDiretoriaSetor")

df_participacao = (
    df_base_calculo_factual
    .withColumn("Total_irrestrita_diretoria",
                F.sum("demanda_irrestrita").over(w_dir))
    .withColumn("Total_receita_irrestrita_diretoria",
            F.sum("receita_irrestrita").over(w_dir))
    .withColumn(
        "Pct_participacao_grupo_qty",
        F.when(F.col("Total_irrestrita_diretoria") > 0,
               F.round((F.col("demanda_irrestrita") / F.col("Total_irrestrita_diretoria")) * 100, 2))
         .otherwise(F.lit(0.00))
    )
    .withColumn(
        "Pct_participacao_grupo_receita",
        F.when(F.col("Total_receita_irrestrita_diretoria") > 0,
               F.round((F.col("receita_irrestrita") / F.col("Total_receita_irrestrita_diretoria")) * 100, 2))
         .otherwise(F.lit(0.00))
    )
    .select(
        "NmAgrupamentoDiretoriaSetor",
        'modelos',
        "grupo_de_necessidade",
        "demanda_irrestrita",
        "Total_irrestrita_diretoria",
        "Pct_participacao_grupo_qty",
        "Pct_participacao_grupo_receita"
    )
    .orderBy("NmAgrupamentoDiretoriaSetor", "grupo_de_necessidade")
)

# totais por diretoria (opcional)
df_totais_diretoria = (
    df_participacao
    .select("NmAgrupamentoDiretoriaSetor", "Total_irrestrita_diretoria")
    .dropDuplicates()
    .orderBy("NmAgrupamentoDiretoriaSetor")
)

df_participacao.display()
# df_totais_diretoria.display()

# COMMAND ----------

df_gdn_telefonia = (
    df_participacao
    #.filter(F.col("Pct_participacao_grupo_receita") <= 20)
    #.filter(F.col("Pct_participacao_grupo_receita") >= 1)
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA TELEFONIA CELULAR')
    .select('grupo_de_necessidade','modelos', 'Pct_participacao_grupo_qty', 'Pct_participacao_grupo_receita')
    .filter(F.col('grupo_de_necessidade') == 'Telef pp')
)

df_gdn_telefonia.display()

# COMMAND ----------

de_para_modelos_gemeos.filter(F.col('grupo_de_necessidade') == 'Telef pp').select("grupo_de_necessidade", "modelos").distinct().display()

# COMMAND ----------

df_gdn_telas = (
    df_participacao
    .filter(F.col("Pct_participacao_grupo_receita") <= 10)
    .filter(F.col("Pct_participacao_grupo_receita") >= 3)
    .filter(F.col("NmAgrupamentoDiretoriaSetor") == 'DIRETORIA DE TELAS')
    .select('grupo_de_necessidade', 'Pct_participacao_grupo_qty', 'Pct_participacao_grupo_receita')
)

df_gdn_telas.display()

# COMMAND ----------

# 1) pegar lista de CdSku
sku_list = (
    de_para_modelos_gemeos
    .filter(F.col("grupo_de_necessidade") == "TV 50 ALTO P")
    .select("CdSku")
    .rdd.flatMap(lambda x: x)
    .collect()
)

(
    spark.table('data_engineering_prd.app_venda.mercadoria')
    .select('NmMarca', 'NmSku')
    .filter(F.col('CdSkuLoja').isin(sku_list))
).display()
