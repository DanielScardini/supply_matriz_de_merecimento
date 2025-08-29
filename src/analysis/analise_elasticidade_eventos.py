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
    
    Args:
        categoria: Nome da categoria/diretoria
        
    Returns:
        Tuple com os DataFrames de mapeamento
    """
    print("🔄 Carregando mapeamentos de produtos...")
    
    # Mapeamento de modelos e tecnologia
    de_para_modelos_tecnologia = (
        pd.read_csv('../dados_analise/MODELOS_AJUSTE (1).csv', 
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
    
    # Mapeamento de produtos similares (gêmeos) - apenas para categorias que usam
    try:
        de_para_gemeos_tecnologia = (
            pd.read_csv('../dados_analise/ITENS_GEMEOS 2.csv',
                        delimiter=";",
                        encoding='iso-8859-1')
            .drop_duplicates()
        )
        
        # Normalização de nomes de colunas
        de_para_gemeos_tecnologia.columns = (
            de_para_gemeos_tecnologia.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[^\w]+", "_", regex=True)
            .str.strip("_")
        )
        
        print("✅ Mapeamento de gêmeos carregado")
    except FileNotFoundError:
        print("⚠️  Arquivo de mapeamento de gêmeos não encontrado - será usado apenas para categorias que precisam")
        de_para_gemeos_tecnologia = None
    
    print("✅ Mapeamentos de produtos carregados")
    return (
        spark.createDataFrame(
            de_para_modelos_tecnologia
            .rename(columns={"codigo_item": "CdSku"})[['CdSku', 'modelos']]
        ), 
        spark.createDataFrame(
            de_para_gemeos_tecnologia.rename(columns={"sku_loja": "CdSku"})[['CdSku', 'gemeos']]
        )
    )


# COMMAND ----------

def criar_de_para_filial_cd() -> DataFrame:
    """
    Cria o mapeamento filial → CD usando dados da tabela base.
    
    Returns:
        DataFrame com mapeamento filial → CD
    """
    print("🔄 Criando de-para filial → CD...")
    
    # Carrega dados da tabela base para criar mapeamento
    df_base = spark.table('databox.bcg_comum.supply_base_merecimento_diario_v2')
    
    # Cria mapeamento filial → CD
    de_para_filial_cd = (
        df_base
        .select("cdfilial", "cd_primario")
        .distinct()
        .filter(F.col("cdfilial").isNotNull())
        .withColumn(
            "cd_primario",
            F.coalesce(F.col("cd_primario"), F.lit("SEM_CD"))  # ← "SEM_CD" se cd_primario for nulo
        )
        .orderBy("cdfilial")
    )
    
    print(f"✅ De-para filial → CD criado:")
    print(f"  • Total de filiais: {de_para_filial_cd.count():,}")
    print(f"  • CDs únicos: {de_para_filial_cd.select('cd_primario').distinct().count():,}")
    
    return de_para_filial_cd

# COMMAND ----------

df_tabelao_merecimento = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .filter(F.col("NmAgrupamentoDiretoriaSetor").isin(
        "DIRETORIA TELEFONIA CELULAR",
        "DIRETORIA DE TELAS"
    ))
)

de_para_modelos_tecnologia, de_para_gemeos_tecnologia = carregar_mapeamentos_produtos("DIRETORIA TELEFONIA CELULAR")

de_para_modelos_gemeos_tecnologia = (
    de_para_modelos_tecnologia
    .join(de_para_gemeos_tecnologia, on="CdSku", how="left")
    .fillna("SEM CLASSIFICACAO")
)

df_tabelao_merecimento_gemeos = (
    df_tabelao_merecimento
    .join(de_para_modelos_gemeos_tecnologia,
          on="CdSku",
          how="left")
    .join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmRegiaoGeografica")
        .distinct(),
          how="left",
          on="CdFilial")
)


df_tabelao_merecimento_gemeos.limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top gemeos das categorias

# COMMAND ----------

(
  df_tabelao_merecimento_gemeos
  .groupBy("NmAgrupamentoDiretoriaSetor", "gemeos")
  .agg(F.sum("QtMercadoria").alias("total_vendas"))
  .orderBy(F.desc("total_vendas"))
  .filter(~F.col("gemeos").contains("Chip"))
).display()
