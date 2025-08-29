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
    
    # Mapeamento de produtos similares (gêmeos) - apenas para categorias que usam
    try:
        de_para_gemeos_tecnologia = (
            pd.read_csv('dados_analise/ITENS_GEMEOS 2.csv',
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
        de_para_modelos_tecnologia.rename(columns={"codigo_item": "CdSku"})[['CdSku', 'modelos']], 
        de_para_gemeos_tecnologia.rename(columns={"sku_loja": "CdSku"})[['CdSku', 'gemeos']]
    )


# COMMAND ----------

def aplicar_mapeamentos_produtos(df: DataFrame, categoria: str, 
                                de_para_modelos: pd.DataFrame, 
                                de_para_gemeos: pd.DataFrame = None) -> DataFrame:
    """
    Aplica os mapeamentos de produtos ao DataFrame base.
    
    Args:
        df: DataFrame base
        categoria: Nome da categoria
        de_para_modelos: DataFrame com mapeamento de modelos
        de_para_gemeos: DataFrame com mapeamento de gêmeos (opcional)
        
    Returns:
        DataFrame com os mapeamentos aplicados
    """
    print(f"🔄 Aplicando mapeamentos para categoria: {categoria}")
    
    # Converte pandas DataFrame para Spark DataFrame
    df_modelos_spark = spark.createDataFrame(de_para_modelos)
    
    # Aplica mapeamento de modelos
    df_com_modelos = df.join(
        df_modelos_spark,
        on="CdSku",
        how="left"
    )
    
    # Aplica mapeamento de gêmeos apenas se necessário
    if (de_para_gemeos is not None and 
        REGRAS_AGRUPAMENTO[categoria]["coluna_grupo_necessidade"] == "gemeos"):
        
        df_gemeos_spark = spark.createDataFrame(de_para_gemeos)
        df_com_mapeamentos = df_com_modelos.join(
            df_gemeos_spark,
            on="CdSku",
            how="left"
        )
        print("✅ Mapeamento de gêmeos aplicado")
    else:
        df_com_mapeamentos = df_com_modelos
        print("ℹ️  Mapeamento de gêmeos não aplicado (não necessário para esta categoria)")
    
    print("✅ Mapeamentos de produtos aplicados")
    return df_com_mapeamentos

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
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v2').limit(10000)
)

df_tabelao_merecimento.display()

