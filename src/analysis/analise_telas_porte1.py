# Databricks notebook source
# Line plot único (QtMercadoria = vendas, EstoqueLoja = estoque)
from pyspark.sql import functions as F
import pandas as pd
import plotly.graph_objects as go
from pyspark.sql.window import Window

df_analise_1886 = spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
df_analise_1886.cache()

df_analise_1886 = df_analise_1886.filter(
    (F.col("CdSku").isin(5359058,
                         5335701,
                         5164591,
                         5307686,
                        5286409))
    & (F.col("DtAtual") >= '2025-06-01')
    & (F.col("CdFilial") == 1886)

)

# Preparação dos dados
df_plot = (
    df_analise_1886
    .select(
        F.to_date("DtAtual").alias("Dt"),
        F.col("QtMercadoria").cast("int").alias("vendas"),
        F.col("EstoqueLoja").cast("int").alias("estoque")
    )
    .groupBy("Dt")
    .agg(
        F.sum("vendas").alias("vendas"),
        F.sum("estoque").alias("estoque")
    )
    .orderBy("Dt")
    .toPandas()
)

df_plot = df_plot.sort_values("Dt")
df_plot["Dt"] = pd.to_datetime(df_plot["Dt"])

# Gráfico
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df_plot["Dt"],
    y=df_plot["vendas"],
    mode="lines+markers",
    name="vendas",
    line=dict(width=2)
))

fig.add_trace(go.Scatter(
    x=df_plot["Dt"],
    y=df_plot["estoque"],
    mode="lines+markers",
    name="estoque",
    line=dict(width=2)
))

# Layout profissional
fig.update_layout(
    title="Vendas e Estoque ao Longo do Tempo",
    xaxis_title="Data",
    yaxis_title="Quantidade",
    paper_bgcolor="#f2f2f2",
    plot_bgcolor="#f2f2f2",
    font=dict(size=12),
    height=500,
    margin=dict(l=60, r=30, t=60, b=40),
)

fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#d9d9d9", tickformat="%Y-%m-%d")
fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="#d9d9d9", tickformat="d")

fig.show()

# COMMAND ----------

# MAGIC %sql SELECT * FROM databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia

# COMMAND ----------

# Calcula soma por filial e percentual sobre o total
w_total = Window.partitionBy(F.lit(1))

df_analise = spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
df_analise.cache()

df_analise = df_analise.filter(
    (F.col("CdSku").isin(5359058,
                         5335701,
                         5164591,
                         5307686,
                        5286409))
    & (F.col("DtAtual") >= '2025-06-01')
    #& (F.col("CdFilial") == 1886)
)


resultado_full = (
    df_analise
    .groupBy("CdFilial")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria"),
        F.round(F.sum("deltaRuptura"), 1).alias("deltaRuptura")
    )
    .withColumn("QtDemanda", F.col("QtMercadoria") + F.col("deltaRuptura"))
    .withColumn("total_geral", F.round(F.sum("QtDemanda").over(w_total),1))
    .withColumn("perc_vs_total_%", F.round(F.col("QtDemanda") / F.col("total_geral") * 100, 2))
    .orderBy(F.desc("QtMercadoria"))
)

display(resultado_full)

# COMMAND ----------

# MAGIC %sql SELECT DISTINCT CdSku FROM databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia
# MAGIC
# MAGIC WHERE grupo_de_necessidade = 'TV 65 MEDIO'

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any

!pip install openpyxl

# Inicialização do Spark
spark = SparkSession.builder.appName("calculo_matriz_merecimento_unificado").getOrCreate()

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

FILIAIS_OUTLET = [2528, 3604]

# Flag para escolher fonte do de-para
USAR_DE_PARA_EXCEL = True  # True = Excel, False = CSV antigo

def criar_tabela_de_para_grupo_necessidade_direto(hoje: datetime, usar_excel: bool = True) -> int:
    """
    Lê diretamente do arquivo de de-para, trata os dados e salva na tabela.
    
    Args:
        hoje: Data/hora atual para timestamp
        usar_excel: True para Excel, False para CSV antigo
        
    Returns:
        int: Número de registros salvos
    """
    
    try:
        if usar_excel:
            # Carregar do Excel
            print("📁 Carregando de-para do Excel (de_para_gemeos_tecnologia.xlsx)...")
            de_para_df = pd.read_excel(
                "/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/de_para_gemeos_tecnologia.xlsx",
                sheet_name="de_para"
            )
        else:
            # Carregar do CSV antigo
            print("📁 Carregando de-para do CSV antigo (ITENS_GEMEOS 2.csv)...")
            de_para_df = pd.read_csv(
                "/dbfs/mnt/datalake/bcg_comum/ITENS_GEMEOS 2.csv",
                sep=";",
                encoding="utf-8"
            )
        
        print(f"  ✅ Arquivo carregado: {len(de_para_df)} registros")
        
        # Padronizar nomes das colunas
        de_para_df.columns = (
            de_para_df.columns
            .str.lower()
            .str.replace(r"[^\w]+", "_", regex=True)
            .str.strip("_")
        )
        
        # Mapear colunas para o formato esperado
        if 'sku' in de_para_df.columns and 'gemeos' in de_para_df.columns:
            de_para_df = de_para_df.rename(columns={'sku': 'CdSku', 'gemeos': 'grupo_de_necessidade'})
        elif 'cdsku' in de_para_df.columns and 'gemeos' in de_para_df.columns:
            de_para_df = de_para_df.rename(columns={'cdsku': 'CdSku', 'gemeos': 'grupo_de_necessidade'})
        elif 'sku' in de_para_df.columns and 'gemeos' in de_para_df.columns:
            de_para_df = de_para_df.rename(columns={'sku': 'CdSku', 'gemeos': 'grupo_de_necessidade'})
        else:
            raise ValueError(f"Colunas não encontradas. Disponíveis: {list(de_para_df.columns)}")
        
        # Garantir que CdSku seja string
        de_para_df['CdSku'] = de_para_df['CdSku'].astype(str)
        
        # Remover duplicatas e valores nulos
        de_para_df = de_para_df.dropna(subset=['CdSku', 'grupo_de_necessidade'])
        de_para_df = de_para_df.drop_duplicates(subset=['CdSku'])
        
        # Adicionar timestamp
        de_para_df['DtAtualizacao'] = hoje
        
        # Converter para Spark DataFrame
        df_spark = spark.createDataFrame(de_para_df)
        
        # Salvar tabela em modo overwrite
        df_spark.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia")
        
        count_registros = len(de_para_df)
        print(f"✅ Tabela supply_de_para_modelos_gemeos_tecnologia atualizada com {count_registros} registros")
        
        return count_registros
        
    except Exception as e:
        print(f"❌ Erro ao criar tabela de de-para: {str(e)}")
        raise

criar_tabela_de_para_grupo_necessidade_direto(hoje, usar_excel = True)
