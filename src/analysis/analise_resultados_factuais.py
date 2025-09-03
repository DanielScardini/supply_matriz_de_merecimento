# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import List, Optional

# Inicialização do Spark
spark = SparkSession.builder.appName("analise_resultados_factuais").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingestão de bases

# COMMAND ----------

def carregar_mapeamentos_produtos(categoria: str) -> tuple:
    """
    Carrega os arquivos de mapeamento de produtos para a categoria específica.
    """
    print("🔄 Carregando mapeamentos de produtos...")
    
    de_para_modelos_tecnologia = (
        pd.read_csv('dados_analise/MODELOS_AJUSTE (1).csv', 
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
            pd.read_csv('dados_analise/ITENS_GEMEOS 2.csv',
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

df_matriz_nova = {}

df_matriz_nova['TELAS'] = (
  spark.table('databox.bcg_comum.supply_matriz_merecimento_de_telas')
  .select('grupo_de_necessidade','CdFilial', 'CdSku', 'Merecimento_Final_Media90_Qt_venda_sem_ruptura')
)

df_matriz_nova['TELEFONIA'] = (
  spark.table('databox.bcg_comum.supply_matriz_merecimento_telefonia_celular')
  .select('grupo_de_necessidade','CdFilial', 'CdSku', 'Merecimento_Final_Media90_Qt_venda_sem_ruptura')

)

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

df_base_calculo_factual = (
  spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3')
  .filter(F.col('DtAtual') >= '2025-08-01')
)

df_matriz_nova['TELEFONIA'].cache()
df_matriz_nova['TELAS'].cache()
df_matriz_neogrid.cache()
df_base_calculo_factual.cache()

# COMMAND ----------

df_matriz_neogrid.groupby('CdSku').agg(F.sum('PercMatrizNeogrid')).display()

# COMMAND ----------

df_proporcao_factual = (
    df_base_calculo_factual
    .filter(F.col("NmAgrupamentoDiretoriaSetor").isin('DIRETORIA TELEFONIA CELULAR', 'DIRETORIA DE TELAS'))
    .fillna(0, subset=['deltaRuptura', 'QtMercadoria'])
    .withColumn("QtDemanda", F.col("QtMercadoria") + F.col("deltaRuptura"))
    .join(de_para_modelos_gemeos,
          how='left',
          on='CdSku')
    .withColumn('grupo_de_necessidade', F.col("gemeos"))
    .groupBy('CdFilial', 'grupo_de_necessidade')
    .agg(
        F.sum('QtDemanda').alias('QtDemanda'),
    )
)

# partindo do df_proporcao_factual já agregado por CdFilial × grupo_de_necessidade
w_grp = Window.partitionBy("grupo_de_necessidade")

df_proporcao_factual_pct = (
    df_proporcao_factual
    .withColumn("Total_QtDemanda", F.sum(F.col("QtDemanda")).over(w_grp))
    .withColumn(
        "Proporcao_Interna_QtDemanda",
        F.when(F.col("Total_QtDemanda") > 0, F.col("QtDemanda") / F.col("Total_QtDemanda")).otherwise(F.lit(0.0))
    )
    .withColumn("Percentual_QtDemanda", F.round(F.col("Proporcao_Interna_QtDemanda") * 100.0, 2))
    .select('grupo_de_necessidade', 'CdFilial', 'Percentual_QtDemanda', 'QtDemanda')
)

df_proporcao_factual_pct.cache()

df_proporcao_factual_pct.display()

# COMMAND ----------

df_matriz_neogrid_agg = (
  df_matriz_neogrid
  .groupBy('CdFilial', 'grupo_de_necessidade')
  .agg(
    F.mean('PercMatrizNeogrid').alias('PercMatrizNeogrid')
  )
)

df_matriz_nova_agg = {}
df_matriz_nova_agg['TELAS'] = (
  df_matriz_nova['TELAS']
  .groupBy('CdFilial', 'grupo_de_necessidade')
  .agg(
    F.round(100*F.mean('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('PercMatrizNova')
  )         
)

df_matriz_nova_agg['TELEFONIA'] = (
  df_matriz_nova['TELEFONIA']
  .groupBy('CdFilial', 'grupo_de_necessidade')
  .agg(
    F.round(100*F.mean('Merecimento_Final_Media90_Qt_venda_sem_ruptura'), 2).alias('PercMatrizNova')
  )         
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Geral

# COMMAND ----------

df_comparacao = {}

df_comparacao['TELAS'] = (
  df_matriz_nova_agg['TELAS']
  .join(df_matriz_neogrid_agg, on=['CdFilial', 'grupo_de_necessidade'], how='inner')
  .join(df_proporcao_factual_pct, on=['grupo_de_necessidade', 'CdFilial'], how='inner')
)

#df_comparacao.display()

df_comparacao['TELAS'].cache()
df_comparacao['TELAS'].count()

df_comparacao['TELEFONIA'] = (
  df_matriz_nova_agg['TELEFONIA']
  .join(df_matriz_neogrid_agg, on=['CdFilial', 'grupo_de_necessidade'], how='inner')
  .join(df_proporcao_factual_pct, on=['grupo_de_necessidade', 'CdFilial'], how='inner')
)

#df_comparacao.display()

df_comparacao['TELAS'].cache()
df_comparacao['TELAS'].count()

df_comparacao['TELEFONIA'].cache()
df_comparacao['TELEFONIA'].count()

# COMMAND ----------

from pyspark.sql import functions as F

# smape para cada linha
df_smape = (
    df_comparacao['TELAS']
    .withColumn(
        "SMAPE_MatrizNeogrid",
        200 * F.abs(F.col("PercMatrizNeogrid") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNeogrid")) + F.abs(F.col("Percentual_QtDemanda")))
    )
    .withColumn(
        "SMAPE_MatrizNova",
        200 * F.abs(F.col("PercMatrizNova") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNova")) + F.abs(F.col("Percentual_QtDemanda")))
    )
)

# média simples do SMAPE (não ponderado)
df_result_smape = df_smape.agg(
    F.mean("SMAPE_MatrizNeogrid").alias("SMAPE_MatrizNeogrid"),
    F.mean("SMAPE_MatrizNova").alias("SMAPE_MatrizNova")
)

# weighted SMAPE usando QtDemanda
df_wsmape = (
    df_smape
    .withColumn(
        "WSMAPE_MatrizNeogrid",
        (200 * F.abs(F.col("PercMatrizNeogrid") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNeogrid")) + F.abs(F.col("Percentual_QtDemanda"))))
        * F.col("QtDemanda")
    )
    .withColumn(
        "WSMAPE_MatrizNova",
        (200 * F.abs(F.col("PercMatrizNova") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNova")) + F.abs(F.col("Percentual_QtDemanda"))))
        * F.col("QtDemanda")
    )
)

df_result_wsmape = df_wsmape.agg(
    (F.sum("WSMAPE_MatrizNeogrid") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNeogrid"),
    (F.sum("WSMAPE_MatrizNova") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNova")
)

# resultados
df_result_smape.display()
df_result_wsmape.display()

# COMMAND ----------

from pyspark.sql import functions as F

# smape para cada linha
df_smape = (
    df_comparacao['TELEFONIA']
    .filter(F.col("grupo_de_necessidade") != 'Chip')
    .withColumn(
        "SMAPE_MatrizNeogrid",
        200 * F.abs(F.col("PercMatrizNeogrid") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNeogrid")) + F.abs(F.col("Percentual_QtDemanda")))
    )
    .withColumn(
        "SMAPE_MatrizNova",
        200 * F.abs(F.col("PercMatrizNova") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNova")) + F.abs(F.col("Percentual_QtDemanda")))
    )
)

# média simples do SMAPE (não ponderado)
df_result_smape = df_smape.agg(
    F.mean("SMAPE_MatrizNeogrid").alias("SMAPE_MatrizNeogrid"),
    F.mean("SMAPE_MatrizNova").alias("SMAPE_MatrizNova")
)

# weighted SMAPE usando QtDemanda
df_wsmape = (
    df_smape
    .withColumn(
        "WSMAPE_MatrizNeogrid",
        (200 * F.abs(F.col("PercMatrizNeogrid") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNeogrid")) + F.abs(F.col("Percentual_QtDemanda"))))
        * F.col("QtDemanda")
    )
    .withColumn(
        "WSMAPE_MatrizNova",
        (200 * F.abs(F.col("PercMatrizNova") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNova")) + F.abs(F.col("Percentual_QtDemanda"))))
        * F.col("QtDemanda")
    )
)

df_result_wsmape = df_wsmape.agg(
    (F.sum("WSMAPE_MatrizNeogrid") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNeogrid"),
    (F.sum("WSMAPE_MatrizNova") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNova")
)

# resultados
df_result_smape.display()
df_result_wsmape.display()

# COMMAND ----------

from pyspark.sql import functions as F

# smape para cada linha
df_smape = (
    df_comparacao['TELEFONIA']
    .filter(F.col("grupo_de_necessidade") == 'Telef pp')
    .withColumn(
        "SMAPE_MatrizNeogrid",
        200 * F.abs(F.col("PercMatrizNeogrid") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNeogrid")) + F.abs(F.col("Percentual_QtDemanda")))
    )
    .withColumn(
        "SMAPE_MatrizNova",
        200 * F.abs(F.col("PercMatrizNova") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNova")) + F.abs(F.col("Percentual_QtDemanda")))
    )
)

# média simples do SMAPE (não ponderado)
df_result_smape = df_smape.agg(
    F.mean("SMAPE_MatrizNeogrid").alias("SMAPE_MatrizNeogrid"),
    F.mean("SMAPE_MatrizNova").alias("SMAPE_MatrizNova")
)

# weighted SMAPE usando QtDemanda
df_wsmape = (
    df_smape
    .withColumn(
        "WSMAPE_MatrizNeogrid",
        (200 * F.abs(F.col("PercMatrizNeogrid") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNeogrid")) + F.abs(F.col("Percentual_QtDemanda"))))
        * F.col("QtDemanda")
    )
    .withColumn(
        "WSMAPE_MatrizNova",
        (200 * F.abs(F.col("PercMatrizNova") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNova")) + F.abs(F.col("Percentual_QtDemanda"))))
        * F.col("QtDemanda")
    )
)

df_result_wsmape = df_wsmape.agg(
    (F.sum("WSMAPE_MatrizNeogrid") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNeogrid"),
    (F.sum("WSMAPE_MatrizNova") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNova")
)

# resultados
df_result_smape.display()
df_result_wsmape.display()

# COMMAND ----------

from pyspark.sql import functions as F

# smape para cada linha
df_smape = (
    df_comparacao['TELEFONIA']
    .filter(F.col("grupo_de_necessidade") == 'Telef pp')

    .withColumn(
        "SMAPE_MatrizNeogrid",
        200 * F.abs(F.col("PercMatrizNeogrid") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNeogrid")) + F.abs(F.col("Percentual_QtDemanda")))
    )
    .withColumn(
        "SMAPE_MatrizNova",
        200 * F.abs(F.col("PercMatrizNova") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNova")) + F.abs(F.col("Percentual_QtDemanda")))
    )
)

# média simples do SMAPE (não ponderado)
df_result_smape = df_smape.agg(
    F.mean("SMAPE_MatrizNeogrid").alias("SMAPE_MatrizNeogrid"),
    F.mean("SMAPE_MatrizNova").alias("SMAPE_MatrizNova")
)

# weighted SMAPE usando QtDemanda
df_wsmape = (
    df_smape
    .withColumn(
        "WSMAPE_MatrizNeogrid",
        (200 * F.abs(F.col("PercMatrizNeogrid") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNeogrid")) + F.abs(F.col("Percentual_QtDemanda"))))
        * F.col("QtDemanda")
    )
    .withColumn(
        "WSMAPE_MatrizNova",
        (200 * F.abs(F.col("PercMatrizNova") - F.col("Percentual_QtDemanda")) /
        (F.abs(F.col("PercMatrizNova")) + F.abs(F.col("Percentual_QtDemanda"))))
        * F.col("QtDemanda")
    )
)

df_result_wsmape = df_wsmape.agg(
    (F.sum("WSMAPE_MatrizNeogrid") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNeogrid"),
    (F.sum("WSMAPE_MatrizNova") / F.sum("QtDemanda")).alias("WSMAPE_MatrizNova")
)

# resultados
df_result_smape.display()
df_result_wsmape.display()

# COMMAND ----------

# === Plotly scatters ===
import plotly.express as px


df_filial_mean = (
    df_comparacao['TELEFONIA']
    .filter(F.col("grupo_de_necessidade") == 'Telef pp')
    .join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica"),
        on="CdFilial",
        how="left"
    )
    .groupBy("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica")
    .agg(
        F.avg("Percentual_QtDemanda").alias("x_real"),
        F.avg("PercMatrizNova").alias("y_nova"),
        F.avg("PercMatrizNeogrid").alias("y_neogrid"),
    )
    # Extrai o número do porte
    .withColumn("PorteNum_raw", F.regexp_replace(F.col("NmPorteLoja"), "[^0-9]", ""))
    .withColumn("PorteNum", F.col("PorteNum_raw").cast("int"))
    # Garante valores válidos 1–6
    .withColumn("PorteNum", F.when(F.col("PorteNum").between(1,6), F.col("PorteNum")*1.5).otherwise(F.lit(1)))
)

pdf = df_filial_mean.toPandas()


# Definição de paleta: tons de azul e vermelho
# Ajuste a ordem ou adicione mais se tiver >2 regiões
palette = ["#1f77b4", "#d62728", "#aec7e8", "#ff9896"]

def make_scatter(df, y_col, y_label, title):
    fig = px.scatter(
        df,
        x="x_real",
        y=y_col,
        size="PorteNum",                 
        color="NmRegiaoGeografica",      
        color_discrete_sequence=palette,
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
        title=dict(text=title, x=0.5, xanchor="center"),
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
        width=1200,   # largura 4
        height=400,   # altura 3
    )

    fig.update_traces(marker=dict(line=dict(width=0.6, color="rgba(0,0,0,0.35)")))
    # linha de referência y=x
    fig.add_shape(
        type="line", x0=0, y0=0, x1=1, y1=1,
        line=dict(color="rgba(0,0,0,0.45)", width=0.2, dash="dash")
    )
    return fig

fig_nova = make_scatter(pdf, "y_nova",
                        "PercMatrizNova médio por filial (previsão)",
                        "Real vs Matriz Nova – por filial")
fig_neogrid = make_scatter(pdf, "y_neogrid",
                           "PercMatrizNeogrid médio por filial (previsão)",
                           "Real vs Matriz Neogrid – por filial")

fig_nova.show()
fig_neogrid.show()

# COMMAND ----------

# === Plotly scatters ===
import plotly.express as px

# Definição de paleta: tons de azul e vermelho
# Ajuste a ordem ou adicione mais se tiver >2 regiões
palette = ["#1f77b4", "#d62728", "#aec7e8", "#ff9896"]

def make_scatter(df, y_col, y_label, title):
    fig = px.scatter(
        df,
        x="x_real",
        y=y_col,
        size="PorteNum",                 
        color="NmRegiaoGeografica",      
        color_discrete_sequence=palette,
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
        title=dict(text=title, x=0.5, xanchor="center"),
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
        width=1200,   # largura 4
        height=400,   # altura 3
    )

    fig.update_traces(marker=dict(line=dict(width=0.6, color="rgba(0,0,0,0.35)")))
    # linha de referência y=x
    fig.add_shape(
        type="line", x0=0, y0=0, x1=1, y1=1,
        line=dict(color="rgba(0,0,0,0.45)", width=0.2, dash="dash")
    )
    return fig

fig_nova = make_scatter(pdf, "y_nova",
                        "PercMatrizNova médio por filial (previsão)",
                        "Real vs Matriz Nova – por filial")
fig_neogrid = make_scatter(pdf, "y_neogrid",
                           "PercMatrizNeogrid médio por filial (previsão)",
                           "Real vs Matriz Neogrid – por filial")

fig_nova.show()
fig_neogrid.show()

# COMMAND ----------

df_comparacao_porte = {}

df_comparacao_porte['TELAS'] = (
    df_comparacao['TELAS']
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica"),
          how="left",
          on="CdFilial")
    .groupBy("NmPorteLoja")
    .agg(
        F.avg("Percentual_QtDemanda").alias("Percentual_QtDemanda"),
        F.avg("PercMatrizNova").alias("PercMatrizNova"),
        F.avg("PercMatrizNeogrid").alias("PercMatrizNeogrid"),
        F.sum("QtDemanda").alias("TotalQtDemanda")
    
    )
)

df_comparacao_regiao = {}

df_comparacao_regiao['TELAS'] = (
    df_comparacao['TELAS']
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica"),
          how="left",
          on="CdFilial")
    .groupBy("NmRegiaoGeografica")
    .agg(
        F.avg("Percentual_QtDemanda").alias("Percentual_QtDemanda"),
        F.avg("PercMatrizNova").alias("PercMatrizNova"),
        F.avg("PercMatrizNeogrid").alias("PercMatrizNeogrid"),
        F.sum("QtDemanda").alias("TotalQtDemanda")
    
    )
)

df_comparacao_porte['TELAS'].display()

# COMMAND ----------

from pyspark.sql import functions as F

# ---------- PORTE ----------
df_p = (
    df_comparacao_porte['TELAS']
    .withColumn("den_nova",     F.abs(F.col("PercMatrizNova"))     + F.abs(F.col("Percentual_QtDemanda")))
    .withColumn("den_neogrid",  F.abs(F.col("PercMatrizNeogrid"))  + F.abs(F.col("Percentual_QtDemanda")))
    .withColumn("SMAPE_MatrizNova",
                F.when(F.col("den_nova") > 0,
                       200 * F.abs(F.col("PercMatrizNova") - F.col("Percentual_QtDemanda")) / F.col("den_nova"))
                 .otherwise(F.lit(0.0)))
    .withColumn("SMAPE_MatrizNeogrid",
                F.when(F.col("den_neogrid") > 0,
                       200 * F.abs(F.col("PercMatrizNeogrid") - F.col("Percentual_QtDemanda")) / F.col("den_neogrid"))
                 .otherwise(F.lit(0.0)))
    .withColumn("WSMAPE_term_MatrizNova",    F.col("SMAPE_MatrizNova")    * F.col("TotalQtDemanda"))
    .withColumn("WSMAPE_term_MatrizNeogrid", F.col("SMAPE_MatrizNeogrid") * F.col("TotalQtDemanda"))
)

# SMAPE médio por porte
smape_por_porte = df_p.select(
    "NmPorteLoja",
    "SMAPE_MatrizNova",
    "SMAPE_MatrizNeogrid"
)

# WSMAPE global (toda a tabela)
wsmape_porte = df_p.agg(
    (F.sum("WSMAPE_term_MatrizNova")    / F.sum("TotalQtDemanda")).alias("WSMAPE_MatrizNova"),
    (F.sum("WSMAPE_term_MatrizNeogrid") / F.sum("TotalQtDemanda")).alias("WSMAPE_MatrizNeogrid")
)

# ---------- REGIÃO ----------
df_r = (
    df_comparacao_regiao['TELAS']
    .withColumn("den_nova",     F.abs(F.col("PercMatrizNova"))     + F.abs(F.col("Percentual_QtDemanda")))
    .withColumn("den_neogrid",  F.abs(F.col("PercMatrizNeogrid"))  + F.abs(F.col("Percentual_QtDemanda")))
    .withColumn("SMAPE_MatrizNova",
                F.when(F.col("den_nova") > 0,
                       200 * F.abs(F.col("PercMatrizNova") - F.col("Percentual_QtDemanda")) / F.col("den_nova"))
                 .otherwise(F.lit(0.0)))
    .withColumn("SMAPE_MatrizNeogrid",
                F.when(F.col("den_neogrid") > 0,
                       200 * F.abs(F.col("PercMatrizNeogrid") - F.col("Percentual_QtDemanda")) / F.col("den_neogrid"))
                 .otherwise(F.lit(0.0)))
    .withColumn("WSMAPE_term_MatrizNova",    F.col("SMAPE_MatrizNova")    * F.col("TotalQtDemanda"))
    .withColumn("WSMAPE_term_MatrizNeogrid", F.col("SMAPE_MatrizNeogrid") * F.col("TotalQtDemanda"))
)

# SMAPE médio por região
smape_por_regiao = df_r.select(
    "NmRegiaoGeografica",
    "SMAPE_MatrizNova",
    "SMAPE_MatrizNeogrid"
)

# WSMAPE global (toda a tabela)
wsmape_regiao = df_r.agg(
    (F.sum("WSMAPE_term_MatrizNova")    / F.sum("TotalQtDemanda")).alias("WSMAPE_MatrizNova"),
    (F.sum("WSMAPE_term_MatrizNeogrid") / F.sum("TotalQtDemanda")).alias("WSMAPE_MatrizNeogrid")
)

# Visualizar
smape_por_porte.show()
wsmape_porte.show()
smape_por_regiao.show()
wsmape_regiao.show()

# COMMAND ----------

(
    df_comparacao['TELEFONIA']
    .filter(F.col("grupo_de_necessidade") == 'Telef pp')
    .join(spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
          .select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica"),
          how="left",
          on="CdFilial")
).display()

# COMMAND ----------

df_base_calculo_factual.limit(1).display()

# COMMAND ----------

df_porte_bucket_filial = (
    df_base_calculo_factual
    .join(de_para_modelos_gemeos,
          how="left",
          on="CdSku")
    
    .groupBy("grupo_de_necessidade", "CdFilial", "NmPorteLoja")
    .agg(
        F.median('DDE').alias('DDE_medio'),
    )
    .filter(F.col("grupo_de_necessidade") == 'Telef pp')
    .filter(F.col("NmPorteLoja").isNotNull())
    .filter(F.col("NmPorteLoja") != '-')
)

df_porte_bucket_filial.display()


# COMMAND ----------

df_porte_bucket_filial_agg = (
    df_porte_bucket_filial
    .withColumn(
        "bucket_DDE",
        F.when(F.col("DDE_medio") < 15, "0–15 dias")
         .when((F.col("DDE_medio") >= 15) & (F.col("DDE_medio") < 30), "15–30 dias")
         .when((F.col("DDE_medio") >= 30) & (F.col("DDE_medio") < 60), "30–60 dias")
         .when((F.col("DDE_medio") >= 60) & (F.col("DDE_medio") < 90), "60–90 dias")
         .otherwise("90+ dias")
    )
    .groupBy("grupo_de_necessidade", "bucket_DDE")
    .agg(
        F.count("*").alias("qtd_filiais"),
        F.mean("DDE_medio").alias("DDE_medio")
    )
)

df_porte_bucket_filial_agg.display()

# COMMAND ----------


df_porte_bucket = (
    df_porte_bucket_filial
    .withColumn(
        "bucket_DDE",
        F.when(F.col("DDE_medio") < 15, "0–15 dias")
         .when((F.col("DDE_medio") >= 15) & (F.col("DDE_medio") < 30), "15–30 dias")
         .when((F.col("DDE_medio") >= 30) & (F.col("DDE_medio") < 60), "30–60 dias")
         .when((F.col("DDE_medio") >= 60) & (F.col("DDE_medio") < 90), "60–90 dias")
         .otherwise("90+ dias")
    )
    .groupBy("grupo_de_necessidade", "NmPorteLoja")
    .agg(
        F.count("*").alias("qtd_filiais"),
        F.mean("DDE_medio").alias("DDE_medio")
    )
    .orderBy("NmPorteLoja")
)

df_porte_bucket.display()




# COMMAND ----------


w = Window.partitionBy("grupo_de_necessidade", "NmPorteLoja")
df_bucket_pct = (
    df_porte_bucket_filial
    .withColumn(
        "bucket_DDE",
        F.when(F.col("DDE") < 15, "0–15 dias")
         .when((F.col("DDE") >= 15) & (F.col("DDE") < 30), "15–30 dias")
         .when((F.col("DDE") >= 30) & (F.col("DDE") < 60), "30–60 dias")
         .when((F.col("DDE") >= 60) & (F.col("DDE") < 90), "60–90 dias")
         .otherwise("90+ dias")
    )
    .withColumn("total_filiais", F.sum("qtd_filiais").over(w))
    .withColumn("pct_bucket", F.col("qtd_filiais") / F.col("total_filiais"))
    .withColumn("pct_bucket_perc", (F.col("pct_bucket") * 100.0))
    )



df_bucket_pct.display()


# COMMAND ----------

from pyspark.sql import functions as F

df_bucket = (
    df_base_calculo_factual
    .join(de_para_modelos_gemeos, how="left", on="CdSku")
    .groupBy("grupo_de_necessidade", "NmPorteLoja")
    .agg(
        F.mean('DDE').alias('DDE_medio'),
    )
    .filter(F.col("grupo_de_necessidade") == 'Telef Alto')
    # criar bucket
    .withColumn(
        "bucket_DDE",
        F.when(F.col("DDE_medio") < 15, "0–15 dias")
         .when((F.col("DDE_medio") >= 15) & (F.col("DDE_medio") < 30), "15–30 dias")
         .when((F.col("DDE_medio") >= 30) & (F.col("DDE_medio") < 60), "30–60 dias")
         .when((F.col("DDE_medio") >= 60) & (F.col("DDE_medio") < 90), "60–90 dias")
         .otherwise("90+ dias")
    )
    # .groupBy("bucket_DDE", "NmPorteLoja")
    # .agg(F.count("*").alias("qtd_filiais"))
    # .orderBy("bucket_DDE")
)

df_bucket.display()
