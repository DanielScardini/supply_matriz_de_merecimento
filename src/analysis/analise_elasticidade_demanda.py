# Databricks notebook source
# MAGIC %md
# MAGIC # Análise de Elasticidade de Demanda por Gêmeos
# MAGIC
# MAGIC Este notebook analisa a elasticidade de demanda dos produtos gêmeos, criando gráficos de barras empilhadas
# MAGIC que mostram a dinâmica de vendas ao longo do tempo, agrupando por year_month e quebrando por porte de loja
# MAGIC e região geográfica.
# MAGIC
# MAGIC **Objetivo**: Identificar padrões de elasticidade de demanda dos top 5 gêmeos de cada NmAgrupamentoDiretoriaSetor
# MAGIC **Escopo**: Análise temporal de vendas com quebras por porte de loja e região geográfica
# MAGIC
# MAGIC **Visualizações**:
# MAGIC - Gráfico de barras empilhadas: Vendas mensais (k unidades) por porte de loja
# MAGIC - Gráfico de barras empilhadas: Proporção % de vendas por porte de loja

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e Configurações Iniciais

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import List, Optional

# Inicialização do Spark
spark = SparkSession.builder.appName("analise_elasticidade_demanda").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregamento de Dados Base

# COMMAND ----------

# Carrega dados base de merecimento com todas as diretorias
df_base_merecimento = (
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v2')
    .filter(F.col("NmAgrupamentoDiretoriaSetor").isin(
        "DIRETORIA TELEFONIA CELULAR",
        "DIRETORIA DE TELAS",
        "DIRETORIA DE LINHA BRANCA",
        "DIRETORIA LINHA LEVE",
        "DIRETORIA INFO/PERIFERICOS"
    ))
)

print(f"✅ Dados base carregados: {df_base_merecimento.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carregamento de Mapeamento de Gêmeos

# COMMAND ----------

# Carrega mapeamento de gêmeos
try:
    de_para_gemeos = (
        pd.read_csv('/dbfs/dados_analise/ITENS_GEMEOS 2.csv',
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
except FileNotFoundError:
    print("⚠️  Arquivo de mapeamento de gêmeos não encontrado - criando dados de exemplo")
    # Cria dados de exemplo para demonstração
    de_para_gemeos = pd.DataFrame({
        'sku_loja': ['SKU001', 'SKU002', 'SKU003', 'SKU004', 'SKU005'],
        'gemeos': ['TELAS 43" PREMIUM', 'TELAS 55" SMART', 'CELULAR FLAGSHIP', 'GELADEIRA PREMIUM', 'NOTEBOOK GAMER']
    })

# Converte para DataFrame do Spark
df_gemeos = spark.createDataFrame(de_para_gemeos.rename(columns={"sku_loja": "CdSku"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Join com Dados de Gêmeos e Região

# COMMAND ----------

# Join com dados base e região geográfica
df_completo = (
    df_base_merecimento
    .join(df_gemeos, on="CdSku", how="left")
    .join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmRegiaoGeografica")
        .distinct(),
        on="CdFilial",
        how="left"
    )
    .filter(F.col("gemeos").isNotNull())
    .filter(~F.col("gemeos").contains("Chip"))  # Filtra chips conforme exemplo
)

print(f"✅ Dados completos preparados: {df_completo.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Identificação dos Top 5 Gêmeos por Diretoria

# COMMAND ----------

# Identifica os top 5 gêmeos de cada diretoria
top_gemeos = (
    df_completo
    .groupBy("NmAgrupamentoDiretoriaSetor", "gemeos")
    .agg(F.sum("QtMercadoria").alias("total_vendas"))
    .orderBy("NmAgrupamentoDiretoriaSetor", F.desc("total_vendas"))
)

# Aplica window para pegar top 5 de cada diretoria
w = Window.partitionBy("NmAgrupamentoDiretoriaSetor").orderBy(F.desc("total_vendas"))
top_5_gemeos = (
    top_gemeos
    .withColumn("rank", F.rank().over(w))
    .filter(F.col("rank") <= 5)
    .drop("rank")
)

print("✅ Top 5 gêmeos identificados por diretoria")
display(top_5_gemeos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Preparação de Dados para Gráficos

# COMMAND ----------

# Filtra apenas os top gêmeos
df_top = (
    df_completo
    .join(top_5_gemeos.select("NmAgrupamentoDiretoriaSetor", "gemeos"), 
          on=["NmAgrupamentoDiretoriaSetor", "gemeos"], 
          how="inner")
)

# Agrega por year_month, gemeo, porte de loja e região
df_agregado = (
    df_top
    .groupBy(
        "year_month",
        "gemeos",
        "NmAgrupamentoDiretoriaSetor",
        "DsPorteLoja",
        "NmRegiaoGeografica"
    )
    .agg(
        F.sum("QtMercadoria").alias("qt_vendas"),
        F.sum("Receita").alias("receita_total")
    )
    .orderBy("year_month", "gemeos")
)

# Converte para pandas para plotagem
df_graficos = df_agregado.toPandas()

# Converte year_month para formato de data
df_graficos['year_month'] = pd.to_datetime(df_graficos['year_month'].astype(str), format='%Y%m')

# Preenche valores nulos
df_graficos['DsPorteLoja'] = df_graficos['DsPorteLoja'].fillna('SEM PORTE')
df_graficos['NmRegiaoGeografica'] = df_graficos['NmRegiaoGeografica'].fillna('SEM REGIÃO')

print(f"✅ Dados preparados para gráficos: {len(df_graficos):,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Função para Criação dos Gráficos de Elasticidade

# COMMAND ----------

def criar_grafico_elasticidade(
    df: pd.DataFrame, 
    gemeo: str, 
    diretoria: str
) -> go.Figure:
    """
    Cria gráfico de elasticidade seguindo o molde da imagem de referência.
    
    Args:
        df: DataFrame pandas com dados preparados
        gemeo: Nome do gêmeo para filtrar
        diretoria: Nome da diretoria
        
    Returns:
        Figura plotly com dois gráficos de barras empilhadas
    """
    # Filtra dados para o gêmeo específico
    df_gemeo = df[df['gemeos'] == gemeo].copy()
    
    if df_gemeo.empty:
        print(f"⚠️  Nenhum dado encontrado para o gêmeo: {gemeo}")
        return go.Figure()
    
    # Agrupa por year_month e porte de loja
    df_agrupado = (
        df_gemeo
        .groupby(['year_month', 'DsPorteLoja'])
        .agg({
            'qt_vendas': 'sum',
            'receita_total': 'sum'
        })
        .reset_index()
    )
    
    # Pivota para formato de barras empilhadas
    df_pivot = df_agrupado.pivot(
        index='year_month', 
        columns='DsPorteLoja', 
        values='qt_vendas'
    ).fillna(0)
    
    # Calcula proporções percentuais
    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100
    
    # Cria subplots
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"Vendas mensais (k unid.) de {gemeo} por porte de loja",
            f"Proporção % de vendas de {gemeo} por porte de loja"
        ],
        specs=[[{"type": "bar"}, {"type": "bar"}]]
    )
    
    # Cores para porte de loja (gradiente de azul)
    cores_porte = {
        'Porte 6': '#1f4e79',  # Mais escuro
        'Porte 5': '#2d5a8b',
        'Porte 4': '#3b669d',
        'Porte 3': '#4972af',
        'Porte 2': '#577ec1',
        'Porte 1': '#658ad3',  # Mais claro
        'SEM PORTE': '#cccccc'
    }
    
    # Gráfico 1: Vendas mensais em k unidades
    for porte in df_pivot.columns:
        if porte in cores_porte:
            fig.add_trace(
                go.Bar(
                    x=df_pivot.index.strftime('%b/%y'),
                    y=df_pivot[porte] / 1000,  # Converte para k unidades
                    name=porte,
                    marker_color=cores_porte[porte],
                    showlegend=True,
                    hovertemplate=f'<b>{porte}</b><br>' +
                                'Mês: %{x}<br>' +
                                'Vendas: %{y:.1f}k unid.<br>' +
                                '<extra></extra>'
                ),
                row=1, col=1
            )
    
    # Gráfico 2: Proporção percentual
    for porte in df_prop.columns:
        if porte in cores_porte:
            fig.add_trace(
                go.Bar(
                    x=df_prop.index.strftime('%b/%y'),
                    y=df_prop[porte],
                    name=porte,
                    marker_color=cores_porte[porte],
                    showlegend=False,
                    hovertemplate=f'<b>{porte}</b><br>' +
                                'Mês: %{x}<br>' +
                                'Proporção: %{y:.1f}%<br>' +
                                '<extra></extra>'
                ),
                row=1, col=2
            )
    
    # Adiciona valores totais no topo das barras (gráfico 1)
    totais_mensais = df_pivot.sum(axis=1) / 1000
    for i, total in enumerate(totais_mensais):
        fig.add_annotation(
            x=df_pivot.index[i].strftime('%b/%y'),
            y=total + 0.5,
            text=f"{total:.1f}k",
            showarrow=False,
            font=dict(size=10, color='black'),
            xref='x',
            yref='y'
        )
    
    # Configurações do layout
    fig.update_layout(
        title={
            'text': f"Eventos e apostas | Dinâmica de vendas se altera significativamente em eventos e apostas, impactando a proporção de merecimento<br><sub>{gemeo} - {diretoria}</sub>",
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 16}
        },
        barmode='stack',
        height=600,
        width=1200,
        plot_bgcolor='#F8F8FF',  # Off-white conforme regras
        paper_bgcolor='white',
        font=dict(family="Arial, sans-serif"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Configurações dos eixos
    fig.update_xaxes(
        title_text="Mês",
        tickangle=45,
        row=1, col=1
    )
    fig.update_xaxes(
        title_text="Mês",
        tickangle=45,
        row=1, col=2
    )
    
    fig.update_yaxes(
        title_text="Vendas mensais (k unid.)",
        row=1, col=1
    )
    
    fig.update_yaxes(
        title_text="Proporção % de vendas",
        row=1, col=2
    )
    
    return fig

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Criação dos Gráficos para Cada Top Gêmeo

# MAGIC %md
# MAGIC ### Execução da Análise Completa

# COMMAND ----------

print("🚀 Iniciando criação dos gráficos de elasticidade...")

# Cria gráficos para cada top gêmeo
for _, row in top_5_gemeos.toPandas().iterrows():
    diretoria = row['NmAgrupamentoDiretoriaSetor']
    gemeo = row['gemeos']
    
    print(f"  • Criando gráfico para: {gemeo} ({diretoria})")
    
    # Cria gráfico
    fig = criar_grafico_elasticidade(df_graficos, gemeo, diretoria)
    
    if fig.data:  # Verifica se o gráfico tem dados
        # Salva gráfico no DBFS
        nome_arquivo = f"elasticidade_{gemeo.replace(' ', '_').replace('"', '')}_{diretoria.replace(' ', '_')}.html"
        fig.write_html(f"/dbfs/outputs/{nome_arquivo}")
        print(f"    ✅ Gráfico salvo: {nome_arquivo}")
        
        # Exibe gráfico
        display(fig)
    else:
        print(f"    ⚠️  Nenhum dado para criar gráfico")

print("\n✅ Análise de elasticidade concluída!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumo dos Top Gêmeos por Diretoria

# COMMAND ----------

# Exibe resumo dos top gêmeos
print("📋 Resumo dos Top Gêmeos por Diretoria:")
top_gemeos_pandas = top_5_gemeos.toPandas()
for diretoria in top_gemeos_pandas['NmAgrupamentoDiretoriaSetor'].unique():
    print(f"\n{diretoria}:")
    gemeos_diretoria = top_gemeos_pandas[top_gemeos_pandas['NmAgrupamentoDiretoriaSetor'] == diretoria]
    for _, row in gemeos_diretoria.iterrows():
        print(f"  • {row['gemeos']}: {row['total_vendas']:,.0f} unidades")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Estatísticas dos Dados Preparados

# COMMAND ----------

# Exibe dados preparados para inspeção
print(f"📊 Dados preparados para gráficos: {len(df_graficos):,} registros")
print(f"   • Período: {df_graficos['year_month'].min().strftime('%b/%Y')} a {df_graficos['year_month'].max().strftime('%b/%Y')}")
print(f"   • Gêmeos únicos: {df_graficos['gemeos'].nunique()}")
print(f"   • Portes de loja: {df_graficos['DsPorteLoja'].nunique()}")
print(f"   • Regiões geográficas: {df_graficos['NmRegiaoGeografica'].nunique()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Análise por Região Geográfica

# COMMAND ----------

# Análise adicional: quebra por região geográfica
print("🌍 Análise por Região Geográfica:")

# Agrupa por gêmeo e região
df_regiao = (
    df_graficos
    .groupby(['gemeos', 'NmRegiaoGeografica'])
    .agg({
        'qt_vendas': 'sum',
        'receita_total': 'sum'
    })
    .reset_index()
)

# Exibe top 3 regiões por gêmeo
for gemeo in df_graficos['gemeos'].unique()[:3]:  # Primeiros 3 gêmeos
    print(f"\n{gemeo}:")
    df_gemeo_regiao = df_regiao[df_regiao['gemeos'] == gemeo].nlargest(3, 'qt_vendas')
    for _, row in df_gemeo_regiao.iterrows():
        print(f"  • {row['NmRegiaoGeografica']}: {row['qt_vendas']:,.0f} unidades")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Visualização dos Dados Agregados

# COMMAND ----------

# Exibe dados agregados para inspeção
print("📊 Dados Agregados por Mês e Gêmeo:")
df_agregado_display = (
    df_agregado
    .orderBy("year_month", "gemeos")
    .limit(20)
)

display(df_agregado_display)
