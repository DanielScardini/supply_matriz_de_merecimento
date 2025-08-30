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
# MAGIC - Gráfico de barras empilhadas: Vendas mensais (k unidades) por região geográfica
# MAGIC - Gráfico de barras empilhadas: Proporção % de vendas por região geográfica

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
    spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    .filter(F.col("NmAgrupamentoDiretoriaSetor").isin(
        "DIRETORIA TELEFONIA CELULAR",
        "DIRETORIA DE TELAS",
        # "DIRETORIA DE LINHA BRANCA",
        # "DIRETORIA LINHA LEVE",
        # "DIRETORIA INFO/PERIFERICOS"
    ))
    .filter(F.col("year_month").isNotNull())
)

print(f"✅ Dados base carregados: {df_base_merecimento.count():,} registros")

df_base_merecimento.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carregamento de Mapeamento de Gêmeos

# COMMAND ----------

# Carrega mapeamento de gêmeos

de_para_gemeos = (
    pd.read_csv('../dados_analise/ITENS_GEMEOS 2.csv',
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
    .filter(~F.col("gemeos").contains("Chip"))  # Filtra chips conforme exemplo'
    .filter(F.col("gemeos") != "-")  # Filtra chips conforme exemplo'

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
# MAGIC 
# MAGIC **IMPORTANTE**: Implementada solução para garantir que todos os meses sejam incluídos nos gráficos,
# MAGIC mesmo quando não há vendas para todos os portes de loja em um determinado mês.

# COMMAND ----------

# Filtra apenas os top gêmeos
df_top = (
    df_completo
    .join(top_5_gemeos.select("NmAgrupamentoDiretoriaSetor", "gemeos"), 
          on=["NmAgrupamentoDiretoriaSetor", "gemeos"], 
          how="inner")
)

print(f"🔍 Debug: Dados após join com top gêmeos: {df_top.count():,} registros")
print(f"🔍 Debug: Gêmeos únicos após join: {df_top.select('gemeos').distinct().count()}")
print(f"🔍 Debug: Diretorias únicas após join: {df_top.select('NmAgrupamentoDiretoriaSetor').distinct().count()}")
print(f"🔍 Debug: Meses únicos após join: {df_top.select('year_month').distinct().count()}")
print(f"🔍 Debug: Sample de gêmeos: {df_top.select('gemeos').distinct().limit(5).collect()}")
print(f"🔍 Debug: Verificando se há dados para cada gêmeo:")
for gemeo in top_5_gemeos.select('gemeos').distinct().collect():
    gemeo_nome = gemeo['gemeos']
    count_gemeo = df_top.filter(F.col('gemeos') == gemeo_nome).count()
    print(f"    • {gemeo_nome}: {count_gemeo:,} registros")

# Agrega por year_month, gemeo, porte de loja e região
df_agregado = (
    df_top
    .groupBy(
        "year_month",
        "gemeos",
        "NmAgrupamentoDiretoriaSetor",
        "NmPorteLoja",
        "NmRegiaoGeografica"
    )
    .agg(
        F.sum("QtMercadoria").alias("qt_vendas"),
    )
    .orderBy("year_month", "gemeos")
)

print(f"🔍 Debug: Dados após agregação: {df_agregado.count():,} registros")
print(f"🔍 Debug: Gêmeos únicos após agregação: {df_agregado.select('gemeos').distinct().count()}")
print(f"🔍 Debug: Meses únicos após agregação: {df_agregado.select('year_month').distinct().count()}")
print(f"🔍 Debug: Sample de gêmeos após agregação: {df_agregado.select('gemeos').distinct().limit(5).collect()}")

# Converte para pandas para plotagem
df_graficos = df_agregado.toPandas()

print(f"🔍 Debug: Dados após conversão para pandas - Total: {len(df_graficos):,}")
print(f"🔍 Debug: Colunas disponíveis: {list(df_graficos.columns)}")
print(f"🔍 Debug: Sample de dados: {df_graficos.head(3).to_dict('records')}")
print(f"🔍 Debug: Dados antes da conversão - Total: {len(df_graficos):,}")
print(f"🔍 Debug: year_month antes da conversão: {df_graficos['year_month'].head().tolist()}")
print(f"🔍 Debug: year_month tipo: {df_graficos['year_month'].dtype}")

# Converte year_month para formato de data com tratamento de erros
df_graficos['year_month'] = pd.to_datetime(df_graficos['year_month'].astype(str), format='%Y%m', errors='coerce')

print(f"🔍 Debug: year_month depois da conversão: {df_graficos['year_month'].head().tolist()}")
print(f"🔍 Debug: year_month tipo depois: {df_graficos['year_month'].dtype}")
print(f"🔍 Debug: Valores NaT encontrados: {df_graficos['year_month'].isna().sum()}")
print(f"🔍 Debug: Valores válidos: {df_graficos['year_month'].notna().sum()}")

# Remove registros com datas inválidas
df_graficos = df_graficos[df_graficos['year_month'].notna()].copy()
print(f"🔍 Debug: Dados após remoção de datas inválidas: {len(df_graficos):,} registros")

# Remove lojas sem porte e preenche valores nulos
df_graficos = df_graficos[df_graficos['NmPorteLoja'].notna() & (df_graficos['NmPorteLoja'] != '')]
df_graficos['NmRegiaoGeografica'] = df_graficos['NmRegiaoGeografica'].fillna('SEM REGIÃO')

print(f"✅ Dados preparados para gráficos: {len(df_graficos):,} registros")

# Verifica se há dados válidos antes de tentar formatar datas
if len(df_graficos) > 0 and df_graficos['year_month'].notna().any():
    print(f"🔍 Debug: Período total dos dados: {df_graficos['year_month'].min().strftime('%b/%Y')} a {df_graficos['year_month'].max().strftime('%b/%Y')}")
    print(f"🔍 Debug: Total de meses únicos: {df_graficos['year_month'].nunique()}")
    print(f"🔍 Debug: Meses disponíveis: {sorted(df_graficos['year_month'].dt.strftime('%Y-%m').unique())}")
else:
    print("🔍 Debug: Nenhum dado válido encontrado após preparação")
    print("🔍 Debug: Verificando dados originais...")

print(f"🔍 Debug: Gêmeos únicos: {df_graficos['gemeos'].nunique() if len(df_graficos) > 0 else 0}")
print(f"🔍 Debug: Regiões únicas: {df_graficos['NmRegiaoGeografica'].nunique() if len(df_graficos) > 0 else 0}")
print(f"🔍 Debug: Portes únicos: {df_graficos['NmPorteLoja'].nunique() if len(df_graficos) > 0 else 0}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Função para Criação dos Gráficos de Elasticidade

# COMMAND ----------

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def criar_grafico_elasticidade_porte(
    df: pd.DataFrame,
    gemeo: str,
    diretoria: str
) -> go.Figure:
    """Gráfico de elasticidade por porte com anotações visíveis."""
    df_gemeo = df[df['gemeos'] == gemeo].copy()
    if df_gemeo.empty:
        print(f"⚠️  Nenhum dado encontrado para o gêmeo: {gemeo}")
        return go.Figure()
    
    print(f"    🔍 Debug: Dados do gêmeo {gemeo}: {len(df_gemeo)} registros")
    print(f"    🔍 Debug: Meses disponíveis: {sorted(df_gemeo['year_month'].unique())}")
    print(f"    🔍 Debug: Portes disponíveis: {df_gemeo['NmPorteLoja'].unique()}")

    df_agrupado = (
        df_gemeo.groupby(['year_month', 'NmPorteLoja'])
        .agg({'qt_vendas': 'sum'}).reset_index()
    )

    # Garante que todos os meses sejam incluídos, mesmo sem vendas
    df_pivot = (
        df_agrupado.pivot(index='year_month', columns='NmPorteLoja', values='qt_vendas')
        .fillna(0)
        .reindex(sorted(df_gemeo['year_month'].unique()))  # Inclui todos os meses unicos
        .sort_index()
    )
    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"<b>Vendas mensais (k unid.) de {gemeo} por porte de loja</b>",
            f"<b>Proporção % de vendas de {gemeo} por porte de loja</b>",
        ],
        specs=[[{"type": "bar"}, {"type": "bar"}]],
        horizontal_spacing=0.12,
    )

    ordem = ['PORTE 6','PORTE 5','PORTE 4','PORTE 3','PORTE 2','PORTE 1']
    portas = [p for p in ordem if p in df_pivot.columns]
    cores = {
        'PORTE 6':'#1a365d','PORTE 5':'#2c5282','PORTE 4':'#3182ce',
        'PORTE 3':'#4299e1','PORTE 2':'#63b3ed','PORTE 1':'#90cdf4'
    }

    x_labels = pd.to_datetime(df_pivot.index).strftime('%b/%y').tolist()
    x_labels_prop = pd.to_datetime(df_prop.index).strftime('%b/%y').tolist()

    # barras
    for p in portas:
        fig.add_trace(
            go.Bar(
                x=x_labels, y=(df_pivot[p]/1000), name=p, marker_color=cores[p],
                marker_line_color="#FFFFFF", marker_line_width=0.7,
                hovertemplate=f"<b>{p}</b><br>Mês: %{ '{' }x{'}' }<br>Vendas: %{ '{' }y:.1f{'}' }k<extra></extra>"
            ),
            row=1, col=1
        )
    for p in portas:
        fig.add_trace(
            go.Bar(
                x=x_labels_prop, y=df_prop[p], name=p, marker_color=cores[p], showlegend=False,
                marker_line_color="#FFFFFF", marker_line_width=0.7,
                hovertemplate=f"<b>{p}</b><br>Mês: %{ '{' }x{'}' }<br>Proporção: %{ '{' }y:.1f{'}' }%<extra></extra>"
            ),
            row=1, col=2
        )

    # eixo X categórico fixo
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=x_labels, row=1, col=1)
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=x_labels_prop, row=1, col=2)

    # headroom + anotações
    totais = (df_pivot.sum(axis=1)/1000).astype(float)
    y_max = float(totais.max())*1.25 if len(totais) else 1
    fig.update_yaxes(range=[0, y_max], row=1, col=1)

    for xi, total in zip(x_labels, totais):
        fig.add_annotation(
            x=xi, y=float(total), text=f"{float(total):.1f}k",
            showarrow=False, yshift=14,
            font=dict(size=12, color='#2c3e50'),
            row=1, col=1
        )

    # layout
    fig.update_layout(
        title=dict(
            text=(f"<b>Eventos e apostas | Dinâmica de vendas se altera significativamente</b>"
                  f"<br><sub style='color:#7f8c8d'>{gemeo} - {diretoria} - APENAS PORTE DE LOJA</sub>"),
            x=0.5, xanchor='center', pad=dict(t=10, b=6)
        ),
        barmode='stack', bargap=0.15, bargroupgap=0.04,
        height=780, width=1400,
        plot_bgcolor='#F2F2F2', paper_bgcolor='#F2F2F2',
        legend=dict(orientation="h", x=0.5, xanchor="center", y=-0.22),
        margin=dict(l=90, r=90, t=170, b=160, pad=12),
        hovermode="x unified",
    )
    # subtítulos
    for i, ann in enumerate(fig.layout.annotations):
        if i < 2:
            ann.update(y=1.09, yanchor='bottom', font=dict(size=13, color='#2c3e50'))

    # eixos
    fig.update_yaxes(range=[0, 100], row=1, col=2, title_text="<b>Proporção % de vendas</b>")
    fig.update_yaxes(title_text="<b>Vendas mensais (k unid.)</b>", row=1, col=1)
    fig.update_xaxes(title_text="<b>Mês</b>", row=1, col=1)
    fig.update_xaxes(title_text="<b>Mês</b>", row=1, col=2)

    return fig


def criar_grafico_elasticidade_porte_regiao(
    df: pd.DataFrame,
    gemeo: str,
    diretoria: str
) -> go.Figure:
    """Gráfico de elasticidade por porte + região com anotações visíveis."""
    df_gemeo = df[df['gemeos'] == gemeo].copy()
    if df_gemeo.empty:
        print(f"⚠️  Nenhum dado encontrado para o gêmeo: {gemeo}")
        return go.Figure()

    df_agrupado = (
        df_gemeo.groupby(['year_month', 'NmPorteLoja', 'NmRegiaoGeografica'])
        .agg({'qt_vendas': 'sum'}).reset_index()
    )
    df_agrupado['porte_regiao'] = df_agrupado['NmPorteLoja'] + ' - ' + df_agrupado['NmRegiaoGeografica']

    df_pivot = (
        df_agrupado.pivot(index='year_month', columns='porte_regiao', values='qt_vendas')
        .fillna(0)
        .reindex(sorted(df_gemeo['year_month'].unique()))  # Inclui todos os meses unicos
        .sort_index()
    )
    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"<b>Vendas mensais (k unid.) de {gemeo} por porte + região</b>",
            f"<b>Proporção % de vendas de {gemeo} por porte + região</b>",
        ],
        specs=[[{"type": "bar"}, {"type": "bar"}]],
        horizontal_spacing=0.12,
    )

    cores = {
        'PORTE 6':'#1a365d','PORTE 5':'#2c5282','PORTE 4':'#3182ce',
        'PORTE 3':'#4299e1','PORTE 2':'#63b3ed','PORTE 1':'#90cdf4'
    }
    ordem = list(cores.keys())
    cols_ord = []
    for p in ordem:
        cols_ord.extend([c for c in df_pivot.columns if c.startswith(p)])

    x_labels = pd.to_datetime(df_pivot.index).strftime('%b/%y').tolist()
    x_labels_prop = pd.to_datetime(df_prop.index).strftime('%b/%y').tolist()

    for c in cols_ord:
        p = c.split(' - ')[0]
        fig.add_trace(
            go.Bar(
                x=x_labels, y=(df_pivot[c]/1000), name=c,
                marker_color=cores[p],
                marker_line_color="#FFFFFF", marker_line_width=0.7,
                hovertemplate=f"<b>{c}</b><br>Mês: %{ '{' }x{'}' }<br>Vendas: %{ '{' }y:.1f{'}' }k<extra></extra>"
            ),
            row=1, col=1
        )
    for c in cols_ord:
        p = c.split(' - ')[0]
        fig.add_trace(
            go.Bar(
                x=x_labels_prop, y=df_prop[c], name=c, showlegend=False,
                marker_color=cores[p],
                marker_line_color="#FFFFFF", marker_line_width=0.7,
                hovertemplate=f"<b>{c}</b><br>Mês: %{ '{' }x{'}' }<br>Proporção: %{ '{' }y:.1f{'}' }%<extra></extra>"
            ),
            row=1, col=2
        )

    # eixos categóricos
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=x_labels, row=1, col=1)
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=x_labels_prop, row=1, col=2)

    # headroom + anotações
    totais = (df_pivot.sum(axis=1)/1000).astype(float)
    y_max = float(totais.max())*1.25 if len(totais) else 1
    fig.update_yaxes(range=[0, y_max], row=1, col=1)

    for xi, total in zip(x_labels, totais):
        fig.add_annotation(
            x=xi, y=float(total), text=f"{float(total):.1f}k",
            showarrow=False, yshift=14,
            font=dict(size=12, color='#2c3e50'),
            row=1, col=1
        )

    fig.update_layout(
        title=dict(
            text=(f"<b>Eventos e apostas | Dinâmica de vendas se altera significativamente</b>"
                  f"<br><sub style='color:#7f8c8d'>{gemeo} - {diretoria} - PORTE + REGIÃO</sub>"),
            x=0.5, xanchor='center', pad=dict(t=10, b=6)
        ),
        barmode='stack', bargap=0.15, bargroupgap=0.04,
        height=780, width=1400,
        plot_bgcolor='#F2F2F2', paper_bgcolor='#F2F2F2',
        legend=dict(orientation="h", x=0.5, xanchor="center", y=-0.22),
        margin=dict(l=90, r=90, t=180, b=170, pad=12),
        hovermode="x unified",
    )
    for i, ann in enumerate(fig.layout.annotations):
        if i < 2:
            ann.update(y=1.09, yanchor='bottom', font=dict(size=13, color='#2c3e50'))

    # eixos
    fig.update_yaxes(title_text="<b>Vendas mensais (k unid.)</b>", row=1, col=1)
    fig.update_yaxes(title_text="<b>Proporção % de vendas</b>", range=[0, 100], row=1, col=2)
    fig.update_xaxes(title_text="<b>Mês</b>", row=1, col=1)
    fig.update_xaxes(title_text="<b>Mês</b>", row=1, col=2)

    return fig


def criar_grafico_elasticidade_regiao(
    df: pd.DataFrame,
    gemeo: str,
    diretoria: str
) -> go.Figure:
    """Gráfico de elasticidade apenas por região geográfica com anotações visíveis."""
    df_gemeo = df[df['gemeos'] == gemeo].copy()
    if df_gemeo.empty:
        print(f"⚠️  Nenhum dado encontrado para o gêmeo: {gemeo}")
        return go.Figure()
    
    print(f"    🔍 Debug: Dados do gêmeo {gemeo}: {len(df_gemeo)} registros")
    print(f"    🔍 Debug: Meses disponíveis: {sorted(df_gemeo['year_month'].unique())}")
    print(f"    🔍 Debug: Regiões disponíveis: {df_gemeo['NmRegiaoGeografica'].unique()}")

    df_agrupado = (
        df_gemeo.groupby(['year_month', 'NmRegiaoGeografica'])
        .agg({'qt_vendas': 'sum'}).reset_index()
    )
    
    # Garante que todos os meses sejam incluídos, mesmo sem vendas
    df_pivot = (
        df_agrupado.pivot(index='year_month', columns='NmRegiaoGeografica', values='qt_vendas')
        .fillna(0)
        .reindex(sorted(df_gemeo['year_month'].unique()))  # Inclui todos os meses unicos
        .sort_index()
    )
    
    print(f"    🔍 Debug: Pivot criado com {len(df_pivot)} meses e {len(df_pivot.columns)} regiões")
    print(f"    🔍 Debug: Colunas do pivot: {list(df_pivot.columns)}")
    
    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"<b>Vendas mensais (k unid.) de {gemeo} por região geográfica</b>",
            f"<b>Proporção % de vendas de {gemeo} por região geográfica</b>",
        ],
        specs=[[{"type": "bar"}, {"type": "bar"}]],
        horizontal_spacing=0.12,
    )

    # Cores para regiões (esquema de cores profissional)
    cores_regioes = {
        'SUL': '#1a365d', 'SUDESTE': '#2c5282', 'CENTRO-OESTE': '#3182ce',
        'NORDESTE': '#4299e1', 'NORTE': '#63b3ed', 'SEM REGIÃO': '#90cdf4'
    }
    
    # Ordem das regiões (mais importantes primeiro)
    ordem_regioes = ['SUDESTE', 'SUL', 'NORDESTE', 'CENTRO-OESTE', 'NORTE', 'SEM REGIÃO']
    regioes = [r for r in ordem_regioes if r in df_pivot.columns]
    
    print(f"    🔍 Debug: Regiões encontradas: {regioes}")
    print(f"    🔍 Debug: Total de regiões: {len(regioes)}")

    x_labels = pd.to_datetime(df_pivot.index).strftime('%b/%y').tolist()
    x_labels_prop = pd.to_datetime(df_prop.index).strftime('%b/%y').tolist()
    
    print(f"    🔍 Debug: Labels X criados: {len(x_labels)} meses")

    # Barras para vendas absolutas
    for regiao in regioes:
        cor = cores_regioes.get(regiao, '#90cdf4')
        fig.add_trace(
            go.Bar(
                x=x_labels, y=(df_pivot[regiao]/1000), name=regiao, marker_color=cor,
                marker_line_color="#FFFFFF", marker_line_width=0.7,
                hovertemplate=f"<b>{regiao}</b><br>Mês: %{ '{' }x{'}' }<br>Vendas: %{ '{' }y:.1f{'}' }k<extra></extra>"
            ),
            row=1, col=1
        )
    
    # Barras para proporções
    for regiao in regioes:
        cor = cores_regioes.get(regiao, '#90cdf4')
        fig.add_trace(
            go.Bar(
                x=x_labels_prop, y=df_prop[regiao], name=regiao, showlegend=False,
                marker_line_color="#FFFFFF", marker_line_width=0.7,
                hovertemplate=f"<b>{regiao}</b><br>Mês: %{ '{' }x{'}' }<br>Proporção: %{ '{' }y:.1f{'}' }%<extra></extra>"
            ),
            row=1, col=2
        )

    # Eixo X categórico fixo
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=x_labels, row=1, col=1)
    fig.update_xaxes(type='category', categoryorder='array', categoryarray=x_labels_prop, row=1, col=2)

    # Headroom + anotações
    totais = (df_pivot.sum(axis=1)/1000).astype(float)
    y_max = float(totais.max())*1.25 if len(totais) else 1
    fig.update_yaxes(range=[0, y_max], row=1, col=1)

    for xi, total in zip(x_labels, totais):
        fig.add_annotation(
            x=xi, y=float(total), text=f"{float(total):.1f}k",
            showarrow=False, yshift=14,
            font=dict(size=12, color='#2c3e50'),
            row=1, col=1
        )

    # Layout
    fig.update_layout(
        title=dict(
            text=(f"<b>Análise Regional | Dinâmica de vendas por região geográfica</b>"
                  f"<br><sub style='color:#7f8c8d'>{gemeo} - {diretoria} - APENAS REGIÃO GEOGRÁFICA</sub>"),
            x=0.5, xanchor='center', pad=dict(t=10, b=6)
        ),
        barmode='stack', bargap=0.15, bargroupgap=0.04,
        height=780, width=1400,
        plot_bgcolor='#F2F2F2', paper_bgcolor='#F2F2F2',
        legend=dict(orientation="h", x=0.5, xanchor="center", y=-0.22),
        margin=dict(l=90, r=90, t=170, b=160, pad=12),
        hovermode="x unified",
    )
    
    # Subtítulos
    for i, ann in enumerate(fig.layout.annotations):
        if i < 2:
            ann.update(y=1.09, yanchor='bottom', font=dict(size=13, color='#2c3e50'))

    # Eixos
    fig.update_yaxes(range=[0, 100], row=1, col=2, title_text="<b>Proporção % de vendas</b>")
    fig.update_yaxes(title_text="<b>Vendas mensais (k unid.)</b>", row=1, col=1)
    fig.update_xaxes(title_text="<b>Mês</b>", row=1, col=1)
    fig.update_xaxes(title_text="<b>Mês</b>", row=1, col=2)

    return fig


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Criação dos Gráficos para Cada Top Gêmeo
# MAGIC
# MAGIC %md
# MAGIC ### Execução da Análise Completa - Três Versões

# COMMAND ----------

print("🚀 Iniciando criação dos gráficos de elasticidade...")
print("📊 Serão criadas três versões para cada gêmeo:")
print("   1. APENAS por porte de loja")
print("   2. Por porte de loja + região geográfica")
print("   3. APENAS por região geográfica")
print("🎨 Gráficos configurados com alta resolução para slides profissionais")
!pip install -U kaleido
# Configurações globais para alta qualidade
import plotly.io as pio
pio.kaleido.scale = 2.0  # Aumenta escala para exportação de alta resolução

# Cria gráficos para cada top gêmeo
for _, row in top_5_gemeos.toPandas().iterrows():
    diretoria = row['NmAgrupamentoDiretoriaSetor']
    gemeo = row['gemeos']
    
    print(f"\n🎯 Processando: {gemeo} ({diretoria})")
    
    # VERSÃO 1: Apenas por porte de loja
    print(f"  📈 Criando versão APENAS por porte de loja...")
    fig_porte = criar_grafico_elasticidade_porte(df_graficos, gemeo, diretoria)
    
    if fig_porte and fig_porte.data:
        print(f"    ✅ Gráfico APENAS por porte criado com sucesso")
        print(f"    💾 Configurações de alta resolução aplicadas")
        fig_porte.show()
        
        # Salva versão de alta resolução para slides
        try:
            fig_porte.write_image(f"grafico_porte_{gemeo.replace(' ', '_')}.png", 
                                width=1400, height=900, scale=2)
            print(f"    💾 Imagem de alta resolução salva: grafico_porte_{gemeo.replace(' ', '_')}.png")
        except Exception as e:
            print(f"    ⚠️  Erro ao salvar imagem: {e}")
    else:
        print(f"    ⚠️  Nenhum dado para gráfico APENAS por porte")
    
    # VERSÃO 2: Por porte de loja + região geográfica
    print(f"  🌍 Criando versão por porte + região geográfica...")
    fig_porte_regiao = criar_grafico_elasticidade_porte_regiao(df_graficos, gemeo, diretoria)
    
    if fig_porte_regiao and fig_porte_regiao.data:
        print(f"    ✅ Gráfico por porte + região criado com sucesso")
        print(f"    💾 Configurações de alta resolução aplicadas")
        fig_porte_regiao.show()
        
        # Salva versão de alta resolução para slides
        try:
            fig_porte_regiao.write_image(f"grafico_porte_regiao_{gemeo.replace(' ', '_')}.png", 
                                       width=1400, height=900, scale=2)
            print(f"    💾 Imagem de alta resolução salva: grafico_porte_regiao_{gemeo.replace(' ', '_')}.png")
        except Exception as e:
            print(f"    ⚠️  Erro ao salvar imagem: {e}")
    else:
        print(f"    ⚠️  Nenhum dado para gráfico por porte + região")
    
    # VERSÃO 3: Apenas por região geográfica
    print(f"  🗺️  Criando versão APENAS por região geográfica...")
    fig_regiao = criar_grafico_elasticidade_regiao(df_graficos, gemeo, diretoria)
    
    if fig_regiao and fig_regiao.data:
        print(f"    ✅ Gráfico APENAS por região criado com sucesso")
        print(f"    💾 Configurações de alta resolução aplicadas")
        fig_regiao.show()
        
        # Salva versão de alta resolução para slides
        try:
            fig_regiao.write_image(f"grafico_regiao_{gemeo.replace(' ', '_')}.png", 
                                 width=1400, height=900, scale=2)
            print(f"    💾 Imagem de alta resolução salva: grafico_regiao_{gemeo.replace(' ', '_')}.png")
        except Exception as e:
            print(f"    ⚠️  Erro ao salvar imagem: {e}")
    else:
        print(f"    ⚠️  Nenhum dado para gráfico APENAS por região")

print("\n✅ Análise de elasticidade concluída!")
print(f"📊 Total de gráficos criados: {len(top_5_gemeos.toPandas()) * 3} (3 versões por gêmeo)")
print("🎨 Todos os gráficos foram exibidos usando plotly.show()")
print("💾 Imagens de alta resolução salvas para uso em slides profissionais")
print("🎯 Portes organizados em ordem descendente (Porte 6 no topo)")
print("🎨 Fundo concrete (#F2F2F2) aplicado para estética profissional")

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
print(f"   • Portes de loja: {df_graficos['NmPorteLoja'].nunique()}")
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
