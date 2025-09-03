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
    spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3')
    .filter(F.col("NmAgrupamentoDiretoriaSetor").isin(
        "DIRETORIA TELEFONIA CELULAR",
        "DIRETORIA DE TELAS",
        # "DIRETORIA DE LINHA BRANCA",
        # "DIRETORIA LINHA LEVE",
        # "DIRETORIA INFO/PERIFERICOS"
    ))
    .filter(F.col("year_month").isNotNull())
)
df_base_merecimento.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carregamento de Mapeamento de Gêmeos

# COMMAND ----------

# Carrega mapeamento de gêmeos

de_para_gemeos = (
    pd.read_csv('/Users/scardinidaniel/Library/CloudStorage/OneDrive-TheBostonConsultingGroup,Inc/Documents/GitHub/CB_supply_matriz_merecimento/supply_matriz_de_merecimento/src/dados_analise/ITENS_GEMEOS 2.csv',
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Identificação dos Top 5 Gêmeos por Diretoria

# COMMAND ----------

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

# Converte para pandas para plotagem
df_graficos = df_agregado.toPandas()

# Converte year_month para formato de data com tratamento de erros
df_graficos['year_month'] = pd.to_datetime(df_graficos['year_month'].astype(str), format='%Y%m', errors='coerce')

# # Remove registros com datas inválidas
# df_graficos = df_graficos[df_graficos['year_month'].notna()].copy()
# print(f"🔍 Debug: Dados após remoção de datas inválidas: {len(df_graficos):,} registros")

# Preenche valores nulos em vez de remover registros
df_graficos['NmPorteLoja'] = df_graficos['NmPorteLoja'].fillna('SEM PORTE')
df_graficos['NmRegiaoGeografica'] = df_graficos['NmRegiaoGeografica'].fillna('SEM REGIÃO')

print(f"✅ Dados preparados para gráficos: {len(df_graficos):,} registros")

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
    
    # print(f"    🔍 Debug: Dados do gêmeo {gemeo}: {len(df_gemeo)} registros")
    # print(f"    🔍 Debug: Meses disponíveis: {sorted(df_gemeo['year_month'].unique())}")
    # print(f"    🔍 Debug: Portes disponíveis: {df_gemeo['NmPorteLoja'].unique()}")

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
    
    # Calcula proporções garantindo que somem 100% exato
    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100
    
    # Garante que as proporções somem exatamente 100% (corrige erros de arredondamento)
    for idx in df_prop.index:
        row_sum = df_prop.loc[idx].sum()
        if abs(row_sum - 100) > 0.01:  # Se a diferença for maior que 0.01%
            # Normaliza para somar exatamente 100%
            df_prop.loc[idx] = (df_prop.loc[idx] / row_sum) * 100
    
    print(f"    🔍 Debug: Verificação das proporções (porte):")
    for idx in df_prop.index:
        row_sum = df_prop.loc[idx].sum()
        print(f"      • {idx.strftime('%b/%Y')}: {row_sum:.2f}%")

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"<b>Vendas mensais (k unid.) de {gemeo} por porte de loja</b>",
            f"<b>Proporção % de vendas de {gemeo} por porte de loja</b>",
        ],
        specs=[[{"type": "bar"}, {"type": "bar"}]],
        horizontal_spacing=0.12,
    )

    ordem = ['PORTE 6','PORTE 5','PORTE 4','PORTE 3','PORTE 2','PORTE 1', 'SEM PORTE']
    portas = [p for p in ordem if p in df_pivot.columns]
    cores = {
        'PORTE 6':'#1a365d','PORTE 5':'#2c5282','PORTE 4':'#3182ce',
        'PORTE 3':'#4299e1','PORTE 2':'#63b3ed','PORTE 1':'#90cdf4',
        'SEM PORTE':'#808080'  # Cinza para SEM PORTE
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

    # eixos - Garante altura consistente das barras de proporção
    fig.update_yaxes(range=[0, 100], row=1, col=2, title_text="<b>Proporção % de vendas</b>", 
                     tickmode='linear', dtick=20, tickformat='.0f')
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
    
    # Calcula proporções garantindo que somem 100% exato
    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100
    
    # Garante que as proporções somem exatamente 100% (corrige erros de arredondamento)
    for idx in df_prop.index:
        row_sum = df_prop.loc[idx].sum()
        if abs(row_sum - 100) > 0.001:  # Tolerância mais rigorosa (0.001%)
            # Normaliza para somar exatamente 100%
            df_prop.loc[idx] = (df_prop.loc[idx] / row_sum) * 100
            # Arredonda para 2 casas decimais para evitar problemas de precisão
            df_prop.loc[idx] = df_prop.loc[idx].round(2)
            # Garante que a soma seja exatamente 100.00
            df_prop.loc[idx] = df_prop.loc[idx].fillna(0)
            # Ajusta a maior categoria para garantir soma = 100%
            max_col = df_prop.loc[idx].idxmax()
            df_prop.loc[idx, max_col] = 100 - (df_prop.loc[idx].sum() - df_prop.loc[idx, max_col])
    
    # print(f"    🔍 Debug: Verificação das proporções (porte+região):")
    # for idx in df_prop.index:
    #     row_sum = df_prop.loc[idx].sum()
    #     print(f"      • {idx.strftime('%b/%Y')}: {row_sum:.2f}%")

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
        'PORTE 3':'#4299e1','PORTE 2':'#63b3ed','PORTE 1':'#90cdf4',
        'SEM PORTE':'#808080'  # Cinza para SEM PORTE
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
    
    # Garante altura consistente das barras de proporção
    fig.update_yaxes(range=[0, 100], row=1, col=2, title_text="<b>Proporção % de vendas</b>", 
                     tickmode='linear', dtick=20, tickformat='.0f')
    fig.update_yaxes(title_text="<b>Vendas mensais (k unid.)</b>", row=1, col=1)

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
    
    # print(f"    🔍 Debug: Dados do gêmeo {gemeo}: {len(df_gemeo)} registros")
    # print(f"    🔍 Debug: Meses disponíveis: {sorted(df_gemeo['year_month'].unique())}")
    # print(f"    🔍 Debug: Regiões disponíveis: {df_gemeo['NmRegiaoGeografica'].unique()}")

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
    
    # print(f"    🔍 Debug: Pivot criado com {len(df_pivot)} meses e {len(df_pivot.columns)} regiões")
    # print(f"    🔍 Debug: Colunas do pivot: {list(df_pivot.columns)}")
    
    # Calcula proporções garantindo que somem 100% exato
    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100
    
    # Garante que as proporções somem exatamente 100% (corrige erros de arredondamento)
    for idx in df_pivot.index:
        row_sum = df_prop.loc[idx].sum()
        if abs(row_sum - 100) > 0.001:  # Tolerância mais rigorosa (0.001%)
            # Normaliza para somar exatamente 100%
            df_prop.loc[idx] = (df_prop.loc[idx] / row_sum) * 100
            # Arredonda para 2 casas decimais para evitar problemas de precisão
            df_prop.loc[idx] = df_prop.loc[idx].round(2)
            # Garante que a soma seja exatamente 100.00
            df_prop.loc[idx] = df_prop.loc[idx].fillna(0)
            # Ajusta a maior categoria para garantir soma = 100%
            max_col = df_prop.loc[idx].idxmax()
            df_prop.loc[idx, max_col] = 100 - (df_prop.loc[idx].sum() - df_prop.loc[idx, max_col])
    
    # print(f"    🔍 Debug: Verificação das proporções (região):")
    # for idx in df_pivot.index:
    #     row_sum = df_prop.loc[idx].sum()
    #     print(f"      • {idx.strftime('%b/%Y')}: {row_sum:.2f}%")

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"<b>Vendas mensais (k unid.) de {gemeo} por região geográfica</b>",
            f"<b>Proporção % de vendas de {gemeo} por região geográfica</b>",
        ],
        specs=[[{"type": "bar"}, {"type": "bar"}]],
        horizontal_spacing=0.12,
    )

    # Detecta automaticamente os nomes reais das regiões nas colunas
    regioes_reais = list(df_pivot.columns)
    # print(f"    🔍 Debug: Regiões reais encontradas nas colunas: {regioes_reais}")
    
    # Cria mapeamento de cores dinâmico baseado nas regiões reais
    # Usa tons de azul semelhantes aos portes, com cinza para SEM REGIÃO
    cores_regioes = {
        'Sul': '#1a365d',
        'Sudeste': '#2c5282',
        'Norte': '#3182ce',
        'Nordeste': '#4299e1',
        'Centro Oeste': '#63b3ed',
        'SEM REGIÃO': '#808080'
    }    
    # print(f"    🔍 Debug: Cores atribuídas: {cores_regioes}")
    
    # Usa as regiões reais encontradas
    regioes = regioes_reais
    
    # print(f"    🔍 Debug: Regiões encontradas: {regioes}")
    # print(f"    🔍 Debug: Total de regiões: {len(regioes)}")

    x_labels = pd.to_datetime(df_pivot.index).strftime('%b/%y').tolist()
    x_labels_prop = pd.to_datetime(df_prop.index).strftime('%b/%y').tolist()
    
    # print(f"    🔍 Debug: Labels X criados: {len(x_labels)} meses")

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
                x=x_labels_prop, y=df_prop[regiao], name=regiao, showlegend=False, marker_color=cor,
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

    # Eixos - Garante altura consistente das barras de proporção
    fig.update_yaxes(range=[0, 100], row=1, col=2, title_text="<b>Proporção % de vendas</b>", 
                     tickmode='linear', dtick=20, tickformat='.0f')
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

from typing import Dict, List, Optional

def analisar_elasticidade_demanda_com_estatisticas(df_base: DataFrame, categoria: str) -> Dict[str, DataFrame]:
    """
    Analisa elasticidade de demanda com cálculo de média e desvio padrão da participação
    para cada agrupamento (porte, porte+região, região) e flagga meses com desvios significativos.
    """
    print(f"�� Analisando elasticidade de demanda com estatísticas para: {categoria}")
    
    # Filtrar dados da categoria específica
    df_categoria = df_base.filter(F.col("NmAgrupamentoDiretoriaSetor") == categoria)
    
    # Carregar mapeamento de gêmeos
    try:
        de_para_gemeos = (
            pd.read_csv('/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/ITENS_GEMEOS 2.csv',
                        delimiter=";", encoding='iso-8859-1')
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
        
        df_gemeos = spark.createDataFrame(de_para_gemeos.rename(columns={"sku_loja": "CdSku"}))
        
    except Exception as e:
        print(f"⚠️  Erro ao carregar mapeamento de gêmeos: {e}")
        return {}
    
    # Join com dados de gêmeos e região
    df_completo = (
        df_categoria
        .join(df_gemeos, on="CdSku", how="left")
        .join(
            spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
            .select("CdFilial", "NmRegiaoGeografica")
            .distinct(),
            on="CdFilial", how="left"
        )
        .filter(F.col("gemeos").isNotNull())
        .filter(~F.col("gemeos").contains("Chip"))
        .filter(F.col("gemeos") != "-")
    )
    
    # Identificar top 5 gêmeos
    top_gemeos = (
        df_completo
        .groupBy("gemeos")
        .agg(F.sum("QtMercadoria").alias("total_vendas"))
        .orderBy(F.desc("total_vendas"))
        .limit(5)
    )
    
    # Filtrar apenas top gêmeos
    df_top = df_completo.join(top_gemeos.select("gemeos"), on="gemeos", how="inner")
    
    # Preparar dados agregados por mês
    df_agregado = (
        df_top
        .groupBy("year_month", "gemeos", "NmPorteLoja", "NmRegiaoGeografica")
        .agg(F.sum("QtMercadoria").alias("qt_vendas"))
        .orderBy("year_month", "gemeos")
    )
    
    resultados = {}
    
    # 1. ANÁLISE POR PORTE DE LOJA
    print("📊 Calculando estatísticas por porte de loja...")
    df_porte = analisar_agrupamento_estatisticas(df_agregado, "NmPorteLoja", "PORTE")
    resultados["porte"] = df_porte
    
    # 2. ANÁLISE POR PORTE + REGIÃO
    print("📊 Calculando estatísticas por porte + região...")
    df_agregado_porte_regiao = (
        df_agregado
        .withColumn("porte_regiao", F.concat_ws(" - ", F.col("NmPorteLoja"), F.col("NmRegiaoGeografica")))
        .groupBy("year_month", "gemeos", "porte_regiao")
        .agg(F.sum("qt_vendas").alias("qt_vendas"))
    )
    df_porte_regiao = analisar_agrupamento_estatisticas(df_agregado_porte_regiao, "porte_regiao", "PORTE_REGIAO")
    resultados["porte_regiao"] = df_porte_regiao
    
    # 3. ANÁLISE POR REGIÃO
    print("📊 Calculando estatísticas por região...")
    df_regiao = analisar_agrupamento_estatisticas(df_agregado, "NmRegiaoGeografica", "REGIAO")
    resultados["regiao"] = df_regiao
    
    print(f"✅ Análise de elasticidade com estatísticas concluída para {categoria}")
    return resultados

def analisar_agrupamento_estatisticas(df_agregado: DataFrame, coluna_agrupamento: str, tipo_agrupamento: str) -> DataFrame:
    """
    Calcula estatísticas (média, desvio padrão) da participação para um agrupamento específico
    e flagga meses com desvios significativos.
    """
    print(f"  🔍 Processando agrupamento: {tipo_agrupamento}")
    
    # Calcular participação por mês e gêmeo
    w_total_mes_gemeo = Window.partitionBy("year_month", "gemeos")
    
    df_com_participacao = (
        df_agregado
        .withColumn("total_mes_gemeo", F.sum("qt_vendas").over(w_total_mes_gemeo))
        .withColumn(
            "participacao_percentual",
            F.when(F.col("total_mes_gemeo") > 0,
                   F.col("qt_vendas") / F.col("total_mes_gemeo") * 100)
            .otherwise(0.0)
        )
        .filter(F.col("total_mes_gemeo") > 0)  # Remove meses sem vendas
    )
    
    # Calcular estatísticas por agrupamento e gêmeo
    w_stats = Window.partitionBy("gemeos", coluna_agrupamento)
    
    df_com_stats = (
        df_com_participacao
        .withColumn("media_participacao", F.avg("participacao_percentual").over(w_stats))
        .withColumn("desvio_padrao_participacao", F.stddev("participacao_percentual").over(w_stats))
        .withColumn("min_participacao", F.min("participacao_percentual").over(w_stats))
        .withColumn("max_participacao", F.max("participacao_percentual").over(w_stats))
        .withColumn("qtd_meses", F.count("year_month").over(w_stats))
    )
    
    # Calcular flags de desvio
    df_com_flags = (
        df_com_stats
        .withColumn("desvio_1_sigma", F.abs(F.col("participacao_percentual") - F.col("media_participacao")))
        .withColumn(
            "flag_1_desvio_acima",
            F.when(F.col("participacao_percentual") > (F.col("media_participacao") + F.col("desvio_padrao_participacao")), F.lit(1)).otherwise(0)
        )
        .withColumn(
            "flag_1_desvio_abaixo",
            F.when(F.col("participacao_percentual") < (F.col("media_participacao") - F.col("desvio_padrao_participacao")), F.lit(1)).otherwise(0)
        )
        .withColumn(
            "flag_2_desvios_acima",
            F.when(F.col("participacao_percentual") > (F.col("media_participacao") + 2 * F.col("desvio_padrao_participacao")), F.lit(1)).otherwise(0)
        )
        .withColumn(
            "flag_2_desvios_abaixo",
            F.when(F.col("participacao_percentual") < (F.col("media_participacao") - 2 * F.col("desvio_padrao_participacao")), F.lit(1)).otherwise(0)
        )
        .withColumn(
            "flag_3_desvios_acima",
            F.when(F.col("participacao_percentual") > (F.col("media_participacao") + 3 * F.col("desvio_padrao_participacao")), F.lit(1)).otherwise(0)
        )
        .withColumn(
            "flag_3_desvios_abaixo",
            F.when(F.col("participacao_percentual") < (F.col("media_participacao") - 3 * F.col("desvio_padrao_participacao")), F.lit(1)).otherwise(0)
        )
        .withColumn("tipo_agrupamento", F.lit(tipo_agrupamento))
    )
    
    # Selecionar colunas finais
    colunas_finais = [
        "year_month", "gemeos", coluna_agrupamento, "qt_vendas", "participacao_percentual",
        "media_participacao", "desvio_padrao_participacao", "min_participacao", "max_participacao",
        "qtd_meses", "desvio_1_sigma",
        "flag_1_desvio_acima", "flag_1_desvio_abaixo",
        "flag_2_desvios_acima", "flag_2_desvios_abaixo", 
        "flag_3_desvios_acima", "flag_3_desvios_abaixo",
        "tipo_agrupamento"
    ]
    
    df_resultado = df_com_flags.select(*colunas_finais)
    
    print(f"    ✅ Estatísticas calculadas para {tipo_agrupamento}")
    print(f"    📊 Registros processados: {df_resultado.count():,}")
    
    return df_resultado

    # Executar análise
resultados = analisar_elasticidade_demanda_com_estatisticas(df_base_merecimento, "DIRETORIA DE TELAS")

# Acessar resultados por agrupamento
df_porte = resultados["porte"]
df_porte_regiao = resultados["porte_regiao"] 
df_regiao = resultados["regiao"]

# COMMAND ----------

df_porte.cache()
df_porte_regiao.cache()
df_regiao.cache()

df_porte_regiao.display()
df_regiao.display()

# COMMAND ----------

df_graficos.display()

# COMMAND ----------

# Faz o pivot: linhas = Porte, colunas = Mês, valores = soma das vendas
pivot_df = (
    df_graficos
    .query("gemeos == 'TV 50 ALTO P'")
    .query("NmPorteLoja != '-'")
    .query("NmPorteLoja != 'SEM PORTE'")

    .pivot_table(
        index="NmPorteLoja",
        columns=df_graficos["year_month"].dt.to_period("M"),
        values="qt_vendas",
        aggfunc="sum",
        fill_value=0
    ).sort_index(ascending=False)
)

# Converte os nomes das colunas para string (ano/mês)
pivot_df.columns = pivot_df.columns.astype(str)

# Resetando o index para enxergar como coluna
pivot_df = pivot_df.reset_index()

print('TV 50 ALTO P')
pivot_df.display()

# COMMAND ----------

import pandas as pd

def build_pivots_por_porte(
    df_graficos: pd.DataFrame,
    gemeos_list: list[str],
    excluir_portes: tuple[str, ...] = ("-", "SEM PORTE", "SEM REGIÃO"),
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Para cada 'gemeos' na lista:
      - faz pivot com soma de qt_vendas por Porte x Mês
      - gera uma versão em percentuais coluna-a-coluna (cada mês = 100%)
      - percentuais formatados como string com vírgula decimal
    Retorna dois dicts: {gemeos: pivot_valores}, {gemeos: pivot_percentual}
    """

    work = (
        df_graficos
        .loc[
            df_graficos["gemeos"].isin(gemeos_list)
            & ~df_graficos["NmRegiaoGeografica"].isin(excluir_portes)
        ]
        .assign(period=df_graficos["year_month"].dt.to_period("M"))
    )

    pivots_val = {}
    pivots_pct = {}

    for g in gemeos_list:
        sub = work.loc[work["gemeos"] == g, ["NmRegiaoGeografica", "period", "qt_vendas"]]

        # Pivot valores absolutos
        pivot_df = (
            sub.pivot_table(
                index="NmRegiaoGeografica",
                columns="period",
                values="qt_vendas",
                aggfunc="sum",
                fill_value=0,
            )
            .sort_index(ascending=False)
        )

        pivot_df.columns = pivot_df.columns.astype(str)
        pivot_df = pivot_df.reset_index()
        pivot_df.insert(0, "gemeos", g)
        pivots_val[g] = pivot_df

        # Pivot percentuais
        pct = pivot_df.drop(columns=["gemeos", "NmRegiaoGeografica"])
        col_sums = pct.sum(axis=0)
        pct_df = (pct.div(col_sums.where(col_sums != 0), axis=1) * 100).round(2)

        # Converte para string e troca ponto por vírgula
        pct_df = pct_df.astype(str).apply(lambda col: col.str.replace(".", ","))

        pct_df.insert(0, "NmRegiaoGeografica", pivot_df["NmRegiaoGeografica"])
        pct_df.insert(0, "gemeos", g)
        pivots_pct[g] = pct_df

    return pivots_val, pivots_pct


# -----------------------
# Exemplo de uso
# -----------------------
gemeos_alvo = ["TV 50 ALTO P", "TV 43 PP", "Telef pp", "Telef Medio 256GB"]
pivots, pct_pivots = build_pivots_por_porte(df_graficos, gemeos_alvo)

print("Valores - TV 50 ALTO P")
display(pivots["TV 50 ALTO P"])

print("Percentuais - TV 50 ALTO P")
display(pct_pivots["TV 50 ALTO P"])

# COMMAND ----------

print("Valores - TV 43 PP")
display(pivots["TV 43 PP"])

print("Percentuais - TV 43 PP")
display(pct_pivots["TV 43 PP"])

# COMMAND ----------

print("Valores - Telef pp")
display(pivots["Telef pp"])

print("Percentuais - Telef pp")
display(pct_pivots["Telef pp"])

# COMMAND ----------

print("Valores - Telef Medio 256GB")
display(pivots["Telef Medio 256GB"])

print("Percentuais - Telef Medio 256GB")
display(pct_pivots["Telef Medio 256GB"])

# COMMAND ----------

import pandas as pd

def build_pivots_por_porte(
    df_graficos: pd.DataFrame,
    gemeos_list: list[str],
    excluir_portes: tuple[str, ...] = ("-", "SEM PORTE", "SEM REGIÃO"),
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Para cada 'gemeos' na lista:
      - faz pivot com soma de qt_vendas por Porte x Mês
      - gera uma versão em percentuais coluna-a-coluna (cada mês = 100%)
      - percentuais formatados como string com vírgula decimal
    Retorna dois dicts: {gemeos: pivot_valores}, {gemeos: pivot_percentual}
    """

    work = (
        df_graficos
        .loc[
            df_graficos["gemeos"].isin(gemeos_list)
            & ~df_graficos["NmPorteLoja"].isin(excluir_portes)
        ]
        .assign(period=df_graficos["year_month"].dt.to_period("M"))
    )

    pivots_val = {}
    pivots_pct = {}

    for g in gemeos_list:
        sub = work.loc[work["gemeos"] == g, ["NmPorteLoja", "period", "qt_vendas"]]

        # Pivot valores absolutos
        pivot_df = (
            sub.pivot_table(
                index="NmPorteLoja",
                columns="period",
                values="qt_vendas",
                aggfunc="sum",
                fill_value=0,
            )
            .sort_index(ascending=False)
        )

        pivot_df.columns = pivot_df.columns.astype(str)
        pivot_df = pivot_df.reset_index()
        pivot_df.insert(0, "gemeos", g)
        pivots_val[g] = pivot_df

        # Pivot percentuais
        pct = pivot_df.drop(columns=["gemeos", "NmPorteLoja"])
        col_sums = pct.sum(axis=0)
        pct_df = (pct.div(col_sums.where(col_sums != 0), axis=1) * 100).round(2)

        # Converte para string e troca ponto por vírgula
        pct_df = pct_df.astype(str).apply(lambda col: col.str.replace(".", ","))

        pct_df.insert(0, "NmPorteLoja", pivot_df["NmPorteLoja"])
        pct_df.insert(0, "gemeos", g)
        pivots_pct[g] = pct_df

    return pivots_val, pivots_pct


# -----------------------
# Exemplo de uso
# -----------------------
gemeos_alvo = ["TV 50 ALTO P", "TV 43 PP", "Telef pp", "Telef Medio 256GB"]
pivots, pct_pivots = build_pivots_por_porte(df_graficos, gemeos_alvo)

print("Valores - TV 50 ALTO P")
display(pivots["TV 50 ALTO P"])

print("Percentuais - TV 50 ALTO P")
display(pct_pivots["TV 50 ALTO P"])
