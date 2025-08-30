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

df_base_merecimento.display()

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

# Converte year_month para formato de data
df_graficos['year_month'] = pd.to_datetime(df_graficos['year_month'].astype(str), format='%Y%m')

# Remove lojas sem porte e preenche valores nulos
df_graficos = df_graficos[df_graficos['NmPorteLoja'].notna() & (df_graficos['NmPorteLoja'] != '')]
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
    """
    Gráfico de elasticidade por porte de loja com melhor espaçamento vertical.
    """
    df_gemeo = df[df['gemeos'] == gemeo].copy()
    if df_gemeo.empty:
        print(f"⚠️  Nenhum dado encontrado para o gêmeo: {gemeo}")
        return go.Figure()

    df_agrupado = (
        df_gemeo
        .groupby(['year_month', 'NmPorteLoja'])
        .agg({'qt_vendas': 'sum'})
        .reset_index()
    )

    df_pivot = df_agrupado.pivot(
        index='year_month',
        columns='NmPorteLoja',
        values='qt_vendas'
    ).fillna(0).sort_index()

    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"<b>Vendas mensais (k unid.) de {gemeo} por porte de loja</b>",
            f"<b>Proporção % de vendas de {gemeo} por porte de loja</b>"
        ],
        specs=[[{"type": "bar"}, {"type": "bar"}]],
        horizontal_spacing=0.12
    )

    ordem_portes = ['PORTE 6', 'PORTE 5', 'PORTE 4', 'PORTE 3', 'PORTE 2', 'PORTE 1']
    portes_validos = [p for p in ordem_portes if p in df_pivot.columns]

    cores_porte = {
        'PORTE 6': '#1a365d',
        'PORTE 5': '#2c5282',
        'PORTE 4': '#3182ce',
        'PORTE 3': '#4299e1',
        'PORTE 2': '#63b3ed',
        'PORTE 1': '#90cdf4',
    }

    x1 = pd.to_datetime(df_pivot.index).strftime('%b/%y')
    x2 = pd.to_datetime(df_prop.index).strftime('%b/%y')

    for porte in portes_validos:
        fig.add_trace(
            go.Bar(
                x=x1,
                y=df_pivot[porte] / 1000,
                name=porte,
                marker_color=cores_porte[porte],
                marker_line_color="#FFFFFF",
                marker_line_width=0.7,
                hovertemplate=(
                    f'<b>{porte}</b><br>'
                    'Mês: %{x}<br>'
                    'Vendas: %{y:.1f}k unid.<extra></extra>'
                )
            ),
            row=1, col=1
        )

    for porte in portes_validos:
        fig.add_trace(
            go.Bar(
                x=x2,
                y=df_prop[porte],
                name=porte,
                marker_color=cores_porte[porte],
                marker_line_color="#FFFFFF",
                marker_line_width=0.7,
                showlegend=False,
                hovertemplate=(
                    f'<b>{porte}</b><br>'
                    'Mês: %{x}<br>'
                    'Proporção: %{y:.1f}%<extra></extra>'
                )
            ),
            row=1, col=2
        )

    totais_mensais = df_pivot.sum(axis=1) / 1000
    for i, total in enumerate(totais_mensais):
        fig.add_annotation(
            x=x1[i],
            y=total,
            yanchor="bottom",
            yshift=14,  # mais distância do topo da barra
            text=f"{total:.1f}k",
            showarrow=False,
            font=dict(size=12, color='#2c3e50', family="Arial, sans-serif"),
            xref='x',
            yref='y'
        )

    # Headroom dinâmico no eixo Y do 1º subplot
    y_max = float(totais_mensais.max()) * 1.18 if len(totais_mensais) else None

    fig.update_layout(
        title=dict(
            text=(
                f"<b>Eventos e apostas | Dinâmica de vendas se altera significativamente em eventos e apostas, "
                f"impactando a proporção de merecimento</b>"
                f"<br><sub style='color:#7f8c8d; font-size:14px;'>{gemeo} - {diretoria} - APENAS PORTE DE LOJA</sub>"
            ),
            x=0.5, xanchor='center', y=0.98,
            font=dict(size=18, color='#2c3e50', family="Arial, sans-serif"),
            pad=dict(t=10, b=6)  # respiro entre título/sub e o gráfico
        ),
        barmode='stack',
        bargap=0.15,
        bargroupgap=0.04,
        height=780,
        width=1400,
        plot_bgcolor='#F2F2F2',
        paper_bgcolor='#F2F2F2',
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(
            title_text="Porte de loja",
            orientation="h",
            x=0.5, xanchor="center",
            y=-0.22, yanchor="top",  # mais baixo
            bgcolor='rgba(255,255,255,0.9)',
            bordercolor='#bdc3c7',
            borderwidth=1,
            font=dict(size=11, color='#2c3e50'),
            tracegroupgap=6
        ),
        margin=dict(l=90, r=90, t=170, b=160, pad=12),
        hoverlabel=dict(font_size=12, namelength=-1),
        hovermode="x unified",
        showlegend=True
    )

    # Subtítulos dos subplots mais altos
    for i, ann in enumerate(fig.layout.annotations):
        if i < 2:
            ann.update(y=1.09, yanchor='bottom', font=dict(size=13, color='#2c3e50'))

    fig.update_xaxes(
        title_text="<b>Mês</b>",
        tickangle=30, ticks="outside", ticklen=6,
        title_standoff=20, automargin=True,
        tickfont=dict(size=11, color='#34495e'),
        gridcolor='rgba(255,255,255,0.8)', zerolinecolor='#bdc3c7',
        row=1, col=1
    )
    fig.update_xaxes(
        title_text="<b>Mês</b>",
        tickangle=30, ticks="outside", ticklen=6,
        title_standoff=20, automargin=True,
        tickfont=dict(size=11, color='#34495e'),
        gridcolor='rgba(255,255,255,0.8)', zerolinecolor='#bdc3c7',
        row=1, col=2
    )
    fig.update_yaxes(
        title_text="<b>Vendas mensais (k unid.)</b>",
        range=[0, y_max] if y_max else None,
        ticks="outside", ticklen=6,
        title_standoff=22, automargin=True,
        tickfont=dict(size=11, color='#34495e'),
        gridcolor='rgba(255,255,255,0.8)', zerolinecolor='#bdc3c7',
        row=1, col=1
    )
    fig.update_yaxes(
        title_text="<b>Proporção % de vendas</b>",
        range=[0, 100],
        ticks="outside", ticklen=6,
        title_standoff=22, automargin=True,
        tickfont=dict(size=11, color='#34495e'),
        gridcolor='rgba(255,255,255,0.8)', zerolinecolor='#bdc3c7',
        row=1, col=2
    )

    return fig


def criar_grafico_elasticidade_porte_regiao(
    df: pd.DataFrame,
    gemeo: str,
    diretoria: str
) -> go.Figure:
    """
    Gráfico de elasticidade por porte + região com melhor espaçamento vertical.
    """
    df_gemeo = df[df['gemeos'] == gemeo].copy()
    if df_gemeo.empty:
        print(f"⚠️  Nenhum dado encontrado para o gêmeo: {gemeo}")
        return go.Figure()

    df_agrupado = (
        df_gemeo
        .groupby(['year_month', 'NmPorteLoja', 'NmRegiaoGeografica'])
        .agg({'qt_vendas': 'sum'})
        .reset_index()
    )
    df_agrupado['porte_regiao'] = df_agrupado['NmPorteLoja'] + ' - ' + df_agrupado['NmRegiaoGeografica']

    df_pivot = df_agrupado.pivot(
        index='year_month',
        columns='porte_regiao',
        values='qt_vendas'
    ).fillna(0).sort_index()

    df_prop = df_pivot.div(df_pivot.sum(axis=1), axis=0) * 100

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=[
            f"<b>Vendas mensais (k unid.) de {gemeo} por porte de loja + região</b>",
            f"<b>Proporção % de vendas de {gemeo} por porte de loja + região</b>"
        ],
        specs=[[{"type": "bar"}, {"type": "bar"}]],
        horizontal_spacing=0.12
    )

    ordem_portes = ['PORTE 6', 'PORTE 5', 'PORTE 4', 'PORTE 3', 'PORTE 2', 'PORTE 1']
    cores_base = {
        'PORTE 6': '#1a365d',
        'PORTE 5': '#2c5282',
        'PORTE 4': '#3182ce',
        'PORTE 3': '#4299e1',
        'PORTE 2': '#63b3ed',
        'PORTE 1': '#90cdf4',
    }

    colunas_ordenadas = []
    for porte in ordem_portes:
        colunas_porte = [col for col in df_pivot.columns if col.startswith(porte)]
        colunas_ordenadas.extend(colunas_porte)

    x1 = pd.to_datetime(df_pivot.index).strftime('%b/%y')
    x2 = pd.to_datetime(df_prop.index).strftime('%b/%y')

    for col in colunas_ordenadas:
        porte = col.split(' - ')[0]
        if porte in cores_base:
            fig.add_trace(
                go.Bar(
                    x=x1,
                    y=df_pivot[col] / 1000,
                    name=col,
                    marker_color=cores_base[porte],
                    marker_line_color="#FFFFFF",
                    marker_line_width=0.7,
                    hovertemplate=(
                        f'<b>{col}</b><br>'
                        'Mês: %{x}<br>'
                        'Vendas: %{y:.1f}k unid.<extra></extra>'
                    )
                ),
                row=1, col=1
            )

    for col in colunas_ordenadas:
        porte = col.split(' - ')[0]
        if porte in cores_base:
            fig.add_trace(
                go.Bar(
                    x=x2,
                    y=df_prop[col],
                    name=col,
                    marker_color=cores_base[porte],
                    marker_line_color="#FFFFFF",
                    marker_line_width=0.7,
                    showlegend=False,
                    hovertemplate=(
                        f'<b>{col}</b><br>'
                        'Mês: %{x}<br>'
                        'Proporção: %{y:.1f}%<extra></extra>'
                    )
                ),
                row=1, col=2
            )

    totais_mensais = df_pivot.sum(axis=1) / 1000
    for i, total in enumerate(totais_mensais):
        fig.add_annotation(
            x=x1[i],
            y=total,
            yanchor="bottom",
            yshift=14,
            text=f"{total:.1f}k",
            showarrow=False,
            font=dict(size=12, color='#2c3e50', family="Arial, sans-serif"),
            xref='x',
            yref='y'
        )

    y_max = float(totais_mensais.max()) * 1.18 if len(totais_mensais) else None

    fig.update_layout(
        title=dict(
            text=(
                f"<b>Eventos e apostas | Dinâmica de vendas se altera significativamente em eventos e apostas, "
                f"impactando a proporção de merecimento</b>"
                f"<br><sub style='color:#7f8c8d; font-size:14px;'>{gemeo} - {diretoria} - PORTE DE LOJA + REGIÃO GEOGRÁFICA</sub>"
            ),
            x=0.5, xanchor='center', y=0.98,
            font=dict(size=18, color='#2c3e50', family="Arial, sans-serif"),
            pad=dict(t=10, b=6)
        ),
        barmode='stack',
        bargap=0.15,
        bargroupgap=0.04,
        height=780,
        width=1400,
        plot_bgcolor='#F2F2F2',
        paper_bgcolor='#F2F2F2',
        font=dict(family="Arial, sans-serif", size=12),
        legend=dict(
            title_text="Porte + Região",
            orientation="h",
            x=0.5, xanchor="center",
            y=-0.22, yanchor="top",
            bgcolor='rgba(255,255,255,0.9)',
            bordercolor='#bdc3c7',
            borderwidth=1,
            font=dict(size=11, color='#2c3e50'),
            tracegroupgap=6
        ),
        margin=dict(l=90, r=90, t=180, b=170, pad=12),
        hoverlabel=dict(font_size=12, namelength=-1),
        hovermode="x unified",
        showlegend=True
    )

    for i, ann in enumerate(fig.layout.annotations):
        if i < 2:
            ann.update(y=1.09, yanchor='bottom', font=dict(size=13, color='#2c3e50'))

    fig.update_xaxes(
        title_text="<b>Mês</b>",
        tickangle=30, ticks="outside", ticklen=6,
        title_standoff=20, automargin=True,
        tickfont=dict(size=11, color='#34495e'),
        gridcolor='rgba(255,255,255,0.8)', zerolinecolor='#bdc3c7',
        row=1, col=1
    )
    fig.update_xaxes(
        title_text="<b>Mês</b>",
        tickangle=30, ticks="outside", ticklen=6,
        title_standoff=20, automargin=True,
        tickfont=dict(size=11, color='#34495e'),
        gridcolor='rgba(255,255,255,0.8)', zerolinecolor='#bdc3c7',
        row=1, col=2
    )
    fig.update_yaxes(
        title_text="<b>Vendas mensais (k unid.)</b>",
        range=[0, y_max] if y_max else None,
        ticks="outside", ticklen=6,
        title_standoff=22, automargin=True,
        tickfont=dict(size=11, color='#34495e'),
        gridcolor='rgba(255,255,255,0.8)', zerolinecolor='#bdc3c7',
        row=1, col=1
    )
    fig.update_yaxes(
        title_text="<b>Proporção % de vendas</b>",
        range=[0, 100],
        ticks="outside", ticklen=6,
        title_standoff=22, automargin=True,
        tickfont=dict(size=11, color='#34495e'),
        gridcolor='rgba(255,255,255,0.8)', zerolinecolor='#bdc3c7',
        row=1, col=2
    )

    return fig

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Criação dos Gráficos para Cada Top Gêmeo
# MAGIC
# MAGIC %md
# MAGIC ### Execução da Análise Completa - Duas Versões

# COMMAND ----------

print("🚀 Iniciando criação dos gráficos de elasticidade...")
print("📊 Serão criadas duas versões para cada gêmeo:")
print("   1. APENAS por porte de loja")
print("   2. Por porte de loja + região geográfica")
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
    
    if fig_porte.data:
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
    
    if fig_porte_regiao.data:
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

print("\n✅ Análise de elasticidade concluída!")
print(f"📊 Total de gráficos criados: {len(top_5_gemeos.toPandas()) * 2} (2 versões por gêmeo)")
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
