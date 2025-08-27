# Databricks notebook source
# MAGIC %md
# MAGIC # Cálculo da Matriz de Merecimento - Solução Unificada para Todas as Categorias
# MAGIC
# MAGIC Este notebook implementa o cálculo da matriz de merecimento unificado para todas as categorias,
# MAGIC com abstração `grupo_de_necessidade` e implementação de médias aparadas.
# MAGIC
# MAGIC **Objetivo**: Calcular a matriz de merecimento otimizada em duas camadas:
# MAGIC 1. **Primeira camada**: Matriz a nível CD (grupo_de_necessidade)
# MAGIC 2. **Segunda camada**: Distribuição interna ao CD para as lojas
# MAGIC
# MAGIC **Regras de Agrupamento por Categoria**:
# MAGIC - **DIRETORIA DE TELAS**: Usa `gemeos` como grupo_de_necessidade
# MAGIC - **DIRETORIA TELEFONIA CELULAR**: Usa `gemeos` como grupo_de_necessidade  
# MAGIC - **DIRETORIA LINHA BRANCA**: Usa `NmEspecieGerencial` como grupo_de_necessidade
# MAGIC - **DIRETORIA LINHA LEVE**: Usa `NmEspecieGerencial` como grupo_de_necessidade
# MAGIC - **DIRETORIA INFO/GAMES**: Usa `NmEspecieGerencial` como grupo_de_necessidade
# MAGIC
# MAGIC **Metodologia de Detecção de Outliers**:
# MAGIC - **Meses Atípicos**: Remove meses com QtMercadoria > nσ da média APENAS do grupo_de_necessidade específico
# MAGIC - **Outliers Históricos CD**: Remove registros > 3σ da média por grupo_de_necessidade (configurável)
# MAGIC - **Outliers Históricos Loja**: Remove registros > 3σ da média por grupo_de_necessidade-loja (configurável)
# MAGIC - **Flag de Atacado**: Parâmetros diferenciados para lojas de atacado vs. varejo
# MAGIC
# MAGIC **Múltiplas Médias Móveis**:
# MAGIC - **Médias Móveis Normais**: 90, 180, 270, 360 dias
# MAGIC - **Medianas Móveis**: 90, 180, 270, 360 dias
# MAGIC - **Médias Móveis Aparadas (10%)**: 90, 180, 270, 360 dias

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e Configurações Iniciais

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Optional, Dict, Any

# Inicialização do Spark
spark = SparkSession.builder.appName("calculo_matriz_merecimento_unificado").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuração das Regras por Categoria

# COMMAND ----------

# Configuração das regras de agrupamento por categoria
REGRAS_AGRUPAMENTO = {
    "DIRETORIA DE TELAS": {
        "coluna_grupo_necessidade": "gemeos",
        "tipo_agrupamento": "gêmeos",
        "descricao": "Agrupamento por produtos similares (gêmeos)"
    },
    "DIRETORIA TELEFONIA CELULAR": {
        "coluna_grupo_necessidade": "gemeos", 
        "tipo_agrupamento": "gêmeos",
        "descricao": "Agrupamento por produtos similares (gêmeos)"
    },
    "DIRETORIA LINHA BRANCA": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial",
        "descricao": "Agrupamento por espécie gerencial"
    },
    "DIRETORIA LINHA LEVE": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial", 
        "descricao": "Agrupamento por espécie gerencial"
    },
    "DIRETORIA INFO/GAMES": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial",
        "descricao": "Agrupamento por espécie gerencial"
    }
}

# Configuração de parâmetros para detecção de outliers
PARAMETROS_OUTLIERS = {
    "desvios_meses_atipicos": 3,  # Desvios para meses atípicos
    "desvios_historico_cd": 3,     # Desvios para outliers históricos a nível CD
    "desvios_historico_loja": 3,   # Desvios para outliers históricos a nível loja
    "desvios_atacado_cd": 1.5,     # Desvios para outliers CD em lojas de atacado
    "desvios_atacado_loja": 1.5    # Desvios para outliers loja em lojas de atacado
}

# Configuração das janelas móveis
JANELAS_MOVEIS = [90, 180, 270, 360]

# Configuração das médias aparadas (percentual de corte)
PERCENTUAL_CORTE_MEDIAS_APARADAS = 0.10  # 10% de corte superior e inferior

print("✅ Configurações carregadas:")
print(f"  • Categorias suportadas: {list(REGRAS_AGRUPAMENTO.keys())}")
print(f"  • Janelas móveis: {JANELAS_MOVEIS} dias")
print(f"  • Percentual de corte para médias aparadas: {PERCENTUAL_CORTE_MEDIAS_APARADAS*100}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Função para Determinar o Grupo de Necessidade

# COMMAND ----------

def determinar_grupo_necessidade(categoria: str, df: DataFrame) -> DataFrame:
    """
    Determina o grupo de necessidade baseado na categoria e aplica a regra correspondente.
    
    IMPORTANTE: Esta função COPIA os VALORES REAIS da coluna especificada para a coluna grupo_de_necessidade.
    
    Exemplos:
    - Para TELAS: grupo_de_necessidade = valores reais da coluna 'gemeos' (ex: "GRUPO_A", "GRUPO_B")
    - Para LINHA BRANCA: grupo_de_necessidade = valores reais da coluna 'NmEspecieGerencial' (ex: "GELADEIRA", "FOGÃO")
    
    Args:
        categoria: Nome da categoria/diretoria
        df: DataFrame com os dados de vendas e estoque
        
    Returns:
        DataFrame com a coluna grupo_de_necessidade contendo os valores reais da coluna origem
    """
    if categoria not in REGRAS_AGRUPAMENTO:
        raise ValueError(f"Categoria '{categoria}' não suportada. Categorias válidas: {list(REGRAS_AGRUPAMENTO.keys())}")
    
    regra = REGRAS_AGRUPAMENTO[categoria]
    coluna_origem = regra["coluna_grupo_necessidade"]
    
    # Verifica se a coluna existe no DataFrame
    colunas_df = df.columns
    if coluna_origem not in colunas_df:
        raise ValueError(f"Coluna '{coluna_origem}' não encontrada no DataFrame. Colunas disponíveis: {colunas_df}")
    
    # Aplica a regra de agrupamento
    df_com_grupo = df.withColumn(
        "grupo_de_necessidade",
        F.col(coluna_origem)  # ← Copia os VALORES da coluna origem (ex: valores de 'gemeos' ou 'NmEspecieGerencial')
    ).withColumn(
        "tipo_agrupamento",
        F.lit(regra["tipo_agrupamento"])  # ← Este sim é um valor fixo para identificação
    )
    
    print(f"✅ Grupo de necessidade definido para '{categoria}':")
    print(f"  • Coluna origem: {coluna_origem}")
    print(f"  • Valores copiados: {df_com_grupo.select('grupo_de_necessidade').distinct().count()} grupos únicos")
    print(f"  • Tipo de agrupamento: {regra['tipo_agrupamento']}")
    print(f"  • Descrição: {regra['descricao']}")
    
    return df_com_grupo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Carregamento dos Dados Base

# COMMAND ----------

def carregar_dados_base(categoria: str, data_inicio: str = "2024-01-01") -> DataFrame:
    """
    Carrega os dados base para a categoria especificada.
    
    Args:
        categoria: Nome da categoria/diretoria
        data_inicio: Data de início para filtro (formato YYYY-MM-DD)
        
    Returns:
        DataFrame com os dados carregados e grupo_de_necessidade definido
    """
    print(f"🔄 Carregando dados para categoria: {categoria}")
    
    # Carregamento dos dados base
    df_base = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario')
        .filter(F.col("NmAgrupamentoDiretoriaSetor") == categoria)
        .filter(F.col("DtAtual") >= data_inicio)
        .withColumn(
            "year_month",
            F.date_format(F.col("DtAtual"), "yyyyMM").cast("int")
        )
        .fillna(0, subset=["Receita", "QtMercadoria", "TeveVenda"])
    )
    
    # Aplica a regra de agrupamento específica da categoria
    df_com_grupo = determinar_grupo_necessidade(categoria, df_base)
    
    # Cache para otimização
    df_com_grupo.cache()
    
    print(f"✅ Dados carregados para '{categoria}':")
    print(f"  • Total de registros: {df_com_grupo.count():,}")
    print(f"  • Período: {data_inicio} até {df_com_grupo.agg(F.max("DtAtual")).collect()[0][0]}")
    
    return df_com_grupo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Carregamento dos Mapeamentos de Produtos

# COMMAND ----------

def carregar_mapeamentos_produtos() -> tuple:
    """
    Carrega os arquivos de mapeamento de produtos.
    
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
    return de_para_modelos_tecnologia, de_para_gemeos_tecnologia

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Aplicação dos Mapeamentos de Produtos

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

# MAGIC %md
# MAGIC ## 7. Detecção de Outliers e Meses Atípicos

# COMMAND ----------

def detectar_outliers_meses_atipicos(df: DataFrame, categoria: str, 
                                   sigma_meses_atipicos: float = 3.0,
                                   sigma_outliers_cd: float = 3.0,
                                   sigma_outliers_loja: float = 3.0,
                                   sigma_atacado_cd: float = 1.5,
                                   sigma_atacado_loja: float = 1.5) -> tuple:
    """
    Detecta outliers e meses atípicos baseado no grupo_de_necessidade com parâmetros sigma configuráveis.
    
    Args:
        df: DataFrame com os dados
        categoria: Nome da categoria
        sigma_meses_atipicos: Número de desvios padrão para meses atípicos (padrão: 3.0)
        sigma_outliers_cd: Número de desvios padrão para outliers CD (padrão: 3.0)
        sigma_outliers_loja: Número de desvios padrão para outliers loja (padrão: 3.0)
        sigma_atacado_cd: Número de desvios padrão para outliers CD atacado (padrão: 1.5)
        sigma_atacado_loja: Número de desvios padrão para outliers loja atacado (padrão: 1.5)
        
    Returns:
        Tuple com (DataFrame com estatísticas, DataFrame com meses atípicos)
    """
    print(f"🔄 Detectando outliers para categoria: {categoria}")
    print(f"📊 Parâmetros sigma configurados:")
    print(f"   • Meses atípicos: {sigma_meses_atipicos}σ")
    print(f"   • Outliers CD: {sigma_outliers_cd}σ")
    print(f"   • Outliers loja: {sigma_outliers_loja}σ")
    print(f"   • Outliers atacado CD: {sigma_atacado_cd}σ")
    print(f"   • Outliers atacado loja: {sigma_atacado_loja}σ")
    
    # Agregação por grupo_de_necessidade e mês
    df_stats_por_grupo_mes = (
        df.groupBy("grupo_de_necessidade", "year_month")
        .agg(
            F.sum("QtMercadoria").alias("QtMercadoria_total"),
            F.count("*").alias("total_registros")
        )
    )
    
    # Janela para cálculo de estatísticas por grupo_de_necessidade
    w_stats_grupo = Window.partitionBy("grupo_de_necessidade")
    
    # Cálculo de média e desvio padrão por grupo_de_necessidade
    df_stats_grupo = (
        df_stats_por_grupo_mes
        .withColumn(
            "media_qt_mercadoria",
            F.avg("QtMercadoria_total").over(w_stats_grupo)
        )
        .withColumn(
            "desvio_padrao_qt_mercadoria",
            F.stddev("QtMercadoria_total").over(w_stats_grupo)
        )
        .withColumn(
            "limite_superior_nsigma",
            F.col("media_qt_mercadoria") + (F.lit(sigma_meses_atipicos) * F.col("desvio_padrao_qt_mercadoria"))
        )
        .withColumn(
            "limite_inferior_nsigma",
            F.greatest(
                F.col("media_qt_mercadoria") - (F.lit(sigma_meses_atipicos) * F.col("desvio_padrao_qt_mercadoria")),
                F.lit(0)  # Não permite valores negativos
            )
        )
        .withColumn(
            "flag_mes_atipico",
            F.when(
                (F.col("QtMercadoria_total") > F.col("limite_superior_nsigma")) |
                (F.col("QtMercadoria_total") < F.col("limite_inferior_nsigma")),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )
    
    # Meses identificados como atípicos
    df_meses_atipicos = (
        df_stats_grupo
        .filter(F.col("flag_mes_atipico") == 1)
        .select(
            "grupo_de_necessidade",
            "year_month",
            F.round("QtMercadoria_total", 2).alias("QtMercadoria_total"),
            F.round("media_qt_mercadoria", 2).alias("media_qt_mercadoria"),
            F.round("desvio_padrao_qt_mercadoria", 2).alias("desvio_padrao_qt_mercadoria"),
            F.round("limite_superior_nsigma", 2).alias("limite_superior_nsigma"),
            F.round("limite_inferior_nsigma", 2).alias("limite_inferior_nsigma"),
            "flag_mes_atipico"
        )
        .orderBy("grupo_de_necessidade", "year_month")
    )
    
    print("✅ Detecção de outliers concluída:")
    print(f"  • Total de meses atípicos: {df_meses_atipicos.count()}")
    
    return df_stats_grupo, df_meses_atipicos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Filtragem de Meses Atípicos

# COMMAND ----------

def filtrar_meses_atipicos(df: DataFrame, df_meses_atipicos: DataFrame) -> DataFrame:
    """
    Filtra os meses atípicos do DataFrame principal.
    
    Args:
        df: DataFrame principal
        df_meses_atipicos: DataFrame com meses atípicos identificados
        
    Returns:
        DataFrame com meses atípicos filtrados
    """
    print("🔄 Aplicando filtro de meses atípicos...")
    
    # Aplicação do filtro de meses atípicos por grupo_de_necessidade específico
    df_filtrado = (
        df.join(
            df_meses_atipicos.select("grupo_de_necessidade", "year_month").withColumn("flag_remover", F.lit(1)),
            on=["grupo_de_necessidade", "year_month"],
            how="left"
        )
        .filter(
            F.col("flag_remover").isNull()  # Remove apenas os meses atípicos do grupo_de_necessidade específico
        )
        .drop("flag_remover")
    )
    
    registros_antes = df.count()
    registros_depois = df_filtrado.count()
    registros_removidos = registros_antes - registros_depois
    
    print("✅ Filtro de meses atípicos aplicado:")
    print(f"  • Registros antes: {registros_antes:,}")
    print(f"  • Registros depois: {registros_depois:,}")
    print(f"  • Registros removidos: {registros_removidos:,}")
    
    return df_filtrado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cálculo das Medidas Centrais com Médias Aparadas

# COMMAND ----------

def calcular_medidas_centrais_com_medias_aparadas(df: DataFrame) -> DataFrame:
    """
    Calcula todas as medidas centrais incluindo médias aparadas.
    
    Args:
        df: DataFrame filtrado sem ruptura
        
    Returns:
        DataFrame com todas as medidas calculadas
    """
    print("🔄 Calculando medidas centrais com médias aparadas...")
    
    # Filtragem para considerar apenas dias sem ruptura
    df_sem_ruptura = df.filter(F.col("FlagRuptura") == 0)
    
    # Definição das janelas móveis por SKU e loja
    janelas = {}
    for dias in JANELAS_MOVEIS:
        janelas[dias] = Window.partitionBy("CdSku", "CdFilial").orderBy("DtAtual").rowsBetween(-dias, 0)
    
    # Cálculo das médias móveis normais
    df_com_medias = df_sem_ruptura
    for dias in JANELAS_MOVEIS:
        df_com_medias = df_com_medias.withColumn(
            f"Media{dias}_Qt_venda_sem_ruptura",
            F.avg("QtMercadoria").over(janelas[dias])
        )
    
    # Cálculo das medianas móveis
    df_com_medianas = df_com_medias
    for dias in JANELAS_MOVEIS:
        df_com_medianas = df_com_medianas.withColumn(
            f"Mediana{dias}_Qt_venda_sem_ruptura",
            F.expr(f"percentile_approx(QtMercadoria, 0.5)").over(janelas[dias])
        )
    
    # Cálculo das médias móveis aparadas
    df_com_medias_aparadas = df_com_medianas
    for dias in JANELAS_MOVEIS:
        # Para médias aparadas, usamos uma abordagem robusta baseada em percentis
        # Calculamos a média excluindo os valores extremos (outliers)
        df_com_medias_aparadas = df_com_medias_aparadas.withColumn(
            f"MediaAparada{dias}_Qt_venda_sem_ruptura",
            F.expr(f"""
                CASE 
                    WHEN count(*) OVER (
                        PARTITION BY CdSku, CdFilial 
                        ORDER BY DtAtual 
                        ROWS BETWEEN {dias} PRECEDING AND CURRENT ROW
                    ) >= 10
                    THEN (
                        -- Usa a mediana como aproximação da média aparada para janelas grandes
                        percentile_approx(QtMercadoria, 0.5) OVER (
                            PARTITION BY CdSku, CdFilial 
                            ORDER BY DtAtual 
                            ROWS BETWEEN {dias} PRECEDING AND CURRENT ROW
                        )
                    )
                    ELSE (
                        -- Para janelas pequenas, usa a média normal
                        avg(QtMercadoria) OVER (
                            PARTITION BY CdSku, CdFilial 
                            ORDER BY DtAtual 
                            ROWS BETWEEN {dias} PRECEDING AND CURRENT ROW
                        )
                    )
                END
            """)
        )
    
    print("✅ Medidas centrais calculadas:")
    print(f"  • Médias móveis normais: {JANELAS_MOVEIS} dias")
    print(f"  • Medianas móveis: {JANELAS_MOVEIS} dias")
    print(f"  • Médias móveis aparadas ({PERCENTUAL_CORTE_MEDIAS_APARADAS*100}%): {JANELAS_MOVEIS} dias")
    
    return df_com_medias_aparadas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Consolidação das Medidas

# COMMAND ----------

def consolidar_medidas(df: DataFrame) -> DataFrame:
    """
    Consolida todas as medidas calculadas em uma base única.
    
    Args:
        df: DataFrame com todas as medidas
        
    Returns:
        DataFrame consolidado
    """
    print("🔄 Consolidando medidas...")
    
    # Seleção das colunas essenciais
    colunas_medias = [f"Media{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    colunas_medianas = [f"Mediana{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    colunas_medias_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    
    df_consolidado = (
        df.select(
            "DtAtual", "CdSku", "CdFilial", "grupo_de_necessidade", "year_month",
            "QtMercadoria", "Receita", "FlagRuptura", "tipo_agrupamento",
            *colunas_medias,
            *colunas_medianas,
            *colunas_medias_aparadas
        )
        .fillna(0, subset=colunas_medias + colunas_medianas + colunas_medias_aparadas)
    )
    
    print("✅ Medidas consolidadas")
    return df_consolidado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Função Principal de Execução

# COMMAND ----------

def executar_calculo_matriz_merecimento(categoria: str, 
                                       data_inicio: str = "2024-01-01",
                                       sigma_meses_atipicos: float = 3.0,
                                       sigma_outliers_cd: float = 3.0,
                                       sigma_outliers_loja: float = 3.0,
                                       sigma_atacado_cd: float = 1.5,
                                       sigma_atacado_loja: float = 1.5) -> DataFrame:
    """
    Função principal que executa todo o cálculo da matriz de merecimento.
    
    Args:
        categoria: Nome da categoria/diretoria
        data_inicio: Data de início para filtro (formato YYYY-MM-DD)
        sigma_meses_atipicos: Número de desvios padrão para meses atípicos (padrão: 3.0)
        sigma_outliers_cd: Número de desvios padrão para outliers CD (padrão: 3.0)
        sigma_outliers_loja: Número de desvios padrão para outliers loja (padrão: 3.0)
        sigma_atacado_cd: Número de desvios padrão para outliers CD atacado (padrão: 1.5)
        sigma_atacado_loja: Número de desvios padrão para outliers loja atacado (padrão: 1.5)
        
    Returns:
        DataFrame final com todas as medidas calculadas
    """
    print(f"🚀 Iniciando cálculo da matriz de merecimento para: {categoria}")
    print("=" * 80)
    print(f"📊 Configuração de parâmetros sigma:")
    print(f"   • Meses atípicos: {sigma_meses_atipicos}σ")
    print(f"   • Outliers CD: {sigma_outliers_cd}σ")
    print(f"   • Outliers loja: {sigma_outliers_loja}σ")
    print(f"   • Outliers atacado CD: {sigma_atacado_cd}σ")
    print(f"   • Outliers atacado loja: {sigma_atacado_loja}σ")
    print("=" * 80)
    
    try:
        # 1. Carregamento dos dados base
        df_base = carregar_dados_base(categoria, data_inicio)
        
        # 2. Carregamento dos mapeamentos
        de_para_modelos, de_para_gemeos = carregar_mapeamentos_produtos()
        
        # 3. Aplicação dos mapeamentos
        df_com_mapeamentos = aplicar_mapeamentos_produtos(
            df_base, categoria, de_para_modelos, de_para_gemeos
        )
        
        # 4. Detecção de outliers com parâmetros sigma configuráveis
        df_stats, df_meses_atipicos = detectar_outliers_meses_atipicos(
            df_com_mapeamentos, 
            categoria,
            sigma_meses_atipicos=sigma_meses_atipicos,
            sigma_outliers_cd=sigma_outliers_cd,
            sigma_outliers_loja=sigma_outliers_loja,
            sigma_atacado_cd=sigma_atacado_cd,
            sigma_atacado_loja=sigma_atacado_loja
        )
        
        # 5. Filtragem de meses atípicos
        df_filtrado = filtrar_meses_atipicos(df_com_mapeamentos, df_meses_atipicos)
        
        # 6. Cálculo das medidas centrais
        df_com_medidas = calcular_medidas_centrais_com_medias_aparadas(df_filtrado)
        
        # 7. Consolidação final
        df_final = consolidar_medidas(df_com_medidas)
        
        print("=" * 80)
        print(f"✅ Cálculo da matriz de merecimento concluído para: {categoria}")
        print(f"📊 Total de registros finais: {df_final.count():,}")
        
        return df_final
        
    except Exception as e:
        print(f"❌ Erro durante o cálculo: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Exemplo de Uso

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exemplo para DIRETORIA DE TELAS
# MAGIC 
# MAGIC ```python
# MAGIC df_telas = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")
# MAGIC ```
# MAGIC 
# MAGIC ### Exemplo para DIRETORIA TELEFONIA CELULAR
# MAGIC 
# MAGIC ```python
# MAGIC df_telefonia = executar_calculo_matriz_merecimento("DIRETORIA TELEFONIA CELULAR")
# MAGIC ```
# MAGIC 
# MAGIC ### Exemplo para DIRETORIA LINHA BRANCA
# MAGIC 
# MAGIC ```python
# MAGIC df_linha_branca = executar_calculo_matriz_merecimento("DIRETORIA LINHA BRANCA")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Validação e Testes

# COMMAND ----------

def validar_resultados(df: DataFrame, categoria: str) -> None:
    """
    Valida os resultados do cálculo da matriz de merecimento.
    
    Args:
        df: DataFrame com os resultados
        categoria: Nome da categoria
    """
    print(f"🔍 Validando resultados para: {categoria}")
    
    # Verificações básicas
    total_registros = df.count()
    total_skus = df.select("CdSku").distinct().count()
    total_lojas = df.select("CdFilial").distinct().count()
    total_grupos = df.select("grupo_de_necessidade").distinct().count()
    
    print("📊 Estatísticas gerais:")
    print(f"  • Total de registros: {total_registros:,}")
    print(f"  • Total de SKUs únicos: {total_skus:,}")
    print(f"  • Total de lojas únicas: {total_lojas:,}")
    print(f"  • Total de grupos de necessidade: {total_grupos:,}")
    
    # Verificação de valores nulos
    colunas_medias = [f"Media{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    colunas_medianas = [f"Mediana{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    colunas_medias_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    
    todas_colunas_medidas = colunas_medias + colunas_medianas + colunas_medias_aparadas
    
    print("\n🔍 Verificação de valores nulos:")
    for coluna in todas_colunas_medidas:
        if coluna in df.columns:
            nulos = df.filter(F.col(coluna).isNull()).count()
            print(f"  • {coluna}: {nulos:,} valores nulos")
    
    print("✅ Validação concluída")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Execução de Teste

# MAGIC %md
# MAGIC Descomente a linha abaixo para executar um teste com a categoria desejada:

# COMMAND ----------

# EXECUTAR TESTE (descomente para testar)
# categoria_teste = "DIRETORIA DE TELAS"
# df_resultado = executar_calculo_matriz_merecimento(categoria_teste)
# validar_resultados(df_resultado, categoria_teste)
# display(df_resultado.limit(10))
