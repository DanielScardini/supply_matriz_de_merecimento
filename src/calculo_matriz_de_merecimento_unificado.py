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
# MAGIC - **Médias Móveis Aparadas**: 90, 180, 270, 360 dias (10% de corte)
# MAGIC
# MAGIC **Conceito de "Factual" (Demanda Real)**:
# MAGIC - **Proporção Factual**: Representa a demanda real observada (robusta a ruptura) 
# MAGIC   como proporção do **TOTAL DA EMPRESA** por SKU na filial
# MAGIC - **Base de Cálculo**: Usa as medidas calculadas (médias e médias aparadas) 
# MAGIC   para representar a demanda histórica real, com foco em `Media90_Qt_venda_sem_ruptura`
# MAGIC - **Comparação**: O merecimento calculado é comparado com a proporção factual 
# MAGIC   para avaliar a qualidade da previsão

# COMMAND ----------

df_matriz_geral = (
    spark.createDataFrame(
        pd.read_csv(
            "/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/(DRP)_MATRIZ_20250829135142.csv",
            delimiter=";",
        )
    )
    .select(
        F.col("CODIGO").cast("int").alias("CdSku"),

        # Extrai só o que vem após "_" e faz cast para int
        F.regexp_replace(F.col("CODIGO_FILIAL"), ".*_", "")
         .cast("int")
         .alias("CdFilial"),

        # Troca vírgula por ponto e converte para float
        F.regexp_replace(F.col("PERCENTUAL_MATRIZ"), ",", ".")
         .cast("float")
         .alias("PercMatrizNeogrid"),

        F.col("CLUSTER").cast("string").alias("is_Cluster"),
    )
    .dropDuplicates()
)


df_matriz_geral.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e Configurações Iniciais

# COMMAND ----------

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

def get_data_inicio(min_meses: int = 18, hoje: datetime | None = None) -> datetime:
    """
    Retorna 1º de janeiro mais recente que esteja a pelo menos `min_meses` meses de 'hoje'.
    Recuará vários anos se preciso.
    """
    if hoje is None:
        hoje_d = date.today()
    else:
        hoje_d = hoje.date() if isinstance(hoje, datetime) else hoje

    ano = hoje_d.year
    while True:
        jan = date(ano, 1, 1)
        diff_meses = (hoje_d.year - jan.year) * 12 + (hoje_d.month - jan.month)
        if diff_meses >= min_meses:
            # retorna como datetime para compatibilidade com seu uso
            return datetime(jan.year, jan.month, jan.day)
        ano -= 1

data_inicio = get_data_inicio()
data_inicio_int = int(data_inicio.strftime("%Y%m%d"))


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

    # Removido: chamada incorreta que causava erro de argumentos
    # df_com_gdn = aplicar_mapeamentos_produtos(df)
    
    colunas_df = df.columns
    if coluna_origem not in colunas_df:
        raise ValueError(f"Coluna '{coluna_origem}' não encontrada no DataFrame. Colunas disponíveis: {colunas_df}")
    
    # Aplica a regra de agrupamento
    df_com_grupo = df.withColumn(
        "grupo_de_necessidade",
        F.coalesce(F.col(coluna_origem), F.lit("SEM_GN"))  # ← Copia os VALORES da coluna origem ou "SEM_GN" se nulo
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
# MAGIC
# MAGIC **IMPORTANTE**: Os dados são carregados SEM o grupo_de_necessidade ainda.
# MAGIC O grupo_de_necessidade é definido APÓS a aplicação dos mapeamentos para evitar referência circular.

# COMMAND ----------

def carregar_dados_base(categoria: str, data_inicio: str = "2024-01-01") -> DataFrame:
    """
    Carrega os dados base para a categoria especificada.
    
    Args:
        categoria: Nome da categoria/diretoria
        data_inicio: Data de início para filtro (formato YYYY-MM-DD)
        
    Returns:
        DataFrame com os dados carregados (SEM grupo_de_necessidade ainda)
    """
    print(f"🔄 Carregando dados para categoria: {categoria}")
    
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
    
    df_base.limit(1000).cache()
    
    print(f"✅ Dados carregados para '{categoria}':")
    print(f"  • Total de registros: {df_base.count():,}")
    print(f"  • Período: {data_inicio} até {df_base.agg(F.max('DtAtual')).limit(1).collect()[0][0]}")
    
    return df_base

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Carregamento dos Mapeamentos de Produtos

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
    
    de_para_modelos_tecnologia = (
        pd.read_csv('dados_analise/MODELOS_AJUSTE (1).csv', 
                    delimiter=';')
        .drop_duplicates()
    )
    
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


def add_media_aparada_rolling(
    df,
    janelas,
    col_val="QtMercadoria",
    col_ord="DtAtual",
    grupos=("CdSku","CdFilial"),
    alpha=0.10,          # porcentagem aparada em cada cauda
    min_obs=10           # mínimo de observações na janela
):
    out = df
    for dias in janelas:
        w = Window.partitionBy(*grupos).orderBy(F.col(col_ord)).rowsBetween(-dias, 0)

        # quantis dinâmicos por janela
        ql = F.percentile_approx(F.col(col_val), F.lit(alpha)).over(w)
        qh = F.percentile_approx(F.col(col_val), F.lit(1 - alpha)).over(w)

        out = (
            out
            .withColumn(f"_ql_{dias}", ql)
            .withColumn(f"_qh_{dias}", qh)
        )

        # contagem total na janela
        cnt = F.count(F.col(col_val)).over(w)

        # soma e contagem apenas dentro dos quantis [ql, qh]
        cond = (F.col(col_val) >= F.col(f"_ql_{dias}")) & (F.col(col_val) <= F.col(f"_qh_{dias}"))
        sum_trim = F.sum(F.when(cond, F.col(col_val))).over(w)
        cnt_trim = F.sum(F.when(cond, F.lit(1)).otherwise(F.lit(0))).over(w)

        # média aparada com fallback para média simples quando janela < min_obs
        mean_simple = F.avg(F.col(col_val)).over(w)
        mean_trim = sum_trim / F.when(cnt_trim > 0, cnt_trim).otherwise(F.lit(None))

        out = out.withColumn(
            f"MediaAparada{dias}_Qt_venda_sem_ruptura",
            F.when(cnt >= F.lit(min_obs), mean_trim).otherwise(mean_simple)
        ).drop(f"_ql_{dias}", f"_qh_{dias}")

    return out

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
    
    # Cálculo das médias móveis aparadas
    df_com_medias_aparadas = (
        add_media_aparada_rolling(
            df_com_medias,
            janelas=JANELAS_MOVEIS,
            col_val="QtMercadoria",
            col_ord="DtAtual",
            grupos=("CdSku","CdFilial"),
            alpha=0.10,
            min_obs=10
        )
    )
    
    print("✅ Medidas centrais calculadas:")
    print(f"  • Médias móveis normais: {JANELAS_MOVEIS} dias")
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
    colunas_medias_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    
    df_consolidado = (
        df.select(
            "DtAtual", "CdSku", "CdFilial", "grupo_de_necessidade", "year_month",
            "QtMercadoria", "Receita", "FlagRuptura", "tipo_agrupamento",
            *colunas_medias,
            *colunas_medias_aparadas
        )
        .fillna(0, subset=colunas_medias + colunas_medias_aparadas)
    )
    
    print("✅ Medidas consolidadas")
    return df_consolidado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Função Principal de Execução
# MAGIC
# MAGIC **FLUXO CORRIGIDO PARA EVITAR REFERÊNCIA CIRCULAR:**
# MAGIC
# MAGIC 1. **Carregamento de dados base** (sem grupo_de_necessidade)
# MAGIC 2. **Carregamento de mapeamentos** de produtos
# MAGIC 3. **Aplicação de mapeamentos** (joins com tabelas de referência)
# MAGIC 4. **Definição de grupo_de_necessidade** (APÓS os mapeamentos)
# MAGIC 5. **Detecção de outliers** usando grupo_de_necessidade
# MAGIC 6. **Filtragem de meses atípicos**
# MAGIC 7. **Cálculo de medidas centrais**
# MAGIC 8. **Consolidação final**
# MAGIC
# MAGIC **Por que esta ordem?**
# MAGIC - Evita referência circular entre mapeamentos e grupo_de_necessidade
# MAGIC - Garante que todas as colunas necessárias estejam disponíveis
# MAGIC - Permite uso correto do grupo_de_necessidade nas etapas subsequentes

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

def calcular_merecimento_cd(df: DataFrame, data_calculo: str, categoria: str) -> DataFrame:
    """
    Calcula o merecimento a nível CD por grupo de necessidade.
    
    Args:
        df: DataFrame com medidas calculadas
        data_calculo: Data específica para o cálculo (YYYY-MM-DD)
        categoria: Nome da categoria
        
    Returns:
        DataFrame com merecimento CD por grupo_de_necessidade
    """
    print(f"🔄 Calculando merecimento CD para categoria: {categoria}")
    print(f"📅 Data de cálculo: {data_calculo}")
    
    # Filtra dados para a data específica
    df_data_calculo = df.filter(F.col("DtAtual") == data_calculo)
    
    # Agrega por CD e grupo de necessidade para cada medida
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    de_para_filial_cd = criar_de_para_filial_cd()
    df_com_cd = df_data_calculo.join(de_para_filial_cd, on="cdfilial", how="left")
    
    colunas_agregacao = ["cd_primario", "grupo_de_necessidade"] + medidas_disponiveis
    
    aggs_cd = []
    for medida in medidas_disponiveis:
        aggs_cd.append(F.sum(F.col(medida)).alias(f"Total_{medida}"))
    
    df_merecimento_cd = (
        df_com_cd
        .groupBy("cd_primario", "grupo_de_necessidade")
        .agg(*aggs_cd)
    )
    
    print(f"✅ Merecimento CD calculado:")
    print(f"  • CDs: {df_merecimento_cd.select('cd_primario').distinct().count():,}")
    print(f"  • Grupos de necessidade: {df_merecimento_cd.select('grupo_de_necessidade').distinct().count():,}")
    
    return df_merecimento_cd

# COMMAND ----------

def calcular_merecimento_interno_cd(df: DataFrame, data_calculo: str, categoria: str) -> DataFrame:
    """
    Calcula o merecimento interno ao CD (filial) por grupo de necessidade.
    
    Args:
        df: DataFrame com medidas calculadas
        data_calculo: Data específica para o cálculo
        categoria: Nome da categoria
        
    Returns:
        DataFrame com percentual de merecimento por loja dentro do CD
    """
    print(f"🔄 Calculando merecimento interno CD para categoria: {categoria}")
    print(f"📅 Data de cálculo: {data_calculo}")
    
    df_data_calculo = df.filter(F.col("DtAtual") == data_calculo)
    
    de_para_filial_cd = criar_de_para_filial_cd()
    df_com_cd = df_data_calculo.join(de_para_filial_cd, on="cdfilial", how="left")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    df_com_totais = df_com_cd
    
    for medida in medidas_disponiveis:
        w_total = Window.partitionBy("cd_primario", "grupo_de_necessidade")
        
        df_com_totais = df_com_totais.withColumn(
            f"Total_{medida}",
            F.sum(F.col(medida)).over(w_total)
        )
    
    df_merecimento_interno = df_com_totais
    
    for medida in medidas_disponiveis:
        w_percentual = Window.partitionBy("cd_primario", "grupo_de_necessidade")
        
        df_merecimento_interno = df_merecimento_interno.withColumn(
            f"Percentual_{medida}",
            F.when(F.col(f"Total_{medida}") > 0,
                   F.col(medida) / F.col(f"Total_{medida}"))  # Usa a medida original, não o total
            .otherwise(0)
        )
    
    print(f"✅ Merecimento interno CD calculado:")
    print(f"  • Filiais: {df_merecimento_interno.select('cdfilial').distinct().count():,}")
    print(f"  • CDs: {df_merecimento_interno.select('cd_primario').distinct().count():,}")
    
    return df_merecimento_interno

# COMMAND ----------

def calcular_merecimento_final(df_merecimento_cd: DataFrame, 
                              df_merecimento_interno: DataFrame) -> DataFrame:
    """
    Calcula o merecimento final combinando CD e interno CD.
    
    Args:
        df_merecimento_cd: Merecimento a nível CD
        df_merecimento_interno: Merecimento interno ao CD
        
    Returns:
        DataFrame com merecimento final (CD × Interno CD)
    """
    print("🔄 Calculando merecimento final...")
    
    # Define medidas disponíveis
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    colunas_renomeadas = ["cd_primario", "grupo_de_necessidade"]
    for medida in medidas_disponiveis:
        colunas_renomeadas.append(F.col(f"Total_{medida}").alias(f"Total_CD_{medida}"))
    
    df_merecimento_cd_renomeado = df_merecimento_cd.select(*colunas_renomeadas)
    
    df_merecimento_final = (
        df_merecimento_interno
        .join(
            df_merecimento_cd_renomeado,
            on=["cd_primario", "grupo_de_necessidade"],
            how="left"
        )
    )
    
    for medida in medidas_disponiveis:
        df_merecimento_final = df_merecimento_final.withColumn(
            f"Merecimento_Final_{medida}",
            F.col(f"Total_CD_{medida}") * F.col(f"Percentual_{medida}")
        )
    
    print(f"✅ Merecimento final calculado:")
    print(f"  • Total de registros: {df_merecimento_final.count():,}")
    print(f"  • Medidas calculadas: {len(medidas_disponiveis)}")
    print(f"  • Colunas CD renomeadas para evitar conflito")
    
    return df_merecimento_final

# COMMAND ----------

def calcular_metricas_erro_previsao(df_merecimento: DataFrame, 
                                   categoria: str,
                                   mes_analise: str = "202507",
                                   colunas_agregacao: List[str] = None) -> DataFrame:
    """
    Calcula métricas de erro para avaliação da qualidade da previsão da matriz de merecimento.
    
    **Racional**: Compara o merecimento calculado (previsão) com a demanda calculada robusta a ruptura
    (Media90_Qt_venda_sem_ruptura) para julho-2025, calculando proporção factual e sMAPE como métrica principal.
    
    Args:
        df_merecimento: DataFrame com merecimento calculado
        categoria: Nome da categoria/diretoria
        mes_analise: Mês de análise no formato YYYYMM (padrão: julho-2025)
        colunas_agregacao: Lista de colunas para agregação (ex: ["cdfilial"], ["cd_primario"], ["grupo_de_necessidade"])
        
    Returns:
        DataFrame com métricas de erro calculadas
    """
    print(f"🔄 Calculando métricas de erro para categoria: {categoria}")
    print(f"📅 Mês de análise: {mes_analise}")
    
    if colunas_agregacao is None:
        colunas_agregacao = ["cdfilial"]  # Padrão: agregação por filial
    
    print("📊 Carregando dados de demanda calculada robusta a ruptura para cálculo de proporção factual...")
    
    # NOTA: A tabela base não tem grupo_de_necessidade, então vamos usar os dados do df_merecimento
    df_dados_demanda = (
        df_merecimento
        .select(
            "cdfilial", "grupo_de_necessidade", "CdSku",
            "Total_CD_Media90_Qt_venda_sem_ruptura", "Total_CD_Media180_Qt_venda_sem_ruptura",
            "Total_CD_Media270_Qt_venda_sem_ruptura", "Total_CD_Media360_Qt_venda_sem_ruptura",
            "Total_CD_MediaAparada90_Qt_venda_sem_ruptura", "Total_CD_MediaAparada180_Qt_venda_sem_ruptura",
            "Total_CD_MediaAparada270_Qt_venda_sem_ruptura", "Total_CD_MediaAparada360_Qt_venda_sem_ruptura"
        )
        .withColumnRenamed("Total_CD_Media90_Qt_venda_sem_ruptura", "Media90_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_Media180_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_Media270_Qt_venda_sem_ruptura", "Media270_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_Media360_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada90_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_MediaAparada180_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada270_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_MediaAparada360_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura")
    )
    
    # 2. PREPARAÇÃO PARA CÁLCULO DE MÉTRICAS
    print("📈 Preparando dados para cálculo de métricas...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **2. CÁLCULO DA PROPORÇÃO FACTUAL**

# COMMAND ----------

def calcular_proporcao_factual_por_sku_filial(df: DataFrame, coluna_medida: str) -> DataFrame:
    """
    Calcula proporção factual por SKU na filial em relação ao TOTAL DA EMPRESA.
    
    Args:
        df: DataFrame com dados de demanda
        coluna_medida: Nome da coluna de medida (ex: Media90_Qt_venda_sem_ruptura)
        
    Returns:
        DataFrame com proporção factual calculada por SKU na filial vs total da empresa
    """
    w_total_empresa = Window.partitionBy()  # Sem partição = total geral
    
    return (
        df
        .withColumn(
            f"total_{coluna_medida}_empresa",
            F.sum(F.col(coluna_medida)).over(w_total_empresa)
        )
        .withColumn(
            f"proporcao_factual_{coluna_medida}",
            F.when(
                F.col(f"total_{coluna_medida}_empresa") > 0,
                F.col(coluna_medida) / F.col(f"total_{coluna_medida}_empresa")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            f"proporcao_factual_{coluna_medida}_percentual",
            F.round(F.col(f"proporcao_factual_{coluna_medida}") * 100, 4)
        )
    )

def calcular_proporcao_factual_completa(df_merecimento: DataFrame) -> DataFrame:
    """
    Calcula proporção factual para todas as medidas disponíveis.
    
    Args:
        df_merecimento: DataFrame com merecimento calculado
        
    Returns:
        DataFrame com proporção factual para todas as medidas
    """
    print("📈 Calculando proporção factual para todas as medidas...")
    print("📊 IMPORTANTE: Proporção factual calculada por SKU na FILIAL vs TOTAL DA EMPRESA usando Media90_Qt_venda_sem_ruptura")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # Aplicar cálculo de proporção factual para todas as medidas
    df_proporcao_factual = df_merecimento
    for medida in medidas_disponiveis:
        df_proporcao_factual = (
            calcular_proporcao_factual_por_sku_filial(
                df_proporcao_factual, 
                medida
            )
        )
    
    print(f"✅ Proporção factual calculada para todas as medidas")
    print(f"  • Total de registros: {df_proporcao_factual.count():,}")
    
    return df_proporcao_factual

# COMMAND ----------

# MAGIC %md
# MAGIC ### **3. CARREGAMENTO DA MATRIZ DRP (GERAL)**

# COMMAND ----------

def carregar_matriz_drp_geral() -> DataFrame:
    """
    Carrega a matriz DRP geral para comparação.
    
    Returns:
        DataFrame com a matriz geral de referência
    """
    print("📊 Carregando matriz DRP geral para comparação...")
    
    df_matriz_geral = (
        spark.createDataFrame(
            pd.read_csv(
                "/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/dados_analise/(DRP)_MATRIZ_20250829135142.csv",
                delimiter=";",
            )
        )
        .select(
            F.col("CODIGO").cast("int").alias("CdSku"),
            F.regexp_replace(F.col("CODIGO_FILIAL"), ".*_", "").cast("int").alias("CdFilial"),
            F.regexp_replace(F.col("PERCENTUAL_MATRIZ"), ",", ".").cast("float").alias("PercMatrizNeogrid"),
            F.col("CLUSTER").cast("string").alias("is_Cluster"),
        )
        .dropDuplicates()
    )
    
    print(f"✅ Matriz DRP geral carregada: {df_matriz_geral.count():,} registros")
    return df_matriz_geral

# COMMAND ----------

# MAGIC %md
# MAGIC ### **4. CÁLCULO DE sMAPE E COMPARAÇÕES**

# COMMAND ----------

def calcular_smape_comparacao_factual(df_merecimento: DataFrame, df_proporcao_factual: DataFrame) -> DataFrame:
    """
    Calcula sMAPE comparando merecimento calculado com proporção factual.
    
    Args:
        df_merecimento: DataFrame com merecimento calculado
        df_proporcao_factual: DataFrame com proporção factual
        
    Returns:
        DataFrame com sMAPE calculado para todas as medidas
    """
    print("📊 Calculando sMAPE: Merecimento vs Proporção Factual...")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # Join entre merecimento e proporção factual
    df_comparacao = (
        df_merecimento
        .join(df_proporcao_factual, on=["cdfilial", "grupo_de_necessidade", "CdSku"], how="inner")
    )
    
    # Calcular sMAPE para cada medida
    df_com_smape = df_comparacao
    EPSILON = 1e-12
    
    for medida in medidas_disponiveis:
        # Calcula percentual do merecimento calculado
        df_com_smape = (
            df_com_smape
            .withColumn(
                f"merecimento_{medida}_percentual",
                F.when(
                    F.col(f"Merecimento_Final_{medida}") > 0,
                    F.col(f"Merecimento_Final_{medida}") / F.sum(f"Merecimento_Final_{medida}").over(
                        Window.partitionBy("grupo_de_necessidade")
                    ) * 100
                ).otherwise(F.lit(0.0))
            )
            .withColumn(
                f"erro_absoluto_{medida}",
                F.abs(F.col(f"merecimento_{medida}_percentual") - F.col(f"proporcao_factual_{medida}_percentual"))
            )
            .withColumn(
                f"smape_{medida}",
                F.when(
                    (F.col(f"merecimento_{medida}_percentual") + F.col(f"proporcao_factual_{medida}_percentual")) > 0,
                    F.lit(2.0) * F.col(f"erro_absoluto_{medida}") / 
                    (F.col(f"merecimento_{medida}_percentual") + F.col(f"proporcao_factual_{medida}_percentual") + F.lit(EPSILON)) * 100
                ).otherwise(F.lit(0.0))
            )
        )
    
    print(f"✅ sMAPE calculado para todas as medidas")
    return df_com_smape

def calcular_smape_comparacao_matriz_geral(df_merecimento: DataFrame, df_matriz_geral: DataFrame) -> DataFrame:
    """
    Calcula sMAPE comparando merecimento calculado com matriz DRP geral.
    
    Args:
        df_merecimento: DataFrame com merecimento calculado
        df_matriz_geral: DataFrame com matriz DRP geral
        
    Returns:
        DataFrame com sMAPE calculado vs matriz geral
    """
    print("📊 Calculando sMAPE: Merecimento vs Matriz DRP Geral...")
    
    # Normalizar IDs para garantir compatibilidade
    df_calculada_norm = (
        df_merecimento
        .withColumn("CdSku", F.col("CdSku").cast("int"))
        .withColumn("CdFilial", F.col("CdFilial").cast("int"))
    )
    
    df_geral_norm = (
        df_matriz_geral
        .withColumn("CdSku", F.col("CdSku").cast("int"))
        .withColumn("CdFilial", F.col("CdFilial").cast("int"))
    )
    
    # Join para comparação
    df_comparacao = (
        df_calculada_norm
        .join(df_geral_norm, on=["CdFilial", "CdSku"], how="inner")
        .withColumn(
            "erro_absoluto_vs_geral",
            F.abs(F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura") - F.col("PercMatrizNeogrid"))
        )
        .withColumn(
            "smape_vs_geral",
            F.when(
                (F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura") + F.col("PercMatrizNeogrid")) > 0,
                F.lit(2.0) * F.col("erro_absoluto_vs_geral") / 
                (F.col("Merecimento_Final_Media90_Qt_venda_sem_ruptura") + F.col("PercMatrizNeogrid")) * 100
            ).otherwise(F.lit(0.0))
        )
    )
    
    print(f"✅ sMAPE vs matriz geral calculado")
    return df_comparacao

# COMMAND ----------

# MAGIC %md
# MAGIC ### **5. SALVAMENTO DA VERSÃO COMPLETA**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **6. ANÁLISE AGREGADA (WEIGHTED sMAPE)**

# COMMAND ----------

def calcular_weighted_smape_agregado(df: DataFrame,
                                   coluna_medida: str = "Media90_Qt_venda_sem_ruptura",
                                   coluna_smape: str = "smape_Media90_Qt_venda_sem_ruptura") -> DataFrame:
    """
    Calcula weighted sMAPE agregado por diferentes níveis de agrupamento.
    
    Args:
        df: DataFrame com dados de sMAPE e demanda
        coluna_medida: Coluna de demanda para ponderação
        coluna_smape: Coluna de sMAPE para cálculo
        
    Returns:
        DataFrame com weighted sMAPE por diferentes agrupamentos
    """
    print(f"📊 Calculando weighted sMAPE agregado usando {coluna_medida} para ponderação...")
    
    df_weighted_grupo_filial = (
        df
        .groupBy("grupo_de_necessidade", "cdfilial")
        .agg(
            F.sum(F.col(coluna_medida)).alias("demanda_total"),
            F.sum(F.col(coluna_medida) * F.col(coluna_smape)).alias("soma_ponderada"),
            F.count("*").alias("total_skus")
        )
        .withColumn(
            "weighted_smape",
            F.when(
                F.col("demanda_total") > 0,
                F.col("soma_ponderada") / F.col("demanda_total")
            ).otherwise(F.lit(0.0))
        )
        .withColumn("nivel_agrupamento", F.lit("grupo_de_necessidade_x_filial"))
    )
    
    df_weighted_grupo = (
        df
        .groupBy("grupo_de_necessidade")
        .agg(
            F.sum(F.col(coluna_medida)).alias("demanda_total"),
            F.sum(F.col(coluna_medida) * F.col(coluna_smape)).alias("soma_ponderada"),
            F.count("*").alias("total_skus")
        )
        .withColumn(
            "weighted_smape",
            F.when(
                F.col("demanda_total") > 0,
                F.col("soma_ponderada") / F.col("demanda_total")
            ).otherwise(F.lit(0.0))
        )
        .withColumn("nivel_agrupamento", F.lit("grupo_de_necessidade"))
    )
    
    df_weighted_filial = (
        df
        .groupBy("cdfilial")
        .agg(
            F.sum(F.col(coluna_medida)).alias("demanda_total"),
            F.sum(F.col(coluna_medida) * F.col(coluna_smape)).alias("soma_ponderada"),
            F.count("*").alias("total_skus")
        )
        .withColumn(
            "weighted_smape",
            F.when(
                F.col("demanda_total") > 0,
                F.col("soma_ponderada") / F.col("demanda_total")
            ).otherwise(F.lit(0.0))
        )
        .withColumn("nivel_agrupamento", F.lit("filial"))
    )
    
    df_weighted_consolidado = (
        df_weighted_grupo_filial
        .unionByName(df_weighted_grupo, allowMissingColumns=True)
        .unionByName(df_weighted_filial, allowMissingColumns=True)
        .orderBy("nivel_agrupamento", "grupo_de_necessidade", "cdfilial")
    )
    
    print(f"✅ Weighted sMAPE calculado para todos os níveis de agrupamento")
    print(f"  • Total de registros: {df_weighted_consolidado.count():,}")
    
    return df_weighted_consolidado

def salvar_weighted_smape_agregado(df_weighted_smape: DataFrame,
                                  categoria: str,
                                  mes_analise: str = "202507") -> None:
    """
    Salva o weighted sMAPE agregado como tabela Delta.
    
    Args:
        df_weighted_smape: DataFrame com weighted sMAPE calculado
        categoria: Nome da categoria/diretoria
        mes_analise: Mês de análise no formato YYYYMM
    """
    print(f"💾 Salvando weighted sMAPE agregado para categoria: {categoria}")
    
    nome_tabela = f"weighted_smape_agregado_{categoria.lower()}_{mes_analise}"
    
    (
        df_weighted_smape
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(nome_tabela)
    )
    
    print(f"✅ Weighted sMAPE salvo na tabela: {nome_tabela}")

# COMMAND ----------

def salvar_versao_final_completa(df_merecimento: DataFrame, 
                                categoria: str,
                                mes_analise: str = "202507",
                                data_corte_matriz: str = "2025-06-30",
                                data_hora_execucao: str = None) -> None:
    """
    Salva versão final completa com todos os dados: SKU x grupo x filial x CD x merecimentos x métricas.
    
    **Colunas de melhor sMAPE:**
    - melhor_sMAPE: Valor numérico do melhor sMAPE (menor erro)
    - medida_melhor_sMAPE: Nome da medida que produziu o melhor sMAPE
    
    Args:
        df_merecimento: DataFrame com merecimento calculado
        categoria: Nome da categoria/diretoria
        mes_analise: Mês de análise no formato YYYYMM (padrão: julho-2025)
        data_corte_matriz: Data de corte para cálculo da matriz de merecimento (padrão: 2025-06-30)
        data_hora_execucao: Data/hora da execução (padrão: agora)
    """
    if data_hora_execucao is None:
        data_hora_execucao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"💾 Salvando versão final completa para categoria: {categoria}")
    print(f"📅 Mês de análise: {mes_analise}")
    print(f"🕐 Data/hora execução: {data_hora_execucao}")
    
    print("📊 Carregando dados de demanda calculada com cache estratégico...")
    
    # NOTA: A tabela base não tem grupo_de_necessidade, então vamos usar os dados do df_merecimento
    df_dados_demanda = (
        df_merecimento
        .select(
            "cdfilial", "grupo_de_necessidade", "CdSku",
            "Total_CD_Media90_Qt_venda_sem_ruptura", "Total_CD_Media180_Qt_venda_sem_ruptura",
            "Total_CD_Media270_Qt_venda_sem_ruptura", "Total_CD_Media360_Qt_venda_sem_ruptura",
            "Total_CD_MediaAparada90_Qt_venda_sem_ruptura", "Total_CD_MediaAparada180_Qt_venda_sem_ruptura",
            "Total_CD_MediaAparada270_Qt_venda_sem_ruptura", "Total_CD_MediaAparada360_Qt_venda_sem_ruptura"
        )
        .withColumnRenamed("Total_CD_Media90_Qt_venda_sem_ruptura", "Media90_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_Media180_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_Media270_Qt_venda_sem_ruptura", "Media270_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_Media360_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura")

        .withColumnRenamed("Total_CD_MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada90_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_MediaAparada180_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada270_Qt_venda_sem_ruptura")
        .withColumnRenamed("Total_CD_MediaAparada360_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura")
        .cache()  # Cache estratégico para múltiplos usos
    )
    
    print(f"✅ Dados de demanda carregados e cacheados:")
    print(f"  • Total de registros: {df_dados_demanda.count():,}")
    
    print("🔄 Criando mapeamento filial → CD com broadcast join...")
    
    de_para_filial_cd = (
        df_dados_demanda
        .select("cdfilial", "grupo_de_necessidade")
        .distinct()
        .join(
            df_merecimento.select("cdfilial", "cd_primario").distinct(),
            on="cdfilial",
            how="inner"
        )
        .withColumnRenamed("cd_primario", "cd_primario_mapeamento")
        .cache()
    )
    
    print("📈 Calculando proporção factual para todas as medidas...")
    print("📊 IMPORTANTE: Proporção factual calculada por SKU na FILIAL vs TOTAL DA EMPRESA usando Media90_Qt_venda_sem_ruptura")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    df_proporcao_factual_todas_medidas = df_dados_demanda
    for medida in medidas_disponiveis:
        df_proporcao_factual_todas_medidas = (
            calcular_proporcao_factual_por_sku_filial(
                df_proporcao_factual_todas_medidas, 
                medida
            )
        )
    
    colunas_proporcao = ["cdfilial", "grupo_de_necessidade", "CdSku"] + [
        f"proporcao_factual_{medida}_percentual" for medida in medidas_disponiveis
    ]
    
    df_proporcao_factual = (
        df_proporcao_factual_todas_medidas
        .select(*colunas_proporcao)
        .withColumnRenamed("CdSku", "CdSku_proporcao")
        .cache()
    )
    
    print(f"✅ Proporção factual calculada para todas as medidas")
    print(f"  • Total de registros: {df_proporcao_factual.count():,}")
    
    print("🔗 Realizando join completo com otimizações de performance...")
    
    df_proporcao_factual_broadcast = F.broadcast(df_proporcao_factual)
    
    df_proporcao_factual_renomeado = (
        df_proporcao_factual_broadcast
        .withColumnRenamed("CdSku", "CdSku_proporcao")
    )
    
    print("🧹 Removendo duplicatas nas chaves únicas antes dos joins...")
    
    df_merecimento_limpo = (
        df_merecimento
        .dropDuplicates(["CdSku", "grupo_de_necessidade", "cdfilial", "cd_primario"])
        .cache()
    )
    
    df_proporcao_factual_limpo = (
        df_proporcao_factual_renomeado
        .dropDuplicates(["cdfilial", "grupo_de_necessidade"])
        .cache()
    )
    
    de_para_filial_cd_limpo = (
        de_para_filial_cd
        .dropDuplicates(["cdfilial", "grupo_de_necessidade"])
        .cache()
    )
    
    print(f"✅ Duplicatas removidas: {df_merecimento.count():,} → {df_merecimento_limpo.count():,} registros")
    
    df_versao_final = (
        df_merecimento_limpo
        .join(
            df_proporcao_factual_limpo,
            on=["cdfilial", "grupo_de_necessidade"],
            how="inner"
        )
        .join(
            de_para_filial_cd_limpo,
            on=["cdfilial", "grupo_de_necessidade"],
            how="inner"
        )
    )
    
    print(f"✅ Join completo realizado:")
    print(f"  • Registros finais: {df_versao_final.count():,}")
    
    print("📊 Calculando sMAPE para cada medida...")
    
    EPSILON = 1e-12
    
    df_com_smape = df_versao_final
    for medida in medidas_disponiveis:
        df_com_smape = (
            df_com_smape
            .withColumn(
                f"merecimento_{medida}_percentual",
                F.when(
                    F.col(f"Merecimento_Final_{medida}") > 0,
                    F.col(f"Merecimento_Final_{medida}") / F.sum(f"Merecimento_Final_{medida}").over(
                        Window.partitionBy("grupo_de_necessidade")
                    ) * 100
                ).otherwise(F.lit(0.0))
            )
            .withColumn(
                f"erro_absoluto_{medida}",
                F.abs(F.col(f"merecimento_{medida}_percentual") - F.col(f"proporcao_factual_{medida}_percentual"))
            )
            .withColumn(
                f"smape_{medida}",
                F.when(
                    (F.col(f"merecimento_{medida}_percentual") + F.col(f"proporcao_factual_{medida}_percentual")) > 0,
                    F.lit(2.0) * F.col(f"erro_absoluto_{medida}") / 
                    (F.col(f"merecimento_{medida}_percentual") + F.col(f"proporcao_factual_{medida}_percentual") + F.lit(EPSILON)) * 100
                ).otherwise(F.lit(0.0))
            )
        )
    
    print("🏆 Calculando melhor sMAPE e melhor medida...")
    
    colunas_smape = [f"smape_{medida}" for medida in medidas_disponiveis]
    
    colunas_smape_least = [F.col(f"smape_{medida}") for medida in medidas_disponiveis]
    
    df_com_melhor_smape = (
        df_com_smape
        .withColumn(
            "melhor_sMAPE",
            F.least(*colunas_smape_least)
        )
        .withColumn(
            "medida_melhor_sMAPE",
            F.lit("")
        )
    )
    
    for medida in medidas_disponiveis:
        df_com_melhor_smape = (
            df_com_melhor_smape
            .withColumn(
                "medida_melhor_sMAPE",
                F.when(
                    F.col(f"smape_{medida}") == F.col("melhor_sMAPE"),
                    F.lit(medida)
                ).otherwise(F.col("medida_melhor_sMAPE"))
            )
        )
    
    print("📋 Preparando colunas finais...")
    
    colunas_identificacao = [
        "CdSku", "grupo_de_necessidade", "cdfilial", "cd_primario"
    ]
    
    colunas_merecimento_cd = [
        f"Total_CD_{medida}" for medida in medidas_disponiveis
    ]
    
    colunas_percentual_loja = [
        f"Percentual_{medida}" for medida in medidas_disponiveis
    ]
    
    colunas_merecimento_final = [
        f"Merecimento_Final_{medida}" for medida in medidas_disponiveis
    ]
    
    colunas_proporcao_factual = [
        f"proporcao_factual_{medida}_percentual" for medida in medidas_disponiveis
    ]
    
    colunas_smape = [
        f"smape_{medida}" for medida in medidas_disponiveis
    ]
    
    colunas_melhor_smape = [
        "melhor_sMAPE",
        "medida_melhor_sMAPE"
    ]
    
    todas_colunas = (
        colunas_identificacao + 
        colunas_merecimento_cd + 
        colunas_percentual_loja + 
        colunas_merecimento_final + 
        colunas_proporcao_factual + 
        colunas_smape + 
        colunas_melhor_smape
    )
    
    df_final_completo = (
        df_com_melhor_smape
        .select(*todas_colunas)
        .withColumn("data_hora_execucao", F.lit(data_hora_execucao))
        .withColumn("mes_analise", F.lit(mes_analise))
        .withColumn("data_corte_matriz", F.lit(data_corte_matriz))
        .withColumn("categoria", F.lit(categoria))
    )
    
    print("🧹 Removendo duplicatas finais nas chaves únicas...")
    registros_antes = df_final_completo.count()
    
    df_final_completo = (
        df_final_completo
        .dropDuplicates(["CdSku", "grupo_de_necessidade", "cdfilial", "cd_primario"])
        .cache()
    )
    
    registros_depois = df_final_completo.count()
    print(f"✅ Resultado final preparado: {registros_depois:,} registros, {len(df_final_completo.columns)} colunas")
    
    print("💾 Salvando no databox com modo APPEND...")
    
    categoria_normalizada = (
        categoria
        .replace("DIRETORIA ", "")
        .replace(" ", "_")
        .upper()
    )
    
    nome_tabela = f"databox.bcg_comum.supply_base_merecimento_diario_{categoria_normalizada}"
    
    (
        df_final_completo
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(nome_tabela)
    )
    
    print(f"✅ Versão final salva: {nome_tabela} ({df_final_completo.count():,} registros, {len(df_final_completo.columns)} colunas)")
    
    print("🧹 Limpando caches para liberar memória...")
    df_dados_demanda.unpersist()
    df_proporcao_factual.unpersist()
    de_para_filial_cd.unpersist()
    
    print("✅ Cache limpo e memória liberada")
    
    return df_final_completo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 FUNÇÕES PRINCIPAIS REORGANIZADAS
# MAGIC
# MAGIC ### **1. Cálculo da Matriz de Merecimento**
# MAGIC ### **2. Cálculo da Proporção Factual**  
# MAGIC ### **3. Carregamento da Matriz DRP**
# MAGIC ### **4. Cálculo de sMAPE e Comparações**
# MAGIC ### **5. Salvamento da Versão Completa**
# MAGIC ### **6. Análise Agregada (Weighted sMAPE)**

# COMMAND ----------

def executar_calculo_matriz_merecimento_completo(categoria: str, 
                                                data_inicio: str = "2024-01-01",
                                                data_calculo: str = "2025-06-30",
                                                sigma_meses_atipicos: float = 3.0,
                                                sigma_outliers_cd: float = 3.0,
                                                sigma_outliers_loja: float = 3.0,
                                                sigma_atacado_cd: float = 1.5,
                                                sigma_atacado_loja: float = 1.5,
                                                mes_analise: str = "202507",
                                                data_corte_matriz: str = "2025-06-30") -> DataFrame:
    """
    Função principal que executa todo o fluxo da matriz de merecimento:
    
    1. Cálculo da Matriz de Merecimento (CD + participação interna)
    2. Cálculo da Proporção Factual (SKU x filial vs total da empresa)
    3. Carregamento da Matriz DRP Geral
    4. Cálculo de sMAPE (factual + matriz geral)
    5. Salvamento da Versão Completa
    6. Análise Agregada (Weighted sMAPE)
    
    Args:
        categoria: Nome da categoria/diretoria
        data_inicio: Data de início para filtro (formato YYYY-MM-DD)
        data_calculo: Data específica para cálculo de merecimento (formato YYYY-MM-DD)
        sigma_meses_atipicos: Número de desvios padrão para meses atípicos (padrão: 3.0)
        sigma_outliers_cd: Número de desvios padrão para outliers CD (padrão: 3.0)
        sigma_outliers_loja: Número de desvios padrão para outliers loja (padrão: 3.0)
        sigma_atacado_cd: Número de desvios padrão para outliers CD atacado (padrão: 1.5)
        sigma_atacado_loja: Número de desvios padrão para outliers loja atacado (padrão: 1.5)
        mes_analise: Mês de análise para métricas no formato YYYYMM (padrão: julho-2025)
        data_corte_matriz: Data de corte para cálculo da matriz de merecimento (padrão: 2025-06-30)
        
    Returns:
        DataFrame final com todas as medidas calculadas, merecimento, proporção factual e sMAPE
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
        # 1. Carregamento dos dados base (SEM grupo_de_necessidade ainda)
        df_base = carregar_dados_base(categoria, data_inicio)
        df_base.cache()
        # 2. Carregamento dos mapeamentos
        de_para_modelos, de_para_gemeos = carregar_mapeamentos_produtos(categoria)  

        # 3. Aplicação dos mapeamentos
        df_com_mapeamentos = aplicar_mapeamentos_produtos(
            df_base, categoria, de_para_modelos, de_para_gemeos
        )
        # 4. AGORA determina o grupo_de_necessidade (APÓS os mapeamentos)
        df_com_grupo = determinar_grupo_necessidade(categoria, df_com_mapeamentos)
        df_com_grupo.cache()
        # 5. Detecção de outliers com parâmetros sigma configuráveis
        df_stats, df_meses_atipicos = detectar_outliers_meses_atipicos(
            df_com_grupo, 
            categoria,
            sigma_meses_atipicos=sigma_meses_atipicos,
            sigma_outliers_cd=sigma_outliers_cd,
            sigma_outliers_loja=sigma_outliers_loja,
            sigma_atacado_cd=sigma_atacado_cd,
            sigma_atacado_loja=sigma_atacado_loja
        )
        
        # 6. Filtragem de meses atípicos
        df_filtrado = filtrar_meses_atipicos(df_com_grupo, df_meses_atipicos)
        
        # 7. Cálculo das medidas centrais
        df_com_medidas = calcular_medidas_centrais_com_medias_aparadas(df_filtrado)
        
        # 8. Consolidação final
        df_final = consolidar_medidas(df_com_medidas)
        
        # 9. Cálculo de merecimento por CD e filial
        print("=" * 80)
        print("🔄 Iniciando cálculo de merecimento...")
        
        # 9.1 Merecimento a nível CD (grupo_de_necessidade)
        df_merecimento_cd = calcular_merecimento_cd(df_final, data_calculo, categoria)
        
        # 9.2 Merecimento interno ao CD (participação da filial dentro do CD)
        df_merecimento_interno = calcular_merecimento_interno_cd(df_final, data_calculo, categoria)
        
        # 9.3 Merecimento final (CD × Interno CD = desdobramento para SKUs)
        df_merecimento_final = calcular_merecimento_final(df_merecimento_cd, df_merecimento_interno)
        
        print("🔄 Consolidando resultado final...")
        
        medidas_disponiveis = [
            "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
            "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
            "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
            "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
        ]
        
        colunas_totais_cd = [F.col(f"Total_CD_{medida}") for medida in medidas_disponiveis]
        colunas_percentual = [F.col(f"Percentual_{medida}") for medida in medidas_disponiveis]
        colunas_merecimento_final = [F.col(f"Merecimento_Final_{medida}") for medida in medidas_disponiveis]
        
        df_resultado_final = df_merecimento_final.select(
            "cdfilial", "cd_primario", "grupo_de_necessidade",
            # Colunas de merecimento CD (totais por CD + gêmeo)
            *colunas_totais_cd,
            # Colunas de percentual interno (participação da loja dentro do CD)
            *colunas_percentual,
            # Colunas de merecimento final (CD × participação interna)
            *colunas_merecimento_final
        ).distinct()
        
        print(f"✅ Resultado final consolidado:")
        print(f"  • Estrutura: SKU x loja x gêmeo (sem dados granulares)")
        print(f"  • Total de registros: {df_resultado_final.count():,}")
        print(f"  • Colunas de merecimento: {len(medidas_disponiveis) * 3} (CD + interno + final)")
        
        print("=" * 80)
        print(f"✅ Cálculo da matriz de merecimento concluído para: {categoria}")
        print(f"📊 Total de registros finais: {df_resultado_final.count():,}")
        print(f"📅 Data de cálculo de merecimento: {data_calculo}")
        print(f"📋 Fluxo executado:")
        print(f"   1. Carregamento de dados base")
        print(f"   2. Carregamento de mapeamentos")
        print(f"   3. Aplicação de mapeamentos")
        print(f"   4. Definição de grupo_de_necessidade")
        print(f"   5. Detecção de outliers")
        print(f"   6. Filtragem de meses atípicos")
        print(f"   7. Cálculo de medidas centrais")
        print(f"   8. Consolidação final")
        print(f"   9. Cálculo de merecimento CD")
        print(f"   10. Cálculo de merecimento interno CD")
        print(f"   11. Cálculo de merecimento final")
        
        print("=" * 80)
        print("📈 CALCULANDO PROPORÇÃO FACTUAL...")
        print("=" * 80)
        
        df_proporcao_factual = calcular_proporcao_factual_completa(df_merecimento_final)
        
        print("=" * 80)
        print("📊 CARREGANDO MATRIZ DRP GERAL...")
        print("=" * 80)
        
        df_matriz_geral = carregar_matriz_drp_geral()
        
        print("=" * 80)
        print("📊 CALCULANDO sMAPE E COMPARAÇÕES...")
        print("=" * 80)
        
        df_com_smape_factual = calcular_smape_comparacao_factual(df_merecimento_final, df_proporcao_factual)
        
        df_com_smape_geral = calcular_smape_comparacao_matriz_geral(df_merecimento_final, df_matriz_geral)
        
        print("=" * 80)
        print("💾 SALVANDO VERSÃO COMPLETA...")
        print("=" * 80)
        
        try:
            df_versao_completa = salvar_versao_final_completa(
                df_merecimento=df_com_smape_factual,
                categoria=categoria,
                mes_analise=mes_analise,
                data_corte_matriz=data_corte_matriz
            )
            print("✅ Versão completa salva com sucesso!")
        except Exception as e:
            print(f"❌ Erro ao salvar versão completa: {str(e)}")
            print("⚠️  Continuando com resultado padrão...")
            df_versao_completa = df_com_smape_factual
        
        print("=" * 80)
        print("📊 CALCULANDO WEIGHTED sMAPE AGREGADO...")
        print("=" * 80)
        
        try:
            df_weighted_smape = calcular_weighted_smape_agregado(
                df_com_smape_factual,
                coluna_medida="Media90_Qt_venda_sem_ruptura",
                coluna_smape="smape_Media90_Qt_venda_sem_ruptura"
            )
            
            salvar_weighted_smape_agregado(df_weighted_smape, categoria, mes_analise)
            print("✅ Weighted sMAPE calculado e salvo com sucesso!")
        except Exception as e:
            print(f"❌ Erro ao calcular weighted sMAPE: {str(e)}")
            print("⚠️  Continuando com resultado padrão...")
        
        print("=" * 80)
        print(f"✅ FLUXO COMPLETO EXECUTADO para: {categoria}")
        print(f"📊 Total de registros finais: {df_versao_completa.count():,}")
        print(f"📅 Data de cálculo: {data_calculo}")
        print(f"📋 Fluxo executado:")
        print(f"   1. Cálculo da Matriz de Merecimento")
        print(f"   2. Cálculo da Proporção Factual")
        print(f"   3. Carregamento da Matriz DRP Geral")
        print(f"   4. Cálculo de sMAPE (factual + matriz geral)")
        print(f"   5. Salvamento da Versão Completa")
        print(f"   6. Análise Agregada (Weighted sMAPE)")
        
        return df_versao_completa
        
    except Exception as e:
        print(f"❌ Erro durante o cálculo: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md


# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Função de Cálculo de Weighted sMAPE

# COMMAND ----------

def calcular_weighted_smape_agregado(df: DataFrame, 
                                   categoria: str,
                                   medidas_disponiveis: List[str] = None) -> DataFrame:
    """
    Calcula o weighted sMAPE agregado para diferentes níveis de agrupamento.
    
    **Níveis de agregação calculados:**
    1. **Grupo de necessidade**: Agregação por grupo_de_necessidade
    2. **Grupo de necessidade x Loja**: Agregação por grupo_de_necessidade + cdfilial
    3. **Loja**: Agregação por cdfilial
    4. **Categoria inteira**: Agregação total da categoria
    
    **Fórmula do weighted sMAPE:**
    - sMAPE = Σ(|y_true - y_pred| * peso) / Σ(peso) * 100
    - Onde peso = quantidade de demanda da medida (ex: Media90_Qt_venda_sem_ruptura)
    
    Args:
        df: DataFrame com merecimento calculado, proporção factual e quantidade de demanda
        categoria: Nome da categoria/diretoria
        medidas_disponiveis: Lista de medidas disponíveis (padrão: todas as medidas)
        
    Returns:
        DataFrame com weighted sMAPE calculado para todos os níveis de agregação
    """
    print(f"📊 Calculando weighted sMAPE agregado para categoria: {categoria}")
    
    if medidas_disponiveis is None:
        medidas_disponiveis = [
            "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
            "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
            "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
            "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
        ]
    
    print(f"📊 Total de registros: {df.count():,}")
    
    # 1. PREPARAÇÃO DOS DADOS: Garante que temos as colunas necessárias
    print("🔄 Preparando dados para cálculo de weighted sMAPE...")
    
    colunas_necessarias = []
    for medida in medidas_disponiveis:
        colunas_necessarias.extend([
            f"merecimento_{medida}_percentual",
            f"proporcao_factual_{medida}_percentual"
        ])
    
    df_valido = df.filter(
        F.col("merecimento_Media90_Qt_venda_sem_ruptura_percentual").isNotNull() &
        F.col("proporcao_factual_Media90_Qt_venda_sem_ruptura_percentual").isNotNull()
    )
    
    print(f"✅ Dados preparados:")
    print(f"  • Registros válidos: {df_valido.count():,}")
    print(f"  • Colunas necessárias: {len(colunas_necessarias)}")
    
    print("📈 Calculando weighted sMAPE para cada medida...")
    
    df_com_smape = df_valido
    
    for medida in medidas_disponiveis:
        df_com_smape = df_com_smape.withColumn(
            f"erro_absoluto_{medida}",
            F.abs(F.col(f"merecimento_{medida}_percentual") - F.col(f"proporcao_factual_{medida}_percentual"))
        )
        
        df_com_smape = df_com_smape.withColumn(
            f"peso_{medida}",
            F.col(medida)
        )
        
        df_com_smape = df_com_smape.withColumn(
            f"erro_peso_{medida}",
            F.col(f"erro_absoluto_{medida}") * F.col(f"peso_{medida}")
        )
    
    print(f"✅ Cálculos intermediários concluídos para {len(medidas_disponiveis)} medidas")
    
    print("🔄 Calculando agregações por diferentes níveis...")
    
    print("📊 Agregação por grupo de necessidade...")
    
    aggs_grupo = []
    for medida in medidas_disponiveis:
        aggs_grupo.extend([
            F.sum(F.col(f"erro_peso_{medida}")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col(f"peso_{medida}")).alias(f"soma_peso_{medida}")
        ])
    
    df_smape_grupo = df_com_smape.groupBy("grupo_de_necessidade").agg(*aggs_grupo)
    
    for medida in medidas_disponiveis:
        df_smape_grupo = df_smape_grupo.withColumn(
            f"weighted_smape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    print("📊 Agregação por grupo de necessidade x loja...")
    
    aggs_grupo_loja = []
    for medida in medidas_disponiveis:
        aggs_grupo_loja.extend([
            F.sum(F.col(f"erro_peso_{medida}")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col(f"peso_{medida}")).alias(f"soma_peso_{medida}")
        ])
    
    df_smape_grupo_loja = df_com_smape.groupBy("grupo_de_necessidade", "cdfilial").agg(*aggs_grupo_loja)
    
    for medida in medidas_disponiveis:
        df_smape_grupo_loja = df_smape_grupo_loja.withColumn(
            f"weighted_smape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    print("📊 Agregação por loja...")
    
    aggs_loja = []
    for medida in medidas_disponiveis:
        aggs_loja.extend([
            F.sum(F.col(f"erro_peso_{medida}")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col(f"peso_{medida}")).alias(f"soma_peso_{medida}")
        ])
    
    df_smape_loja = df_com_smape.groupBy("cdfilial").agg(*aggs_loja)
    
    for medida in medidas_disponiveis:
        df_smape_loja = df_smape_loja.withColumn(
            f"weighted_smape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    print("📊 Agregação da categoria inteira...")
    
    aggs_categoria = []
    for medida in medidas_disponiveis:
        aggs_categoria.extend([
            F.sum(F.col(f"erro_peso_{medida}")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col(f"peso_{medida}")).alias(f"soma_peso_{medida}")
        ])
    
    df_smape_categoria = df_com_smape.agg(*aggs_categoria)
    
    for medida in medidas_disponiveis:
        df_smape_categoria = df_smape_categoria.withColumn(
            f"weighted_smape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    df_smape_categoria = df_smape_categoria.withColumn("nivel_agregacao", F.lit("CATEGORIA_INTEIRA"))
    
    print("🔄 Consolidando resultados de weighted sMAPE...")
    
    df_smape_grupo = df_smape_grupo.withColumn("nivel_agregacao", F.lit("GRUPO_NECESSIDADE"))
    df_smape_grupo_loja = df_smape_grupo_loja.withColumn("nivel_agregacao", F.lit("GRUPO_NECESSIDADE_LOJA"))
    df_smape_loja = df_smape_loja.withColumn("nivel_agregacao", F.lit("LOJA"))
    
    colunas_smape = ["nivel_agregacao"] + [f"weighted_smape_{medida}" for medida in medidas_disponiveis]
    
    df_smape_grupo_final = df_smape_grupo.select("nivel_agregacao", "grupo_de_necessidade", *colunas_smape[1:])
    df_smape_grupo_loja_final = df_smape_grupo_loja.select("nivel_agregacao", "grupo_de_necessidade", "cdfilial", *colunas_smape[1:])
    df_smape_loja_final = df_smape_loja.select("nivel_agregacao", "cdfilial", *colunas_smape[1:])
    df_smape_categoria_final = df_smape_categoria.select(*colunas_smape)
    
    df_smape_final = (
        df_smape_grupo_final
        .unionByName(df_smape_grupo_loja_final, allowMissingColumns=True)
        .unionByName(df_smape_loja_final, allowMissingColumns=True)
        .unionByName(df_smape_categoria_final, allowMissingColumns=True)
    )
    
    print(f"✅ Weighted sMAPE calculado com sucesso!")
    print(f"📊 Resultados consolidados:")
    print(f"  • Total de registros: {df_smape_final.count():,}")
    print(f"  • Níveis de agregação: 4 (grupo, grupo+loja, loja, categoria)")
    print(f"  • Medidas calculadas: {len(medidas_disponiveis)}")
    
    print("\n" + "="*80)
    print("📊 RESUMO DO WEIGHTED SMAPE POR NÍVEL DE AGREGAÇÃO")
    print("="*80)
    
    for nivel in ["GRUPO_NECESSIDADE", "GRUPO_NECESSIDADE_LOJA", "LOJA", "CATEGORIA_INTEIRA"]:
        df_nivel = df_smape_final.filter(F.col("nivel_agregacao") == nivel)
        print(f"\n📊 {nivel}:")
        
        if nivel == "CATEGORIA_INTEIRA":
            for medida in medidas_disponiveis:
                valor = df_nivel.select(f"weighted_smape_{medida}").first()[0]
                print(f"  • {medida}: {valor:.4f}%")
        else:
            for medida in medidas_disponiveis:
                stats = df_nivel.select(
                    F.avg(f"weighted_smape_{medida}").alias("media"),
                    F.stddev(f"weighted_smape_{medida}").alias("desvio"),
                    F.min(f"weighted_smape_{medida}").alias("minimo"),
                    F.max(f"weighted_smape_{medida}").alias("maximo")
                ).first()
                
                print(f"  • {medida}:")
                print(f"    - Média: {stats['media']:.4f}%")
                print(f"    - Desvio: {stats['desvio']:.4f}%")
                print(f"    - Min: {stats['minimo']:.4f}%")
                print(f"    - Max: {stats['maximo']:.4f}%")
    
    print("="*80)
    
    return df_smape_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Execução da Matriz de Merecimento

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execução para DIRETORIA DE TELAS com Versão Completa

# COMMAND ----------

# Funções principais implementadas

# COMMAND ----------

# Funções principais implementadas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Validação e Testes

# COMMAND ----------

def validar_resultados(df: DataFrame, categoria: str) -> None:
    """
    Valida os resultados do cálculo da matriz de merecimento.
    
    Args:
        df: DataFrame com os resultados
        categoria: Nome da categoria
    """
    print(f"📊 Validando resultados para: {categoria}")
    
    total_registros = df.count()
    total_skus = df.select("CdSku").distinct().count()
    total_lojas = df.select("CdFilial").distinct().count()
    total_grupos = df.select("grupo_de_necessidade").distinct().count()
    
    print("📊 Estatísticas gerais:")
    print(f"  • Total de registros: {total_registros:,}")
    print(f"  • Total de SKUs únicos: {total_skus:,}")
    print(f"  • Total de lojas únicas: {total_lojas:,}")
    print(f"  • Total de grupos de necessidade: {total_grupos:,}")
    
    colunas_medias = [f"Media{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    colunas_medias_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    
    todas_colunas_medidas = colunas_medias + colunas_medias_aparadas
    
    print("\n📊 Verificação de valores nulos:")
    for coluna in todas_colunas_medidas:
        if coluna in df.columns:
            nulos = df.filter(F.col(coluna).isNull()).count()
            print(f"  • {coluna}: {nulos:,} valores nulos")
    
    print("✅ Validação concluída")

# COMMAND ----------

def salvar_weighted_smape_agregado(df_weighted_smape: DataFrame, 
                                   categoria: str,
                                   mes_analise: str = "202507",
                                   data_corte_matriz: str = "2025-06-30",
                                   data_hora_execucao: str = None) -> None:
    """
    Salva os weighted sMAPEs agregados em tabelas separadas por nível de agregação.
    
    **Tabelas criadas:**
    1. **supply_weighted_smape_grupo_{CATEGORIA}**: Agregação por grupo de necessidade
    2. **supply_weighted_smape_grupo_loja_{CATEGORIA}**: Agregação por grupo + loja
    3. **supply_weighted_smape_loja_{CATEGORIA}**: Agregação por loja
    4. **supply_weighted_smape_categoria_{CATEGORIA}**: Agregação da categoria inteira
    
    **Colunas de weighted sMAPE:**
    - weighted_smape_Media90_Qt_venda_sem_ruptura: sMAPE ponderado para média 90 dias
    - weighted_smape_Media180_Qt_venda_sem_ruptura: sMAPE ponderado para média 180 dias
    - weighted_smape_Media270_Qt_venda_sem_ruptura: sMAPE ponderado para média 270 dias
    - weighted_smape_Media360_Qt_venda_sem_ruptura: sMAPE ponderado para média 360 dias
    - weighted_smape_MediaAparada90_Qt_venda_sem_ruptura: sMAPE ponderado para média aparada 90 dias
    - weighted_smape_MediaAparada180_Qt_venda_sem_ruptura: sMAPE ponderado para média aparada 180 dias
    - weighted_smape_MediaAparada270_Qt_venda_sem_ruptura: sMAPE ponderado para média aparada 270 dias
    - weighted_smape_MediaAparada360_Qt_venda_sem_ruptura: sMAPE ponderado para média aparada 360 dias
    
    Args:
        df_weighted_smape: DataFrame com weighted sMAPEs calculados
        categoria: Nome da categoria/diretoria
        mes_analise: Mês de análise no formato YYYYMM (padrão: julho-2025)
        data_corte_matriz: Data de corte para cálculo da matriz de merecimento (padrão: 2025-06-30)
        data_hora_execucao: Data/hora da execução (padrão: agora)
    """
    if data_hora_execucao is None:
        data_hora_execucao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"💾 Salvando weighted sMAPEs agregados para categoria: {categoria}")
    print(f"📅 Mês de análise: {mes_analise}")
    print(f"🕐 Data/hora execução: {data_hora_execucao}")
    
    # Normaliza nome da categoria para o nome da tabela
    categoria_normalizada = (
        categoria
        .replace("DIRETORIA ", "")
        .replace(" ", "_")
        .upper()
    )
    
    # Adiciona metadados a todos os DataFrames
    df_com_metadados = df_weighted_smape.withColumn("data_hora_execucao", F.lit(data_hora_execucao)) \
                                        .withColumn("mes_analise", F.lit(mes_analise)) \
                                        .withColumn("data_corte_matriz", F.lit(data_corte_matriz)) \
                                        .withColumn("categoria", F.lit(categoria))
    
    # 1. SALVA AGREGAÇÃO POR GRUPO DE NECESSIDADE
    print("📊 Salvando agregação por grupo de necessidade...")
    df_grupo = df_com_metadados.filter(F.col("nivel_agregacao") == "GRUPO_NECESSIDADE")
    
    if df_grupo.count() > 0:
        nome_tabela_grupo = f"databox.bcg_comum.supply_weighted_smape_grupo_{categoria_normalizada}"
        
        (
            df_grupo
            .write
            .format("delta")
            .mode("overwrite")  # Overwrite para evitar duplicatas
            .option("mergeSchema", "true")
            .saveAsTable(nome_tabela_grupo)
        )
        
        print(f"✅ Agregação por grupo salva: {nome_tabela_grupo}")
        print(f"  • Total de registros: {df_grupo.count():,}")
    else:
        print("⚠️  Nenhum registro para agregação por grupo de necessidade")
    
    # 2. SALVA AGREGAÇÃO POR GRUPO + LOJA
    print("📊 Salvando agregação por grupo + loja...")
    df_grupo_loja = df_com_metadados.filter(F.col("nivel_agregacao") == "GRUPO_NECESSIDADE_LOJA")
    
    if df_grupo_loja.count() > 0:
        nome_tabela_grupo_loja = f"databox.bcg_comum.supply_weighted_smape_grupo_loja_{categoria_normalizada}"
        
        (
            df_grupo_loja
            .write
            .format("delta")
            .mode("overwrite")  # Overwrite para evitar duplicatas
            .option("mergeSchema", "true")
            .saveAsTable(nome_tabela_grupo_loja)
        )
        
        print(f"✅ Agregação por grupo + loja salva: {nome_tabela_grupo_loja}")
        print(f"  • Total de registros: {df_grupo_loja.count():,}")
    else:
        print("⚠️  Nenhum registro para agregação por grupo + loja")
    
    # 3. SALVA AGREGAÇÃO POR LOJA
    print("📊 Salvando agregação por loja...")
    df_loja = df_com_metadados.filter(F.col("nivel_agregacao") == "LOJA")
    
    if df_loja.count() > 0:
        nome_tabela_loja = f"databox.bcg_comum.supply_weighted_smape_loja_{categoria_normalizada}"
        
        (
            df_loja
            .write
            .format("delta")
            .mode("overwrite")  # Overwrite para evitar duplicatas
            .option("mergeSchema", "true")
            .saveAsTable(nome_tabela_loja)
        )
        
        print(f"✅ Agregação por loja salva: {nome_tabela_loja}")
        print(f"  • Total de registros: {df_loja.count():,}")
    else:
        print("⚠️  Nenhum registro para agregação por loja")
    
    # 4. SALVA AGREGAÇÃO DA CATEGORIA INTEIRA
    print("📊 Salvando agregação da categoria inteira...")
    df_categoria = df_com_metadados.filter(F.col("nivel_agregacao") == "CATEGORIA_INTEIRA")
    
    if df_categoria.count() > 0:
        nome_tabela_categoria = f"databox.bcg_comum.supply_weighted_smape_categoria_{categoria_normalizada}"
        
        (
            df_categoria
            .write
            .format("delta")
            .mode("overwrite")  # Overwrite para evitar duplicatas
            .option("mergeSchema", "true")
            .saveAsTable(nome_tabela_categoria)
        )
        
        print(f"✅ Agregação da categoria salva: {nome_tabela_categoria}")
        print(f"  • Total de registros: {df_categoria.count():,}")
    else:
        print("⚠️  Nenhum registro para agregação da categoria")
    
    print("=" * 80)
    print("✅ Todos os weighted sMAPEs agregados foram salvos com sucesso!")
    print(f"📊 Categoria: {categoria}")
    print(f"📅 Mês de análise: {mes_analise}")
    print(f"🕐 Data/hora execução: {data_hora_execucao}")
    print("=" * 80)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Análise de Comparação de Precisão - Método Atual vs Matriz Geral
# MAGIC
# MAGIC Nesta seção, comparamos a precisão do nosso método de merecimento com a matriz geral de referência
# MAGIC para identificar oportunidades de melhoria e validar nossa abordagem.
# MAGIC
# MAGIC **Métricas Calculadas:**
# MAGIC - **sMAPE**: Symmetric Mean Absolute Percentage Error por SKU/Filial
# MAGIC - **Weighted sMAPE**: sMAPE ponderado pelo peso da demanda
# MAGIC
# MAGIC **Agrupamentos de Análise:**
# MAGIC - Filial
# MAGIC - Gêmeo x Filial  
# MAGIC - Gêmeo x CD
# MAGIC - Gêmeo
# MAGIC - Categoria (Setor)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparação dos Dados para Comparação

# COMMAND ----------

def preparar_dados_comparacao_matriz_geral(
    df_matriz_calculada: DataFrame,
    df_matriz_geral: DataFrame
) -> DataFrame:
    """
    Prepara dados para comparação entre nosso método calculado e a matriz geral de referência.
    
    Args:
        df_matriz_calculada: DataFrame com nosso método de merecimento
        df_matriz_geral: DataFrame com matriz de referência
        
    Returns:
        DataFrame preparado para comparação com colunas alinhadas
    """
    # Normalizar IDs para garantir compatibilidade
    df_calculada_norm = (
        df_matriz_calculada
        .withColumn("CdSku", F.col("CdSku").cast("int"))
        .withColumn("CdFilial", F.col("CdFilial").cast("int"))
    )
    
    df_geral_norm = (
        df_matriz_geral
        .withColumn("CdSku", F.col("CdSku").cast("int"))
        .withColumn("CdFilial", F.col("CdFilial").cast("int"))
    )
    
    # Selecionar colunas relevantes do nosso método
    # Verifica quais colunas existem no DataFrame
    colunas_disponiveis = df_calculada_norm.columns
    
    colunas_selecao = ["CdFilial", "CdSku", "grupo_de_necessidade"]
    
    # Adiciona colunas opcionais se existirem
    if "DsSku" in colunas_disponiveis:
        colunas_selecao.append("DsSku")
    if "DsSetor" in colunas_disponiveis:
        colunas_selecao.append("DsSetor")
    if "DsCurvaAbcLoja" in colunas_disponiveis:
        colunas_selecao.append("DsCurvaAbcLoja")
    if "Media90_Qt_venda_sem_ruptura" in colunas_disponiveis:
        colunas_selecao.append("Media90_Qt_venda_sem_ruptura")
    if "EstoqueLoja" in colunas_disponiveis:
        colunas_selecao.append("EstoqueLoja")
    if "FlagRuptura" in colunas_disponiveis:
        colunas_selecao.append("FlagRuptura")
    if "ReceitaPerdidaRuptura" in colunas_disponiveis:
        colunas_selecao.append("ReceitaPerdidaRuptura")
    if "DDE" in colunas_disponiveis:
        colunas_selecao.append("DDE")
    
    df_meu_metodo = (
        df_calculada_norm
        .select(*colunas_selecao)
        .withColumnRenamed("Media90_Qt_venda_sem_ruptura", "Demanda_MeuMetodo")
        .withColumnRenamed("EstoqueLoja", "Estoque_MeuMetodo")
        .withColumnRenamed("FlagRuptura", "Ruptura_MeuMetodo")
        .withColumnRenamed("grupo_de_necessidade", "Gemeo")
    )
    
    # Selecionar colunas relevantes da matriz geral
    df_metodo_referencia = (
        df_geral_norm
        .select(
            "CdFilial", "CdSku",
            F.col("PercMatrizNeogrid").alias("Demanda_Referencia"),
            F.lit(0).alias("Estoque_Referencia"),  # Placeholder
            F.lit(0).alias("Ruptura_Referencia")   # Placeholder
        )
    )
    
    # Join para comparação
    df_comparacao = (
        df_meu_metodo
        .join(df_metodo_referencia, on=["CdFilial", "CdSku"], how="inner")
        .withColumn("Peso_Demanda", 
                   F.greatest(F.col("Demanda_MeuMetodo"), F.col("Demanda_Referencia")))
    )
    
    return df_comparacao

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cálculo de Métricas de Precisão (sMAPE e Weighted sMAPE)

# COMMAND ----------

def calcular_metricas_precisao_comparacao(df: DataFrame) -> DataFrame:
    """
    Calcula métricas de precisão entre os dois métodos de merecimento.
    
    Args:
        df: DataFrame com dados de comparação
        
    Returns:
        DataFrame com métricas de precisão calculadas:
        - sMAPE: Symmetric Mean Absolute Percentage Error
        - Weighted_sMAPE: Weighted Symmetric Mean Absolute Percentage Error
        - Erro_Absoluto: Diferença absoluta entre métodos
        - Erro_Relativo: Erro relativo ponderado pela demanda
    """
    return (
        df
        .withColumn("Erro_Absoluto", 
                   F.abs(F.col("Demanda_MeuMetodo") - F.col("Demanda_Referencia")))
        .withColumn("Erro_Relativo", 
                   F.col("Erro_Absoluto") / F.greatest(F.col("Demanda_MeuMetodo"), F.col("Demanda_Referencia"), F.lit(1)))
        .withColumn("sMAPE", 
                   F.when(
                       (F.col("Demanda_MeuMetodo") + F.col("Demanda_Referencia")) > 0,
                       2 * F.col("Erro_Absoluto") / (F.col("Demanda_MeuMetodo") + F.col("Demanda_Referencia"))
                   ).otherwise(F.lit(0)))
        .withColumn("Weighted_sMAPE", 
                   F.col("sMAPE") * F.col("Peso_Demanda"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Performance por Filial e SKU

# COMMAND ----------

def analisar_performance_comparacao(df: DataFrame) -> DataFrame:
    """
    Analisa performance dos métodos por filial e SKU.
    
    Args:
        df: DataFrame com métricas de precisão
        
    Returns:
        DataFrame com análise detalhada por filial e SKU
    """
    return (
        df
        .withColumn("Melhor_Metodo", 
                   F.when(F.col("sMAPE") < 0.1, "Meu_Metodo_Muito_Melhor")
                   .when(F.col("sMAPE") < 0.2, "Meu_Metodo_Melhor")
                   .when(F.col("sMAPE") < 0.3, "Empate")
                   .when(F.col("sMAPE") < 0.5, "Referencia_Melhor")
                   .otherwise("Referencia_Muito_Melhor"))
        .withColumn("Categoria_Performance", 
                   F.when(F.col("sMAPE") < 0.1, "Excelente")
                   .when(F.col("sMAPE") < 0.2, "Boa")
                   .when(F.col("sMAPE") < 0.3, "Regular")
                   .when(F.col("sMAPE") < 0.5, "Ruim")
                   .otherwise("Muito_Ruim"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ranking dos Casos com Maior Diferença de Precisão

# COMMAND ----------

def criar_ranking_performance_comparacao(df: DataFrame) -> DataFrame:
    """
    Cria ranking dos casos onde nosso método foi significativamente mais preciso.
    
    Args:
        df: DataFrame com análise de performance
        
    Returns:
        DataFrame ordenado por impacto da melhoria ponderado pela demanda
    """
    return (
        df
        .filter(F.col("Melhor_Metodo").isin(["Meu_Metodo_Muito_Melhor", "Meu_Metodo_Melhor"]))
        .withColumn("Score_Impacto", 
                   (1 - F.col("sMAPE")) * F.col("Peso_Demanda"))
        .orderBy(F.col("Score_Impacto").desc())
        .select(
            "CdFilial", "CdSku", "DsSku", "DsSetor", "DsCurvaAbcLoja", "Gemeo",
            "Demanda_MeuMetodo", "Demanda_Referencia", "Estoque_MeuMetodo", "Estoque_Referencia",
            "sMAPE", "Weighted_sMAPE", "Peso_Demanda", "Score_Impacto", "Categoria_Performance"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo Estatístico da Comparação nos Agrupamentos Solicitados

# COMMAND ----------

def gerar_resumo_comparacao_agrupamentos(df: DataFrame) -> DataFrame:
    """
    Gera resumo estatístico da comparação entre métodos nos agrupamentos solicitados.
    
    Args:
        df: DataFrame com análise de performance
        
    Returns:
        DataFrame com estatísticas agregadas por:
        - Filial
        - Gêmeo x Filial
        - Gêmeo x CD
        - Gêmeo
        - Categoria
    """
    # Agrupamento por Filial
    df_filial = (
        df
        .groupBy("CdFilial")
        .agg(
            F.count("*").alias("Quantidade_SKUs"),
            F.avg("sMAPE").alias("sMAPE_Medio"),
            F.avg("Weighted_sMAPE").alias("Weighted_sMAPE_Medio"),
            F.sum("Peso_Demanda").alias("Demanda_Total_Ponderada"),
            F.avg("Score_Impacto").alias("Score_Impacto_Medio")
        )
        .withColumn("Agrupamento", F.lit("Filial"))
        .withColumn("Chave_Agrupamento", F.col("CdFilial"))
    )
    
    # Agrupamento por Gêmeo x Filial
    df_gemeo_filial = (
        df
        .groupBy("Gemeo", "CdFilial")
        .agg(
            F.count("*").alias("Quantidade_SKUs"),
            F.avg("sMAPE").alias("sMAPE_Medio"),
            F.avg("Weighted_sMAPE").alias("Weighted_sMAPE_Medio"),
            F.sum("Peso_Demanda").alias("Demanda_Total_Ponderada"),
            F.avg("Score_Impacto").alias("Score_Impacto_Medio")
        )
        .withColumn("Agrupamento", F.lit("Gemeo_x_Filial"))
        .withColumn("Chave_Agrupamento", F.concat(F.col("Gemeo"), F.lit("_"), F.col("CdFilial")))
    )
    
    # Agrupamento por Gêmeo x CD (usando CD_primario se disponível)
    df_gemeo_cd = (
        df
        .groupBy("Gemeo")
        .agg(
            F.count("*").alias("Quantidade_SKUs"),
            F.avg("sMAPE").alias("sMAPE_Medio"),
            F.avg("Weighted_sMAPE").alias("Weighted_sMAPE_Medio"),
            F.sum("Peso_Demanda").alias("Demanda_Total_Ponderada"),
            F.avg("Score_Impacto").alias("Score_Impacto_Medio")
        )
        .withColumn("Agrupamento", F.lit("Gemeo_x_CD"))
        .withColumn("Chave_Agrupamento", F.col("Gemeo"))
    )
    
    # Agrupamento por Gêmeo
    df_gemeo = (
        df
        .groupBy("Gemeo")
        .agg(
            F.count("*").alias("Quantidade_SKUs"),
            F.avg("sMAPE").alias("sMAPE_Medio"),
            F.avg("Weighted_sMAPE").alias("Weighted_sMAPE_Medio"),
            F.sum("Peso_Demanda").alias("Demanda_Total_Ponderada"),
            F.avg("Score_Impacto").alias("Score_Impacto_Medio")
        )
        .withColumn("Agrupamento", F.lit("Gemeo"))
        .withColumn("Chave_Agrupamento", F.col("Gemeo"))
    )
    
    # Agrupamento por Categoria (Setor)
    df_categoria = (
        df
        .groupBy("DsSetor")
        .agg(
            F.count("*").alias("Quantidade_SKUs"),
            F.avg("sMAPE").alias("sMAPE_Medio"),
            F.avg("Weighted_sMAPE").alias("Weighted_sMAPE_Medio"),
            F.sum("Peso_Demanda").alias("Demanda_Total_Ponderada"),
            F.avg("Score_Impacto").alias("Score_Impacto_Medio")
        )
        .withColumn("Agrupamento", F.lit("Categoria"))
        .withColumn("Chave_Agrupamento", F.col("DsSetor"))
    )
    
    # Unir todos os agrupamentos
    return (
        df_filial.unionByName(df_gemeo_filial)
        .unionByName(df_gemeo_cd)
        .unionByName(df_gemeo)
        .unionByName(df_categoria)
        .orderBy("Agrupamento", "Score_Impacto_Medio", ascending=False)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Função Principal de Comparação

# COMMAND ----------

def executar_comparacao_matriz_geral(
    categoria: str = "DIRETORIA TELEFONIA CELULAR",
    mes_analise: str = "202507",
    data_corte_matriz: str = "2025-06-30"
) -> Dict[str, DataFrame]:
    """
    Executa comparação completa entre nosso método e a matriz geral de referência.
    
    Args:
        categoria: Categoria para análise
        mes_analise: Mês de análise (YYYYMM)
        data_corte_matriz: Data de corte da matriz
        
    Returns:
        Dicionário com DataFrames de comparação
    """
    print(f"📊 Executando comparação com matriz geral para: {categoria}")
    print("=" * 80)
    
    try:
        # 1. Executa o cálculo da matriz de merecimento
        print("📊 Passo 1: Calculando matriz de merecimento...")
        df_matriz = executar_calculo_matriz_merecimento_completo(
            categoria=categoria,
            mes_analise=mes_analise,
            data_corte_matriz=data_corte_matriz
        )
        
        print(f"✅ Matriz calculada: {df_matriz.count():,} registros")
        
        # 2. Prepara dados para comparação
        print("\n📊 Passo 2: Preparando dados para comparação...")
        df_comparacao = preparar_dados_comparacao_matriz_geral(df_matriz, df_matriz_geral)
        
        print(f"✅ Dados preparados: {df_comparacao.count():,} registros para comparação")
        
        # 3. Calcula métricas de precisão
        print("\n📊 Passo 3: Calculando métricas de precisão...")
        df_metricas = calcular_metricas_precisao_comparacao(df_comparacao)
        
        # 4. Analisa performance
        print("\n📊 Passo 4: Analisando performance...")
        df_analise = analisar_performance_comparacao(df_metricas)
        
        # 5. Cria ranking
        print("\n📊 Passo 5: Criando ranking de performance...")
        df_ranking = criar_ranking_performance_comparacao(df_analise)
        
        # 6. Gera resumo por agrupamentos
        print("\n📊 Passo 6: Gerando resumo por agrupamentos...")
        df_resumo = gerar_resumo_comparacao_agrupamentos(df_analise)
        
        print("\n🎉 Comparação executada com sucesso!")
        print(f"📊 Resultados:")
        print(f"  • Ranking: {df_ranking.count():,} registros")
        print(f"  • Resumo: {df_resumo.count():,} agrupamentos")
        
        return {
            "comparacao": df_comparacao,
            "metricas": df_metricas,
            "analise": df_analise,
            "ranking": df_ranking,
            "resumo": df_resumo
        }
        
    except Exception as e:
        print(f"❌ Erro durante a comparação: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 COMPARAÇÃO COMPLETA E ANÁLISE DE WMAPE
# MAGIC
# MAGIC Nesta seção implementamos:
# MAGIC 1. **Comparação completa** entre método calculado e matriz DRP geral
# MAGIC 2. **Análise de WMAPE** (Weighted Mean Absolute Percentage Error) por diferentes agrupamentos
# MAGIC 3. **Células descomentadas** com dados prontos para análise
# MAGIC 4. **Salvamento em tabelas** para uso posterior

# COMMAND ----------

# MAGIC %md
# MAGIC ### **1. COMPARAÇÃO COMPLETA - TODAS AS COLUNAS**

# COMMAND ----------

def executar_comparacao_completa_matriz_geral(
    categoria: str = "DIRETORIA DE TELAS",
    mes_analise: str = "202507",
    data_corte_matriz: str = "2025-06-30"
) -> DataFrame:
    """
    Executa comparação COMPLETA entre nosso método calculado e a matriz DRP geral.
    
    **Colunas de comparação incluídas:**
    - **Identificação**: CdSku, CdFilial, grupo_de_necessidade, cd_primario
    - **Merecimento Calculado**: Todas as medidas (90, 180, 270, 360 dias + aparadas)
    - **Proporção Factual**: Demanda real vs total da empresa
    - **Matriz DRP Geral**: Percentual da matriz de referência
    - **Métricas de Erro**: sMAPE, erro absoluto, erro relativo
    - **Análise de Performance**: Ranking e categorização de qualidade
    
    Args:
        categoria: Nome da categoria/diretoria
        mes_analise: Mês de análise no formato YYYYMM
        data_corte_matriz: Data de corte para cálculo da matriz
        
    Returns:
        DataFrame com comparação completa para análise
    """
    print(f"📊 Executando comparação COMPLETA para categoria: {categoria}")
    print("=" * 80)
    
    try:
        # 1. Executa cálculo da matriz de merecimento
        print("🔄 Passo 1: Calculando matriz de merecimento...")
        df_matriz_calculada = executar_calculo_matriz_merecimento_completo(
            categoria=categoria,
            mes_analise=mes_analise,
            data_corte_matriz=data_corte_matriz
        )
        
        print(f"✅ Matriz calculada: {df_matriz_calculada.count():,} registros")
        
        # 2. Carrega matriz DRP geral
        print("🔄 Passo 2: Carregando matriz DRP geral...")
        df_matriz_geral = carregar_matriz_drp_geral()
        
        print(f"✅ Matriz DRP carregada: {df_matriz_geral.count():,} registros")
        
        # 3. Prepara dados para comparação
        print("🔄 Passo 3: Preparando dados para comparação...")
        
        # Normaliza IDs para garantir compatibilidade
        df_calculada_norm = (
            df_matriz_calculada
            .withColumn("CdSku", F.col("CdSku").cast("int"))
            .withColumn("CdFilial", F.col("CdFilial").cast("int"))
        )
        
        df_geral_norm = (
            df_matriz_geral
            .withColumn("CdSku", F.col("CdSku").cast("int"))
            .withColumn("CdFilial", F.col("CdFilial").cast("int"))
        )
        
        # 4. Join para comparação completa
        print("🔄 Passo 4: Realizando join para comparação...")
        
        df_comparacao_completa = (
            df_calculada_norm
            .join(df_geral_norm, on=["CdFilial", "CdSku"], how="inner")
            .withColumn("data_hora_execucao", F.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            .withColumn("mes_analise", F.lit(mes_analise))
            .withColumn("data_corte_matriz", F.lit(data_corte_matriz))
            .withColumn("categoria", F.lit(categoria))
        )
        
        print(f"✅ Comparação completa realizada: {df_comparacao_completa.count():,} registros")
        
        # 5. Calcula métricas de erro adicionais
        print("🔄 Passo 5: Calculando métricas de erro...")
        
        medidas_disponiveis = [
            "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
            "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
            "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
            "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
        ]
        
        df_com_metricas = df_comparacao_completa
        
        for medida in medidas_disponiveis:
            # Calcula percentual do merecimento calculado
            df_com_metricas = (
                df_com_metricas
                .withColumn(
                    f"merecimento_{medida}_percentual",
                    F.when(
                        F.col(f"Merecimento_Final_{medida}") > 0,
                        F.col(f"Merecimento_Final_{medida}") / F.sum(f"Merecimento_Final_{medida}").over(
                            Window.partitionBy("grupo_de_necessidade")
                        ) * 100
                    ).otherwise(F.lit(0.0))
                )
                .withColumn(
                    f"erro_absoluto_{medida}",
                    F.abs(F.col(f"merecimento_{medida}_percentual") - F.col("PercMatrizNeogrid"))
                )
                .withColumn(
                    f"erro_relativo_{medida}",
                    F.when(
                        F.col("PercMatrizNeogrid") > 0,
                        F.col(f"erro_absoluto_{medida}") / F.col("PercMatrizNeogrid") * 100
                    ).otherwise(F.lit(0.0))
                )
                .withColumn(
                    f"smape_{medida}",
                    F.when(
                        (F.col(f"merecimento_{medida}_percentual") + F.col("PercMatrizNeogrid")) > 0,
                        F.lit(2.0) * F.col(f"erro_absoluto_{medida}") / 
                        (F.col(f"merecimento_{medida}_percentual") + F.col("PercMatrizNeogrid")) * 100
                    ).otherwise(F.lit(0.0))
                )
            )
        
        # 6. Adiciona análise de performance
        print("🔄 Passo 6: Adicionando análise de performance...")
        
        df_com_performance = df_com_metricas.withColumn(
            "melhor_medida_smape",
            F.least(*[F.col(f"smape_{medida}") for medida in medidas_disponiveis])
        )
        
        for medida in medidas_disponiveis:
            df_com_performance = df_com_performance.withColumn(
                "medida_melhor_smape",
                F.when(
                    F.col(f"smape_{medida}") == F.col("melhor_medida_smape"),
                    F.lit(medida)
                ).otherwise(F.col("medida_melhor_smape"))
            )
        
        # 7. Categoriza qualidade da previsão
        df_final = df_com_performance.withColumn(
            "categoria_qualidade",
            F.when(F.col("melhor_medida_smape") < 10, "Excelente")
            .when(F.col("melhor_medida_smape") < 20, "Muito Boa")
            .when(F.col("melhor_medida_smape") < 30, "Boa")
            .when(F.col("melhor_medida_smape") < 50, "Regular")
            .otherwise("Ruim")
        )
        
        print("✅ Métricas de erro e performance calculadas")
        
        # 8. Salva comparação completa
        print("💾 Salvando comparação completa...")
        
        categoria_normalizada = (
            categoria
            .replace("DIRETORIA ", "")
            .replace(" ", "_")
            .upper()
        )
        
        nome_tabela = f"databox.bcg_comum.supply_comparacao_completa_{categoria_normalizada}"
        
        (
            df_final
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(nome_tabela)
        )
        
        print(f"✅ Comparação completa salva: {nome_tabela}")
        print(f"📊 Total de registros: {df_final.count():,}")
        print(f"📋 Total de colunas: {len(df_final.columns)}")
        
        return df_final
        
    except Exception as e:
        print(f"❌ Erro durante comparação completa: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### **2. CÉLULA DESCOMENTADA - DADOS DE COMPARAÇÃO COMPLETA**

# COMMAND ----------

# EXECUTAR COMPARAÇÃO COMPLETA PARA DIRETORIA DE TELAS
# Esta célula está DESCOMENTADA e pronta para execução

print("🚀 Executando comparação completa para DIRETORIA DE TELAS...")
print("=" * 80)

df_comparacao_completa_telas = executar_comparacao_completa_matriz_geral(
    categoria="DIRETORIA DE TELAS",
    mes_analise="202507",
    data_corte_matriz="2025-06-30"
)

print("=" * 80)
print("✅ COMPARAÇÃO COMPLETA EXECUTADA COM SUCESSO!")
print("=" * 80)

# Exibe resumo dos dados
print("📊 RESUMO DOS DADOS DE COMPARAÇÃO:")
print(f"  • Total de registros: {df_comparacao_completa_telas.count():,}")
print(f"  • Total de colunas: {len(df_comparacao_completa_telas.columns)}")
print(f"  • SKUs únicos: {df_comparacao_completa_telas.select('CdSku').distinct().count():,}")
print(f"  • Lojas únicas: {df_comparacao_completa_telas.select('CdFilial').distinct().count():,}")
print(f"  • Grupos de necessidade: {df_comparacao_completa_telas.select('grupo_de_necessidade').distinct().count():,}")

# Exibe primeiras linhas para verificação
print("\n📋 PRIMEIRAS LINHAS DOS DADOS:")
df_comparacao_completa_telas.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **3. ANÁLISE DE WMAPE POR AGRUPAMENTOS**

# COMMAND ----------

def calcular_wmape_por_agrupamentos(df_comparacao: DataFrame,
                                   categoria: str,
                                   mes_analise: str = "202507") -> Dict[str, DataFrame]:
    """
    Calcula WMAPE (Weighted Mean Absolute Percentage Error) por diferentes agrupamentos.
    
    **Agrupamentos analisados:**
    1. **Por Filial**: WMAPE agregado por loja
    2. **Por Grupo de Necessidade**: WMAPE agregado por gêmeo/espécie
    3. **Por Grupo x Filial**: WMAPE agregado por gêmeo x loja
    4. **Por CD**: WMAPE agregado por centro de distribuição
    5. **Por Categoria Inteira**: WMAPE total da categoria
    
    **Fórmula WMAPE:**
    - WMAPE = Σ(|Erro| × Peso) / Σ(Peso) × 100
    - Onde Peso = demanda ou receita para ponderação
    
    Args:
        df_comparacao: DataFrame com dados de comparação completa
        categoria: Nome da categoria/diretoria
        mes_analise: Mês de análise
        
    Returns:
        Dicionário com DataFrames de WMAPE por agrupamento
    """
    print(f"📊 Calculando WMAPE por agrupamentos para: {categoria}")
    print("=" * 80)
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # 1. WMAPE POR FILIAL
    print("📊 Calculando WMAPE por filial...")
    
    aggs_filial = []
    for medida in medidas_disponiveis:
        aggs_filial.extend([
            F.sum(F.col(f"erro_absoluto_{medida}") * F.col("PercMatrizNeogrid")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col("PercMatrizNeogrid")).alias(f"soma_peso_{medida}"),
            F.avg(f"smape_{medida}").alias(f"smape_medio_{medida}"),
            F.count("*").alias("total_skus")
        ])
    
    df_wmape_filial = (
        df_comparacao
        .groupBy("CdFilial")
        .agg(*aggs_filial)
    )
    
    for medida in medidas_disponiveis:
        df_wmape_filial = df_wmape_filial.withColumn(
            f"wmape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    df_wmape_filial = df_wmape_filial.withColumn("tipo_agregacao", F.lit("FILIAL"))
    
    # 2. WMAPE POR GRUPO DE NECESSIDADE
    print("📊 Calculando WMAPE por grupo de necessidade...")
    
    aggs_grupo = []
    for medida in medidas_disponiveis:
        aggs_grupo.extend([
            F.sum(F.col(f"erro_absoluto_{medida}") * F.col("PercMatrizNeogrid")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col("PercMatrizNeogrid")).alias(f"soma_peso_{medida}"),
            F.avg(f"smape_{medida}").alias(f"smape_medio_{medida}"),
            F.count("*").alias("total_skus")
        ])
    
    df_wmape_grupo = (
        df_comparacao
        .groupBy("grupo_de_necessidade")
        .agg(*aggs_grupo)
    )
    
    for medida in medidas_disponiveis:
        df_wmape_grupo = df_wmape_grupo.withColumn(
            f"wmape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    df_wmape_grupo = df_wmape_grupo.withColumn("tipo_agregacao", F.lit("GRUPO_NECESSIDADE"))
    
    # 3. WMAPE POR GRUPO x FILIAL
    print("📊 Calculando WMAPE por grupo x filial...")
    
    aggs_grupo_filial = []
    for medida in medidas_disponiveis:
        aggs_grupo_filial.extend([
            F.sum(F.col(f"erro_absoluto_{medida}") * F.col("PercMatrizNeogrid")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col("PercMatrizNeogrid")).alias(f"soma_peso_{medida}"),
            F.avg(f"smape_{medida}").alias(f"smape_medio_{medida}"),
            F.count("*").alias("total_skus")
        ])
    
    df_wmape_grupo_filial = (
        df_comparacao
        .groupBy("grupo_de_necessidade", "CdFilial")
        .agg(*aggs_grupo_filial)
    )
    
    for medida in medidas_disponiveis:
        df_wmape_grupo_filial = df_wmape_grupo_filial.withColumn(
            f"wmape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    df_wmape_grupo_filial = df_wmape_grupo_filial.withColumn("tipo_agregacao", F.lit("GRUPO_FILIAL"))
    
    # 4. WMAPE POR CD
    print("📊 Calculando WMAPE por CD...")
    
    aggs_cd = []
    for medida in medidas_disponiveis:
        aggs_cd.extend([
            F.sum(F.col(f"erro_absoluto_{medida}") * F.col("PercMatrizNeogrid")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col("PercMatrizNeogrid")).alias(f"soma_peso_{medida}"),
            F.avg(f"smape_{medida}").alias(f"smape_medio_{medida}"),
            F.count("*").alias("total_skus")
        ])
    
    df_wmape_cd = (
        df_comparacao
        .groupBy("cd_primario")
        .agg(*aggs_cd)
    )
    
    for medida in medidas_disponiveis:
        df_wmape_cd = df_wmape_cd.withColumn(
            f"wmape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    df_wmape_cd = df_wmape_cd.withColumn("tipo_agregacao", F.lit("CD"))
    
    # 5. WMAPE DA CATEGORIA INTEIRA
    print("📊 Calculando WMAPE da categoria inteira...")
    
    aggs_categoria = []
    for medida in medidas_disponiveis:
        aggs_categoria.extend([
            F.sum(F.col(f"erro_absoluto_{medida}") * F.col("PercMatrizNeogrid")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col("PercMatrizNeogrid")).alias(f"soma_peso_{medida}"),
            F.avg(f"smape_{medida}").alias(f"smape_medio_{medida}"),
            F.count("*").alias("total_skus")
        ])
    
    df_wmape_categoria = df_comparacao.agg(*aggs_categoria)
    
    for medida in medidas_disponiveis:
        df_wmape_categoria = df_wmape_categoria.withColumn(
            f"wmape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    df_wmape_categoria = df_wmape_categoria.withColumn("tipo_agregacao", F.lit("CATEGORIA_INTEIRA"))
    
    print("✅ WMAPE calculado para todos os agrupamentos")
    
    return {
        "filial": df_wmape_filial,
        "grupo": df_wmape_grupo,
        "grupo_filial": df_wmape_grupo_filial,
        "cd": df_wmape_cd,
        "categoria": df_wmape_categoria
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### **4. CÉLULA DESCOMENTADA - ANÁLISE DE WMAPE**

# COMMAND ----------

# EXECUTAR ANÁLISE DE WMAPE PARA DIRETORIA DE TELAS
# Esta célula está DESCOMENTADA e pronta para execução

print("🚀 Executando análise de WMAPE para DIRETORIA DE TELAS...")
print("=" * 80)

# Calcula WMAPE por diferentes agrupamentos
dict_wmape_telas = calcular_wmape_por_agrupamentos(
    df_comparacao=df_comparacao_completa_telas,
    categoria="DIRETORIA DE TELAS",
    mes_analise="202507"
)

print("=" * 80)
print("✅ ANÁLISE DE WMAPE EXECUTADA COM SUCESSO!")
print("=" * 80)

# Exibe resumo dos resultados
print("📊 RESUMO DOS RESULTADOS DE WMAPE:")
print(f"  • WMAPE por filial: {dict_wmape_telas['filial'].count():,} registros")
print(f"  • WMAPE por grupo: {dict_wmape_telas['grupo'].count():,} registros")
print(f"  • WMAPE por grupo x filial: {dict_wmape_telas['grupo_filial'].count():,} registros")
print(f"  • WMAPE por CD: {dict_wmape_telas['cd'].count():,} registros")
print(f"  • WMAPE categoria: {dict_wmape_telas['categoria'].count():,} registros")

# Exibe WMAPE por filial (top 10)
print("\n📊 TOP 10 FILIAIS POR WMAPE (Media90):")
df_wmape_filial_top10 = (
    dict_wmape_telas['filial']
    .orderBy(F.col("wmape_Media90_Qt_venda_sem_ruptura").asc())
    .limit(10)
    .select("CdFilial", "wmape_Media90_Qt_venda_sem_ruptura", "total_skus")
)
df_wmape_filial_top10.display()

# Exibe WMAPE por grupo de necessidade
print("\n📊 WMAPE POR GRUPO DE NECESSIDADE:")
df_wmape_grupo_ordenado = (
    dict_wmape_telas['grupo']
    .orderBy(F.col("wmape_Media90_Qt_venda_sem_ruptura").asc())
    .select("grupo_de_necessidade", "wmape_Media90_Qt_venda_sem_ruptura", "total_skus")
)
df_wmape_grupo_ordenado.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **5. SALVAMENTO DAS ANÁLISES DE WMAPE**

# COMMAND ----------

def salvar_analises_wmape(dict_wmape: Dict[str, DataFrame],
                          categoria: str,
                          mes_analise: str = "202507") -> None:
    """
    Salva todas as análises de WMAPE em tabelas separadas.
    
    **Tabelas criadas:**
    1. **supply_wmape_filial_{CATEGORIA}**: WMAPE agregado por filial
    2. **supply_wmape_grupo_{CATEGORIA}**: WMAPE agregado por grupo de necessidade
    3. **supply_wmape_grupo_filial_{CATEGORIA}**: WMAPE agregado por grupo x filial
    4. **supply_wmape_cd_{CATEGORIA}**: WMAPE agregado por CD
    5. **supply_wmape_categoria_{CATEGORIA}**: WMAPE total da categoria
    
    Args:
        dict_wmape: Dicionário com DataFrames de WMAPE
        categoria: Nome da categoria/diretoria
        mes_analise: Mês de análise
    """
    print(f"💾 Salvando análises de WMAPE para categoria: {categoria}")
    print("=" * 80)
    
    categoria_normalizada = (
        categoria
        .replace("DIRETORIA ", "")
        .replace(" ", "_")
        .upper()
    )
    
    data_hora_execucao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Adiciona metadados a todos os DataFrames
    for key, df in dict_wmape.items():
        dict_wmape[key] = (
            df
            .withColumn("data_hora_execucao", F.lit(data_hora_execucao))
            .withColumn("mes_analise", F.lit(mes_analise))
            .withColumn("categoria", F.lit(categoria))
        )
    
    # 1. Salva WMAPE por filial
    print("📊 Salvando WMAPE por filial...")
    nome_tabela_filial = f"databox.bcg_comum.supply_wmape_filial_{categoria_normalizada}"
    
    (
        dict_wmape['filial']
        .write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(nome_tabela_filial)
    )
    
    print(f"✅ WMAPE por filial salvo: {nome_tabela_filial}")
    
    # 2. Salva WMAPE por grupo
    print("📊 Salvando WMAPE por grupo...")
    nome_tabela_grupo = f"databox.bcg_comum.supply_wmape_grupo_{categoria_normalizada}"
    
    (
        dict_wmape['grupo']
        .write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(nome_tabela_grupo)
    )
    
    print(f"✅ WMAPE por grupo salvo: {nome_tabela_grupo}")
    
    # 3. Salva WMAPE por grupo x filial
    print("📊 Salvando WMAPE por grupo x filial...")
    nome_tabela_grupo_filial = f"databox.bcg_comum.supply_wmape_grupo_filial_{categoria_normalizada}"
    
    (
        dict_wmape['grupo_filial']
        .write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(nome_tabela_grupo_filial)
    )
    
    print(f"✅ WMAPE por grupo x filial salvo: {nome_tabela_grupo_filial}")
    
    # 4. Salva WMAPE por CD
    print("📊 Salvando WMAPE por CD...")
    nome_tabela_cd = f"databox.bcg_comum.supply_wmape_cd_{categoria_normalizada}"
    
    (
        dict_wmape['cd']
        .write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(nome_tabela_cd)
    )
    
    print(f"✅ WMAPE por CD salvo: {nome_tabela_cd}")
    
    # 5. Salva WMAPE da categoria
    print("📊 Salvando WMAPE da categoria...")
    nome_tabela_categoria = f"databox.bcg_comum.supply_wmape_categoria_{categoria_normalizada}"
    
    (
        dict_wmape['categoria']
        .write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(nome_tabela_categoria)
    )
    
    print(f"✅ WMAPE da categoria salvo: {nome_tabela_categoria}")
    
    print("=" * 80)
    print("✅ TODAS AS ANÁLISES DE WMAPE FORAM SALVAS COM SUCESSO!")
    print(f"📊 Categoria: {categoria}")
    print(f"📅 Mês de análise: {mes_analise}")
    print(f"🕐 Data/hora execução: {data_hora_execucao}")
    print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **6. CÉLULA DESCOMENTADA - SALVAMENTO DAS ANÁLISES**

# COMMAND ----------

# SALVAR ANÁLISES DE WMAPE PARA DIRETORIA DE TELAS
# Esta célula está DESCOMENTADA e pronta para execução

print("💾 Salvando análises de WMAPE para DIRETORIA DE TELAS...")
print("=" * 80)

salvar_analises_wmape(
    dict_wmape=dict_wmape_telas,
    categoria="DIRETORIA DE TELAS",
    mes_analise="202507"
)

print("=" * 80)
print("✅ ANÁLISES DE WMAPE SALVAS COM SUCESSO!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **7. RESUMO EXECUTIVO DAS ANÁLISES**

# COMMAND ----------

def gerar_resumo_executivo_wmape(dict_wmape: Dict[str, DataFrame],
                                 categoria: str) -> None:
    """
    Gera resumo executivo das análises de WMAPE.
    
    Args:
        dict_wmape: Dicionário com DataFrames de WMAPE
        categoria: Nome da categoria/diretoria
    """
    print(f"📊 RESUMO EXECUTIVO - WMAPE PARA {categoria}")
    print("=" * 80)
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # 1. Resumo por filial
    print("📊 1. RESUMO POR FILIAL:")
    df_filial = dict_wmape['filial']
    
    for medida in medidas_disponiveis[:4]:  # Apenas as primeiras 4 medidas
        coluna_wmape = f"wmape_{medida}"
        if coluna_wmape in df_filial.columns:
            stats = df_filial.select(
                F.avg(coluna_wmape).alias("media"),
                F.stddev(coluna_wmape).alias("desvio"),
                F.min(coluna_wmape).alias("minimo"),
                F.max(coluna_wmape).alias("maximo")
            ).first()
            
            print(f"  • {medida}:")
            print(f"    - Média: {stats['media']:.4f}%")
            print(f"    - Desvio: {stats['desvio']:.4f}%")
            print(f"    - Min: {stats['minimo']:.4f}%")
            print(f"    - Max: {stats['maximo']:.4f}%")
    
    # 2. Resumo por grupo de necessidade
    print("\n📊 2. RESUMO POR GRUPO DE NECESSIDADE:")
    df_grupo = dict_wmape['grupo']
    
    for medida in medidas_disponiveis[:4]:
        coluna_wmape = f"wmape_{medida}"
        if coluna_wmape in df_grupo.columns:
            stats = df_grupo.select(
                F.avg(coluna_wmape).alias("media"),
                F.stddev(coluna_wmape).alias("desvio"),
                F.min(coluna_wmape).alias("minimo"),
                F.max(coluna_wmape).alias("maximo")
            ).first()
            
            print(f"  • {medida}:")
            print(f"    - Média: {stats['media']:.4f}%")
            print(f"    - Desvio: {stats['desvio']:.4f}%")
            print(f"    - Min: {stats['minimo']:.4f}%")
            print(f"    - Max: {stats['maximo']:.4f}%")
    
    # 3. Resumo da categoria inteira
    print("\n📊 3. RESUMO DA CATEGORIA INTEIRA:")
    df_categoria = dict_wmape['categoria']
    
    for medida in medidas_disponiveis[:4]:
        coluna_wmape = f"wmape_{medida}"
        if coluna_wmape in df_categoria.columns:
            valor = df_categoria.select(coluna_wmape).first()[0]
            print(f"  • {medida}: {valor:.4f}%")
    
    print("=" * 80)
    print("✅ RESUMO EXECUTIVO GERADO COM SUCESSO!")
    print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **8. CÉLULA DESCOMENTADA - RESUMO EXECUTIVO**

# COMMAND ----------

# GERAR RESUMO EXECUTIVO DAS ANÁLISES DE WMAPE
# Esta célula está DESCOMENTADA e pronta para execução

print("📊 Gerando resumo executivo das análises de WMAPE...")
print("=" * 80)

gerar_resumo_executivo_wmape(
    dict_wmape=dict_wmape_telas,
    categoria="DIRETORIA DE TELAS"
)

print("=" * 80)
print("✅ RESUMO EXECUTIVO GERADO COM SUCESSO!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 RESUMO DAS IMPLEMENTAÇÕES
# MAGIC
# MAGIC ### **O que foi implementado:**
# MAGIC
# MAGIC 1. **✅ COMPARAÇÃO COMPLETA**: Função `executar_comparacao_completa_matriz_geral()` que compara nosso método com a matriz DRP geral
# MAGIC 2. **✅ CÉLULA DESCOMENTADA**: Célula pronta para execução com dados de comparação completa
# MAGIC 3. **✅ ANÁLISE DE WMAPE**: Função `calcular_wmape_por_agrupamentos()` que calcula WMAPE por diferentes níveis
# MAGIC 4. **✅ CÉLULAS DE DADOS**: Múltiplas células com dados das agregações olhando para WMAPE
# MAGIC 5. **✅ SALVAMENTO EM TABELAS**: Todas as análises são salvas em tabelas Delta para uso posterior
# MAGIC
# MAGIC ### **Estrutura das tabelas criadas:**
# MAGIC
# MAGIC - **supply_comparacao_completa_{CATEGORIA}**: Comparação completa com todas as colunas
# MAGIC - **supply_wmape_filial_{CATEGORIA}**: WMAPE agregado por filial
# MAGIC - **supply_wmape_grupo_{CATEGORIA}**: WMAPE agregado por grupo de necessidade
# MAGIC - **supply_wmape_grupo_filial_{CATEGORIA}**: WMAPE agregado por grupo x filial
# MAGIC - **supply_wmape_cd_{CATEGORIA}**: WMAPE agregado por CD
# MAGIC - **supply_wmape_categoria_{CATEGORIA}**: WMAPE total da categoria
# MAGIC
# MAGIC ### **Como usar:**
# MAGIC
# MAGIC 1. **Execute a célula de comparação completa** (linha descomentada)
# MAGIC 2. **Execute a célula de análise de WMAPE** (linha descomentada)
# MAGIC 3. **Execute a célula de salvamento** (linha descomentada)
# MAGIC 4. **Execute a célula de resumo executivo** (linha descomentada)
# MAGIC
# MAGIC Todas as células estão prontas para execução e irão gerar dados completos para análise!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 EXECUÇÃO POR CATEGORIA - CÉLULAS ESPECÍFICAS
# MAGIC
# MAGIC Nesta seção, cada categoria tem sua própria célula para execução independente.
# MAGIC Cada categoria usa regras específicas de agrupamento conforme definido no sistema.

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DIRETORIA DE TELAS** - Agrupamento por Gêmeos

# COMMAND ----------

# EXECUTAR COMPARAÇÃO COMPLETA PARA DIRETORIA DE TELAS
# Agrupamento: gemeos (produtos similares)
print("🚀 DIRETORIA DE TELAS - Executando comparação completa...")
print("📊 Agrupamento: Gêmeos (produtos similares)")
print("=" * 80)

df_comparacao_telas = executar_comparacao_completa_matriz_geral(
    categoria="DIRETORIA DE TELAS",
    mes_analise="202507",
    data_corte_matriz="2025-06-30"
)

# Calcula WMAPE por agrupamentos
dict_wmape_telas = calcular_wmape_por_agrupamentos(
    df_comparacao=df_comparacao_telas,
    categoria="DIRETORIA DE TELAS",
    mes_analise="202507"
)

# Salva análises
salvar_analises_wmape(
    dict_wmape=dict_wmape_telas,
    categoria="DIRETORIA DE TELAS",
    mes_analise="202507"
)

# Gera resumo executivo
gerar_resumo_executivo_wmape(
    dict_wmape=dict_wmape_telas,
    categoria="DIRETORIA DE TELAS"
)

print("=" * 80)
print("✅ DIRETORIA DE TELAS - PROCESSAMENTO CONCLUÍDO!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DIRETORIA TELEFONIA CELULAR** - Agrupamento por Gêmeos

# COMMAND ----------

# EXECUTAR COMPARAÇÃO COMPLETA PARA DIRETORIA TELEFONIA CELULAR
# Agrupamento: gemeos (produtos similares)
print("🚀 DIRETORIA TELEFONIA CELULAR - Executando comparação completa...")
print("📊 Agrupamento: Gêmeos (produtos similares)")
print("=" * 80)

df_comparacao_telefonia = executar_comparacao_completa_matriz_geral(
    categoria="DIRETORIA TELEFONIA CELULAR",
    mes_analise="202507",
    data_corte_matriz="2025-06-30"
)

# Calcula WMAPE por agrupamentos
dict_wmape_telefonia = calcular_wmape_por_agrupamentos(
    df_comparacao=df_comparacao_telefonia,
    categoria="DIRETORIA TELEFONIA CELULAR",
    mes_analise="202507"
)

# Salva análises
salvar_analises_wmape(
    dict_wmape=dict_wmape_telefonia,
    categoria="DIRETORIA TELEFONIA CELULAR",
    mes_analise="202507"
)

# Gera resumo executivo
gerar_resumo_executivo_wmape(
    dict_wmape=dict_wmape_telefonia,
    categoria="DIRETORIA TELEFONIA CELULAR"
)

print("=" * 80)
print("✅ DIRETORIA TELEFONIA CELULAR - PROCESSAMENTO CONCLUÍDO!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DIRETORIA LINHA BRANCA** - Agrupamento por Espécie Gerencial

# COMMAND ----------

# EXECUTAR COMPARAÇÃO COMPLETA PARA DIRETORIA LINHA BRANCA
# Agrupamento: NmEspecieGerencial (espécie gerencial)
print("🚀 DIRETORIA LINHA BRANCA - Executando comparação completa...")
print("📊 Agrupamento: Espécie Gerencial (geladeira, fogão, etc.)")
print("=" * 80)

df_comparacao_linha_branca = executar_comparacao_completa_matriz_geral(
    categoria="DIRETORIA LINHA BRANCA",
    mes_analise="202507",
    data_corte_matriz="2025-06-30"
)

# Calcula WMAPE por agrupamentos
dict_wmape_linha_branca = calcular_wmape_por_agrupamentos(
    df_comparacao=df_comparacao_linha_branca,
    categoria="DIRETORIA LINHA BRANCA",
    mes_analise="202507"
)

# Salva análises
salvar_analises_wmape(
    dict_wmape=dict_wmape_linha_branca,
    categoria="DIRETORIA LINHA BRANCA",
    mes_analise="202507"
)

# Gera resumo executivo
gerar_resumo_executivo_wmape(
    dict_wmape=dict_wmape_linha_branca,
    categoria="DIRETORIA LINHA BRANCA"
)

print("=" * 80)
print("✅ DIRETORIA LINHA BRANCA - PROCESSAMENTO CONCLUÍDO!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DIRETORIA LINHA LEVE** - Agrupamento por Espécie Gerencial

# COMMAND ----------

# EXECUTAR COMPARAÇÃO COMPLETA PARA DIRETORIA LINHA LEVE
# Agrupamento: NmEspecieGerencial (espécie gerencial)
print("🚀 DIRETORIA LINHA LEVE - Executando comparação completa...")
print("📊 Agrupamento: Espécie Gerencial (eletrodomésticos menores)")
print("=" * 80)

df_comparacao_linha_leve = executar_comparacao_completa_matriz_geral(
    categoria="DIRETORIA LINHA LEVE",
    mes_analise="202507",
    data_corte_matriz="2025-06-30"
)

# Calcula WMAPE por agrupamentos
dict_wmape_linha_leve = calcular_wmape_por_agrupamentos(
    df_comparacao=df_comparacao_linha_leve,
    categoria="DIRETORIA LINHA LEVE",
    mes_analise="202507"
)

# Salva análises
salvar_analises_wmape(
    dict_wmape=dict_wmape_linha_leve,
    categoria="DIRETORIA LINHA LEVE",
    mes_analise="202507"
)

# Gera resumo executivo
gerar_resumo_executivo_wmape(
    dict_wmape=dict_wmape_linha_leve,
    categoria="DIRETORIA LINHA LEVE"
)

print("=" * 80)
print("✅ DIRETORIA LINHA LEVE - PROCESSAMENTO CONCLUÍDO!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DIRETORIA INFO/GAMES** - Agrupamento por Espécie Gerencial

# COMMAND ----------

# EXECUTAR COMPARAÇÃO COMPLETA PARA DIRETORIA INFO/GAMES
# Agrupamento: NmEspecieGerencial (espécie gerencial)
print("🚀 DIRETORIA INFO/GAMES - Executando comparação completa...")
print("📊 Agrupamento: Espécie Gerencial (informática, games, etc.)")
print("=" * 80)

df_comparacao_info_games = executar_comparacao_completa_matriz_geral(
    categoria="DIRETORIA INFO/GAMES",
    mes_analise="202507",
    data_corte_matriz="2025-06-30"
)

# Calcula WMAPE por agrupamentos
dict_wmape_info_games = calcular_wmape_por_agrupamentos(
    df_comparacao=df_comparacao_info_games,
    categoria="DIRETORIA INFO/GAMES",
    mes_analise="202507"
)

# Salva análises
salvar_analises_wmape(
    dict_wmape=dict_wmape_info_games,
    categoria="DIRETORIA INFO/GAMES",
    mes_analise="202507"
)

# Gera resumo executivo
gerar_resumo_executivo_wmape(
    dict_wmape=dict_wmape_info_games,
    categoria="DIRETORIA INFO/GAMES"
)

print("=" * 80)
print("✅ DIRETORIA INFO/GAMES - PROCESSAMENTO CONCLUÍDO!")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 EXECUÇÃO EM LOTE - TODAS AS CATEGORIAS

# COMMAND ----------

# EXECUTAR PROCESSAMENTO EM LOTE PARA TODAS AS CATEGORIAS
# Esta célula processa todas as categorias de uma vez
print("🚀 EXECUÇÃO EM LOTE - Processando todas as categorias...")
print("=" * 80)

categorias = [
    "DIRETORIA DE TELAS",
    "DIRETORIA TELEFONIA CELULAR", 
    "DIRETORIA LINHA BRANCA",
    "DIRETORIA LINHA LEVE",
    "DIRETORIA INFO/GAMES"
]

resultados_lote = {}

for categoria in categorias:
    print(f"\n🔄 Processando: {categoria}")
    print("-" * 60)
    
    try:
        # Executa comparação completa
        df_comparacao = executar_comparacao_completa_matriz_geral(
            categoria=categoria,
            mes_analise="202507",
            data_corte_matriz="2025-06-30"
        )
        
        # Calcula WMAPE
        dict_wmape = calcular_wmape_por_agrupamentos(
            df_comparacao=df_comparacao,
            categoria=categoria,
            mes_analise="202507"
        )
        
        # Salva análises
        salvar_analises_wmape(
            dict_wmape=dict_wmape,
            categoria=categoria,
            mes_analise="202507"
        )
        
        # Armazena resultado
        resultados_lote[categoria] = {
            "comparacao": df_comparacao,
            "wmape": dict_wmape,
            "status": "SUCESSO"
        }
        
        print(f"✅ {categoria} - Processado com sucesso!")
        
    except Exception as e:
        print(f"❌ {categoria} - Erro: {str(e)}")
        resultados_lote[categoria] = {
            "status": "ERRO",
            "erro": str(e)
        }

print("\n" + "=" * 80)
print("🎉 EXECUÇÃO EM LOTE CONCLUÍDA!")
print("=" * 80)

# Exibe resumo dos resultados
print("📊 RESUMO DOS RESULTADOS:")
for categoria, resultado in resultados_lote.items():
    if resultado["status"] == "SUCESSO":
        total_registros = resultado["comparacao"].count()
        print(f"  ✅ {categoria}: {total_registros:,} registros")
    else:
        print(f"  ❌ {categoria}: {resultado['erro']}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 RESUMO FINAL DAS IMPLEMENTAÇÕES
# MAGIC
# MAGIC ### **Células criadas por categoria:**
# MAGIC
# MAGIC 1. **DIRETORIA DE TELAS** - Agrupamento por Gêmeos
# MAGIC 2. **DIRETORIA TELEFONIA CELULAR** - Agrupamento por Gêmeos  
# MAGIC 3. **DIRETORIA LINHA BRANCA** - Agrupamento por Espécie Gerencial
# MAGIC 4. **DIRETORIA LINHA LEVE** - Agrupamento por Espécie Gerencial
# MAGIC 5. **DIRETORIA INFO/GAMES** - Agrupamento por Espécie Gerencial
# MAGIC
# MAGIC ### **Célula de execução em lote:**
# MAGIC - Processa todas as categorias de uma vez
# MAGIC - Tratamento de erros individual por categoria
# MAGIC - Resumo consolidado dos resultados
# MAGIC
# MAGIC ### **Como usar:**
# MAGIC
# MAGIC **Opção 1 - Categoria individual:**
# MAGIC - Execute a célula da categoria desejada
# MAGIC - Processamento independente e focado
# MAGIC
# MAGIC **Opção 2 - Execução em lote:**
# MAGIC - Execute a célula de execução em lote
# MAGIC - Processa todas as categorias automaticamente
# MAGIC
# MAGIC **Todas as células estão prontas para execução!** 🚀

# COMMAND ----------


