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
# MAGIC   como proporção do total do grupo de necessidade
# MAGIC - **Base de Cálculo**: Usa as medidas calculadas (médias e médias aparadas) 
# MAGIC   para representar a demanda histórica real
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
    
    # Verifica se a coluna existe no DataFrame
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
    
    # Cache para otimização
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
    
    # Adiciona de-para filial → CD
    de_para_filial_cd = criar_de_para_filial_cd()
    df_com_cd = df_data_calculo.join(de_para_filial_cd, on="cdfilial", how="left")
    
    # Agrega por CD e grupo de necessidade
    colunas_agregacao = ["cd_primario", "grupo_de_necessidade"] + medidas_disponiveis
    
    # Cria lista de expressões de agregação
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
    
    # Filtra dados para a data específica
    df_data_calculo = df.filter(F.col("DtAtual") == data_calculo)
    
    # Adiciona de-para filial → CD
    de_para_filial_cd = criar_de_para_filial_cd()
    df_com_cd = df_data_calculo.join(de_para_filial_cd, on="cdfilial", how="left")
    
    # Mesmas medidas disponíveis
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # PRIMEIRO: Calcula os totais por CD + grupo de necessidade para cada filial
    df_com_totais = df_com_cd
    
    for medida in medidas_disponiveis:
        # Janela para calcular total por CD + grupo de necessidade
        w_total = Window.partitionBy("cd_primario", "grupo_de_necessidade")
        
        df_com_totais = df_com_totais.withColumn(
            f"Total_{medida}",
            F.sum(F.col(medida)).over(w_total)
        )
    
    # SEGUNDO: Calcula percentual por filial dentro de cada CD + grupo de necessidade
    df_merecimento_interno = df_com_totais
    
    for medida in medidas_disponiveis:
        # Janela para calcular percentual por CD + grupo de necessidade
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
    
    # CORREÇÃO: Renomeia colunas do df_merecimento_cd para evitar conflito de nomes
    colunas_renomeadas = ["cd_primario", "grupo_de_necessidade"]
    for medida in medidas_disponiveis:
        colunas_renomeadas.append(F.col(f"Total_{medida}").alias(f"Total_CD_{medida}"))
    
    df_merecimento_cd_renomeado = df_merecimento_cd.select(*colunas_renomeadas)
    
    # Join entre merecimento interno CD e CD renomeado (sem conflito de colunas)
    df_merecimento_final = (
        df_merecimento_interno
        .join(
            df_merecimento_cd_renomeado,
            on=["cd_primario", "grupo_de_necessidade"],
            how="left"
        )
    )
    
    # Calcula merecimento final para cada medida
    for medida in medidas_disponiveis:
        df_merecimento_final = df_merecimento_final.withColumn(
            f"Merecimento_Final_{medida}",
            F.col(f"Total_CD_{medida}") * F.col(f"Percentual_{medida}")  # Usa colunas renomeadas
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
    
    # 1. CARREGA DADOS DE DEMANDA CALCULADA (proporção factual) para o mês de análise
    print("📊 Carregando dados de demanda calculada robusta a ruptura para cálculo de proporção factual...")
    
    # Carrega dados com medidas calculadas (incluindo Media90_Qt_venda_sem_ruptura)
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
    
    # 2. CALCULA PROPORÇÃO FACTUAL (demanda calculada robusta a ruptura com média90)
    print("📈 Calculando proporção factual baseada em demanda calculada robusta a ruptura...")
    
    # Janela para calcular totais por grupo de necessidade no mês
    w_grupo_mes = Window.partitionBy("grupo_de_necessidade")
    
    df_proporcao_factual = (
        df_dados_demanda
        .withColumn(
            "total_demanda_grupo_mes",
            F.sum("Media90_Qt_venda_sem_ruptura").over(w_grupo_mes)
        )
        .withColumn(
            "proporcao_factual",
            F.when(
                F.col("total_demanda_grupo_mes") > 0,
                F.col("Media90_Qt_venda_sem_ruptura") / F.col("total_demanda_grupo_mes")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "proporcao_factual_percentual",
            F.round(F.col("proporcao_factual") * 100, 4)
        )
        .select(
            "cdfilial", "grupo_de_necessidade", "CdSku",
            "Media90_Qt_venda_sem_ruptura", "proporcao_factual", "proporcao_factual_percentual"
        )
    )
    
    print(f"✅ Proporção factual calculada:")
    print(f"  • Total de registros: {df_proporcao_factual.count():,}")
    print(f"  • Grupos de necessidade: {df_proporcao_factual.select('grupo_de_necessidade').distinct().count():,}")
    print(f"  • Filiais: {df_proporcao_factual.select('cdfilial').distinct().count():,}")
    
    # 3. JOIN entre merecimento calculado e proporção factual
    print("🔗 Realizando join entre merecimento calculado e proporção factual...")
    
    # Seleciona apenas uma medida para comparação (média 90 dias como padrão)
    medida_comparacao = "Merecimento_Final_Media90_Qt_venda_sem_ruptura"
    
    df_comparacao = (
        df_merecimento
        .select(
            "cdfilial", "cd_primario", "grupo_de_necessidade",
            F.col(medida_comparacao).alias("merecimento_calculado")
        )
        .join(
            df_proporcao_factual,
            on=["cdfilial", "grupo_de_necessidade"],
            how="inner"
        )
        .withColumn(
            "merecimento_calculado_percentual",
            F.when(
                F.col("merecimento_calculado") > 0,
                F.col("merecimento_calculado") / F.sum("merecimento_calculado").over(
                    Window.partitionBy("grupo_de_necessidade")
                ) * 100
            ).otherwise(F.lit(0.0))
        )
        .select(
            "cdfilial", "cd_primario", "grupo_de_necessidade", "CdSku",
            "Media90_Qt_venda_sem_ruptura", "merecimento_calculado", "merecimento_calculado_percentual",
            "proporcao_factual", "proporcao_factual_percentual"
        )
    )
    
    print(f"✅ Join realizado:")
    print(f"  • Registros comparáveis: {df_comparacao.count():,}")
    
    # 4. CALCULA MÉTRICAS DE ERRO (sMAPE como principal)
    print("📊 Calculando métricas de erro (sMAPE)...")
    
    # Parâmetros para sMAPE
    EPSILON = 1e-12
    
    df_com_metricas = (
        df_comparacao
        .withColumn(
            "erro_absoluto",
            F.abs(F.col("merecimento_calculado_percentual") - F.col("proporcao_factual_percentual"))
        )
        .withColumn(
            "smape_numerador",
            F.lit(2.0) * F.col("erro_absoluto")
        )
        .withColumn(
            "smape_denominador",
            F.col("merecimento_calculado_percentual") + F.col("proporcao_factual_percentual") + F.lit(EPSILON)
        )
        .withColumn(
            "smape_individual",
            F.when(
                F.col("smape_denominador") > 0,
                F.col("smape_numerador") / F.col("smape_denominador") * 100
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "erro_quadratico",
            F.pow(F.col("merecimento_calculado_percentual") - F.col("proporcao_factual_percentual"), 2)
        )
    )
    
    # 5. AGREGAÇÃO das métricas conforme solicitado
    print(f"📈 Agregando métricas por: {colunas_agregacao}")
    
    # Janela para agregação
    w_agregacao = Window.partitionBy(*colunas_agregacao) if colunas_agregacao else Window.partitionBy(F.lit(1))
    
    df_metricas_agregadas = (
        df_com_metricas
        .withColumn(
            "total_demanda_agregado",
            F.sum("Media90_Qt_venda_sem_ruptura").over(w_agregacao)
        )
        .withColumn(
            "smape_agregado",
            F.when(
                F.col("total_demanda_agregado") > 0,
                F.sum(F.col("smape_individual") * F.col("Media90_Qt_venda_sem_ruptura")).over(w_agregacao) / F.col("total_demanda_agregado")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "rmse_agregado",
            F.sqrt(
                F.sum(F.col("erro_quadratico") * F.col("Media90_Qt_venda_sem_ruptura")).over(w_agregacao) / F.col("total_demanda_agregado")
            )
        )
        .withColumn(
            "erro_medio_absoluto",
            F.sum(F.col("erro_absoluto") * F.col("Media90_Qt_venda_sem_ruptura")).over(w_agregacao) / F.col("total_demanda_agregado")
        )
        .groupBy(*colunas_agregacao)
        .agg(
            F.first("total_demanda_agregado").alias("total_demanda"),
            F.first("smape_agregado").alias("sMAPE"),
            F.first("rmse_agregado").alias("RMSE"),
            F.first("erro_medio_absoluto").alias("MAE"),
            F.countDistinct("CdSku").alias("total_skus"),
            F.countDistinct("grupo_de_necessidade").alias("total_grupos_necessidade")
        )
        .orderBy(*colunas_agregacao)
    )
    
    print(f"✅ Métricas de erro calculadas:")
    print(f"  • Agregação por: {colunas_agregacao}")
    print(f"  • Total de grupos: {df_metricas_agregadas.count():,}")
    print(f"  • Métricas calculadas: sMAPE, RMSE, MAE")
    
    return df_metricas_agregadas

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
    
    # 1. CACHE ESTRATÉGICO: Carrega dados de demanda calculada com cache
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
    
    # 2. BROADCAST JOIN: Cria mapeamento filial → CD com broadcast para performance
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
        .withColumnRenamed("cd_primario", "cd_primario_mapeamento")  # Renomeia para evitar ambiguidade
        .cache()  # Cache para múltiplos joins
    )
    
    # 3. CALCULA PROPORÇÃO FACTUAL para todas as medidas
    print("📈 Calculando proporção factual para todas as medidas...")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # Janela para calcular totais por grupo de necessidade no mês
    w_grupo_mes = Window.partitionBy("grupo_de_necessidade")
    
    df_proporcao_factual = df_dados_demanda
    for medida in medidas_disponiveis:
        df_proporcao_factual = (
            df_proporcao_factual
            .withColumn(
                f"total_{medida}_grupo_mes",
                F.sum(F.col(medida)).over(w_grupo_mes)
            )
            .withColumn(
                f"proporcao_factual_{medida}",
                F.when(
                    F.col(f"total_{medida}_grupo_mes") > 0,
                    F.col(medida) / F.col(f"total_{medida}_grupo_mes")
                ).otherwise(F.lit(0.0))
            )
            .withColumn(
                f"proporcao_factual_{medida}_percentual",
                F.round(F.col(f"proporcao_factual_{medida}") * 100, 4)
            )
        )
    
    # Seleciona apenas colunas necessárias
    colunas_proporcao = ["cdfilial", "grupo_de_necessidade", "CdSku"] + [
        f"proporcao_factual_{medida}_percentual" for medida in medidas_disponiveis
    ]
    
    df_proporcao_factual = (
        df_proporcao_factual
        .select(*colunas_proporcao)
        .withColumnRenamed("CdSku", "CdSku_proporcao")  # Renomeia para evitar ambiguidade
        .cache()
    )
    
    # Debug: verifica colunas de proporção factual
    print(f"✅ Proporção factual calculada para todas as medidas")
    print(f"🔍 Debug proporção factual:")
    print(f"  • Colunas esperadas: {colunas_proporcao}")
    print(f"  • Colunas disponíveis: {df_proporcao_factual.columns}")
    print(f"  • Total de registros: {df_proporcao_factual.count():,}")
    
    # 4. JOIN COMPLETO com BROADCAST para performance
    print("🔗 Realizando join completo com otimizações de performance...")
    
    # BROADCAST JOIN: df_proporcao_factual é pequeno, pode ser broadcast
    df_proporcao_factual_broadcast = F.broadcast(df_proporcao_factual)
    
    # Renomeia colunas antes do join para evitar ambiguidade
    df_proporcao_factual_renomeado = (
        df_proporcao_factual_broadcast
        .withColumnRenamed("CdSku", "CdSku_proporcao")
    )
    
    # DROP DUPLICATES nas chaves únicas ANTES dos joins para evitar multiplicações
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
    
    print(f"✅ Duplicatas removidas:")
    print(f"  • df_merecimento: {df_merecimento.count():,} → {df_merecimento_limpo.count():,}")
    print(f"  • df_proporcao_factual: {df_proporcao_factual_renomeado.count():,} → {df_proporcao_factual_limpo.count():,}")
    print(f"  • de_para_filial_cd: {de_para_filial_cd.count():,} → {de_para_filial_cd_limpo.count():,}")
    
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
    
    # 5. CALCULA sMAPE para cada medida
    print("📊 Calculando sMAPE para cada medida...")
    
    EPSILON = 1e-12
    
    df_com_smape = df_versao_final
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
    
    # 6. CALCULA MELHOR sMAPE e MELHOR MEDIDA
    print("🏆 Calculando melhor sMAPE e melhor medida...")
    
    # Cria array com todas as medidas para encontrar o melhor
    colunas_smape = [f"smape_{medida}" for medida in medidas_disponiveis]
    
    # Calcula o melhor sMAPE (menor valor) para cada registro
    colunas_smape_least = [F.col(f"smape_{medida}") for medida in medidas_disponiveis]
    
    df_com_melhor_smape = (
        df_com_smape
        .withColumn(
            "melhor_sMAPE",
            F.least(*colunas_smape_least)
        )
        .withColumn(
            "medida_melhor_sMAPE",
            F.lit("")  # Inicializa com string vazia
        )
    )
    
    # Identifica qual medida deu o melhor sMAPE para cada registro
    for medida in medidas_disponiveis:
        df_com_melhor_smape = (
            df_com_melhor_smape
            .withColumn(
                "medida_melhor_sMAPE",
                F.when(
                    F.col(f"smape_{medida}") == F.col("melhor_sMAPE"),
                    F.lit(medida)  # ← Nome da medida que deu o melhor sMAPE
                ).otherwise(F.col("medida_melhor_sMAPE"))
            )
        )
    
    # 7. PREPARA COLUNAS FINAIS
    print("📋 Preparando colunas finais...")
    
    # Colunas de identificação
    colunas_identificacao = [
        "CdSku", "grupo_de_necessidade", "cdfilial", "cd_primario"  # CdSku e cd_primario vêm do df_merecimento
    ]
    
    # Colunas de merecimento CD
    colunas_merecimento_cd = [
        f"Total_CD_{medida}" for medida in medidas_disponiveis
    ]
    
    # Colunas de percentual da loja no CD
    colunas_percentual_loja = [
        f"Percentual_{medida}" for medida in medidas_disponiveis
    ]
    
    # Colunas de merecimento final
    colunas_merecimento_final = [
        f"Merecimento_Final_{medida}" for medida in medidas_disponiveis
    ]
    
    # Colunas de proporção factual
    colunas_proporcao_factual = [
        f"proporcao_factual_{medida}_percentual" for medida in medidas_disponiveis
    ]
    
    # Colunas de sMAPE
    colunas_smape = [
        f"smape_{medida}" for medida in medidas_disponiveis
    ]
    
    # Colunas de melhor sMAPE e medida
    colunas_melhor_smape = [
        "melhor_sMAPE",           # Valor do melhor sMAPE
        "medida_melhor_sMAPE"     # Nome da medida que deu o melhor sMAPE
    ]
    
    # Todas as colunas finais
    todas_colunas = (
        colunas_identificacao + 
        colunas_merecimento_cd + 
        colunas_percentual_loja + 
        colunas_merecimento_final + 
        colunas_proporcao_factual + 
        colunas_smape + 
        colunas_melhor_smape
    )
    
    # Adiciona colunas de metadados
    df_final_completo = (
        df_com_melhor_smape
        .select(*todas_colunas)
        .withColumn("data_hora_execucao", F.lit(data_hora_execucao))
        .withColumn("mes_analise", F.lit(mes_analise))
        .withColumn("data_corte_matriz", F.lit(data_corte_matriz))
        .withColumn("categoria", F.lit(categoria))
    )
    
    # DROP DUPLICATES FINAL nas chaves únicas para garantir resultado limpo
    print("🧹 Removendo duplicatas finais nas chaves únicas...")
    registros_antes = df_final_completo.count()
    
    df_final_completo = (
        df_final_completo
        .dropDuplicates(["CdSku", "grupo_de_necessidade", "cdfilial", "cd_primario"])
        .cache()
    )
    
    registros_depois = df_final_completo.count()
    print(f"✅ Duplicatas finais removidas: {registros_antes:,} → {registros_depois:,} (removidos: {registros_antes - registros_depois:,})")
    
    # Debug: mostra colunas disponíveis
    print(f"🔍 Colunas disponíveis no resultado final:")
    print(f"  • Total de colunas: {len(df_final_completo.columns)}")
    print(f"  • Colunas de sMAPE: {[col for col in df_final_completo.columns if col.startswith('smape_')]}")
    print(f"  • Colunas de proporção factual: {[col for col in df_final_completo.columns if col.startswith('proporcao_factual_')]}")
    print(f"  • Colunas de merecimento final: {[col for col in df_final_completo.columns if col.startswith('Merecimento_Final_')]}")
    
    # Debug adicional: mostra todas as colunas disponíveis
    print(f"🔍 Todas as colunas disponíveis:")
    for i, col in enumerate(df_final_completo.columns):
        print(f"  {i+1:2d}. {col}")
    
    # Debug: mostra medidas disponíveis e colunas esperadas
    print(f"🔍 Debug de medidas e colunas:")
    print(f"  • Medidas disponíveis: {medidas_disponiveis}")
    print(f"  • Colunas de proporção factual esperadas: {colunas_proporcao_factual}")
    print(f"  • Colunas de sMAPE esperadas: {colunas_smape}")
    print(f"  • Todas as colunas finais: {todas_colunas}")
    
    # 8. SALVA NO DATABOX com modo APPEND
    print("💾 Salvando no databox com modo APPEND...")
    
    # Normaliza nome da categoria para o nome da tabela
    categoria_normalizada = (
        categoria
        .replace("DIRETORIA ", "")
        .replace(" ", "_")
        .upper()
    )
    
    nome_tabela = f"databox.bcg_comum.supply_base_merecimento_diario_{categoria_normalizada}"
    
    # Salva com modo APPEND
    (
        df_final_completo
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(nome_tabela)
    )
    
    print(f"✅ Versão final salva com sucesso!")
    print(f"  • Tabela: {nome_tabela}")
    print(f"  • Modo: APPEND")
    print(f"  • Total de registros: {df_final_completo.count():,}")
    print(f"  • Colunas: {len(df_final_completo.columns)}")
    print(f"  • Medidas incluídas: {len(medidas_disponiveis)}")
    
    # 9. LIMPEZA DE CACHE para liberar memória
    print("🧹 Limpando caches para liberar memória...")
    df_dados_demanda.unpersist()
    df_proporcao_factual.unpersist()
    de_para_filial_cd.unpersist()
    
    print("✅ Cache limpo e memória liberada")
    
    return df_final_completo

# COMMAND ----------

def executar_calculo_matriz_merecimento(categoria: str, 
                                       data_inicio: str = "2024-01-01",
                                       data_calculo: str = "2025-06-30",
                                       sigma_meses_atipicos: float = 3.0,
                                       sigma_outliers_cd: float = 3.0,
                                       sigma_outliers_loja: float = 3.0,
                                       sigma_atacado_cd: float = 1.5,
                                       sigma_atacado_loja: float = 1.5,
                                       salvar_versao_completa: bool = False,
                                       mes_analise: str = "202507",
                                       data_corte_matriz: str = "2025-06-30") -> DataFrame:
    """
    Função principal que executa todo o cálculo da matriz de merecimento.
    
    Args:
        categoria: Nome da categoria/diretoria
        data_inicio: Data de início para filtro (formato YYYY-MM-DD)
        data_calculo: Data específica para cálculo de merecimento (formato YYYY-MM-DD)
        sigma_meses_atipicos: Número de desvios padrão para meses atípicos (padrão: 3.0)
        sigma_outliers_cd: Número de desvios padrão para outliers CD (padrão: 3.0)
        sigma_outliers_loja: Número de desvios padrão para outliers loja (padrão: 3.0)
        sigma_atacado_cd: Número de desvios padrão para outliers CD atacado (padrão: 1.5)
        sigma_atacado_loja: Número de desvios padrão para outliers loja atacado (padrão: 1.5)
        salvar_versao_completa: Se True, salva versão completa com métricas no databox (padrão: False)
        mes_analise: Mês de análise para métricas no formato YYYYMM (padrão: julho-2025)
        data_corte_matriz: Data de corte para cálculo da matriz de merecimento (padrão: 2025-06-30)
        
    Returns:
        DataFrame final com todas as medidas calculadas e merecimento
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
        
        # 9.1 Merecimento a nível CD
        df_merecimento_cd = calcular_merecimento_cd(df_final, data_calculo, categoria)
        
        # 9.2 Merecimento interno ao CD (filial)
        df_merecimento_interno = calcular_merecimento_interno_cd(df_final, data_calculo, categoria)
        
        # 9.3 Merecimento final (CD × Interno CD)
        df_merecimento_final = calcular_merecimento_final(df_merecimento_cd, df_merecimento_interno)
        
        # 9.4 Consolidação final: retorna apenas dados de merecimento calculados
        print("🔄 Consolidando resultado final...")
        
        # Define medidas disponíveis para seleção
        medidas_disponiveis = [
            "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
            "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
            "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
            "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
        ]
        
        # Seleciona apenas as colunas de merecimento calculadas (SKU x loja x gêmeo)
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
        
        # 12. SALVA VERSÃO COMPLETA se solicitado
        if salvar_versao_completa:
            print("=" * 80)
            print("💾 SALVANDO VERSÃO COMPLETA COM MÉTRICAS...")
            print("=" * 80)
            
            try:
                df_versao_completa = salvar_versao_final_completa(
                    df_merecimento=df_merecimento_final,
                    categoria=categoria,
                    mes_analise=mes_analise,
                    data_corte_matriz=data_corte_matriz
                )
                print("✅ Versão completa salva com sucesso!")
                print(f"  • Tabela: databox.bcg_comum.supply_base_merecimento_diario_{categoria.replace('DIRETORIA ', '').replace(' ', '_').upper()}")
                print(f"  • Modo: APPEND")
            except Exception as e:
                print(f"❌ Erro ao salvar versão completa: {str(e)}")
                print("⚠️  Continuando com resultado padrão...")
        
        return df_versao_completa
        
    except Exception as e:
        print(f"❌ Erro durante o cálculo: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exemplo de Uso da Função de Weighted sMAPE

# COMMAND ----------

def exemplo_uso_weighted_smape(categoria: str = "DIRETORIA DE TELAS"):
    """
    Exemplo de uso da função de cálculo de weighted sMAPE.
    
    Args:
        categoria: Nome da categoria para teste
    """
    print(f"🚀 Exemplo de uso da função de weighted sMAPE para: {categoria}")
    print("=" * 80)
    
    try:
        # 1. Executa o cálculo da matriz de merecimento
        print("📊 Passo 1: Calculando matriz de merecimento...")
        df_matriz = executar_calculo_matriz_merecimento(
            categoria=categoria,
            salvar_versao_completa=True,
            mes_analise="202507",
            data_corte_matriz="2025-06-30"
        )
        
        print(f"✅ Matriz calculada: {df_matriz.count():,} registros")
        
        # 2. Calcula o weighted sMAPE agregado
        print("\n📊 Passo 2: Calculando weighted sMAPE agregado...")
        df_smape = calcular_weighted_smape_agregado(
            df=df_matriz,
            categoria=categoria
        )
        
        print(f"✅ Weighted sMAPE calculado: {df_smape.count():,} registros")
        
        # 3. Exibe resultados
        print("\n📊 Passo 3: Exibindo resultados...")
        print("🔍 Resultados por nível de agregação:")
        
        for nivel in ["GRUPO_NECESSIDADE", "GRUPO_NECESSIDADE_LOJA", "LOJA", "CATEGORIA_INTEIRA"]:
            df_nivel = df_smape.filter(F.col("nivel_agregacao") == nivel)
            count = df_nivel.count()
            print(f"  • {nivel}: {count:,} registros")
        
        print("\n✅ Exemplo concluído com sucesso!")
        return df_smape
        
    except Exception as e:
        print(f"❌ Erro durante o exemplo: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Executar Exemplo (Descomente para testar)

# COMMAND ----------

# EXECUTAR EXEMPLO (descomente para testar)
# df_smape_exemplo = exemplo_uso_weighted_smape("DIRETORIA DE TELAS")
# display(df_smape_exemplo)

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
    
    print(f"🔍 Medidas disponíveis: {len(medidas_disponiveis)}")
    print(f"📊 Total de registros: {df.count():,}")
    
    # 1. PREPARAÇÃO DOS DADOS: Garante que temos as colunas necessárias
    print("🔄 Preparando dados para cálculo de weighted sMAPE...")
    
    # Verifica se temos as colunas necessárias
    colunas_necessarias = []
    for medida in medidas_disponiveis:
        colunas_necessarias.extend([
            f"merecimento_{medida}_percentual",
            f"proporcao_factual_{medida}_percentual"
        ])
    
    # Filtra apenas registros com dados válidos
    df_valido = df.filter(
        F.col("merecimento_Media90_Qt_venda_sem_ruptura_percentual").isNotNull() &
        F.col("proporcao_factual_Media90_Qt_venda_sem_ruptura_percentual").isNotNull()
    )
    
    print(f"✅ Dados preparados:")
    print(f"  • Registros válidos: {df_valido.count():,}")
    print(f"  • Colunas necessárias: {len(colunas_necessarias)}")
    
    # 2. CÁLCULO DO WEIGHTED SMAPE PARA CADA MEDIDA
    print("📈 Calculando weighted sMAPE para cada medida...")
    
    df_com_smape = df_valido
    
    for medida in medidas_disponiveis:
        # Calcula o erro absoluto
        df_com_smape = df_com_smape.withColumn(
            f"erro_absoluto_{medida}",
            F.abs(F.col(f"merecimento_{medida}_percentual") - F.col(f"proporcao_factual_{medida}_percentual"))
        )
        
        # Calcula o peso (quantidade de demanda da medida)
        df_com_smape = df_com_smape.withColumn(
            f"peso_{medida}",
            F.col(medida)
        )
        
        # Calcula o produto erro * peso
        df_com_smape = df_com_smape.withColumn(
            f"erro_peso_{medida}",
            F.col(f"erro_absoluto_{medida}") * F.col(f"peso_{medida}")
        )
    
    print(f"✅ Cálculos intermediários concluídos para {len(medidas_disponiveis)} medidas")
    
    # 3. AGREGAÇÃO POR DIFERENTES NÍVEIS
    print("🔄 Calculando agregações por diferentes níveis...")
    
    # 3.1 AGREGAÇÃO POR GRUPO DE NECESSIDADE
    print("📊 Agregação por grupo de necessidade...")
    
    # Cria lista de expressões de agregação
    aggs_grupo = []
    for medida in medidas_disponiveis:
        aggs_grupo.extend([
            F.sum(F.col(f"erro_peso_{medida}")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col(f"peso_{medida}")).alias(f"soma_peso_{medida}")
        ])
    
    df_smape_grupo = df_com_smape.groupBy("grupo_de_necessidade").agg(*aggs_grupo)
    
    # Calcula weighted sMAPE para cada grupo de necessidade
    for medida in medidas_disponiveis:
        df_smape_grupo = df_smape_grupo.withColumn(
            f"weighted_smape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    # 3.2 AGREGAÇÃO POR GRUPO DE NECESSIDADE x LOJA
    print("📊 Agregação por grupo de necessidade x loja...")
    
    # Cria lista de expressões de agregação para grupo + loja
    aggs_grupo_loja = []
    for medida in medidas_disponiveis:
        aggs_grupo_loja.extend([
            F.sum(F.col(f"erro_peso_{medida}")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col(f"peso_{medida}")).alias(f"soma_peso_{medida}")
        ])
    
    df_smape_grupo_loja = df_com_smape.groupBy("grupo_de_necessidade", "cdfilial").agg(*aggs_grupo_loja)
    
    # Calcula weighted sMAPE para cada grupo de necessidade x loja
    for medida in medidas_disponiveis:
        df_smape_grupo_loja = df_smape_grupo_loja.withColumn(
            f"weighted_smape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    # 3.3 AGREGAÇÃO POR LOJA
    print("📊 Agregação por loja...")
    
    # Cria lista de expressões de agregação para loja
    aggs_loja = []
    for medida in medidas_disponiveis:
        aggs_loja.extend([
            F.sum(F.col(f"erro_peso_{medida}")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col(f"peso_{medida}")).alias(f"soma_peso_{medida}")
        ])
    
    df_smape_loja = df_com_smape.groupBy("cdfilial").agg(*aggs_loja)
    
    # Calcula weighted sMAPE para cada loja
    for medida in medidas_disponiveis:
        df_smape_loja = df_smape_loja.withColumn(
            f"weighted_smape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    # 3.4 AGREGAÇÃO DA CATEGORIA INTEIRA
    print("📊 Agregação da categoria inteira...")
    
    # Cria lista de expressões de agregação para categoria
    aggs_categoria = []
    for medida in medidas_disponiveis:
        aggs_categoria.extend([
            F.sum(F.col(f"erro_peso_{medida}")).alias(f"soma_erro_peso_{medida}"),
            F.sum(F.col(f"peso_{medida}")).alias(f"soma_peso_{medida}")
        ])
    
    df_smape_categoria = df_com_smape.agg(*aggs_categoria)
    
    # Calcula weighted sMAPE para a categoria inteira
    for medida in medidas_disponiveis:
        df_smape_categoria = df_smape_categoria.withColumn(
            f"weighted_smape_{medida}",
            F.when(
                F.col(f"soma_peso_{medida}") > 0,
                F.round(F.col(f"soma_erro_peso_{medida}") / F.col(f"soma_peso_{medida}") * 100, 4)
            ).otherwise(F.lit(0.0))
        )
    
    # Adiciona identificador de nível
    df_smape_categoria = df_smape_categoria.withColumn("nivel_agregacao", F.lit("CATEGORIA_INTEIRA"))
    
    # 4. CONSOLIDAÇÃO DOS RESULTADOS
    print("🔄 Consolidando resultados de weighted sMAPE...")
    
    # Adiciona identificadores de nível para cada agregação
    df_smape_grupo = df_smape_grupo.withColumn("nivel_agregacao", F.lit("GRUPO_NECESSIDADE"))
    df_smape_grupo_loja = df_smape_grupo_loja.withColumn("nivel_agregacao", F.lit("GRUPO_NECESSIDADE_LOJA"))
    df_smape_loja = df_smape_loja.withColumn("nivel_agregacao", F.lit("LOJA"))
    
    # Seleciona apenas as colunas de weighted sMAPE e identificadores
    colunas_smape = ["nivel_agregacao"] + [f"weighted_smape_{medida}" for medida in medidas_disponiveis]
    
    df_smape_grupo_final = df_smape_grupo.select("nivel_agregacao", "grupo_de_necessidade", *colunas_smape[1:])
    df_smape_grupo_loja_final = df_smape_grupo_loja.select("nivel_agregacao", "grupo_de_necessidade", "cdfilial", *colunas_smape[1:])
    df_smape_loja_final = df_smape_loja.select("nivel_agregacao", "cdfilial", *colunas_smape[1:])
    df_smape_categoria_final = df_smape_categoria.select(*colunas_smape)
    
    # Union de todos os resultados
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
    
    # 5. EXIBE RESULTADOS RESUMIDOS
    print("\n" + "="*80)
    print("📊 RESUMO DO WEIGHTED SMAPE POR NÍVEL DE AGREGAÇÃO")
    print("="*80)
    
    # Resumo por nível de agregação
    for nivel in ["GRUPO_NECESSIDADE", "GRUPO_NECESSIDADE_LOJA", "LOJA", "CATEGORIA_INTEIRA"]:
        df_nivel = df_smape_final.filter(F.col("nivel_agregacao") == nivel)
        print(f"\n🔍 {nivel}:")
        
        if nivel == "CATEGORIA_INTEIRA":
            # Para categoria inteira, mostra apenas os valores
            for medida in medidas_disponiveis:
                valor = df_nivel.select(f"weighted_smape_{medida}").first()[0]
                print(f"  • {medida}: {valor:.4f}%")
        else:
            # Para outros níveis, mostra estatísticas
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

# # Cálculo com salvamento da versão completa
# df_telas_completo = executar_calculo_matriz_merecimento(
#     categoria="DIRETORIA DE TELAS",
#     salvar_versao_completa=True,
#     mes_analise="202507",  # julho-2025
#     data_corte_matriz="2025-06-30"  # data de corte da matriz
# )

# # Exibir resultado
# df_telas_completo.display()

# COMMAND ----------

# Cálculo com salvamento da versão completa
df_telas_completo = executar_calculo_matriz_merecimento(
    categoria="DIRETORIA TELEFONIA CELULAR",
    salvar_versao_completa=True,
    mes_analise="202507",  # julho-2025
    data_corte_matriz="2025-06-30"  # data de corte da matriz
)

# Exibir resultado (limitado para agilizar)
df_telas_completo.display()

# COMMAND ----------

# Verificação da base de merecimento (limitado para agilizar)
spark.table('databox.bcg_comum.supply_base_merecimento_diario_TELEFONIA_CELULAR').limit(1000).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Exemplo de Uso

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exemplo para DIRETORIA DE TELAS
# MAGIC
# MAGIC ```python
# MAGIC # Cálculo padrão
# MAGIC df_telas = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")
# MAGIC
# MAGIC # Cálculo com salvamento da versão completa
# MAGIC df_telas_completo = executar_calculo_matriz_merecimento(
# MAGIC     categoria="DIRETORIA DE TELAS",
# MAGIC     salvar_versao_completa=True,
# MAGIC     mes_analise="202507",  # julho-2025
# MAGIC     data_corte_matriz="2025-06-30"  # data de corte da matriz
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Exemplo para DIRETORIA TELEFONIA CELULAR
# MAGIC
# MAGIC ```python
# MAGIC # Cálculo padrão
# MAGIC df_telefonia = executar_calculo_matriz_merecimento("DIRETORIA TELEFONIA CELULAR")
# MAGIC
# MAGIC # Cálculo com salvamento da versão completa
# MAGIC df_telefonia_completo = executar_calculo_matriz_merecimento(
# MAGIC     categoria="DIRETORIA TELEFONIA CELULAR",
# MAGIC     salvar_versao_completa=True,
# MAGIC     mes_analise="202507",
# MAGIC     data_corte_matriz="2025-06-30"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Exemplo para DIRETORIA LINHA BRANCA
# MAGIC
# MAGIC ```python
# MAGIC # Cálculo padrão
# MAGIC df_linha_branca = executar_calculo_matriz_merecimento("DIRETORIA LINHA BRANCA")
# MAGIC
# MAGIC # Cálculo com salvamento da versão completa
# MAGIC df_linha_branca_completo = executar_calculo_matriz_merecimento(
# MAGIC     categoria="DIRETORIA LINHA BRANCA",
# MAGIC     salvar_versao_completa=True,
# MAGIC     mes_analise="202507",
# MAGIC     data_corte_matriz="2025-06-30"
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
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
    colunas_medias_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    
    todas_colunas_medidas = colunas_medias + colunas_medias_aparadas
    
    print("\n🔍 Verificação de valores nulos:")
    for coluna in todas_colunas_medidas:
        if coluna in df.columns:
            nulos = df.filter(F.col(coluna).isNull()).count()
            print(f"  • {coluna}: {nulos:,} valores nulos")
    
    print("✅ Validação concluída")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Execução de Teste
# MAGIC
# MAGIC %md
# MAGIC Descomente a linha abaixo para executar um teste com a categoria desejada:

# COMMAND ----------

# EXECUTAR TESTE (descomente para testar)
# categoria_teste = "DIRETORIA DE TELAS"
# df_resultado = executar_calculo_matriz_merecimento(categoria_teste)
# validar_resultados(df_resultado, categoria_teste)
# display(df_resultado.limit(10))

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

# MAGIC %md
# MAGIC ### Exemplo para DIRETORIA TELEFONIA CELULAR

# COMMAND ----------

# Cálculo com salvamento da versão completa
df_telas_completo = executar_calculo_matriz_merecimento(
    categoria="DIRETORIA TELEFONIA CELULAR",
    salvar_versao_completa=True,
    mes_analise="202507",  # julho-2025
    data_corte_matriz="2025-06-30"  # data de corte da matriz
)

# Exibir resultado (limitado para agilizar)
df_telas_completo.limit(1000).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exemplo de Cálculo e Salvamento de Weighted sMAPE Agregado

# COMMAND ----------

# Exemplo de cálculo e salvamento automático dos weighted sMAPEs agregados
def exemplo_weighted_smape_com_salvamento(categoria: str = "DIRETORIA TELEFONIA CELULAR"):
    """
    Exemplo completo de cálculo e salvamento dos weighted sMAPEs agregados.
    
    Args:
        categoria: Nome da categoria para teste
    """
    print(f"🚀 Exemplo completo de weighted sMAPE com salvamento para: {categoria}")
    print("=" * 80)
    
    try:
        # 1. Executa o cálculo da matriz de merecimento
        print("📊 Passo 1: Calculando matriz de merecimento...")
        df_matriz = executar_calculo_matriz_merecimento(
            categoria=categoria,
            salvar_versao_completa=True,
            mes_analise="202507",
            data_corte_matriz="2025-06-30"
        )
        
        print(f"✅ Matriz calculada: {df_matriz.count():,} registros")
        
        # 2. Calcula o weighted sMAPE agregado
        print("\n📊 Passo 2: Calculando weighted sMAPE agregado...")
        df_weighted_smape = calcular_weighted_smape_agregado(
            df=df_matriz,
            categoria=categoria
        )
        
        print(f"✅ Weighted sMAPE calculado: {df_weighted_smape.count():,} registros")
        
        # 3. Salva automaticamente nas tabelas separadas
        print("\n💾 Passo 3: Salvando weighted sMAPEs nas tabelas...")
        salvar_weighted_smape_agregado(
            df_weighted_smape=df_weighted_smape,
            categoria=categoria,
            mes_analise="202507",
            data_corte_matriz="2025-06-30"
        )
        
        print("\n🎉 Processo completo executado com sucesso!")
        print("📊 Tabelas criadas:")
        print("  • supply_weighted_smape_grupo_{CATEGORIA}")
        print("  • supply_weighted_smape_grupo_loja_{CATEGORIA}")
        print("  • supply_weighted_smape_loja_{CATEGORIA}")
        print("  • supply_weighted_smape_categoria_{CATEGORIA}")
        
        return df_weighted_smape
        
    except Exception as e:
        print(f"❌ Erro durante o processo: {str(e)}")
        raise

# COMMAND ----------

# Executa o exemplo completo (descomente para executar)
# df_weighted_smape_exemplo = exemplo_weighted_smape_com_salvamento("DIRETORIA TELEFONIA CELULAR")
