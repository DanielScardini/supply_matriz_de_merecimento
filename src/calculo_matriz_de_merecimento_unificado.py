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
# MAGIC - **DIRETORIA LINHA BRANCA**: Usa `NmEspecieGerencial + "_" + DsVoltagem` como grupo_de_necessidade (DsVoltagem nulls preenchidos com "")
# MAGIC - **DIRETORIA LINHA LEVE**: Usa `NmEspecieGerencial + "_" + DsVoltagem` como grupo_de_necessidade (DsVoltagem nulls preenchidos com "")
# MAGIC - **DIRETORIA INFO/GAMES**: Usa `NmEspecieGerencial` como grupo_de_necessidade

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e Configuração Inicial

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

FILIAIS_OUTLET = [2528, 3604]


# COMMAND ----------

def get_data_inicio(min_meses: int = 18, hoje: datetime | None = None) -> datetime:
    """
    Retorna 1º de janeiro mais recente que esteja a pelo menos `min_meses` meses de 'hoje'.
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
        "descricao": "Agrupamento por espécie gerencial + voltagem (DsVoltagem)"
    },
    "DIRETORIA LINHA LEVE": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial", 
        "descricao": "Agrupamento por espécie gerencial + voltagem (DsVoltagem)"
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
    "desvios_atacado_cd": 3,     # Desvios para outliers CD em lojas de atacado
    "desvios_atacado_loja": 1.5    # Desvios para outliers loja em lojas de atacado
}

# Configuração das janelas móveis para médias aparadas
JANELAS_MOVEIS_APARADAS = [90, 180, 270, 360]

# Configuração específica para merecimento CD (sempre 90 dias)
JANELA_CD_MERECIMENTO = 90

# Configuração das médias aparadas (percentual de corte)
PERCENTUAL_CORTE_MEDIAS_APARADAS = 0.01  # 1% de corte superior e inferior

FILIAIS_ATACADO = [
    1671,     # Petrolina - PE
    17,       # Norte Shopping
    1778,     # Shop Tacaruna - PE
    293,      # Varginha - MG
    1003,     # Shop Guarulhos - SP
    1949,     # São Mateus - ES
    1717,     # Fortaleza - CE
    2383,     # Pacajus - CE
    1634,     # Shop Pantanal - MT
    590,      # Contagem - MG
    1485,     # Sorocaba - SP
    2103,     # Caruaru 2 - PE
    2059,     # Arcoverde - PE
    520,      # Shop Bangu - RJ
    4000,     # Berrini - SP
    1157,     # Feira de Santana - BA
    1764,     # Shop Moxuara - ES
    1158,     # Catete - RJ
    376,      # Ponte Nova - MG
    242,      # Montes Claros - MG
    2038,     # Caruarua - PE
    1639,     # Vitoria - ES
    445,      # Ubá - MG
    1679,     # Fortaleza 2 - CE
    1697,     # Mossoró - RN
    39,       # Copacabana 4 - RJ
    1811,     # Shop da Ilha - MA
    461,      # Barra Shopping - RJ
]

print("✅ Configurações carregadas:")
print(f"  • Categorias suportadas: {list(REGRAS_AGRUPAMENTO.keys())}")
print(f"  • Janelas móveis aparadas: {JANELAS_MOVEIS_APARADAS} dias")
print(f"  • Janela CD merecimento: {JANELA_CD_MERECIMENTO} dias")
print(f"  • Percentual de corte para médias aparadas: {PERCENTUAL_CORTE_MEDIAS_APARADAS*100}% (total 2%)")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funções Essenciais para Cálculo

# COMMAND ----------

def determinar_grupo_necessidade(categoria: str, df: DataFrame) -> DataFrame:
    """
    Determina o grupo de necessidade baseado na categoria e aplica a regra correspondente.
    """
    if categoria not in REGRAS_AGRUPAMENTO:
        raise ValueError(f"Categoria '{categoria}' não suportada. Categorias válidas: {list(REGRAS_AGRUPAMENTO.keys())}")
    
    regra = REGRAS_AGRUPAMENTO[categoria]
    coluna_origem = regra["coluna_grupo_necessidade"]

    colunas_df = df.columns
    if coluna_origem not in colunas_df:
        raise ValueError(f"Coluna '{coluna_origem}' não encontrada no DataFrame. Colunas disponíveis: {colunas_df}")
    
    # Verifica se é LINHA BRANCA ou LINHA LEVE para aplicar agrupamento especial
    if categoria in ["DIRETORIA DE LINHA BRANCA", "DIRETORIA LINHA LEVE"]:
        # Verifica se DsVoltagem existe no DataFrame
        if "DsVoltagem" not in colunas_df:
            raise ValueError(f"Coluna 'DsVoltagem' não encontrada no DataFrame para categoria '{categoria}'. Colunas disponíveis: {colunas_df}")
        
        # Cria grupo de necessidade combinando NmEspecieGerencial + "_" + DsVoltagem (nulls preenchidos com "")
        df_com_grupo = df.withColumn(
            "DsVoltagem_filled",
            F.substring(F.coalesce(F.col("DsVoltagem"), F.lit("")), 1, 3)
        ).withColumn(
            "grupo_de_necessidade",
            F.concat(
                F.coalesce(F.col(coluna_origem), F.lit("SEM_GN")),
                F.lit("_"),
                F.col("DsVoltagem_filled")
            )
        ).withColumn(
            "tipo_agrupamento",
            F.lit(regra["tipo_agrupamento"])
        ).drop("DsVoltagem_filled")
        
        print(f"✅ Grupo de necessidade definido para '{categoria}' (com DsVoltagem):")
        print(f"  • Coluna origem: {coluna_origem} + DsVoltagem")
        print(f"  • Valores copiados: {df_com_grupo.select('grupo_de_necessidade').distinct().count()} grupos únicos")
        
    else:
        # Para outras categorias, mantém o comportamento original
        df_com_grupo = df.withColumn(
            "grupo_de_necessidade",
            F.coalesce(F.col(coluna_origem), F.lit("SEM_GN"))
        ).withColumn(
            "tipo_agrupamento",
            F.lit(regra["tipo_agrupamento"])
        )
        
        print(f"✅ Grupo de necessidade definido para '{categoria}':")
        print(f"  • Coluna origem: {coluna_origem}")
        print(f"  • Valores copiados: {df_com_grupo.select('grupo_de_necessidade').distinct().count()} grupos únicos")
    
    return df_com_grupo

# COMMAND ----------

def carregar_dados_base(categoria: str, data_inicio: str = "2024-07-01") -> DataFrame:
    """
    Carrega os dados base para a categoria especificada.
    """
    print(f"🔄 Carregando dados para categoria: {categoria}")
    
    df_base = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
        .filter(F.col("NmAgrupamentoDiretoriaSetor") == categoria)
        #.filter(F.col("NmSetorGerencial") == 'PORTATEIS')
        .filter(F.col("DtAtual") >= data_inicio)
        .withColumn(
            "year_month",
            F.date_format(F.col("DtAtual"), "yyyyMM").cast("int")
        )
        .fillna(0, subset=["Receita", "QtMercadoria", "TeveVenda", "deltaRuptura"])
    )
    
    print(f"✅ Dados carregados para '{categoria}':")
    print(f"  • Total de registros: {df_base.count():,}")
    
    return df_base

# COMMAND ----------

def carregar_de_para_espelhamento() -> DataFrame:
    """
    Carrega o de-para de espelhamento de filiais do arquivo Excel.
    
    Returns:
        DataFrame com colunas: CdFilial_referencia, CdFilial_espelhada
    """
    print("🔄 Carregando de-para de espelhamento de filiais...")

    # noqa: E999
    # pip install openpyxl
    
    try:
        # Carrega o arquivo Excel usando pandas
        df_pandas = pd.read_excel(
            "/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/planilha_governanca/governanca_supply_inputs_matriz_merecimento.xlsx",
            sheet_name="espelhamento_lojas"
        )
        
        # Verifica se o DataFrame não está vazio
        if df_pandas.empty:
            print("ℹ️ Aba 'espelhamento_lojas' está vazia")
            return spark.createDataFrame([], "CdFilial_referencia INT, CdFilial_espelhada INT")
        
        # Renomeia as colunas para padronizar
        df_pandas = df_pandas.rename(columns={
            "CdFilial_referência": "CdFilial_referencia",
            "CdFilial_espelhada": "CdFilial_espelhada"
        })
        
        # Remove linhas com valores nulos
        df_pandas = df_pandas.dropna(subset=["CdFilial_referencia", "CdFilial_espelhada"])
        
        # Converte para DataFrame do Spark
        df_espelhamento = spark.createDataFrame(df_pandas)
        
        print(f"✅ De-para de espelhamento carregado:")
        print(f"  • Total de mapeamentos: {df_espelhamento.count():,}")
        
        if df_espelhamento.count() > 0:
            print("  • Exemplos de espelhamento:")
            df_espelhamento.show(5, truncate=False)
        
        return df_espelhamento
        
    except FileNotFoundError:
        print("⚠️ Arquivo 'governanca_supply_inputs_matriz_merecimento.xlsx' não encontrado")
        print("  • Continuando sem espelhamento...")
        return spark.createDataFrame([], "CdFilial_referencia INT, CdFilial_espelhada INT")
        
    except Exception as e:
        print(f"⚠️ Erro ao carregar de-para de espelhamento: {str(e)}")
        print("  • Continuando sem espelhamento...")
        return spark.createDataFrame([], "CdFilial_referencia INT, CdFilial_espelhada INT")

# COMMAND ----------

def aplicar_espelhamento_filiais(df_base: DataFrame, df_espelhamento: DataFrame) -> DataFrame:
    """
    Aplica o espelhamento de filiais nos dados base.
    
    Para cada filial espelhada, remove os dados existentes e substitui pelos dados 
    da filial de referência.
    
    Args:
        df_base: DataFrame com dados base
        df_espelhamento: DataFrame com de-para de espelhamento
        
    Returns:
        DataFrame com dados espelhados aplicados
    """
    if df_espelhamento.count() == 0:
        print("ℹ️ Nenhum espelhamento para aplicar")
        return df_base
    
    print("🔄 Aplicando espelhamento de filiais...")
    
    # Contar registros antes do espelhamento
    registros_antes = df_base.count()
    
    # Obter lista de filiais que serão espelhadas
    filiais_espelhadas = [row.CdFilial_espelhada for row in df_espelhamento.select("CdFilial_espelhada").distinct().collect()]
    
    print(f"  • Filiais que serão espelhadas: {filiais_espelhadas}")
    
    # Remover dados existentes das filiais que serão espelhadas
    df_sem_espelhadas = df_base.filter(~F.col("CdFilial").isin(filiais_espelhadas))
    
    registros_removidos = registros_antes - df_sem_espelhadas.count()
    print(f"  • Registros removidos das filiais espelhadas: {registros_removidos:,}")
    
    # Criar dados espelhados (copiando da filial de referência)
    df_espelhados = (
        df_base
        .join(
            df_espelhamento,
            df_base.CdFilial == df_espelhamento.CdFilial_referencia,
            "inner"
        )
        .select(
            df_espelhamento.CdFilial_espelhada.alias("CdFilial"),
            *[col for col in df_base.columns if col != "CdFilial"]
        )
    )
    
    # Unir dados sem as filiais espelhadas com os novos dados espelhados
    df_com_espelhamento = df_sem_espelhadas.union(df_espelhados)
    
    # Contar registros após espelhamento
    registros_depois = df_com_espelhamento.count()
    registros_espelhados = df_espelhados.count()
    
    print(f"✅ Espelhamento aplicado:")
    print(f"  • Registros antes: {registros_antes:,}")
    print(f"  • Registros removidos: {registros_removidos:,}")
    print(f"  • Registros espelhados adicionados: {registros_espelhados:,}")
    print(f"  • Registros após: {registros_depois:,}")
    
    # Mostrar exemplos de filiais espelhadas
    if registros_espelhados > 0:
        print("  • Exemplos de filiais espelhadas:")
        df_espelhados.select("CdFilial").distinct().show(5, truncate=False)
    
    return df_com_espelhamento

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
        de_para_modelos_tecnologia.rename(columns={"codigo_item": "CdSku"})[['CdSku', 'modelos']], 
        de_para_gemeos_tecnologia.rename(columns={"sku_loja": "CdSku"})[['CdSku', 'gemeos']] if de_para_gemeos_tecnologia is not None else None
    )

# COMMAND ----------

def aplicar_mapeamentos_produtos(df: DataFrame, categoria: str, 
                                de_para_modelos: pd.DataFrame, 
                                de_para_gemeos: pd.DataFrame = None) -> DataFrame:
    """
    Aplica os mapeamentos de produtos ao DataFrame base.
    """
    print(f"🔄 Aplicando mapeamentos para categoria: {categoria}")
    
    df_modelos_spark = spark.createDataFrame(de_para_modelos)
    
    df_com_modelos = df.join(
        df_modelos_spark,
        on="CdSku",
        how="left"
    )
    
    if (de_para_gemeos is not None and 
        REGRAS_AGRUPAMENTO[categoria]["coluna_grupo_necessidade"] == "gemeos"):
        
        df_gemeos_spark = spark.createDataFrame(de_para_gemeos)
        df_com_mapeamentos = df_com_modelos.join(
            df_gemeos_spark,
            on="CdSku",
            how="left"
        )
        df_com_mapeamentos = (
            df_com_mapeamentos
            .withColumn("gemeos",
                        F.when(F.col("gemeos") == '-', F.col("modelos"))
                        .otherwise(F.col("gemeos"))
            )
        )
        print("✅ Mapeamento de gêmeos aplicado")

        print("ℹ️  Mapeamento de gêmeos aplicado: ", df_com_mapeamentos.count())       

    else:
        df_com_mapeamentos = df_com_modelos
        print("ℹ️  Mapeamento de gêmeos não aplicado")
    
    return df_com_mapeamentos

# COMMAND ----------

def remover_outliers_series_historicas(df: DataFrame, 
                                     coluna_valor: str = "QtMercadoria",
                                     n_sigmas_padrao: float = 3.0,
                                     n_sigmas_atacado: float = 1.5,
                                     filiais_atacado: list = None) -> DataFrame:
    """
    Remove outliers das séries históricas (grupo de necessidade x filial) usando dois métodos:
    
    1. Método padrão: n desvios padrão (3 sigmas por padrão)
    2. Método atacado: n desvios padrão específico para filiais na watchlist de atacado
    
    Os outliers são saturados para exatamente o threshold (média + n*sigmas).
    
    Args:
        df: DataFrame com dados históricos
        coluna_valor: Nome da coluna com os valores a serem tratados
        n_sigmas_padrao: Número de desvios padrão para método padrão
        n_sigmas_atacado: Número de desvios padrão para filiais de atacado
        filiais_atacado: Lista de filiais consideradas de atacado
        
    Returns:
        DataFrame com outliers removidos (saturados no threshold)
    """
    print(f"🔄 Removendo outliers das séries históricas...")
    print(f"  • Coluna valor: {coluna_valor}")
    print(f"  • N sigmas padrão: {n_sigmas_padrao}")
    print(f"  • N sigmas atacado: {n_sigmas_atacado}")
    print(f"  • Filiais atacado: {filiais_atacado if filiais_atacado else 'Não definidas'}")
    
    # Se não há filiais de atacado definidas, usa método padrão para todas
    if not filiais_atacado:
        filiais_atacado = []
    
    # Janela para calcular estatísticas por grupo_de_necessidade x filial
    w_grupo_filial = Window.partitionBy("grupo_de_necessidade", "CdFilial")
    
    # Calcular estatísticas por grupo_de_necessidade x filial
    df_com_stats = (
        df
        .withColumn("media_grupo_filial", F.avg(F.col(coluna_valor)).over(w_grupo_filial))
        .withColumn("desvio_grupo_filial", F.stddev(F.col(coluna_valor)).over(w_grupo_filial))
        .withColumn("is_atacado", F.col("CdFilial").isin(filiais_atacado))
    )
    
    # Calcular thresholds baseado no tipo de filial
    df_com_thresholds = (
        df_com_stats
        .withColumn(
            "n_sigmas_aplicado",
            F.when(F.col("is_atacado"), F.lit(n_sigmas_atacado))
            .otherwise(F.lit(n_sigmas_padrao))
        )
        .withColumn(
            "threshold_superior",
            F.col("media_grupo_filial") + (F.col("n_sigmas_aplicado") * F.col("desvio_grupo_filial"))
        )
        .withColumn(
            "threshold_inferior",
            F.greatest(
                F.col("media_grupo_filial") - (F.col("n_sigmas_aplicado") * F.col("desvio_grupo_filial")),
                F.lit(0)  # Não permite valores negativos
            )
        )
    )
    
    # Aplicar saturação dos outliers
    df_sem_outliers = (
        df_com_thresholds
        .withColumn(
            f"{coluna_valor}_original",
            F.col(coluna_valor)
        )
        .withColumn(
            coluna_valor,
            F.when(
                F.col(coluna_valor) > F.col("threshold_superior"),
                F.col("threshold_superior")
            )
            .when(
                F.col(coluna_valor) < F.col("threshold_inferior"),
                F.col("threshold_inferior")
            )
            .otherwise(F.col(f"{coluna_valor}_original"))
        )
        .withColumn(
            "flag_outlier_removido",
            F.when(
                (F.col(f"{coluna_valor}_original") != F.col(coluna_valor)),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .drop(
            "media_grupo_filial", "desvio_grupo_filial", "is_atacado", 
            "n_sigmas_aplicado", "threshold_superior", "threshold_inferior"
        )
    )
    
    # Calcular estatísticas de remoção
    total_registros = df_sem_outliers.count()
    outliers_removidos = (
        df_sem_outliers
        .filter(F.col("flag_outlier_removido") == 1)
        .count()
    )
    
    print(f"✅ Outliers removidos: {outliers_removidos:,} de {total_registros:,} registros ({outliers_removidos/total_registros*100:.2f}%)")
    
    return df_sem_outliers

# COMMAND ----------

def detectar_outliers_meses_atipicos(df: DataFrame, categoria: str) -> tuple:
    """
    Detecta outliers e meses atípicos baseado no grupo_de_necessidade.
    """
    print(f"🔄 Detectando outliers para categoria: {categoria}")
    
    df_stats_por_grupo_mes = (
        df.groupBy("grupo_de_necessidade", "year_month")
        .agg(
            F.sum("QtMercadoria").alias("QtMercadoria_total"),
            F.count("*").alias("total_registros")
        )
    )
    
    w_stats_grupo = Window.partitionBy("grupo_de_necessidade")
    
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
            F.col("media_qt_mercadoria") + (F.lit(PARAMETROS_OUTLIERS["desvios_meses_atipicos"]) * F.col("desvio_padrao_qt_mercadoria"))
        )
        .withColumn(
            "limite_inferior_nsigma",
            F.greatest(
                F.col("media_qt_mercadoria") - (F.lit(PARAMETROS_OUTLIERS["desvios_meses_atipicos"]) * F.col("desvio_padrao_qt_mercadoria")),
                F.lit(0)
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
    
    df_meses_atipicos = (
        df_stats_grupo
        .filter(F.col("flag_mes_atipico") == 1)
        .select("grupo_de_necessidade", "year_month")
    )
    
    print(f"✅ Detecção de outliers concluída: {df_meses_atipicos.count()} meses atípicos")
    
    return df_stats_grupo, df_meses_atipicos

# COMMAND ----------

def filtrar_meses_atipicos(df: DataFrame, df_meses_atipicos: DataFrame) -> DataFrame:
    """
    Filtra os meses atípicos do DataFrame principal.
    """
    print("🔄 Aplicando filtro de meses atípicos...")
    
    df_filtrado = (
        df.join(
            df_meses_atipicos.withColumn("flag_remover", F.lit(1)),
            on=["grupo_de_necessidade", "year_month"],
            how="left"
        )
        .filter(F.col("flag_remover").isNull())
        .drop("flag_remover")
    )
    
    registros_antes = df.count()
    registros_depois = df_filtrado.count()
    
    print(f"✅ Filtro aplicado: {registros_antes:,} → {registros_depois:,} registros")
    
    return df_filtrado

# COMMAND ----------

def add_media_aparada_rolling(df, janelas, col_val="demanda_robusta", col_ord="DtAtual", 
                              grupos=("CdSku","CdFilial"), alpha=0.10, min_obs=10):
    """
    Adiciona médias aparadas rolling com proteção completa contra NULLs.
    """
    out = df
    
    # ✅ JANELA DE BACKUP 360d para casos extremos
    window_360_backup = Window.partitionBy(*grupos).orderBy(F.col(col_ord)).rowsBetween(-360, 0)
    backup_360_mean = F.avg(F.when(F.col(col_val).isNotNull(), F.col(col_val))).over(window_360_backup)
    
    for dias in janelas:
        w = Window.partitionBy(*grupos).orderBy(F.col(col_ord)).rowsBetween(-dias, 0)

        # Percentis com proteção contra janelas vazias
        ql = F.percentile_approx(F.col(col_val), F.lit(alpha)).over(w)
        qh = F.percentile_approx(F.col(col_val), F.lit(1 - alpha)).over(w)

        out = (
            out
            .withColumn(f"_ql_{dias}", F.coalesce(ql, F.lit(0)))  # ✅ Proteção percentis
            .withColumn(f"_qh_{dias}", F.coalesce(qh, F.lit(float('inf'))))  # ✅ Proteção percentis
        )

        cnt = F.count(F.col(col_val)).over(w)
        cond = (F.col(col_val) >= F.col(f"_ql_{dias}")) & (F.col(col_val) <= F.col(f"_qh_{dias}"))
        sum_trim = F.sum(F.when(cond, F.col(col_val)).otherwise(F.lit(0))).over(w)
        cnt_trim = F.sum(F.when(cond, F.lit(1)).otherwise(F.lit(0))).over(w)

        # ✅ PROTEÇÃO: Média simples com fallback
        mean_simple = F.coalesce(
            F.avg(F.col(col_val)).over(w),  # Média da janela
            backup_360_mean,  # Backup 360d
            F.lit(0)  # Último recurso
        )
        
        # ✅ PROTEÇÃO: Média aparada com fallback para média simples
        mean_trim = F.when(
            cnt_trim > 0, 
            sum_trim / cnt_trim
        ).otherwise(mean_simple)  # Fallback para média simples protegida

        # ✅ PROTEÇÃO: Lógica final com múltiplos fallbacks
        out = out.withColumn(
            f"MediaAparada{dias}_Qt_venda_sem_ruptura",
            F.when(
                cnt >= F.lit(min_obs), 
                F.coalesce(mean_trim, mean_simple, backup_360_mean, F.lit(0))
            )
            .otherwise(
                F.coalesce(mean_simple, backup_360_mean, F.lit(0))
            )
        ).drop(f"_ql_{dias}", f"_qh_{dias}")

    return out

# COMMAND ----------

def calcular_medidas_centrais_com_medias_aparadas(df: DataFrame) -> DataFrame:
    """
    Calcula medidas centrais com médias aparadas e proteção completa contra NULLs.
    """
    print("🔄 Calculando medidas centrais com médias aparadas (protegido)...")
    
    df_sem_ruptura = (
        df
        .withColumn("demanda_robusta",
                    F.col("QtMercadoria") + F.col("deltaRuptura"))
        .withColumn("demanda_robusta",
                    F.when(
                        F.col("CdFilial").isin(FILIAIS_OUTLET), F.lit(0)
                        )
                    .otherwise(F.col("demanda_robusta"))
                    )
        # ✅ GARANTIR que demanda_robusta nunca é NULL
        .withColumn("demanda_robusta", F.coalesce(F.col("demanda_robusta"), F.lit(0)))
    )

    lista = ", ".join(str(f) for f in FILIAIS_OUTLET)
    print(f"🏬 Zerando a demanda das filiais [{lista}] ⚠️ pois não são abastecidas via CD normalmente.")
    
     # Aplicar apenas médias aparadas
    df_com_medias_aparadas = (
        add_media_aparada_rolling(
            df_sem_ruptura,
            janelas=JANELAS_MOVEIS_APARADAS,
            col_val="demanda_robusta",
            col_ord="DtAtual",
            grupos=("grupo_de_necessidade","CdFilial"),
            alpha=PERCENTUAL_CORTE_MEDIAS_APARADAS,
            min_obs=10
        )
    )
    
    print("✅ Medidas centrais calculadas (apenas médias aparadas)")
    return df_com_medias_aparadas

# COMMAND ----------

def consolidar_medidas(df: DataFrame) -> DataFrame:
    """
    Consolida todas as medidas calculadas em uma base única.
    """
    print("🔄 Consolidando medidas (apenas médias aparadas)...")
    
    colunas_medias_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS_APARADAS]
    
    df_consolidado = (
        df.select(
            "DtAtual", "CdFilial", "grupo_de_necessidade", "year_month",
            "QtMercadoria",  "deltaRuptura", "tipo_agrupamento",
            *colunas_medias_aparadas
        )
        .fillna(0, subset=colunas_medias_aparadas)
    )
    
    print("✅ Medidas consolidadas (apenas médias aparadas)")
    return df_consolidado

# COMMAND ----------

def criar_de_para_filial_cd() -> DataFrame:
    """
    Cria o mapeamento filial → CD usando dados da tabela base.
    """
    print("🔄 Criando de-para filial → CD...")
    
    df_base = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
        .filter(F.col("DtAtual") == "2025-08-01")
        .filter(F.col("CdSku").isNotNull())
    )
    
    de_para_filial_cd = (
        df_base
        .select("cdfilial", "cd_secundario")
        .distinct()
        .filter(F.col("cdfilial").isNotNull())
        .withColumn(
            "cd_vinculo",
            F.coalesce(F.col("cd_secundario"), F.lit("SEM_CD"))
        )
        .drop("cd_secundario")
    )
    
    print(f"✅ De-para filial → CD criado: {de_para_filial_cd.count():,} filiais")
    return de_para_filial_cd

# COMMAND ----------

def calcular_merecimento_cd(df: DataFrame, data_calculo: str, categoria: str) -> DataFrame:
    """
    Calcula o merecimento a nível CD por grupo de necessidade usando APENAS média aparada 90 dias.
    Retorna o percentual que cada CD representa dentro da Cia.
    """
    print(f"🔄 Calculando merecimento CD para categoria: {categoria} (média aparada 90 dias)")
    
    df_data_calculo = df.filter(F.col("DtAtual") == data_calculo)

    df_data_calculo = (
        df_data_calculo
        .orderBy('CdFilial')
        .dropDuplicates(subset=['CdFilial'])
    )
    
    # Usar apenas média aparada 90 dias para merecimento CD
    medida_cd = f"MediaAparada{JANELA_CD_MERECIMENTO}_Qt_venda_sem_ruptura"
    
    de_para_filial_cd = criar_de_para_filial_cd()
    df_com_cd = df_data_calculo.join(de_para_filial_cd, on="cdfilial", how="left")
    
    # ✅ AGREGAÇÃO COM PROTEÇÃO DUPLA:
    df_merecimento_cd = (
        df_com_cd
        .groupBy("cd_vinculo", "grupo_de_necessidade")
        .agg(
            # F.sum() já ignora NULLs, mas garantimos com coalesce
            F.sum(F.coalesce(F.col(medida_cd), F.lit(0))).alias(f"Total_{medida_cd}")
        )
    )
    
    # Calcular percentual do CD dentro da Cia
    w_total_cia = Window.partitionBy("grupo_de_necessidade")
    
    df_merecimento_cd = df_merecimento_cd.withColumn(
        f"Total_Cia_{medida_cd}",
        F.sum(F.col(f"Total_{medida_cd}")).over(w_total_cia)
    )
    
    df_merecimento_cd = df_merecimento_cd.withColumn(
        f"Merecimento_CD_{medida_cd}",
        F.when(F.col(f"Total_Cia_{medida_cd}") > 0,
            F.col(f"Total_{medida_cd}") / F.col(f"Total_Cia_{medida_cd}"))
        .otherwise(0)
    )

    df_merecimento_cd = (
        df_merecimento_cd
        .orderBy('cd_vinculo', 'grupo_de_necessidade')
        .dropDuplicates(subset=['cd_vinculo', 'grupo_de_necessidade'])
    )
    print(f"✅ Merecimento CD calculado: {df_merecimento_cd.count():,} registros (média aparada 90 dias)")
    return df_merecimento_cd


# COMMAND ----------

def calcular_merecimento_interno_cd(df: DataFrame, data_calculo: str, categoria: str) -> DataFrame:
    """
    Calcula a proporção interna de cada loja dentro do CD por grupo de necessidade.
    Usa médias aparadas de 90 a 360 dias.
    Mantém colunas: Total_<medida> e Proporcao_Interna_<medida>.
    """
    print(f"🔄 Calculando merecimento interno CD para categoria: {categoria} (médias aparadas 90-360 dias)")
    
    # Filtro pela data
    df_data_calculo = df.filter(F.col("DtAtual") == data_calculo)
    
    # Lista de medidas aparadas disponíveis
    medidas_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS_APARADAS]
    medidas = [m for m in medidas_aparadas if m in df_data_calculo.columns]
    
    print(f"  📊 Medidas disponíveis: {medidas}")
    
    # Join com de-para filial-CD
    de_para_filial_cd = criar_de_para_filial_cd()
    df_com_cd = df_data_calculo.join(de_para_filial_cd, on="cdfilial", how="left")
    
    # Agregar no nível filial × grupo_de_necessidade (somando os SKUs)
    aggs = [F.sum(F.coalesce(F.col(m), F.lit(0))).alias(m) for m in medidas]
    df_filial = (
        df_com_cd
        .groupBy("CdFilial", "cd_vinculo", "grupo_de_necessidade")
        .agg(*aggs)
    )
    
    # Janela no nível cd_vinculo × grupo_de_necessidade
    w_cd_grp = Window.partitionBy("cd_vinculo", "grupo_de_necessidade")
    df_out = df_filial
    for m in medidas:
        df_out = (
            df_out
            .withColumn(f"Total_{m}", F.sum(F.col(m)).over(w_cd_grp))
            .withColumn(
                f"Proporcao_Interna_{m}",
                F.when(F.col(f"Total_{m}") > 0, F.col(m) / F.col(f"Total_{m}")).otherwise(F.lit(0.0))
            )
        )

    print(f"✅ Merecimento interno CD calculado: {df_out.count():,} registros (médias aparadas 90-360 dias)")
    return df_out


# COMMAND ----------

def calcular_merecimento_final(df_merecimento_cd: DataFrame, 
                              df_merecimento_interno: DataFrame) -> DataFrame:
    """
    Calcula o merecimento final: Merecimento_CD × Proporcao_Interna
    Usa apenas médias aparadas de 90 a 360 dias.
    Retorna apenas CdFilial x grupo_de_necessidade com os merecimentos finais
    """
    print("🔄 Calculando merecimento final (médias aparadas 90-360 dias)...")
    
    # Medidas disponíveis (apenas médias aparadas)
    medidas_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS_APARADAS]
    
    # 1. Preparar dados do merecimento CD (cd_vinculo x grupo_de_necessidade)
    # CD usa apenas média aparada 90 dias
    medida_cd = f"MediaAparada{JANELA_CD_MERECIMENTO}_Qt_venda_sem_ruptura"
    colunas_cd = ["cd_vinculo", "grupo_de_necessidade", f"Merecimento_CD_{medida_cd}"]
    
    df_merecimento_cd_limpo = df_merecimento_cd.select(*colunas_cd)
    
    # 2. Adicionar cd_vinculo ao merecimento interno
    de_para_filial_cd = criar_de_para_filial_cd()
    df_merecimento_interno_com_cd = (
        df_merecimento_interno
        .join(de_para_filial_cd, on="CdFilial", how="left")
        .withColumn("cd_vinculo_final", F.coalesce(de_para_filial_cd["cd_vinculo"], F.lit("SEM_CD")))
        .drop("cd_vinculo")  # Remove a coluna ambígua
        .withColumnRenamed("cd_vinculo_final", "cd_vinculo")  # Renomeia para o nome final
    )
    
    # 3. Join entre merecimento CD e merecimento interno
    df_merecimento_final = (
        df_merecimento_interno_com_cd
        .orderBy("CdFilial", "cd_vinculo", "grupo_de_necessidade")
        .dropDuplicates(subset=["CdFilial", "cd_vinculo", "grupo_de_necessidade"])
        .join(
            df_merecimento_cd_limpo,
            on=["cd_vinculo", "grupo_de_necessidade"],
            how="left"
        )
    )
    
    # 4. Calcular merecimento final (multiplicação)
    # Para cada medida aparada, multiplicar pelo merecimento CD (90 dias)
    for medida in medidas_aparadas:
        if f"Proporcao_Interna_{medida}" in df_merecimento_final.columns:
            df_merecimento_final = df_merecimento_final.withColumn(
                f"Merecimento_Final_{medida}",
                F.col(f"Merecimento_CD_{medida_cd}") * F.col(f"Proporcao_Interna_{medida}")
            )
    
    # 5. Selecionar apenas colunas finais: CdFilial x grupo_de_necessidade
    colunas_finais = ["CdFilial", "grupo_de_necessidade"]
    for medida in medidas_aparadas:
        coluna_final = f"Merecimento_Final_{medida}"
        if coluna_final in df_merecimento_final.columns:
            colunas_finais.append(coluna_final)
    
    df_merecimento_final_limpo = df_merecimento_final.select(*colunas_finais)
    
    print(f"✅ Merecimento final calculado: {df_merecimento_final_limpo.count():,} registros")
    print(f"📊 Colunas finais: {colunas_finais}")
    
    # VALIDAÇÃO: Verificar se a multiplicação ainda soma 100% por grupo de necessidade
    print("🔍 Validando se a multiplicação dos dois níveis ainda soma 100%...")
    
    for medida in medidas_aparadas:
        coluna_final = f"Merecimento_Final_{medida}"
        if coluna_final in df_merecimento_final_limpo.columns:
            print(f"  Verificando medida: {medida}")
            
            # Calcular soma por grupo de necessidade
            df_validacao = (
                df_merecimento_final_limpo
                .groupBy("grupo_de_necessidade")
                .agg(F.sum(coluna_final).alias("soma_merecimento_final"))
                .withColumn("diferenca_100", F.abs(F.col("soma_merecimento_final") - 1.0))
                .orderBy(F.desc("diferenca_100"))
            )

    
    return df_merecimento_final_limpo

# COMMAND ----------

def criar_esqueleto_matriz_completa(df_com_grupo: DataFrame, data_calculo: str = "2025-09-15") -> DataFrame:
    """
    Cria esqueleto completo da matriz com cross join entre todas as filiais e SKUs.
    
    Processo:
    1. Pega todas as filiais de roteirizacaolojaativa
    2. Pega todos os SKUs que existem em df_base no dia especificado
    3. Faz cross join entre filiais e SKUs
    4. Adiciona grupo_de_necessidade para cada SKU
    5. Retorna esqueleto pronto para join com merecimento final
    
    Args:
        data_calculo: Data para buscar os SKUs (formato YYYY-MM-DD)
        
    Returns:
        DataFrame com CdFilial, CdSku, grupo_de_necessidade
    """
    print(f"🚀 Criando esqueleto da matriz completa para data: {data_calculo}")
    print("=" * 80)
    
    # 1. Carregar todas as filiais ativas
    print("📊 Passo 1: Carregando todas as filiais ativas...")
    df_filiais = (
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmRegiaoGeografica", "NmPorteLoja")
        .distinct()
        .filter(F.col("CdFilial").isNotNull())
    )
    
    filiais_count = df_filiais.count()
    print(f"  ✅ {filiais_count:,} filiais carregadas")

    df_gdn = df_com_grupo.select("CdSku", "grupo_de_necessidade").distinct()
    
    # 2. Carregar todos os SKUs que existem na data especificada
    print(f"📊 Passo 2: Carregando SKUs existentes em {data_calculo}...")
    df_skus_data = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
        .filter(F.col("DtAtual") == data_calculo)
        .select("CdSku")
        .distinct()
        .join(df_gdn, on="CdSku", how="inner")
        .filter(F.col("CdSku").isNotNull())
        .filter(F.col("grupo_de_necessidade").isNotNull())
    )
    
    skus_count = df_skus_data.count()
    print(f"  ✅ {skus_count:,} SKUs únicos encontrados")
    
    # 3. Cross join entre filiais e SKUs
    print("📊 Passo 3: Criando cross join filiais × SKUs...")
    df_esqueleto = (
        df_filiais
        .crossJoin(df_skus_data)
    )
    
    esqueleto_count = df_esqueleto.count()
    print(f"  ✅ Cross join criado: {esqueleto_count:,} combinações (filiais × SKUs)")
    
    # 4. Adicionar informações adicionais das filiais
    print("📊 Passo 4: Adicionando informações das filiais...")
    df_esqueleto_final = df_esqueleto.select(
        "CdFilial",
        "CdSku", 
        "grupo_de_necessidade",
        "NmRegiaoGeografica",
        "NmPorteLoja"
    )


    return df_esqueleto_final


# COMMAND ----------

def garantir_integridade_dados_pre_merecimento(df: DataFrame) -> DataFrame:
    """
    Garante integridade dos dados ANTES de qualquer cálculo de merecimento.
    Preenche NULLs com média 360d da própria combinação grupo+filial.
    """
    print("🛡️ Garantindo integridade dos dados pré-merecimento...")
    
    # Identificar colunas de médias aparadas
    colunas_medias_aparadas = [col for col in df.columns 
                              if col.startswith("MediaAparada") and col.endswith("_Qt_venda_sem_ruptura")]
    
    if not colunas_medias_aparadas:
        print("  ⚠️ Nenhuma coluna de média aparada encontrada")
        return df
    
    print(f"  📊 Tratando: {colunas_medias_aparadas}")
    
    # Janela de 360 dias para backup
    window_360 = Window.partitionBy("grupo_de_necessidade", "CdFilial").orderBy("DtAtual").rowsBetween(-360, 0)
    
    df_tratado = df
    
    for coluna in colunas_medias_aparadas:
        backup_col = f"{coluna}_backup360"
        
        # Contar NULLs antes
        nulls_antes = df_tratado.filter(F.col(coluna).isNull()).count()
        
        df_tratado = (
            df_tratado
            # Calcular média 360d apenas de valores não-nulos da própria combinação
            .withColumn(
                backup_col,
                F.avg(F.when(F.col(coluna).isNotNull(), F.col(coluna))).over(window_360)
            )
            # Preencher NULL APENAS se há histórico válido da própria combinação
            .withColumn(
                coluna,
                F.when(
                    F.col(coluna).isNull() & F.col(backup_col).isNotNull(), 
                    F.col(backup_col)
                )
                .otherwise(F.col(coluna))  # Mantém NULL se não há histórico próprio
            )
            .drop(backup_col)
        )
        
        # Contar NULLs depois
        nulls_depois = df_tratado.filter(F.col(coluna).isNull()).count()
        nulls_preenchidos = nulls_antes - nulls_depois
        
        print(f"    ✅ {coluna}: {nulls_preenchidos:,} NULLs preenchidos | {nulls_depois:,} restantes")
    
    return df_tratado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Função Principal de Execução

# COMMAND ----------

def executar_calculo_matriz_merecimento_completo(categoria: str, 
                                                data_inicio: str = "2024-07-01",
                                                data_calculo: str = "2025-08-31") -> DataFrame:
    """
    Função principal que executa todo o fluxo da matriz de merecimento.
    """
    print(f"🚀 Iniciando cálculo da matriz de merecimento para: {categoria}")
    print("=" * 80)
    
    try:
        # 1. Carregamento dos dados base
        df_base = carregar_dados_base(categoria, data_inicio)
        df_base.cache()

        # 2. Carregamento e aplicação do espelhamento de filiais
        df_espelhamento = carregar_de_para_espelhamento()
        df_base_com_espelhamento = aplicar_espelhamento_filiais(df_base, df_espelhamento)
        df_base_com_espelhamento.cache()

        # 3. Carregamento dos mapeamentos
        de_para_modelos, de_para_gemeos = carregar_mapeamentos_produtos(categoria)  

        # 4. Aplicação dos mapeamentos
        df_com_mapeamentos = aplicar_mapeamentos_produtos(
            df_base_com_espelhamento, categoria, de_para_modelos, de_para_gemeos
        )
        
        # 5. Definição do grupo_de_necessidade
        df_com_grupo = determinar_grupo_necessidade(categoria, df_com_mapeamentos)
    #     df_com_grupo = (
    #         df_com_grupo
    #         .filter(
    #             F.col("grupo_de_necessidade").isin(
    #         'Telef pp', 
    #         'TV 50 ALTO P', 
    #         'TV 55 ALTO P',
    #         'TV 43 PP',
    #         'TV 75 PP',
    #         'TV 75 ALTO P',
    #         'Telef Medio 256GB',
    #         'Telef Medio 128GB',
    #         'Telef Alto',
    #         )
    #     )
    # )
        df_com_grupo.cache()

        df_agregado = (
            df_com_grupo
            .groupBy("grupo_de_necessidade", "CdFilial", "DtAtual", "year_month")
            .agg(
                F.sum("QtMercadoria").alias("QtMercadoria"),
                F.sum("deltaRuptura").alias("deltaRuptura"),
                F.first("tipo_agrupamento").alias("tipo_agrupamento")
            )
        )
        
        # 6. Detecção de outliers
        df_stats, df_meses_atipicos = detectar_outliers_meses_atipicos(df_agregado, categoria)
        
        # 7. Filtragem de meses atípicos
        df_filtrado = filtrar_meses_atipicos(df_agregado, df_meses_atipicos)
        
        # 8. Remoção de outliers das séries históricas
        print("=" * 80)
        print("🔄 Aplicando remoção de outliers das séries históricas...")
        
        # Definir filiais de atacado (exemplo - ajustar conforme necessário)
        filiais_atacado = FILIAIS_ATACADO  # Lista de filiais consideradas de atacado
        
        df_sem_outliers = remover_outliers_series_historicas(
            df_filtrado,
            coluna_valor="QtMercadoria",
            n_sigmas_padrao=PARAMETROS_OUTLIERS["desvios_historico_loja"],
            n_sigmas_atacado=PARAMETROS_OUTLIERS["desvios_atacado_loja"],
            filiais_atacado=filiais_atacado
        )
        
        # 9. Cálculo das medidas centrais
        df_com_medidas = calcular_medidas_centrais_com_medias_aparadas(df_sem_outliers)
        
        # 10. Consolidação final
        df_final = consolidar_medidas(df_com_medidas)
        
        # ✅ 10.1 NOVO: Garantir integridade dos dados pré-merecimento
        df_final = garantir_integridade_dados_pre_merecimento(df_final)
        
        # 11. Cálculo de merecimento por CD e filial
        print("=" * 80)
        print("🔄 Iniciando cálculo de merecimento...")
        
        # 11.1 Merecimento a nível CD
        df_merecimento_cd = calcular_merecimento_cd(df_final, data_calculo, categoria)
        
        # 11.2 Merecimento interno ao CD
        df_merecimento_interno = calcular_merecimento_interno_cd(df_final, data_calculo, categoria)
        
        # 11.3 Merecimento final
        df_merecimento_final = calcular_merecimento_final(df_merecimento_cd, df_merecimento_interno)

        # Criar o esqueleto
        df_esqueleto = criar_esqueleto_matriz_completa(df_com_grupo, "2025-08-31")

        # Primeiro, identificar todas as colunas de merecimento final
        colunas_merecimento_final = [col for col in df_merecimento_final.columns 
                                if col.startswith('Merecimento_Final_')]


        # Criar dicionário de fillna
        fillna_dict = {col: 0.0 for col in colunas_merecimento_final}

        df_merecimento_sku_filial = (
            df_esqueleto
            .join(
                df_merecimento_final
                .select('grupo_de_necessidade', 'CdFilial', *colunas_merecimento_final)
                .dropDuplicates(subset=['grupo_de_necessidade', 'CdFilial']), 
                on=['grupo_de_necessidade', 'CdFilial'], 
                how='left'
            )
            .fillna(fillna_dict)
        )
        
        print("=" * 80)
        print(f"✅ Cálculo da matriz de merecimento concluído para: {categoria}")
        print(f"📊 Total de registros finais: {df_merecimento_sku_filial.count():,}")
        
        return df_merecimento_sku_filial
        
    except Exception as e:
        print(f"❌ Erro durante o cálculo: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execução Final - Cálculo e Salvamento de Todas as Categorias

# COMMAND ----------

# EXECUTAR CÁLCULO DA MATRIZ DE MERECIMENTO PARA TODAS AS CATEGORIAS
print("🚀 EXECUÇÃO FINAL - Calculando matriz de merecimento para todas as categorias...")
print("=" * 80)

# Lista de todas as categorias disponíveis
categorias = [
    "DIRETORIA DE TELAS",
    #"DIRETORIA TELEFONIA CELULAR", 
    #"DIRETORIA DE LINHA BRANCA",
    #"DIRETORIA LINHA LEVE",
    # "DIRETORIA INFO/PERIFERICOS"
]

resultados_finais = {}

for categoria in categorias:
    print(f"\n🔄 Processando: {categoria}")
    print("-" * 60)
    
    try:
        # Executa cálculo da matriz de merecimento
        df_matriz_final = executar_calculo_matriz_merecimento_completo(
            categoria=categoria,
            data_inicio="2024-07-01",
            data_calculo="2025-09-25"
        )
        
        # Salva em tabela específica da categoria
        categoria_normalizada = (
            categoria
            .replace("DIRETORIA ", "")
            .replace(" ", "_")
            .upper()
        )
        
        nome_tabela = f"databox.bcg_comum.supply_matriz_merecimento_{categoria_normalizada}_teste0110"
        
        print(f"💾 Salvando matriz de merecimento para: {categoria}")
        print(f"📊 Tabela: {nome_tabela}")
        
        (
            df_matriz_final
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(nome_tabela)
        )
        
        # Armazena resultado
        resultados_finais[categoria] = {
            "matriz": df_matriz_final,
            "tabela": nome_tabela,
            "status": "SUCESSO",
            "total_registros": df_matriz_final.count()
        }
        
        print(f"✅ {categoria} - Matriz calculada e salva com sucesso!")
        print(f"📊 Total de registros: {df_matriz_final.count():,}")
        
    except Exception as e:
        print(f"❌ {categoria} - Erro: {str(e)}")
        resultados_finais[categoria] = {
            "status": "ERRO",
            "erro": str(e)
        }

print("\n" + "=" * 80)
print("🎉 CÁLCULO DAS MATRIZES DE MERECIMENTO CONCLUÍDO!")
print("=" * 80)

# Exibe resumo dos resultados
print("📊 RESUMO DOS RESULTADOS:")
for categoria, resultado in resultados_finais.items():
    if resultado["status"] == "SUCESSO":
        print(f"  ✅ {categoria}: {resultado['total_registros']:,} registros → {resultado['tabela']}")
    else:
        print(f"  ❌ {categoria}: {resultado['erro']}")

print("\n" + "=" * 80)
print("🎯 SCRIPT DE CÁLCULO CONCLUÍDO!")
print("📋 Próximo passo: Executar script de análise de factual e comparações")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 RESUMO FINAL DO SCRIPT DE CÁLCULO
# MAGIC
# MAGIC ### **O que este script faz:**
# MAGIC 1. **Calcula matriz de merecimento** para todas as categorias
# MAGIC 2. **Salva em tabelas específicas** por categoria
# MAGIC 3. **PARA AQUI** - Não faz análise de factual nem comparações
# MAGIC
# MAGIC ### **Tabelas criadas:**
# MAGIC - `supply_matriz_merecimento_TELAS`
# MAGIC - `supply_matriz_merecimento_TELEFONIA_CELULAR`
# MAGIC - `supply_matriz_merecimento_LINHA_BRANCA`
# MAGIC - `supply_matriz_merecimento_LINHA_LEVE`
# MAGIC - `supply_matriz_merecimento_INFO_GAMES`
# MAGIC
# MAGIC ### **Próximo passo:**
# MAGIC Executar o script `analise_factual_comparacao_matrizes.py` para:
# MAGIC - Análise de factual
# MAGIC - Cálculo de sMAPE e WMAPE
# MAGIC - Comparação com matriz DRP geral
# MAGIC - Identificação de distorções
# MAGIC
# MAGIC **Este script está completo e finalizado!** 🎉
