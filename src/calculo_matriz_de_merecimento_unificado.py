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
    df_base.cache()
    
    print(f"✅ Dados carregados para '{categoria}':")
    print(f"  • Total de registros: {df_base.count():,}")
    print(f"  • Período: {data_inicio} até {df_base.agg(F.max('DtAtual')).collect()[0][0]}")
    
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
    
    # Cálculo das medianas móveis
    df_com_medianas = df_com_medias
    for dias in JANELAS_MOVEIS:
        df_com_medianas = df_com_medianas.withColumn(
            f"Mediana{dias}_Qt_venda_sem_ruptura",
            F.expr(f"percentile_approx(QtMercadoria, 0.5)").over(janelas[dias])
        )
    
    # Cálculo das médias móveis aparadas
    df_com_medias_aparadas = (
        add_media_aparada_rolling(
            df_com_medianas,
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

def executar_calculo_matriz_merecimento(categoria: str, 
                                       data_inicio: str = "2024-01-01",
                                       data_calculo: str = "2025-06-30",
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
        data_calculo: Data específica para cálculo de merecimento (formato YYYY-MM-DD)
        sigma_meses_atipicos: Número de desvios padrão para meses atípicos (padrão: 3.0)
        sigma_outliers_cd: Número de desvios padrão para outliers CD (padrão: 3.0)
        sigma_outliers_loja: Número de desvios padrão para outliers loja (padrão: 3.0)
        sigma_atacado_cd: Número de desvios padrão para outliers CD atacado (padrão: 1.5)
        sigma_atacado_loja: Número de desvios padrão para outliers loja atacado (padrão: 1.5)
        
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
        
        # 9.4 Join final com todas as informações
        df_resultado_final = df_final.join(
            df_merecimento_final.select("cdfilial", "cd_primario", "grupo_de_necessidade"),
            on=["cdfilial", "grupo_de_necessidade"],
            how="left"
        )
        
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
        
        return df_resultado_final
        
    except Exception as e:
        print(f"❌ Erro durante o cálculo: {str(e)}")
        raise

# COMMAND ----------

df_telas = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")
df_telas.display()

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
# MAGIC ## 13. Cálculo de Merecimento por CD e Filial

# COMMAND ----------

def criar_de_para_filial_cd() -> DataFrame:
    """
    Cria o mapeamento filial → CD usando dados da tabela base.
    
    Returns:
        DataFrame com mapeamento filial → CD
    """
    print("🔄 Criando de-para filial → CD...")
    
    # Carrega dados da tabela base para criar mapeamento
    df_base = spark.table('databox.bcg_comum.supply_base_merecimento_diario')
    
    # Cria mapeamento filial → CD
    de_para_filial_cd = (
        df_base
        .select("cdfilial", "cd_primario")
        .distinct()
        .filter(F.col("cdfilial").isNotNull())
        .filter(F.col("cd_primario").isNotNull())
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
        "Mediana90_Qt_venda_sem_ruptura", "Mediana180_Qt_venda_sem_ruptura",
        "Mediana270_Qt_venda_sem_ruptura", "Mediana360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # Adiciona de-para filial → CD
    de_para_filial_cd = criar_de_para_filial_cd()
    df_com_cd = df_data_calculo.join(de_para_filial_cd, on="cdfilial", how="left")
    
    # Agrega por CD e grupo de necessidade
    colunas_agregacao = ["cd_primario", "grupo_de_necessidade"] + medidas_disponiveis
    
    df_merecimento_cd = (
        df_com_cd
        .groupBy("cd_primario", "grupo_de_necessidade")
        .agg(*[
            F.sum(F.col(medida)).alias(f"Total_{medida}")
            for medida in medidas_disponiveis
        ])
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
        "Mediana90_Qt_venda_sem_ruptura", "Mediana180_Qt_venda_sem_ruptura",
        "Mediana270_Qt_venda_sem_ruptura", "Mediana360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # Calcula percentual por filial dentro de cada CD + grupo de necessidade
    df_merecimento_interno = df_com_cd
    
    for medida in medidas_disponiveis:
        # Janela para calcular percentual por CD + grupo de necessidade
        w_percentual = Window.partitionBy("cd_primario", "grupo_de_necessidade")
        
        df_merecimento_interno = df_merecimento_interno.withColumn(
            f"Percentual_{medida}",
            F.when(F.col(f"Total_{medida}") > 0,
                   F.col(f"Total_{medida}") / F.sum(F.col(f"Total_{medida}")).over(w_percentual))
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
    
    # Join entre merecimento CD e interno CD
    df_merecimento_final = (
        df_merecimento_interno
        .join(
            df_merecimento_cd,
            on=["cd_primario", "grupo_de_necessidade"],
            how="left"
        )
    )
    
    # Calcula merecimento final para cada medida
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "Mediana90_Qt_venda_sem_ruptura", "Mediana180_Qt_venda_sem_ruptura",
        "Mediana270_Qt_venda_sem_ruptura", "Mediana360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    for medida in medidas_disponiveis:
        df_merecimento_final = df_merecimento_final.withColumn(
            f"Merecimento_Final_{medida}",
            F.col(f"Total_{medida}") * F.col(f"Percentual_{medida}")
        )
    
    print(f"✅ Merecimento final calculado:")
    print(f"  • Total de registros: {df_merecimento_final.count():,}")
    print(f"  • Medidas calculadas: {len(medidas_disponiveis)}")
    
    return df_merecimento_final

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
# MAGIC
# MAGIC %md
# MAGIC Descomente a linha abaixo para executar um teste com a categoria desejada:

# COMMAND ----------

# EXECUTAR TESTE (descomente para testar)
# categoria_teste = "DIRETORIA DE TELAS"
# df_resultado = executar_calculo_matriz_merecimento(categoria_teste)
# validar_resultados(df_resultado, categoria_teste)
# display(df_resultado.limit(10))
