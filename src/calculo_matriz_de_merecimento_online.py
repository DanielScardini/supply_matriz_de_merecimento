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

DE_PARA_CONSOLIDACAO_CDS = {
  "14"  : "1401",
  "1635": "1200",
  "1500": "1200",
  "1640": "1401",
  "1088": "1200",
  "4760": "1760",
  "4400": "1400",
  "4887": "1887",
  "4475": "1475",
  "4445": "1445",
  "2200": "1200",
  "1736": "1887",
  "1792": "1887",
  "1875": "1887",
  "1999": "1887",
  "22"  : "1895",
  "1673": "1400",
  "1454": "1778",
}

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
            F.coalesce(F.col("DsVoltagem"), F.lit(""))
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
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3_online')
        .filter(F.col("NmAgrupamentoDiretoriaSetor") == categoria)
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
    Adiciona médias aparadas rolling para diferentes janelas.
    """
    out = df
    for dias in janelas:
        w = Window.partitionBy(*grupos).orderBy(F.col(col_ord)).rowsBetween(-dias, 0)

        ql = F.percentile_approx(F.col(col_val), F.lit(alpha)).over(w)
        qh = F.percentile_approx(F.col(col_val), F.lit(1 - alpha)).over(w)

        out = (
            out
            .withColumn(f"_ql_{dias}", ql)
            .withColumn(f"_qh_{dias}", qh)
        )

        cnt = F.count(F.col(col_val)).over(w)
        cond = (F.col(col_val) >= F.col(f"_ql_{dias}")) & (F.col(col_val) <= F.col(f"_qh_{dias}"))
        sum_trim = F.sum(F.when(cond, F.col(col_val)).otherwise(F.lit(0))).over(w)
        cnt_trim = F.sum(F.when(cond, F.lit(1)).otherwise(F.lit(0))).over(w)

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
    """
    print("🔄 Calculando medidas centrais com médias aparadas...")
    
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
    )       
    
    lista = ", ".join(str(f) for f in FILIAIS_OUTLET)
    print(f"🏬 Zerando a demanda das filiais [{lista}] ⚠️ pois não são abastecidas via CD normalmente.")

    janelas = {}
    for dias in JANELAS_MOVEIS:
        janelas[dias] = Window.partitionBy("CdSku", "CdFilial").orderBy("DtAtual").rowsBetween(-dias, 0)
    
    df_com_medias = df_sem_ruptura
    for dias in JANELAS_MOVEIS:
        df_com_medias = df_com_medias.withColumn(
            f"Media{dias}_Qt_venda_sem_ruptura",
            F.avg("demanda_robusta").over(janelas[dias])
        )
    
    df_com_medias_aparadas = (
        add_media_aparada_rolling(
            df_com_medias,
            janelas=JANELAS_MOVEIS,
            col_val="demanda_robusta",
            col_ord="DtAtual",
            grupos=("CdSku","CdFilial"),
            alpha=PERCENTUAL_CORTE_MEDIAS_APARADAS,
            min_obs=10
        )
    )
    
    print("✅ Medidas centrais calculadas")
    return df_com_medias_aparadas

# COMMAND ----------

def consolidar_medidas(df: DataFrame) -> DataFrame:
    """
    Consolida todas as medidas calculadas em uma base única.
    """
    print("🔄 Consolidando medidas...")
    
    colunas_medias = [f"Media{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    colunas_medias_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS]
    
    df_consolidado = (
        df.select(
            "DtAtual", "CdSku", "CdFilial", "grupo_de_necessidade", "year_month",
            "QtMercadoria", "Receita", "FlagRuptura", "deltaRuptura", "tipo_agrupamento",
            *colunas_medias,
            *colunas_medias_aparadas
        )
        .fillna(0, subset=colunas_medias + colunas_medias_aparadas)
    )
    
    print("✅ Medidas consolidadas")
    return df_consolidado

# COMMAND ----------

def criar_de_para_filial_cd() -> DataFrame:
    """
    Cria o mapeamento filial → CD usando dados da tabela base.
    """
    print("🔄 Criando de-para filial → CD...")
    
    df_base = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3_online')
        .filter(F.col("DtAtual") == "2025-08-01")
        .filter(F.col("CdSku").isNotNull())
        .withColumn("cd_secundario",
            F.when(
                F.col("DsEstoqueLojaDeposito") == 'D', F.col("cdfilial")
            )
            .otherwise(F.col("cd_secundario"))
        )
    )
    

    # normaliza None → 0 para depois filtrar
    dict_norm = {int(k): (int(v) if v is not None else 0) for k, v in DE_PARA_CONSOLIDACAO_CDS.items()}

    # constrói expressão de mapeamento
    mapping_expr = F.create_map([F.lit(x) for kv in dict_norm.items() for x in kv])

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
        # aplica substituição
        .withColumn(
            "cd_vinculo",
            F.coalesce(mapping_expr.getItem(F.col("cd_vinculo").cast("int")), F.col("cd_vinculo"))
        )
        # se quiser descartar os que viraram 0 (None no dict)
        .filter(F.col("cd_vinculo") != F.lit(0))
        .fillna("SEM_CD", subset="cd_vinculo")
    )
        
    print(f"✅ De-para filial → CD criado: {de_para_filial_cd.count():,} filiais")
    return de_para_filial_cd

# COMMAND ----------

def calcular_merecimento_cd(df: DataFrame, data_calculo: str, categoria: str) -> DataFrame:
    """
    Calcula o merecimento a nível CD por grupo de necessidade.
    Retorna o percentual que cada CD representa dentro da Cia.
    """
    print(f"🔄 Calculando merecimento CD para categoria: {categoria}")
    
    df_data_calculo = df.filter(F.col("DtAtual") == data_calculo)

    df_data_calculo = (
        df_data_calculo
        .orderBy('CdSku', 'CdFilial')
        .dropDuplicates(subset=['CdSku', 'CdFilial'])
    )
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    de_para_filial_cd = criar_de_para_filial_cd()
    df_com_cd = df_data_calculo.join(de_para_filial_cd, on="cdfilial", how="left")
    
    aggs_cd = []
    for medida in medidas_disponiveis:
        if medida in df_com_cd.columns:
            aggs_cd.append(F.sum(F.col(medida)).alias(f"Total_{medida}"))
    
    df_merecimento_cd = (
        df_com_cd
        .groupBy("cd_vinculo", "grupo_de_necessidade")
        .agg(*aggs_cd)
    )
    
    # NOVO: Calcular percentual do CD dentro da Cia
    for medida in medidas_disponiveis:
        coluna_total = f"Total_{medida}"
        if coluna_total in df_merecimento_cd.columns:  # ← VERIFICAR Total_{medida}
            w_total_cia = Window.partitionBy("grupo_de_necessidade")
            
            df_merecimento_cd = df_merecimento_cd.withColumn(
                f"Total_Cia_{medida}",
                F.sum(F.col(coluna_total)).over(w_total_cia)  # ← USAR coluna_total
            )
            
            df_merecimento_cd = df_merecimento_cd.withColumn(
                f"Merecimento_CD_{medida}",
                F.when(F.col(f"Total_Cia_{medida}") > 0,
                    F.col(coluna_total) / F.col(f"Total_Cia_{medida}"))  # ← USAR coluna_total
                .otherwise(0)
            )

    df_merecimento_cd = (
        df_merecimento_cd
        .orderBy('cd_vinculo', 'grupo_de_necessidade')
        .dropDuplicates(subset=['cd_vinculo', 'grupo_de_necessidade'])
    )
    print(f"✅ Merecimento CD calculado: {df_merecimento_cd.count():,} registros")
    return df_merecimento_cd

# COMMAND ----------

def calcular_merecimento_interno_cd(df: DataFrame, data_calculo: str, categoria: str) -> DataFrame:
    """
    Calcula a proporção interna de cada loja dentro do CD por grupo de necessidade.
    Mantém colunas: Total_<medida> e Proporcao_Interna_<medida>.
    """
    print(f"🔄 Calculando merecimento interno CD para categoria: {categoria}")
    
    # Filtro pela data
    df_data_calculo = df.filter(F.col("DtAtual") == data_calculo)
    
    # Lista de medidas
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    medidas = [m for m in medidas_disponiveis if m in df_data_calculo.columns]
    
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
    
    # Janela no nível cd_primario × grupo_de_necessidade
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

    print(f"✅ Merecimento interno CD calculado: {df_out.count():,} registros")
    return df_out

# COMMAND ----------

def calcular_merecimento_final(df_merecimento_cd: DataFrame, 
                              df_merecimento_interno: DataFrame) -> DataFrame:
    """
    Calcula o merecimento final: Merecimento_CD × Proporcao_Interna
    Retorna apenas CdFilial x grupo_de_necessidade com os merecimentos finais
    """
    print("🔄 Calculando merecimento final...")
    
    medidas_disponiveis = [
        "Media90_Qt_venda_sem_ruptura", "Media180_Qt_venda_sem_ruptura", 
        "Media270_Qt_venda_sem_ruptura", "Media360_Qt_venda_sem_ruptura",
        "MediaAparada90_Qt_venda_sem_ruptura", "MediaAparada180_Qt_venda_sem_ruptura",
        "MediaAparada270_Qt_venda_sem_ruptura", "MediaAparada360_Qt_venda_sem_ruptura"
    ]
    
    # 1. Preparar dados do merecimento CD (cd_primario x grupo_de_necessidade)
    colunas_cd = ["cd_vinculo", "grupo_de_necessidade"]
    for medida in medidas_disponiveis:
        if f"Merecimento_CD_{medida}" in df_merecimento_cd.columns:
            colunas_cd.append(f"Merecimento_CD_{medida}")
    
    df_merecimento_cd_limpo = df_merecimento_cd.select(*colunas_cd)

    
    # 2. Adicionar cd_primario ao merecimento interno (especificando qual coluna usar)
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
    for medida in medidas_disponiveis:
        if (f"Merecimento_CD_{medida}" in df_merecimento_final.columns and 
            f"Proporcao_Interna_{medida}" in df_merecimento_final.columns):
            df_merecimento_final = df_merecimento_final.withColumn(
                f"Merecimento_Final_{medida}",
                F.col(f"Merecimento_CD_{medida}") * F.col(f"Proporcao_Interna_{medida}")
            )
    
    # 5. Selecionar apenas colunas finais: CdFilial x grupo_de_necessidade
    colunas_finais = ["CdFilial", "grupo_de_necessidade"]
    for medida in medidas_disponiveis:
        coluna_final = f"Merecimento_Final_{medida}"
        if coluna_final in df_merecimento_final.columns:
            colunas_finais.append(coluna_final)
    
    df_merecimento_final_limpo = df_merecimento_final.select(*colunas_finais)
    
    print(f"✅ Merecimento final calculado: {df_merecimento_final_limpo.count():,} registros")
    print(f"📊 Colunas finais: {colunas_finais}")
    
    # VALIDAÇÃO: Verificar se a multiplicação ainda soma 100% por grupo de necessidade
    print("🔍 Validando se a multiplicação dos dois níveis ainda soma 100%...")
    
    for medida in medidas_disponiveis:
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

def criar_esqueleto_matriz_completa(df_com_grupo: DataFrame, data_calculo: str = "2025-07-31") -> DataFrame:
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
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3_online')
        .select("CdFilial")
        .distinct()
        .filter(F.col("CdFilial").isNotNull())
    )
    
    filiais_count = df_filiais.count()
    print(f"  ✅ {filiais_count:,} filiais carregadas")

    df_gdn = df_com_grupo.select("CdSku", "grupo_de_necessidade").distinct()
    
    # 2. Carregar todos os SKUs que existem na data especificada
    print(f"📊 Passo 2: Carregando SKUs existentes em {data_calculo}...")
    df_skus_data = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v3_online')
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
       # "NmRegiaoGeografica",
        #"NmPorteLoja"
    )


    return df_esqueleto_final


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Função Principal de Execução

# COMMAND ----------

def executar_calculo_matriz_merecimento_completo(categoria: str, 
                                                data_inicio: str = "2024-07-01",
                                                data_calculo: str = "2025-07-31") -> DataFrame:
    """
    Função principal que executa todo o fluxo da matriz de merecimento.
    """
    print(f"🚀 Iniciando cálculo da matriz de merecimento para: {categoria}")
    print("=" * 80)
    
    try:
        # 1. Carregamento dos dados base
        df_base = carregar_dados_base(categoria, data_inicio)
        df_base.cache()

        # 2. Carregamento dos mapeamentos
        de_para_modelos, de_para_gemeos = carregar_mapeamentos_produtos(categoria)  

        # 3. Aplicação dos mapeamentos
        df_com_mapeamentos = aplicar_mapeamentos_produtos(
            df_base, categoria, de_para_modelos, de_para_gemeos
        )
        
        # 4. Definição do grupo_de_necessidade
        df_com_grupo = determinar_grupo_necessidade(categoria, df_com_mapeamentos)
        # df_com_grupo = (
        #     df_com_grupo
        #     .filter(
        #         F.col("grupo_de_necessidade").isin('Telef pp', 'TV 50 ALTO P', 'TV 55 ALTO P')
        #     )
        # )
        df_com_grupo.cache()
        
        # 5. Detecção de outliers
        df_stats, df_meses_atipicos = detectar_outliers_meses_atipicos(df_com_grupo, categoria)
        
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
        
        # 9.2 Merecimento interno ao CD
        df_merecimento_interno = calcular_merecimento_interno_cd(df_final, data_calculo, categoria)
        
        # 9.3 Merecimento final
        df_merecimento_final = calcular_merecimento_final(df_merecimento_cd, df_merecimento_interno)

        # Criar o esqueleto
        df_esqueleto = criar_esqueleto_matriz_completa(df_com_grupo, "2025-08-30")

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
    "DIRETORIA TELEFONIA CELULAR", 
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
            data_calculo="2025-08-30"
        )
        
        # Salva em tabela específica da categoria
        categoria_normalizada = (
            categoria
            .replace("DIRETORIA ", "")
            .replace(" ", "_")
            .upper()
        )
        
        nome_tabela = f"databox.bcg_comum.supply_matriz_merecimento_{categoria_normalizada}_online_teste0809"
        
        print(f"💾 Salvando matriz de merecimento para: {categoria}")
        print(f"📊 Tabela: {nome_tabela}")
        
        (
            df_matriz_final
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
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

# COMMAND ----------

categoria = "DIRETORIA TELEFONIA CELULAR"
data_calculo = "2025-08-30"

# 1. Carregamento dos dados base
df_base = carregar_dados_base(categoria, data_inicio)
df_base.cache()

# 2. Carregamento dos mapeamentos
de_para_modelos, de_para_gemeos = carregar_mapeamentos_produtos(categoria)  

# 3. Aplicação dos mapeamentos
df_com_mapeamentos = aplicar_mapeamentos_produtos(
    df_base, categoria, de_para_modelos, de_para_gemeos
)

# 4. Definição do grupo_de_necessidade
df_com_grupo = determinar_grupo_necessidade(categoria, df_com_mapeamentos)
df_com_grupo = (
    df_com_grupo
    .filter(
        F.col("grupo_de_necessidade").isin('Telef pp', 'TV 50 ALTO P', 'TV 55 ALTO P')
    )
)
df_com_grupo.cache()

# 5. Detecção de outliers
df_stats, df_meses_atipicos = detectar_outliers_meses_atipicos(df_com_grupo, categoria)

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

# 9.2 Merecimento interno ao CD
df_merecimento_interno = calcular_merecimento_interno_cd(df_final, data_calculo, categoria)

# 9.3 Merecimento final
df_merecimento_final = calcular_merecimento_final(df_merecimento_cd, df_merecimento_interno)


df_merecimento_final.cache()
df_merecimento_final.display()

# COMMAND ----------

df_merecimento_cd.cache()
df_merecimento_cd.display()

# COMMAND ----------

# Criar o esqueleto
df_esqueleto = criar_esqueleto_matriz_completa(df_com_grupo, "2025-08-30")

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
