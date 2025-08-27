# Databricks notebook source
# MAGIC %md
# MAGIC # Matriz de Merecimento - Preparação de Tabelas
# MAGIC
# MAGIC Este notebook implementa a preparação de tabelas para análise de matriz de merecimento
# MAGIC em sistema de supply chain, utilizando PySpark para processamento de dados.
# MAGIC
# MAGIC **Author**: Scardini  
# MAGIC **Date**: 2025  
# MAGIC **Purpose**: Preparar tabelas para análise de matriz de merecimento e estoque

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports e Configuração Inicial

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
from typing import List, Optional

# Inicialização do Spark
spark = SparkSession.builder.appName("impacto_apostas").getOrCreate()
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
# MAGIC ## Base de Estoque Lojas

# COMMAND ----------

def load_estoque_loja_data(spark: SparkSession) -> DataFrame:
    """
    Carrega dados de estoque das lojas ativas.
    
    Args:
        spark: Sessão do Spark
        current_year: Ano atual para filtro de partição
        
    Returns:
        DataFrame com dados de estoque das lojas, incluindo:
        - Informações da filial e SKU
        - Dados de estoque e classificação
        - Métricas de DDE e faixas
    """
    return (
        spark.read.table("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
        .filter(F.col("DtAtual") >= data_inicio)
        .filter(F.col("StLoja") == "ATIVA")
        .filter(F.col("DsEstoqueLojaDeposito") == "L")
        .select(
            "CdFilial", 
            "CdSku",
            "DsSku",
            "DsSetor",
            "DsCurva",
            "DsCurvaAbcLoja",
            "StLinha",
            "DsObrigatorio",
            F.col("DsTipoEntrega").alias("TipoEntrega"),
            F.col("CdEstoqueFilialAbastecimento").alias("QtdEstoqueCDVinculado"),
            (F.col("VrTotalVv")/F.col("VrVndCmv")).alias("DDE"),
            F.col("QtEstoqueBoaOff").alias("EstoqueLoja"),
            F.col("DsFaixaDde").alias("ClassificacaoDDE"),
            F.col("data_ingestao"),
            F.date_format(F.col("data_ingestao"), "yyyy-MM-dd").alias("DtAtual")    
        )
        .dropDuplicates(["DtAtual", "CdSku", "CdFilial"])
    )

df_estoque_loja = load_estoque_loja_data(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base de Mercadoria

# COMMAND ----------

def load_mercadoria_data(spark: SparkSession) -> DataFrame:
    """
    Carrega dados de mercadorias com suas classificações gerenciais.
    
    Args:
        spark: Sessão do Spark
        
    Returns:
        DataFrame com dados de mercadorias incluindo:
        - SKU da loja
        - Agrupamentos por diretoria, setor, classe e espécie
    """
    return (
        spark.table('data_engineering_prd.app_venda.mercadoria')
        .filter(F.col("StUltimaVersaoMercadoria") == "Y")
        .filter(~
            F.col("NmAgrupamentoDiretoriaSetor")
            .isin(
                ["DIRETORIA DE LINHA BRANCA",
                 "DIRETORIA LINHA LEVE",
                 "DIRETORIA DE TELAS",
                 "DIRETORIA TELEFONIA CELULAR",
                 "DIRETORIA INFO GAMES"]
            )
        )
        .select(
            "CdSkuLoja",
            "NmAgrupamentoDiretoriaSetor",
            "NmSetorGerencial",
            "NmClasseGerencial",
            "NmEspecieGerencial"
        )
        .withColumnRenamed("CdSkuLoja", "CdSku")
        .dropDuplicates(["CdSku"])
    )

df_mercadoria = load_mercadoria_data(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base de Vendas

# COMMAND ----------

def build_sales_view(
    spark: SparkSession,
    start_date: int = data_inicio_int,
    end_date: int = hoje_int,
) -> DataFrame:
    """
    Constrói uma visão unificada e agregada de vendas.
    
    Args:
        spark: Sessão do Spark
        start_date: Data de início no formato YYYYMMDD
        end_date: Data de fim no formato YYYYMMDD
        
    Returns:
        DataFrame com:
        - year_month (YYYYMM)
        - NmUF, NmRegiaoGeografica
        - CdSkuLoja, NmTipoNegocio
        - Receita (sum of VrOperacao)
        - QtMercadoria (sum of QtMercadoria)
        - merchandising attributes from mercadoria table
    """
    # load tables
    df_rateada = spark.table("app_venda.vendafaturadarateada")
    df_nao_rateada = spark.table("app_venda.vendafaturadanaorateada")

    # unify and filter
    df = (
        df_rateada
        .filter(F.col("NmTipoNegocio") == 'LOJA FISICA')
        .join(df_nao_rateada.select("ChaveFatos","QtMercadoria"), on="ChaveFatos")
        .filter(
            F.col("DtAprovacao").between(start_date, end_date)
            & (F.col("VrOperacao") >= 0)
            & (F.col("VrCustoContabilFilialSku") >= 0)
            & (F.col("QtMercadoria") >= 0)
        )
        .withColumn(
            "year_month",
            F.date_format(F.to_date(F.col("DtAprovacao").cast("string"), "yyyyMMdd"), "yyyyMM").cast("int")
        )
        .withColumnRenamed("CdFilialVenda", "CdFilial")
        .withColumn("DtAtual",
            F.date_format(F.to_date(F.col("DtAprovacao").cast("string"), "yyyyMMdd"), "yyyy-MM-dd"))
    )

    # aggregate
    df_agg = (
        df.groupBy(
            "DtAtual",
            "year_month",
            "CdSkuLoja",
            "CdFilial",
        )
        .agg(
            F.sum("VrOperacao").alias("Receita"),
            F.sum("QtMercadoria").alias("QtMercadoria"),
            F.sum("VrCustoContabilFilialSku").alias("Custo")
        )
    )

    # 1) Calendário diário (DateType)
    cal = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(str(start_date)), "yyyyMMdd"),
                    F.to_date(F.lit(str(end_date)),   "yyyyMMdd"),
                    F.expr("interval 1 day")
                )
            ).alias("DtAtual_date")
        )
    )

    # 2) Conjunto de chaves (Filial x SKU) pertinentes a LOJA FISICA
    keys = (
        df
          .select("CdFilial", "CdSkuLoja")
          .dropDuplicates()
    )

    # 3) Grade completa (Dt x Filial x SKU)
    grade = cal.crossJoin(keys)

    # 4) Agregado com Dt como DateType para o join
    df_agg_d = df_agg.withColumn("DtAtual_date", F.to_date("DtAtual"))

    # 5) Left join + zeros onde não houver venda
    result = (
        grade.join(
            df_agg_d,
            on=["DtAtual_date",  "CdSkuLoja", "CdFilial"],
            how="left"
        )
        .withColumn("Receita",      F.coalesce(F.col("Receita"),      F.lit(0.0)))
        .withColumn("QtMercadoria", F.coalesce(F.col("QtMercadoria"), F.lit(0.0)))
        .withColumn("year_month", F.date_format(F.col("DtAtual_date"), "yyyyMM").cast("int"))
        .withColumn("DtAtual",    F.date_format(F.col("DtAtual_date"), "yyyy-MM-dd"))
        .withColumnRenamed("CdSkuLoja", "CdSku")
        .select("DtAtual", "year_month", "CdFilial", "CdSku",  "Receita", "QtMercadoria")
        .withColumn("TeveVenda",
                    F.when(F.col("QtMercadoria") > 0, F.lit(1))
                    .otherwise(F.lit(0)))
    )

    return result

# Executar a função de vendas
sales_df = build_sales_view(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join para Chegar em Estoque e Tabelas

# COMMAND ----------

def create_base_merecimento(
    df_estoque: DataFrame, 
    sales_df: DataFrame, 
    df_mercadoria: DataFrame
) -> DataFrame:
    """
    Cria a base de merecimento unindo estoque, vendas e mercadorias.
    
    Args:
        df_estoque: DataFrame com dados de estoque
        sales_df: DataFrame com dados de vendas
        df_mercadoria: DataFrame com dados de mercadorias
        
    Returns:
        DataFrame unificado com todas as informações base
    """
    return (
        df_estoque
        .join(sales_df, on=["DtAtual", "CdFilial", "CdSku"], how="left")
        .join(df_mercadoria, on="CdSku", how="left")
    )

df_merecimento_base = create_base_merecimento(df_estoque_loja, sales_df, df_mercadoria)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de Métricas de Média Móvel de 90 Dias

# COMMAND ----------

def add_rolling_90_metrics(df: DataFrame) -> DataFrame:
    """
    Adiciona médias móveis de 90 dias para métricas de receita e quantidade.
    
    Args:
        df: DataFrame com dados de estoque e vendas
        
    Returns:
        DataFrame com métricas de média móvel de 90 dias:
        - Media90_Receita_venda_estq: Média de receita dos últimos 90 dias
        - Media90_Qt_venda_estq: Média de quantidade vendida dos últimos 90 dias
        
    Note:
        Considera apenas dias com EstoqueLoja >= 1 para o cálculo.
        Janela calculada por (CdFilial, CdSku) ordenada por dia.
    """
    # Garantir coluna de data e índice numérico de dias para janela por tempo
    df2 = (
        df
        .withColumn("DtAtual_date", F.to_date("DtAtual"))  # espera yyyy-MM-dd
        .withColumn("DayIdx", F.datediff(F.col("DtAtual_date"), F.lit("1970-01-01")))
    )

    # Condição de inclusão no cálculo da média
    cond = (F.col("EstoqueLoja") >= 1)

    # Janela de 90 dias (inclui o dia corrente): range em DIAS usando DayIdx
    w90 = (
        Window
        .partitionBy("CdFilial", "CdSku")
        .orderBy(F.col("DayIdx"))
        .rangeBetween(-89, 0)  # últimos 90 dias
    )

    # Cálculo das médias ignorando dias fora da condição (avg ignora NULL)
    df3 = (
        df2
        .withColumn(
            "Media90_Receita_venda_estq",
            F.avg(F.when(cond, F.col("Receita"))).over(w90)
        )
        .withColumn(
            "Media90_Qt_venda_estq",
            F.avg(F.when(cond, F.col("QtMercadoria"))).over(w90)
        )
    )

    # Preencher ausência de histórico válido com 0.0
    df3 = df3.fillna({
        "Media90_Receita_venda_estq": 0.0,
        "Media90_Qt_venda_estq": 0.0
    })

    # Manter TODAS as colunas existentes + novas métricas
    # Não filtrar por lista específica para evitar perda de dados
    return df3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise com Flags de Ruptura

# COMMAND ----------

def create_analysis_with_rupture_flags(df: DataFrame) -> DataFrame:
    """
    Cria análise com flags de ruptura e métricas calculadas.
    
    Args:
        df: DataFrame com métricas de média móvel de 90 dias
        
    Returns:
        DataFrame com flags e métricas de ruptura:
        - FlagRuptura: Indica se há ruptura (1) ou não (0)
        - deltaRuptura: Diferença entre demanda média e estoque
        - PrecoMedio90: Preço médio dos últimos 90 dias
        - ReceitaPerdidaRuptura: Receita perdida devido à ruptura
    """
    return (
        df
        .withColumn("FlagRuptura",
                    F.when(
                        (F.col("Media90_Qt_venda_estq") > F.col("EstoqueLoja")) &
                        (F.col('DsObrigatorio') == 'S'), F.lit(1))
                    .otherwise(F.lit(0)))
        .withColumn("deltaRuptura",
                    F.when(
                        F.col("FlagRuptura") == 1,
                        F.col("Media90_Qt_venda_estq") - F.col("EstoqueLoja")
                    ))
        .withColumn("PrecoMedio90",
                    F.col("Media90_Receita_venda_estq")/F.col("Media90_Qt_venda_estq"))
        .withColumn("ReceitaPerdidaRuptura",
                F.when(F.col('DsObrigatorio') == 'S',
                   F.col("deltaRuptura") * F.col("PrecoMedio90")
                   )
        .otherwise(F.lit(0))
        )
    )

df_merecimento_base_r90 = add_rolling_90_metrics(df_merecimento_base)
df_merecimento_base_r90 = create_analysis_with_rupture_flags(df_merecimento_base_r90)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Funções de Normalização e Carregamento de Dados

# COMMAND ----------

def normalize_ids(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    Normaliza IDs removendo zeros à esquerda e fazendo trim.
    
    Args:
        df: DataFrame a ser processado
        cols: Lista de colunas de ID para normalizar
        
    Returns:
        DataFrame com IDs normalizados
    """
    for c in cols:
        df = df.withColumn(
            c,
            F.when(F.col(c).isNull(), F.lit(None))
             .otherwise(
                 F.regexp_replace(
                     F.trim(F.col(c).cast("string")),
                     r"^0+(?!$)",   # remove zeros à esquerda, mas preserva "0"
                     ""
                 )
             )
        )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento de Características de CDs e Lojas

# COMMAND ----------

def load_cd_characteristics(spark: SparkSession) -> DataFrame:
    """
    Carrega características dos Centros de Distribuição.
    
    Args:
        spark: Sessão do Spark
        
    Returns:
        DataFrame com características dos CDs:
        - Código e nome da filial
        - Cidade/UF
        - Tipo de filial
    """
    return (
        spark.table("data_engineering_prd.app_operacoesloja.roteirizacaocentrodistribuicao")
        .select(
            F.col("CdFilial"),
            "NmFilial",
            F.concat_ws("/", F.col("NmCidade"), F.col("NmUF")).alias("NmCidade_UF"),
            "NmTipoFilial"
        )
        .distinct()
    )

def load_store_characteristics(spark: SparkSession) -> DataFrame:
    """
    Carrega características das lojas ativas.
    
    Args:
        spark: Sessão do Spark
        
    Returns:
        DataFrame com características das lojas:
        - Informações de bandeira, localização, porte e tipo
        - Coordenadas geográficas
    """
    return (
        spark.table("data_engineering_prd.app_operacoesloja.roteirizacaolojaativa")
        .select(
            F.col("NmBandeira").alias("BandeiraLoja"),
            F.col("CdFilial"),
            F.col("NmFilial").alias("NmLoja"),
            F.col("NmCidade").alias("NmCidadeLoja"),
            F.col("NmUF").alias("NmUFLoja"),
            F.col("NrCEP").alias("CEPLoja"),
            F.col("NmPorteLoja"),
            F.col("NmTipoLoja").alias("TipoLoja"),
            F.col("CdLatitude").alias("LatitudeLoja"),
            F.col("CdLongitude").alias("LongitudeLoja"),
        )
    )

def load_supply_plan_mapping(spark: SparkSession) -> DataFrame:
    """
    Carrega mapeamento de plano de abastecimento.
    
    Args:
        spark: Sessão do Spark
        current_date: Data atual para filtro de ingestão
        
    Returns:
        DataFrame com mapeamento de abastecimento:
        - Relacionamento entre CDs e lojas
        - Lead time e características de entrega
        - Capacidade de carga e horários
    """
    return (
        spark.table("context_abastecimento_inteligente.PlanoAbastecimento")
        .filter(
            (F.col("AaIngestao") == hoje.year) &
            (F.col("MmIngestao") == hoje.month) &
            (F.col("DdIngestao") == hoje.day)
        )
        .select(
            F.col("CdFilialAtende").alias("CD_primario"),
            F.col("CdFilialEntrega").alias("CD_secundario"),
            F.col("CdLoja").alias("CdFilial"),
            F.col("QtdDiasViagem").alias("LeadTime"),
            F.col("QtdCargasDia").alias("QtdCargasDia"),
            F.col("DsCubagemCaminhao").alias("DsCubagemCaminhao"),
            F.col("DsGrupoHorario").alias("DsGrupoHorario"),
            F.col("QtdSegunda"),
            F.col("QtdTerca"),
            F.col("QtdQuarta"),
            F.col("QtdQuinta"),
            F.col("QtdSexta"),
            F.col("QtdSabado"),
            F.col("QtdDomingo"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação do Mapeamento Completo de Abastecimento

# COMMAND ----------

def create_complete_supply_mapping(
    spark: SparkSession, 
    current_date: datetime
) -> DataFrame:
    """
    Cria mapeamento completo de abastecimento com características de CDs e lojas.
    
    Args:
        spark: Sessão do Spark
        current_date: Data atual para filtro
        
    Returns:
        DataFrame completo com mapeamento de abastecimento e características
    """
    # Carregar dados base
    caracteristicas_cd = load_cd_characteristics(spark)
    caracteristicas_loja = load_store_characteristics(spark)
    de_para_filial_CD = load_supply_plan_mapping(spark)
    
    # Normalizar IDs
    caracteristicas_cd = normalize_ids(caracteristicas_cd, ["CdFilial"])
    caracteristicas_loja = normalize_ids(caracteristicas_loja, ["CdFilial"])
    de_para_filial_CD = normalize_ids(de_para_filial_CD, ["CdFilial", "CD_primario", "CD_secundario"])
    
    # Construir mapeamento completo
    return (
        de_para_filial_CD
        # Características da loja
        .join(F.broadcast(caracteristicas_loja), on="CdFilial", how="inner")
        .select(
            "CdFilial",
            "BandeiraLoja", "NmLoja", "NmCidadeLoja", "NmUFLoja", "CEPLoja",
            "NmPorteLoja", "TipoLoja", "LatitudeLoja", "LongitudeLoja",
            "CD_primario", "CD_secundario", "LeadTime", "QtdCargasDia",
            "DsCubagemCaminhao", "DsGrupoHorario",
            "QtdSegunda", "QtdTerca", "QtdQuarta", "QtdQuinta",
            "QtdSexta", "QtdSabado", "QtdDomingo"
        )
        # Características CD primário
        .join(F.broadcast(
            caracteristicas_cd.withColumnRenamed("CdFilial", "CD_primario")),
            on="CD_primario",
            how="left"
        )
        .select(
            "CdFilial", "BandeiraLoja", "NmLoja", "NmCidadeLoja", "NmUFLoja", "CEPLoja",
            "NmPorteLoja", "TipoLoja", "LatitudeLoja", "LongitudeLoja",
            "CD_primario", *[F.col(c).alias(f"{c}_primario") for c in ["NmFilial", "NmCidade_UF", "NmTipoFilial"]],
            "CD_secundario", "LeadTime", "QtdCargasDia", "DsCubagemCaminhao", "DsGrupoHorario",
            "QtdSegunda", "QtdTerca", "QtdQuarta", "QtdQuinta",
            "QtdSexta", "QtdSabado", "QtdDomingo"
        )
        # Características CD secundário
        .join(
            F.broadcast(caracteristicas_cd.withColumnRenamed("CdFilial", "CD_secundario")),
            on="CD_secundario",
            how="left"
        )
        .select(
            "CdFilial", "BandeiraLoja", "NmLoja", "NmCidadeLoja", "NmUFLoja", "CEPLoja",
            "NmPorteLoja", "TipoLoja", "LatitudeLoja", "LongitudeLoja",
            "CdFilial", "Cd_primario", *[F.col(c).alias(f"{c}_primario") for c in ["NmFilial", "NmCidade_UF", "NmTipoFilial"]],
            "CD_secundario", *[F.col(c).alias(f"{c}_secundario") for c in ["NmFilial", "NmCidade_UF", "NmTipoFilial"]],
            "LeadTime", "QtdCargasDia", "DsCubagemCaminhao", "DsGrupoHorario",
            "QtdSegunda", "QtdTerca", "QtdQuarta", "QtdQuinta",
            "QtdSexta", "QtdSabado", "QtdDomingo"
        )
    )

# Carregar mapeamento completo
de_para_filial_CD = create_complete_supply_mapping(spark, hoje)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação da Base Final e Salvamento

# COMMAND ----------

def create_final_merecimento_base(
    df_merecimento: DataFrame, 
    supply_mapping: DataFrame
) -> DataFrame:
    """
    Cria base final de merecimento unindo dados de estoque com mapeamento de abastecimento.
    
    Args:
        df_merecimento: DataFrame base de merecimento
        supply_mapping: DataFrame com mapeamento de abastecimento
        
    Returns:
        DataFrame final com todas as informações de merecimento e abastecimento
    """
    return (
        df_merecimento
        .join(supply_mapping, on="CdFilial", how="left")
    )

df_merecimento_base_cd_loja = create_final_merecimento_base(df_merecimento_base_r90, de_para_filial_CD)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento da Tabela Final

# COMMAND ----------

def save_merecimento_table(df: DataFrame, table_name: str) -> None:
    """
    Salva DataFrame de merecimento como tabela Delta.
    
    Args:
        df: DataFrame a ser salvo
        table_name: Nome da tabela de destino
    """
    (
        df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
        .saveAsTable(table_name)
    )

# Salvar tabela final
save_merecimento_table(
    df_merecimento_base_cd_loja, 
    "databox.bcg_comum.supply_base_merecimento_diario_v2"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Processo Concluído
# MAGIC
# MAGIC A tabela de matriz de merecimento foi criada e salva com sucesso!
# MAGIC
# MAGIC **Tabela de destino**: `databox.bcg_comum.supply_base_merecimento_diario`
# MAGIC
# MAGIC **Conteúdo**:
# MAGIC - Dados de estoque das lojas
# MAGIC - Histórico de vendas com médias móveis de 90 dias
# MAGIC - Análise de ruptura e receita perdida
# MAGIC - Mapeamento completo de abastecimento (CDs e lojas)
# MAGIC - Características geográficas e operacionais

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debug: Verificação de Colunas

# COMMAND ----------

def debug_dataframe_info(df: DataFrame, stage_name: str):
    """
    Função de debug para verificar informações do DataFrame em cada etapa.
    
    Args:
        df: DataFrame para verificar
        stage_name: Nome da etapa para identificação
    """
    print(f"\n🔍 DEBUG - {stage_name}")
    print("=" * 60)
    print(f"📊 Total de registros: {df.count():,}")
    print(f"📋 Total de colunas: {len(df.columns)}")
    print(f"📋 Colunas disponíveis:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2d}. {col}")
    print("-" * 60)

# Verificar colunas em cada etapa
debug_dataframe_info(df_estoque_loja, "Estoque Lojas")
debug_dataframe_info(sales_df, "Vendas")
debug_dataframe_info(df_mercadoria, "Mercadoria")
debug_dataframe_info(df_merecimento_base, "Base Merecimento (após joins)")
