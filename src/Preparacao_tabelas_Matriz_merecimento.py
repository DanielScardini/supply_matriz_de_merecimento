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

print(hoje, hoje_str, hoje_int)

# COMMAND ----------

tabela_old = "databox.bcg_comum.supply_base_merecimento_diario_v3"
tabela_new = "databox.bcg_comum.supply_base_merecimento_diario_v4"

# # 1. Checar se a new já existe
# if spark._jsparkSession.catalog().tableExists(tabela_new):
#     print(f"⚠️ A tabela {tabela_new} já existe. Pode seguir com processo de append.")
# else:
#     # 2. Pegar schema da old
#     schema_old = spark.table(tabela_old).schema
    
#     # 3. Criar DataFrame vazio com esse schema
#     df_empty = spark.createDataFrame([], schema_old)
    
#     # 4. Criar tabela new
#     df_empty.write.saveAsTable(tabela_new)
#     print(f"✅ Tabela {tabela_new} criada vazia com schema de {tabela_old}.")

# COMMAND ----------

def get_data_inicio(hoje: datetime | date | None = None) -> datetime:
    """
    Retorna datetime no dia 1 do mês que está 14 meses antes de 'hoje'.
    """
    if hoje is None:
        hoje_d = date.today()
    elif isinstance(hoje, datetime):
        hoje_d = hoje.date()
    else:
        hoje_d = hoje

    total_meses = hoje_d.year * 12 + hoje_d.month - 1
    ano = total_meses // 12
    mes = total_meses % 12
    if mes == 0:
        ano -= 1
        mes = 12

    return datetime(ano, mes, 1)

# exemplo de uso
data_inicio = get_data_inicio()
data_inicio_int = int(data_inicio.strftime("%Y%m%d"))
print(data_inicio, data_inicio_int)

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
            "DsVoltagem",
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
        .filter(
            F.col("NmAgrupamentoDiretoriaSetor")
            .isin(
                ["DIRETORIA DE LINHA BRANCA",
                 "DIRETORIA LINHA LEVE",
                 "DIRETORIA DE TELAS",
                 "DIRETORIA TELEFONIA CELULAR",
                 "DIRETORIA INFO/PERIFERICOS"
                 ]
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
    df_rateada =(
        spark.table("app_venda.vendafaturadarateada")
        .filter(F.col("NmEstadoMercadoria") != '1 - SALDO')
        .filter(F.col("NmTipoNegocio") == 'LOJA FISICA')
        .filter(
            F.col("DtAprovacao").between(start_date, end_date)
            & (F.col("VrOperacao") >= 0)
            & (F.col("VrCustoContabilFilialSku") >= 0)
        )
    )   
        
    df_nao_rateada = (
        spark.table("app_venda.vendafaturadanaorateada")
        .filter(F.col("QtMercadoria") >= 0)
        )

    # unify and filter
    df = (
        df_rateada
        .join(df_nao_rateada.select("ChaveFatos","QtMercadoria"), on="ChaveFatos")     
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
        .join(df_mercadoria, on="CdSku", how="inner")
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

# MAGIC %sql SELECT * FROM databox.bcg_comum.supply_base_merecimento_diario_v4

# COMMAND ----------

# MAGIC %sql SELECT * FROM databox.bcg_comum.supply_base_merecimento_diario_v4_online

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
            F.col("CdFilial").alias("CdFilial").cast("string"),
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
            F.col("CdFilial").alias("CdFilial").cast("string"),
            F.col("NmFilial").alias("NmLoja"),
            F.col("NmCidade").alias("NmCidadeLoja"),
            F.col("NmUF").alias("NmUFLoja"),
            F.col("NrCEP").alias("CEPLoja"),
            F.col("NmPorteLoja"),
            F.col("NmTipoLoja").alias("TipoLoja"),
            F.col("CdLatitude").alias("LatitudeLoja"),
            F.col("CdLongitude").alias("LongitudeLoja"),
        )
        .dropDuplicates(["CdFilial"])
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
        .select(
            F.col("CdFilialAtende").alias("CD_primario"),
            F.col("CdFilialEntrega").alias("CD_secundario"),
            F.col("CdLoja").alias("CdFilial").cast("int"),
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
        .dropDuplicates(["CdFilial"])
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
            F.col("CdFilial").alias("CdFilial").cast("string"),
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
        .withColumn("year_month",
                    F.date_format(F.col("DtAtual_date"), "yyyyMM").cast("int"))
    )

df_merecimento_base_cd_loja = create_final_merecimento_base(df_merecimento_base_r90, de_para_filial_CD)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Processamento Incremental em Lotes de Meses

# COMMAND ----------

def get_monthly_batches(start_date: datetime, end_date: datetime, batch_size_months: int = 3) -> List[tuple]:
    """
    Gera lotes de meses para processamento incremental.
    
    Args:
        start_date: Data de início
        end_date: Data de fim
        batch_size_months: Tamanho do lote em meses (recomendado: 3-4)
        
    Returns:
        Lista de tuplas (data_inicio_lote, data_fim_lote) para cada lote
    """
    batches = []
    current_date = start_date
    
    while current_date < end_date:
        # Calcular fim do lote
        if current_date.month + batch_size_months > 12:
            # Ajustar para o próximo ano
            next_year = current_date.year + ((current_date.month + batch_size_months - 1) // 12)
            next_month = ((current_date.month + batch_size_months - 1) % 12) + 1
            batch_end = datetime(next_year, next_month, 1) - timedelta(days=1)
        else:
            batch_end = datetime(current_date.year, current_date.month + batch_size_months, 1) - timedelta(days=1)
        
        # Garantir que não ultrapasse a data final
        if batch_end > end_date:
            batch_end = end_date
            
        batches.append((current_date, batch_end))
        
        # Próximo lote
        current_date = batch_end + timedelta(days=1)
    
    return batches

def check_existing_data_for_period(spark: SparkSession, table_name: str, start_date: datetime, end_date: datetime) -> bool:
    """
    Verifica se já existem dados para o período especificado.
    
    Args:
        spark: Sessão do Spark
        table_name: Nome da tabela a verificar
        start_date: Data de início do período
        end_date: Data de fim do período
        
    Returns:
        True se existem dados, False caso contrário
    """
    try:
        # Verificar se a tabela existe
        existing_data = (
            spark.read.table(table_name)
            .filter(
                (F.col("DtAtual") >= start_date.strftime("%Y-%m-%d")) &
                (F.col("DtAtual") <= end_date.strftime("%Y-%m-%d"))
            )
            .select("DtAtual")
            .distinct()
            .count()
        )
        
        # Calcular dias úteis no período (excluindo fins de semana)
        total_days = (end_date - start_date).days + 1
        business_days = sum(1 for i in range(total_days) 
                          if (start_date + timedelta(days=i)).weekday() < 5)
        
        # Considerar que temos dados se pelo menos 80% dos dias úteis estão presentes
        return existing_data >= (business_days * 0.8)
        
    except Exception as e:
        print(f"⚠️ Erro ao verificar dados existentes: {e}")
        return False

def delete_existing_data_for_period(spark: SparkSession, table_name: str, start_date: datetime, end_date: datetime) -> None:
    """
    Remove dados existentes para o período especificado.
    
    Args:
        spark: Sessão do Spark
        table_name: Nome da tabela
        start_date: Data de início do período
        end_date: Data de fim do período
    """
    try:
        # Ler dados existentes
        existing_df = spark.read.table(table_name)
        
        # Filtrar dados fora do período a ser deletado
        data_to_keep = existing_df.filter(
            ~((F.col("DtAtual") >= start_date.strftime("%Y-%m-%d")) &
              (F.col("DtAtual") <= end_date.strftime("%Y-%m-%d")))
        )
        
        # Sobrescrever tabela mantendo apenas dados fora do período
        (
            data_to_keep.write
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .format("delta")
            .saveAsTable(table_name)
        )
        
        print(f"🗑️ Dados deletados para período: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
        
    except Exception as e:
        print(f"❌ Erro ao deletar dados existentes: {e}")
        raise

def process_monthly_batch(
    spark: SparkSession,
    start_date: datetime,
    end_date: datetime,
    table_name: str = "databox.bcg_comum.supply_base_merecimento_diario_v4"
) -> DataFrame:
    """
    Processa um lote de meses específico com gestão inteligente de memória.
    
    Args:
        spark: Sessão do Spark
        start_date: Data de início do lote
        end_date: Data de fim do lote
        table_name: Nome da tabela de destino
        
    Returns:
        DataFrame processado para o período
    """
    print(f"🔄 Processando lote: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    
    # Converter datas para formato inteiro
    start_date_int = int(start_date.strftime("%Y%m%d"))
    end_date_int = int(end_date.strftime("%Y%m%d"))
    
    try:
        # 1. Carregar dados de estoque para o período (NÃO cache - muda a cada lote)
        df_estoque_lote = (
            spark.read.table("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
            .filter(F.col("DtAtual") >= start_date)
            .filter(F.col("DtAtual") <= end_date)
            .filter(F.col("StLoja") == "ATIVA")
            .filter(F.col("DsEstoqueLojaDeposito") == "L")
            .select(
                "CdFilial", "CdSku", "DsSku", "DsSetor", "DsCurva", "DsCurvaAbcLoja",
                "StLinha", "DsObrigatorio", "DsVoltagem", F.col("DsTipoEntrega").alias("TipoEntrega"),
                F.col("CdEstoqueFilialAbastecimento").alias("QtdEstoqueCDVinculado"),
                (F.col("VrTotalVv")/F.col("VrVndCmv")).alias("DDE"),
                F.col("QtEstoqueBoaOff").alias("EstoqueLoja"),
                F.col("DsFaixaDde").alias("ClassificacaoDDE"),
                F.col("data_ingestao"),
                F.date_format(F.col("data_ingestao"), "yyyy-MM-dd").alias("DtAtual")    
            )
            .dropDuplicates(["DtAtual", "CdSku", "CdFilial"])
        )
        
        # 2. Carregar dados de vendas para o período (NÃO cache - muda a cada lote)
        sales_df_lote = build_sales_view(spark, start_date_int, end_date_int)
        
        # 3. Carregar dados de mercadoria (CACHE - não muda entre lotes, reutilizado)
        if not hasattr(process_monthly_batch, '_mercadoria_cached'):
            print("📦 Cacheando dados de mercadoria (reutilizável entre lotes)")
            df_mercadoria_lote = load_mercadoria_data(spark).cache()
            process_monthly_batch._mercadoria_cached = df_mercadoria_lote
            # Forçar materialização
            df_mercadoria_lote.count()
        else:
            print("♻️ Reutilizando dados de mercadoria do cache")
            df_mercadoria_lote = process_monthly_batch._mercadoria_cached
        
        # 4. Criar base de merecimento para o lote
        df_merecimento_lote = create_base_merecimento(df_estoque_lote, sales_df_lote, df_mercadoria_lote)
        
        # 5. Unpersist dados de estoque e vendas (não serão mais usados)
        df_estoque_lote.unpersist()
        sales_df_lote.unpersist()
        print("🧹 Memória liberada: dados de estoque e vendas do lote")
        
        # 6. Adicionar métricas de média móvel de 90 dias
        df_merecimento_lote_r90 = add_rolling_90_metrics(df_merecimento_lote)
        
        # 7. Unpersist dados intermediários
        df_merecimento_lote.unpersist()
        print("🧹 Memória liberada: dados intermediários de merecimento")
        
        # 8. Adicionar flags de ruptura
        df_merecimento_lote_final = create_analysis_with_rupture_flags(df_merecimento_lote_r90)
        
        # 9. Unpersist dados de média móvel
        df_merecimento_lote_r90.unpersist()
        print("🧹 Memória liberada: dados de média móvel")
        
        # 10. Adicionar mapeamento de abastecimento (CACHE - não muda entre lotes)
        if not hasattr(process_monthly_batch, '_supply_mapping_cached'):
            print("📦 Cacheando mapeamento de abastecimento (reutilizável entre lotes)")
            supply_mapping = create_complete_supply_mapping(spark, datetime.now()).cache()
            process_monthly_batch._supply_mapping_cached = supply_mapping
            # Forçar materialização
            supply_mapping.count()
        else:
            print("♻️ Reutilizando mapeamento de abastecimento do cache")
            supply_mapping = process_monthly_batch._supply_mapping_cached
        
        df_merecimento_lote_cd_loja = create_final_merecimento_base(df_merecimento_lote_final, supply_mapping)
        
        # 11. Unpersist dados finais do lote (serão salvos)
        df_merecimento_lote_final.unpersist()
        print("🧹 Memória liberada: dados finais do lote")
        
        return df_merecimento_lote_cd_loja.fillna(0, subset=["Receita", "QtMercadoria", "TeveVenda", "deltaRuptura"])

        
    except Exception as e:
        # Em caso de erro, limpar cache para liberar memória
        print(f"❌ Erro no processamento. Limpando cache...")
        cleanup_batch_memory()
        raise e

def cleanup_batch_memory():
    """
    Limpa cache de dados reutilizáveis entre lotes.
    """
    try:
        if hasattr(process_monthly_batch, '_mercadoria_cached'):
            process_monthly_batch._mercadoria_cached.unpersist()
            delattr(process_monthly_batch, '_mercadoria_cached')
            print("🧹 Cache de mercadoria limpo")
            
        if hasattr(process_monthly_batch, '_supply_mapping_cached'):
            process_monthly_batch._supply_mapping_cached.unpersist()
            delattr(process_monthly_batch, '_supply_mapping_cached')
            print("🧹 Cache de mapeamento de abastecimento limpo")
            
    except Exception as e:
        print(f"⚠️ Erro ao limpar cache: {e}")

def append_monthly_batch_to_table(
    df_batch: DataFrame,
    table_name: str,
    mode: str = "append"
) -> None:
    """
    Adiciona lote processado à tabela de destino.
    
    Args:
        df_batch: DataFrame do lote processado
        table_name: Nome da tabela de destino
        mode: Modo de escrita ("append" ou "overwrite")
    """
    try:
        (
            df_batch.write
            .mode(mode)
            .option("overwriteSchema", "false")  # Manter schema existente
            .format("delta")
            .saveAsTable(table_name)
        )
        
        print(f"✅ Lote salvo com sucesso na tabela {table_name}")
        
    except Exception as e:
        print(f"❌ Erro ao salvar lote: {e}")
        raise

def process_incremental_from_start_date(
    spark: SparkSession,
    start_date: datetime,
    end_date: datetime,
    batch_size_months: int = 3,
    table_name: str = "databox.bcg_comum.supply_base_merecimento_diario_v4"
) -> None:
    """
    Processa dados incrementalmente desde a data de início até hoje com gestão de memória.
    SEMPRE sobrescreve dados existentes para garantir atualização completa.
    
    Args:
        spark: Sessão do Spark
        start_date: Data de início para processamento
        end_date: Data de fim (padrão: hoje)
        batch_size_months: Tamanho do lote em meses
        table_name: Nome da tabela de destino
    """
    if end_date is None:
        end_date = datetime.now() - timedelta(days=1)
    
    print(f"🚀 INICIANDO PROCESSAMENTO INCREMENTAL")
    print(f"📅 Período: {start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}")
    print(f"📦 Tamanho do lote: {batch_size_months} meses")
    print(f"🎯 Tabela de destino: {table_name}")
    print(f"🧠 Gestão inteligente de memória ativada")
    
    # Gerar lotes de meses
    batches = get_monthly_batches(start_date, end_date, batch_size_months)
    
    print(f"📋 Total de lotes a processar: {len(batches)}")
    
    try:
        for i, (batch_start, batch_end) in enumerate(batches, 1):
            print(f"\n🔄 PROCESSANDO LOTE {i}/{len(batches)}")
            print(f"📅 Período do lote: {batch_start.strftime('%Y-%m-%d')} a {batch_end.strftime('%Y-%m-%d')}")
            
            try:
                # Sempre deletar dados existentes para o período e sobrescrever
                print(f"🔄 Processando período: {batch_start.strftime('%Y-%m-%d')} a {batch_end.strftime('%Y-%m-%d')}")
                delete_existing_data_for_period(spark, table_name, batch_start, batch_end)
                
                # Processar lote
                df_batch = process_monthly_batch(spark, batch_start, batch_end, table_name)
                
                # Salvar lote na tabela
                append_monthly_batch_to_table(df_batch, table_name, mode="append")
                
                # Unpersist dados do lote após salvamento
                df_batch.unpersist()
                print(f"🧹 Memória liberada: dados do lote {i}")
                
                print(f"✅ Lote {i} processado e salvo com sucesso!")
                
                # Forçar garbage collection entre lotes
                if i % 1 == 0:  # A cada 1 lotes
                    print("🔄 Forçando limpeza de memória entre lotes...")
                    spark.catalog.clearCache()
                
            except Exception as e:
                print(f"❌ ERRO no lote {i}: {e}")
                print(f"🛑 Processamento interrompido. Verifique o erro e reinicie.")
                raise
        
        print(f"\n🎉 PROCESSAMENTO INCREMENTAL CONCLUÍDO!")
        print(f"📊 Todos os {len(batches)} lotes foram processados com sucesso.")
        
    finally:
        # Sempre limpar cache ao finalizar
        print("🧹 Limpeza final de memória...")
        cleanup_batch_memory()
        spark.catalog.clearCache()
        print("✅ Memória limpa e otimizada!")

def monitor_memory_usage(spark: SparkSession) -> None:
    """
    Monitora uso de memória e cache do Spark.
    
    Args:
        spark: Sessão do Spark
    """
    try:
        print("🧠 MONITORAMENTO DE MEMÓRIA E CACHE")
        print("=" * 50)
        
        # Verificar tabelas em cache
        cached_tables = spark.catalog.listTables()
        cached_count = len([t for t in cached_tables if t.isCached])
        
        print(f"📦 Tabelas em cache: {cached_count}")
        
        # Verificar uso de memória (se disponível)
        try:
            # Tentar obter métricas de memória do Spark
            memory_info = spark.sparkContext.getConf().getAll()
            memory_configs = [conf for conf in memory_info if 'memory' in conf[0].lower()]
            
            if memory_configs:
                print(f"\n⚙️ Configurações de memória:")
                for key, value in memory_configs:
                    print(f"  {key}: {value}")
            else:
                print(f"\n⚙️ Configurações de memória não disponíveis")
                
        except Exception as e:
            print(f"⚠️ Não foi possível obter métricas de memória: {e}")
        
        # Verificar se há dados em cache específicos
        if hasattr(process_monthly_batch, '_mercadoria_cached'):
            print(f"✅ Cache de mercadoria: ATIVO")
        else:
            print(f"❌ Cache de mercadoria: INATIVO")
            
        if hasattr(process_monthly_batch, '_supply_mapping_cached'):
            print(f"✅ Cache de mapeamento: ATIVO")
        else:
            print(f"❌ Cache de mapeamento: INATIVO")
            
    except Exception as e:
        print(f"❌ Erro no monitoramento de memória: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Comportamento de Processamento

# COMMAND ----------

# MAGIC %md
# MAGIC ### **⚠️ IMPORTANTE: Comportamento de Sobrescrita**
# MAGIC
# MAGIC O processamento incremental **SEMPRE sobrescreve** dados existentes para garantir:
# MAGIC - **Atualização completa** dos dados
# MAGIC - **Consistência** entre lotes processados
# MAGIC - **Sem duplicação** ou dados desatualizados
# MAGIC
# MAGIC **Processo para cada lote:**
# MAGIC 1. **Deleta** dados existentes do período
# MAGIC 2. **Processa** novos dados do período
# MAGIC 3. **Salva** dados atualizados
# MAGIC
# MAGIC **Não há verificação de dados existentes** - sempre processa e sobrescreve!

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Execução do Processamento Incremental

# COMMAND ----------

# Executar processamento incremental
# Descomente a linha abaixo para executar
process_incremental_from_start_date(spark, data_inicio, hoje, batch_size_months=3)
