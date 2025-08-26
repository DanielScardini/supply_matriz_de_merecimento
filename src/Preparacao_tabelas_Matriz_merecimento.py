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
from datetime import datetime, timedelta
from typing import List, Optional

# Inicialização do Spark
spark = SparkSession.builder.appName("impacto_apostas").getOrCreate()
hoje = datetime.now()
hoje_str = hoje.strftime("%Y-%m-%d")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base de Estoque Lojas

# COMMAND ----------

def load_estoque_loja_data(spark: SparkSession, current_year: int) -> DataFrame:
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
        .filter(F.col("year_partition") == current_year)
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

df_estoque_loja = load_estoque_loja_data(spark, hoje.year)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualização dos Dados de Estoque
# MAGIC
# MAGIC Exibimos os dados de estoque carregados para verificação
# MAGIC da estrutura e qualidade dos dados.

# COMMAND ----------

df_estoque_loja.display()

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
        .select(
            "CdSkuLoja",
            "DsSku",
            "NmAgrupamentoDiretoriaSetor",
            "NmAgrupamentoSetor",
            "NmAgrupamentoClasse",
            "NmAgrupamentoEspecie",
            "NmAgrupamentoEspecieGerencial"
        )
        .dropDuplicates()
    )

df_mercadoria = load_mercadoria_data(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualização dos Dados de Mercadoria
# MAGIC
# MAGIC Exibimos os dados de mercadoria carregados para verificação
# MAGIC da estrutura e qualidade dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base de Vendas

# COMMAND ----------

from datetime import date

today_int = int(date.today().strftime("%Y%m%d"))

def build_sales_view(
    spark: SparkSession,
    start_date: int = 20250101,
    end_date: int = today_int,
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

    # 3) Cross join para garantir que todas as combinações filial-SKU tenham uma linha por dia
    df_complete = (
        cal.crossJoin(keys)
        .withColumn("DtAtual", F.date_format(F.col("DtAtual_date"), "yyyy-MM-dd"))
        .withColumn("year_month", F.date_format(F.col("DtAtual_date"), "yyyyMM").cast("int"))
        .drop("DtAtual_date")
    )

    # 4) Left join com vendas reais
    df_final = (
        df_complete
        .join(df_agg, on=["DtAtual", "year_month", "CdSkuLoja", "CdFilial"], how="left")
        .fillna(0, subset=["Receita", "QtMercadoria", "Custo"])
    )

    # 5) Join com mercadoria para obter atributos
    df_with_mercadoria = (
        df_final
        .join(df_mercadoria, on="CdSkuLoja", how="left")
        .fillna("N/A", subset=[
            "NmAgrupamentoDiretoriaSetor",
            "NmAgrupamentoSetor", 
            "NmAgrupamentoClasse",
            "NmAgrupamentoEspecie",
            "NmAgrupamentoEspecieGerencial"
        ])
    )

    return df_with_mercadoria

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carregamento dos Dados de Vendas
# MAGIC
# MAGIC Carregamos os dados de vendas utilizando a função build_sales_view
# MAGIC para o período especificado.

# COMMAND ----------

df_vendas = build_sales_view(spark, start_date=20250101, end_date=20251231)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualização dos Dados de Vendas
# MAGIC
# MAGIC Exibimos os dados de vendas carregados para verificação
# MAGIC da estrutura e qualidade dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base de Estoque CD

# COMMAND ----------

def load_estoque_cd_data(spark: SparkSession, current_year: int) -> DataFrame:
    """
    Carrega dados de estoque dos centros de distribuição.
    
    Args:
        spark: Sessão do Spark
        current_year: Ano atual para filtro de partição
        
    Returns:
        DataFrame com dados de estoque dos CDs, incluindo:
        - Informações do CD e SKU
        - Dados de estoque e classificação
        - Métricas de DDE e faixas
    """
    return (
        spark.read.table("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
        .filter(F.col("year_partition") == current_year)
        .filter(F.col("DsEstoqueLojaDeposito") == "D")
        .select(
            "CdEstoqueFilialAbastecimento", 
            "CdSku",
            "DsSku",
            "DsSetor",
            "DsCurva",
            "DsCurvaAbcLoja",
            "StLinha",
            "DsObrigatorio",
            F.col("DsTipoEntrega").alias("TipoEntrega"),
            F.col("CdEstoqueFilialAbastecimento").alias("CdCD"),
            (F.col("VrTotalVv")/F.col("VrVndCmv")).alias("DDE"),
            F.col("QtEstoqueBoaOff").alias("EstoqueCD"),
            F.col("DsFaixaDde").alias("ClassificacaoDDE"),
            F.col("data_ingestao"),
            F.date_format(F.col("data_ingestao"), "yyyy-MM-dd").alias("DtAtual")    
        )
        .dropDuplicates(["DtAtual", "CdSku", "CdEstoqueFilialAbastecimento"])
    )

df_estoque_cd = load_estoque_cd_data(spark, hoje.year)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualização dos Dados de Estoque CD
# MAGIC
# MAGIC Exibimos os dados de estoque dos CDs carregados para verificação
# MAGIC da estrutura e qualidade dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Base de Filiais

# COMMAND ----------

def load_filial_data(spark: SparkSession) -> DataFrame:
    """
    Carrega dados das filiais com informações geográficas e operacionais.
    
    Args:
        spark: Sessão do Spark
        
    Returns:
        DataFrame com dados das filiais incluindo:
        - Código da filial
        - Informações geográficas (UF, região)
        - Status operacional
        - Tipo de filial
    """
    return (
        spark.table('data_engineering_prd.app_venda.filial')
        .filter(F.col("StUltimaVersaoFilial") == "Y")
        .select(
            "CdFilial",
            "DsFilial",
            "NmUF",
            "NmRegiaoGeografica",
            "StFilial",
            "NmTipoFilial"
        )
        .dropDuplicates()
    )

df_filial = load_filial_data(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualização dos Dados de Filiais
# MAGIC
# MAGIC Exibimos os dados das filiais carregados para verificação
# MAGIC da estrutura e qualidade dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consolidação das Bases

# COMMAND ----------

# Join entre estoque loja e mercadoria
df_estoque_loja_mercadoria = (
    df_estoque_loja
    .join(df_mercadoria, on="CdSku", how="left")
    .fillna("N/A", subset=[
        "NmAgrupamentoDiretoriaSetor",
        "NmAgrupamentoSetor", 
        "NmAgrupamentoClasse",
        "NmAgrupamentoEspecie",
        "NmAgrupamentoEspecieGerencial"
    ])
)

# Join com filiais
df_estoque_loja_mercadoria_filial = (
    df_estoque_loja_mercadoria
    .join(df_filial, on="CdFilial", how="left")
    .fillna("N/A", subset=[
        "NmUF",
        "NmRegiaoGeografica",
        "StFilial",
        "NmTipoFilial"
    ])
)

# Join com vendas
df_consolidado = (
    df_estoque_loja_mercadoria_filial
    .join(
        df_vendas.select("DtAtual", "CdSkuLoja", "CdFilial", "Receita", "QtMercadoria"),
        on=["DtAtual", "CdSku", "CdFilial"],
        how="left"
    )
    .fillna(0, subset=["Receita", "QtMercadoria"])
)

print("Base consolidada criada com sucesso!")
print(f"Total de registros: {df_consolidado.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualização da Base Consolidada
# MAGIC
# MAGIC Exibimos a base consolidada para verificação
# MAGIC da estrutura e qualidade dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cálculo de Métricas de Negócio

# COMMAND ----------

# Cálculo de métricas de negócio
df_com_metricas = (
    df_consolidado
    .withColumn(
        "FlagRuptura",
        F.when(F.col("EstoqueLoja") == 0, 1).otherwise(0)
    )
    .withColumn(
        "ReceitaPerdidaRuptura",
        F.when(F.col("FlagRuptura") == 1, F.col("Receita")).otherwise(0)
    )
    .withColumn(
        "deltaRuptura",
        F.when(F.col("FlagRuptura") == 1, F.col("QtMercadoria")).otherwise(0)
    )
    .withColumn(
        "Media90_Qt_venda_estq",
        F.when(F.col("QtMercadoria") > 0, F.col("QtMercadoria") / 90).otherwise(0)
    )
    .withColumn(
        "PrecoMedio90",
        F.when(F.col("QtMercadoria") > 0, F.col("Receita") / F.col("QtMercadoria")).otherwise(0)
    )
)

print("Métricas de negócio calculadas com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualização das Métricas Calculadas
# MAGIC
# MAGIC Exibimos as métricas calculadas para verificação
# MAGIC da qualidade e consistência dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvamento da Base Final

# COMMAND ----------

# Salvamento da base final
df_com_metricas.write.mode("overwrite").saveAsTable("databox.bcg_comum.supply_base_merecimento_diario")

print("✅ Base final salva com sucesso na tabela 'databox.bcg_comum.supply_base_merecimento_diario'")
print(f"📊 Total de registros salvos: {df_com_metricas.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da Execução

# COMMAND ----------

# Estatísticas finais
total_registros = df_com_metricas.count()
total_filiais = df_com_metricas.select("CdFilial").distinct().count()
total_skus = df_com_metricas.select("CdSku").distinct().count()
total_rupturas = df_com_metricas.filter(F.col("FlagRuptura") == 1).count()

print("📋 RESUMO DA EXECUÇÃO")
print("=" * 50)
print(f"📊 Total de registros: {total_registros:,}")
print(f"🏪 Total de filiais: {total_filiais}")
print(f"📦 Total de SKUs: {total_skus}")
print(f"⚠️ Total de rupturas: {total_rupturas:,}")
print(f"📈 Percentual de rupturas: {(total_rupturas/total_registros)*100:.2f}%")

print("\n✅ Notebook executado com sucesso!")
print("🎯 Base de dados pronta para análise da matriz de merecimento!")
