# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window

from datetime import datetime, timedelta

spark = SparkSession.builder.appName("impacto_apostas").getOrCreate()
hoje = datetime.now()
hoje_str = hoje.strftime("%Y-%m-%d")

# COMMAND ----------

# MAGIC %md
# MAGIC # Base de estoque lojas

# COMMAND ----------

df_estoque_loja = (
    spark.read.table("data_engineering_prd.app_logistica.gi_boss_qualidade_estoque")
    .filter(F.col("year_partition") == hoje.year)
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

# COMMAND ----------

# MAGIC %md
# MAGIC # Base de mercadoria

# COMMAND ----------

df_mercadoria = (
    spark.table('data_engineering_prd.app_venda.mercadoria')
    .filter(F.col("StUltimaVersaoMercadoria") == "Y")
    .select(
        "CdSkuLoja",
        "NmAgrupamentoDiretoriaSetor",
        "NmSetorGerencial",
        "NmClasseGerencial",
        "NmEspecieGerencial"
    )
    .withColumnRenamed("CdSkuLoja", "CdSku")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Base de vendas

# COMMAND ----------

# sales_view.py
# Author: Scardini
# Purpose: Build unified, aggregated and enriched sales view in one straightforward function.

from datetime import date

today_int = int(date.today().strftime("%Y%m%d"))

from pyspark.sql import SparkSession, functions as F

def build_sales_view(
    spark: SparkSession,
    start_date: int = 20250101,
    end_date: int = today_int,
):
    """
    Returns a DataFrame with:
      - year_month (YYYYMM)
      - NmUF, NmRegiaoGeografica
      - CdSkuLoja, NmTipoNegocio
      - Receita (sum of VrOperacao)
      - QtMercadoria (sum of QtMercadoria)
      - merchandising attributes from mercadoria table
    """
    # load tables
    df_rateada    = spark.table("app_venda.vendafaturadarateada")
    df_nao_rateada= spark.table("app_venda.vendafaturadanaorateada")

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

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sales_df = build_sales_view(spark)


# COMMAND ----------

# MAGIC %md
# MAGIC # Join para chegar em estoque e tabelas

# COMMAND ----------

df_merecimento_base = (
    df_estoque_loja
    .join(sales_df, on=["DtAtual", "CdFilial", "CdSku"], how="left")
    .join(df_mercadoria, on="CdSku", how="left")
)

# COMMAND ----------


def add_rolling_90_metrics(df):
    """
    Adiciona médias móveis de 90 dias (tempo real, não por linhas) para:
      - Media90_Receita_venda_estq
      - Media90_Qt_venda_estq
    Considerando apenas dias com TeveVenda=1 e EstoqueLoja>1.
    Janela por (CdFilial, CdSku), ordenada por dia.
    """
    # Garantir coluna de data e índice numérico de dias para janela por tempo
    df2 = (
        df
        .withColumn("DtAtual_date", F.to_date("DtAtual"))  # espera yyyy-MM-dd
        .withColumn("DayIdx", F.datediff(F.col("DtAtual_date"), F.lit("1970-01-01")))
    )

    # Condição de inclusão no cálculo da média
    cond = (F.col("EstoqueLoja") >= 1) #| (F.col("TeveVenda") == 1) | 

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

    # (Opcional) Preencher ausência de histórico válido com 0.0
    df3 = df3.fillna({
        "Media90_Receita_venda_estq": 0.0,
        "Media90_Qt_venda_estq": 0.0
    })

    # Reordenar colunas conforme sua lista original + novas métricas
    cols_base = [
        'DtAtual','CdSku','CdFilial','DsSku','DsSetor','DsCurva','DsCurvaAbcLoja',
        'StLinha','TipoEntrega','QtdEstoqueCDVinculado','DDE','EstoqueLoja', 'DsObrigatorio',
        'ClassificacaoDDE','data_ingestao','Ate_30D_Etq_Disp_Qt','Ate_45D_Etq_Disp_Qt',
        'Ate_60D_Etq_Disp_Qt','Ate_75D_Etq_Disp_Qt','Ate_90D_Etq_Disp_Qt',
        'Ate_180D_Etq_Disp_Qt','Ate_270D_Etq_Disp_Qt','Ate_1Ano_Etq_Disp_Qt',
        'Maior_1Ano_Etq_Disp_Qt','classificacao_aposta','year_month',
        'Receita','QtMercadoria','TeveVenda'
    ]

    novas = ["Media90_Receita_venda_estq", "Media90_Qt_venda_estq"]

    # Manter colunas existentes que não estejam na lista (evita erros caso haja extras)
    existentes = [c for c in cols_base if c in df3.columns]
    return df3.select(*existentes, *novas)

# COMMAND ----------

df_merecimento_base_r90 = add_rolling_90_metrics(df_merecimento_base)

df_analise_r90 = (
    df_merecimento_base_r90
    .withColumn("FlagRuptura",
                F.when(F.col("Media90_Qt_venda_estq") > F.col("EstoqueLoja"), F.lit(1))
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

# COMMAND ----------

# 1) Helper para normalizar IDs (string, trim e sem zeros à esquerda)
def normalize_ids(df, cols):
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

# Características de CD
caracteristicas_cd = (
    spark.table("data_engineering_prd.app_operacoesloja.roteirizacaocentrodistribuicao")
    .select(
        F.col("CdFilial"),
        "NmFilial",
        F.concat_ws("/", F.col("NmCidade"), F.col("NmUF")).alias("NmCidade_UF"),
        "NmTipoFilial"
    )
    .distinct()
)
caracteristicas_cd = normalize_ids(caracteristicas_cd, ["CdFilial"])



caracteristicas_loja = (
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

caracteristicas_loja = normalize_ids(caracteristicas_loja, ["CdFilial"])

de_para_filial_CD = (
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

de_para_filial_CD = normalize_ids(de_para_filial_CD, ["CdFilial", "CD_primario", "CD_secundario"])

de_para_filial_CD = (
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
        "CD_primario", *[F.col(c).alias(f"{c}_primario") for c in ["NmFilial", "NmCidade_UF", "NmTipoFilial"]],
        "CD_secundario", *[F.col(c).alias(f"{c}_secundario") for c in ["NmFilial", "NmCidade_UF", "NmTipoFilial"]],
        "LeadTime", "QtdCargasDia", "DsCubagemCaminhao", "DsGrupoHorario",
        "QtdSegunda", "QtdTerca", "QtdQuarta", "QtdQuinta",
        "QtdSexta", "QtdSabado", "QtdDomingo"
    )
)

# COMMAND ----------

df_merecimento_base_cd_loja = (
    df_merecimento_base
    .join(de_para_filial_CD, on="CdFilial", how="left")
)

# COMMAND ----------

(
    df_merecimento_base_cd_loja.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .format("delta")
    .saveAsTable("databox.bcg_comum.supply_base_merecimento_diario")
)
