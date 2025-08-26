# Databricks notebook source
# MAGIC %md
# MAGIC # Métricas de Avaliação da Matriz de Merecimento
# MAGIC
# MAGIC Este módulo implementa as métricas robustas para avaliar a qualidade das alocações da matriz de merecimento:
# MAGIC - **wMAPE**: Weighted Mean Absolute Percentage Error ponderado por volume
# MAGIC - **Cross Entropy**: Entropia cruzada para divergência de distribuições
# MAGIC - **Share Error (SE)**: Erro na distribuição de participações
# MAGIC - **KL Divergence**: Divergência de Kullback-Leibler para comparação
# MAGIC
# MAGIC **Objetivo**: Fornecer métricas padronizadas para avaliação da efetividade da matriz de merecimento
# MAGIC **Escopo**: Aplicável a qualquer matriz de merecimento com dados reais de vendas/demanda

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e Configurações Iniciais

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Optional

# Inicialização do Spark
spark = SparkSession.builder.appName("metricas_matriz_merecimento").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Função para Cálculo de Métricas de Alocação
# MAGIC
# MAGIC Esta função calcula métricas robustas para avaliar a qualidade das alocações:
# MAGIC - **wMAPE**: Weighted Mean Absolute Percentage Error ponderado por volume
# MAGIC - **SE (Share Error)**: Erro na distribuição de participações entre filiais
# MAGIC - **Cross Entropy**: Medida de divergência entre distribuições reais e previstas
# MAGIC - **KL Divergence**: Divergência de Kullback-Leibler para comparação de distribuições
# MAGIC
# MAGIC **Vantagens das métricas:**
# MAGIC - wMAPE é padrão da indústria e bem interpretável
# MAGIC - Cross Entropy é padrão em machine learning para avaliação de distribuições
# MAGIC - Foco em métricas fundamentais e robustas

# COMMAND ----------

def add_allocation_metrics(
    df: DataFrame,
    y_col: str,
    yhat_col: str,
    group_cols: Optional[List[str]] = None,
    epsilon: float = 1e-12
) -> DataFrame:
    """
    Calcula métricas robustas para avaliação de alocações da matriz de merecimento.
    
    Args:
        df: DataFrame Spark com dados de comparação
        y_col: Nome da coluna com valores reais (ex: percentuais de demanda)
        yhat_col: Nome da coluna com valores previstos (ex: percentuais da matriz)
        group_cols: Lista de colunas para agrupamento (ex: ['Cd_primario', 'gemeos'])
        epsilon: Valor pequeno para evitar divisão por zero
        
    Returns:
        DataFrame com métricas calculadas
    """
    if group_cols is None:
        group_cols = []

    w = Window.partitionBy(*group_cols) if group_cols else Window.partitionBy(F.lit(1))
    y, yhat = F.col(y_col).cast("double"), F.col(yhat_col).cast("double")

    # Totais
    Y_tot    = F.sum(y).over(w)
    Yhat_tot = F.sum(yhat).over(w)

    # Shares
    p    = F.when(Y_tot    > 0, y    / Y_tot   ).otherwise(F.lit(0.0))
    phat = F.when(Yhat_tot > 0, yhat / Yhat_tot).otherwise(F.lit(0.0))

    # Erros
    abs_err = F.abs(y - yhat)
    mae_weighted_by_y = abs_err * y

    # sMAPE componentes
    smape_num = 2.0 * abs_err
    smape_den = F.when((y + yhat) > 0, y + yhat).otherwise(F.lit(0.0))

    # Distribuição
    cross_entropy_term = F.when((p > 0) & (phat > 0), -p * F.log(phat + F.lit(epsilon))).otherwise(F.lit(0.0))
    kl_term            = F.when((p > 0) & (phat > 0),  p * F.log((p + F.lit(epsilon)) / (phat + F.lit(epsilon)))).otherwise(F.lit(0.0))

    # Share
    abs_err_share = F.abs(p - phat)
    wmape_share   = abs_err_share * y  # ponderado por volume real

    base = (df
        .withColumn("__y__", y).withColumn("__yhat__", yhat)
        .withColumn("__p__", p).withColumn("__phat__", phat)
        .withColumn("__abs_err__", abs_err)
        .withColumn("__mae_w_by_y__", mae_weighted_by_y)
        .withColumn("__smape_num__", smape_num)
        .withColumn("__smape_den__", smape_den)
        .withColumn("__kl_term__", kl_term)
        .withColumn("__cross_entropy_term__", cross_entropy_term)
        .withColumn("__abs_err_share__", abs_err_share)
        .withColumn("__wmape_share__", wmape_share)
    )

    agg = base.groupBy(*group_cols) if group_cols else base.groupBy()
    res = (agg.agg(
            F.sum("__abs_err__").alias("_sum_abs_err"),
            F.sum("__mae_w_by_y__").alias("_sum_mae_w_by_y"),
            F.sum("__y__").alias("_sum_y"),
            F.sum("__yhat__").alias("_sum_yhat"),
            F.sum("__smape_num__").alias("_sum_smape_num"),
            F.sum("__smape_den__").alias("_sum_smape_den"),
            F.sum(F.abs(F.col("__p__") - F.col("__phat__"))).alias("_SE"),
            F.sum("__kl_term__").alias("_KL"),
            F.sum("__cross_entropy_term__").alias("_cross_entropy"),
            F.sum("__wmape_share__").alias("_num_wmape_share")
        )
        # WMAPE (%)
        .withColumn("wMAPE_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_sum_abs_err")/F.col("_sum_y")*100).otherwise(0.0), 4))
        # sMAPE (%)
        .withColumn("sMAPE_perc", F.round(F.when(F.col("_sum_smape_den") > 0, F.col("_sum_smape_num")/F.col("_sum_smape_den")*100).otherwise(0.0), 4))
        # MAE ponderado
        .withColumn("MAE_weighted_by_y", F.round(F.when(F.col("_sum_y") > 0, F.col("_sum_mae_w_by_y")/F.col("_sum_y")).otherwise(0.0), 4))
        # Shares
        .withColumn("SE_pp", F.round(F.col("_SE") * 100, 4))
        .withColumn("wMAPE_share_perc", F.round(F.when(F.col("_sum_y") > 0, F.col("_num_wmape_share")/F.col("_sum_y")*100).otherwise(0.0), 4))
        # Distribuição
        .withColumn("Cross_entropy", F.when(F.col("_sum_y") > 0, F.col("_cross_entropy")).otherwise(F.lit(0.0)))
        .withColumn("KL_divergence", F.when((F.col("_sum_y") > 0) & (F.col("_sum_yhat") > 0), F.col("_KL")).otherwise(F.lit(0.0)))
        .select(*(group_cols if group_cols else []),
                "wMAPE_perc","sMAPE_perc","MAE_weighted_by_y",
                "SE_pp","wMAPE_share_perc",
                "Cross_entropy","KL_divergence")
    )
    return res

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Função para Cálculo de Métricas Linha a Linha
# MAGIC
# MAGIC Calcula métricas detalhadas para cada linha individual, permitindo análise granular
# MAGIC da performance da matriz de merecimento.

# COMMAND ----------

def calculate_line_metrics(
    df: DataFrame,
    y_col: str,
    yhat_col: str,
    group_cols: Optional[List[str]] = None,
    gamma: float = 1.5,
    epsilon: float = 1e-12
) -> DataFrame:
    """
    Calcula métricas linha a linha para análise detalhada da matriz de merecimento.
    
    Args:
        df: DataFrame Spark com dados de comparação
        y_col: Nome da coluna com valores reais
        yhat_col: Nome da coluna com valores previstos
        group_cols: Lista de colunas para agrupamento
        gamma: Fator de penalização para subestimação (default: 1.5)
        epsilon: Valor pequeno para evitar divisão por zero
        
    Returns:
        DataFrame com métricas linha a linha
    """
    if group_cols is None:
        group_cols = []

    w = Window.partitionBy(*group_cols) if group_cols else Window.partitionBy(F.lit(1))

    return (df
        # Totais sobre dados agrupados
        .withColumn("total_y", F.sum(F.col(y_col)).over(w))
        .withColumn("total_yhat", F.sum(F.col(yhat_col)).over(w))
        
        # Shares sobre dados agrupados
        .withColumn(
            "p", 
            F.when(F.col("total_y") > 0, 
                   F.col(y_col) / F.col("total_y"))
             .otherwise(F.lit(0.0))
        )
        .withColumn(
            "phat", 
            F.when(F.col("total_yhat") > 0, 
                   F.col(yhat_col) / F.col("total_yhat"))
             .otherwise(F.lit(0.0))
        )
        
        # Métricas linha a linha
        .withColumn("abs_err", F.abs(F.col(y_col) - F.col(yhat_col)))
        .withColumn("under", F.greatest(F.lit(0.0), F.col(y_col) - F.col(yhat_col)))
        .withColumn(
            "weight", 
            F.when(F.col(yhat_col) < F.col(y_col), 
                   F.lit(gamma) * F.col(y_col))
             .otherwise(F.col(y_col))
        )
        .withColumn("w_abs", F.col("weight") * F.col("abs_err"))
        
        # KL divergence term
        .withColumn(
            "kl_term", 
            F.when(
                (F.col("p") > 0) & (F.col("phat") > 0),
                F.col("p") * F.log((F.col("p") + F.lit(epsilon)) / (F.col("phat") + F.lit(epsilon)))
            ).otherwise(F.lit(0.0))
        )
        
        # Cross entropy term
        .withColumn(
            "cross_entropy_term", 
            F.when(
                (F.col("p") > 0) & (F.col("phat") > 0),
                -F.col("p") * F.log(F.col("phat") + F.lit(epsilon))
            ).otherwise(F.lit(0.0))
        )
        
        # Percentual de erro
        .withColumn(
            "pct_error",
            F.when(F.col(y_col) > 0,
                   F.round(F.col("abs_err") / F.col(y_col) * 100, 2))
             .otherwise(F.lit(0.0))
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Função para Validação de Dados
# MAGIC
# MAGIC Valida se os dados estão em formato adequado para cálculo das métricas.

# COMMAND ----------

def validate_metrics_data(
    df: DataFrame,
    y_col: str,
    yhat_col: str,
    required_cols: Optional[List[str]] = None
) -> tuple[bool, str]:
    """
    Valida se os dados estão em formato adequado para cálculo das métricas.
    
    Args:
        df: DataFrame Spark para validação
        y_col: Nome da coluna com valores reais
        yhat_col: Nome da coluna com valores previstos
        required_cols: Lista de colunas obrigatórias
        
    Returns:
        Tuple com (is_valid, error_message)
    """
    try:
        # Verificar se as colunas existem
        if y_col not in df.columns:
            return False, f"Coluna '{y_col}' não encontrada no DataFrame"
        
        if yhat_col not in df.columns:
            return False, f"Coluna '{yhat_col}' não encontrada no DataFrame"
        
        # Verificar se as colunas são numéricas
        y_schema = df.select(y_col).schema[0]
        yhat_schema = df.select(yhat_col).schema[0]
        
        if not str(y_schema.dataType).startswith(('IntegerType', 'LongType', 'DoubleType', 'FloatType')):
            return False, f"Coluna '{y_col}' deve ser numérica, encontrado: {y_schema.dataType}"
        
        if not str(yhat_schema.dataType).startswith(('IntegerType', 'LongType', 'DoubleType', 'FloatType')):
            return False, f"Coluna '{yhat_col}' deve ser numérica, encontrado: {yhat_schema.dataType}"
        
        # Verificar colunas obrigatórias se especificadas
        if required_cols:
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                return False, f"Colunas obrigatórias não encontradas: {missing_cols}"
        
        # Verificar se há dados
        if df.count() == 0:
            return False, "DataFrame está vazio"
        
        return True, "Dados válidos para cálculo de métricas"
        
    except Exception as e:
        return False, f"Erro na validação: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Função para Resumo Estatístico das Métricas
# MAGIC
# MAGIC Gera um resumo estatístico das métricas calculadas para análise rápida.

# COMMAND ----------

def generate_metrics_summary(
    df_metrics: DataFrame,
    group_cols: Optional[List[str]] = None
) -> DataFrame:
    """
    Gera um resumo estatístico das métricas calculadas.
    
    Args:
        df_metrics: DataFrame com métricas calculadas
        group_cols: Lista de colunas para agrupamento
        
    Returns:
        DataFrame com resumo estatístico
    """
    if group_cols is None:
        group_cols = []
    
    metric_cols = [
        "wMAPE_perc", "sMAPE_perc", "MAE_weighted_by_y",
        "SE_pp", "wMAPE_share_perc", "Cross_entropy", "KL_divergence"
    ]
    
    # Filtrar apenas colunas que existem
    existing_metrics = [col for col in metric_cols if col in df_metrics.columns]
    
    if not existing_metrics:
        return df_metrics.select(group_cols).limit(0)  # DataFrame vazio
    
    agg_exprs = []
    for metric in existing_metrics:
        agg_exprs.extend([
            F.round(F.avg(metric), 4).alias(f"avg_{metric}"),
            F.round(F.stddev(metric), 4).alias(f"std_{metric}"),
            F.round(F.min(metric), 4).alias(f"min_{metric}"),
            F.round(F.max(metric), 4).alias(f"max_{metric}"),
            F.round(F.percentile_approx(metric, 0.5), 4).alias(f"median_{metric}")
        ])
    
    if group_cols:
        return df_metrics.groupBy(group_cols).agg(*agg_exprs)
    else:
        return df_metrics.agg(*agg_exprs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Função para Comparação de Métricas entre Períodos
# MAGIC
# MAGIC Compara métricas entre diferentes períodos para análise de evolução.

# COMMAND ----------

def compare_metrics_periods(
    df_current: DataFrame,
    df_previous: DataFrame,
    y_col: str,
    yhat_col: str,
    period_col: str,
    group_cols: Optional[List[str]] = None
) -> DataFrame:
    """
    Compara métricas entre dois períodos para análise de evolução.
    
    Args:
        df_current: DataFrame com dados do período atual
        df_previous: DataFrame com dados do período anterior
        y_col: Nome da coluna com valores reais
        yhat_col: Nome da coluna com valores previstos
        period_col: Nome da coluna de período
        group_cols: Lista de colunas para agrupamento
        
    Returns:
        DataFrame com comparação de métricas entre períodos
    """
    # Calcular métricas para cada período
    metrics_current = add_allocation_metrics(
        df_current, y_col, yhat_col, group_cols
    ).withColumn("periodo", F.lit("atual"))
    
    metrics_previous = add_allocation_metrics(
        df_previous, y_col, yhat_col, group_cols
    ).withColumn("periodo", F.lit("anterior"))
    
    # Unir os dois períodos
    df_combined = metrics_current.union(metrics_previous)
    
    # Pivotar para comparação lado a lado
    pivot_cols = ["wMAPE_perc", "sMAPE_perc", "SE_pp", "wMAPE_share_perc"]
    existing_pivot_cols = [col for col in pivot_cols if col in df_combined.columns]
    
    if not existing_pivot_cols:
        return df_combined.select(group_cols + ["periodo"]).limit(0)
    
    pivot_exprs = []
    for metric in existing_pivot_cols:
        pivot_exprs.append(
            F.max(F.when(F.col("periodo") == "atual", F.col(metric))).alias(f"{metric}_atual"),
            F.max(F.when(F.col("periodo") == "anterior", F.col(metric))).alias(f"{metric}_anterior")
        )
    
    if group_cols:
        return df_combined.groupBy(group_cols).agg(*pivot_exprs)
    else:
        return df_combined.agg(*pivot_exprs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Exemplo de Uso das Funções
# MAGIC
# MAGIC Demonstra como usar as funções de métricas em um cenário real.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Exemplo de Cálculo de Métricas Básicas
# MAGIC
# MAGIC ```python
# MAGIC # Exemplo de uso das funções de métricas
# MAGIC 
# MAGIC # 1. Validar dados
# MAGIC is_valid, message = validate_metrics_data(
# MAGIC     df_matriz_comparacao,
# MAGIC     y_col="pct_demanda_perc",
# MAGIC     yhat_col="Percentual_matriz_fixa"
# MAGIC )
# MAGIC 
# MAGIC if is_valid:
# MAGIC     # 2. Calcular métricas agregadas
# MAGIC     df_metrics = add_allocation_metrics(
# MAGIC         df=df_matriz_comparacao,
# MAGIC         y_col="pct_demanda_perc",
# MAGIC         yhat_col="Percentual_matriz_fixa",
# MAGIC         group_cols=["Cd_primario", "gemeos"]
# MAGIC     )
# MAGIC     
# MAGIC     # 3. Calcular métricas linha a linha
# MAGIC     df_line_metrics = calculate_line_metrics(
# MAGIC         df=df_matriz_comparacao,
# MAGIC         y_col="pct_demanda_perc",
# MAGIC         yhat_col="Percentual_matriz_fixa",
# MAGIC         group_cols=["Cd_primario", "gemeos"]
# MAGIC     )
# MAGIC     
# MAGIC     # 4. Gerar resumo estatístico
# MAGIC     df_summary = generate_metrics_summary(
# MAGIC         df_metrics,
# MAGIC         group_cols=["Cd_primario"]
# MAGIC     )
# MAGIC     
# MAGIC     print("✅ Métricas calculadas com sucesso!")
# MAGIC else:
# MAGIC     print(f"❌ Erro na validação: {message}")
# MAGIC ```

# COMMAND ----------

print("✅ Módulo de métricas da matriz de merecimento carregado com sucesso!")
print("📊 Funções disponíveis:")
print("  • add_allocation_metrics(): Métricas agregadas (wMAPE, SE, Cross Entropy, KL)")
print("  • calculate_line_metrics(): Métricas linha a linha")
print("  • validate_metrics_data(): Validação de dados")
print("  • generate_metrics_summary(): Resumo estatístico")
print("  • compare_metrics_periods(): Comparação entre períodos")
