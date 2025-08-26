# Databricks notebook source
# MAGIC %md
# MAGIC # Exemplo de Uso das Métricas da Matriz de Merecimento
# MAGIC
# MAGIC Este notebook demonstra como usar as funções de métricas para avaliar a qualidade
# MAGIC das alocações da matriz de merecimento, comparando valores previstos vs. reais.
# MAGIC
# MAGIC **Objetivo**: Demonstrar o uso prático das funções de métricas implementadas
# MAGIC **Escopo**: Exemplos com dados simulados e reais da matriz de merecimento

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
spark = SparkSession.builder.appName("exemplo_uso_metricas").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Importação das Funções de Métricas
# MAGIC
# MAGIC Importamos as funções de métricas para avaliação da matriz de merecimento.

# COMMAND ----------

# Importação das funções de métricas
from metricas_matriz_merecimento import (
    add_allocation_metrics,
    calculate_line_metrics,
    validate_metrics_data,
    generate_metrics_summary,
    compare_metrics_periods
)

print("✅ Funções de métricas importadas com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criação de Dados Simulados para Demonstração
# MAGIC
# MAGIC Criamos dados simulados para demonstrar o uso das funções de métricas.

# COMMAND ----------

# Dados simulados para demonstração
dados_simulados = [
    # CdFilial, Cd_primario, gemeos, demanda_real, demanda_prevista
    (1, "CD_A", "GEMEO_1", 100.0, 95.0),
    (2, "CD_A", "GEMEO_1", 80.0, 85.0),
    (3, "CD_A", "GEMEO_1", 120.0, 110.0),
    (4, "CD_B", "GEMEO_1", 90.0, 100.0),
    (5, "CD_B", "GEMEO_1", 110.0, 105.0),
    (6, "CD_B", "GEMEO_1", 95.0, 90.0),
    (1, "CD_A", "GEMEO_2", 150.0, 140.0),
    (2, "CD_A", "GEMEO_2", 130.0, 135.0),
    (3, "CD_A", "GEMEO_2", 160.0, 155.0),
    (4, "CD_B", "GEMEO_2", 140.0, 150.0),
    (5, "CD_B", "GEMEO_2", 170.0, 165.0),
    (6, "CD_B", "GEMEO_2", 125.0, 130.0)
]

# Criação do DataFrame Spark
df_simulado = spark.createDataFrame(
    dados_simulados,
    ["CdFilial", "Cd_primario", "gemeos", "demanda_real", "demanda_prevista"]
)

print("✅ Dados simulados criados:")
print(f"  • Total de registros: {df_simulado.count()}")
print(f"  • CDs: {df_simulado.select('Cd_primario').distinct().count()}")
print(f"  • Gêmeos: {df_simulado.select('gemeos').distinct().count()}")
print(f"  • Filiais: {df_simulado.select('CdFilial').distinct().count()}")

# Visualização dos dados
print("\n📊 Dados simulados:")
df_simulado.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validação dos Dados
# MAGIC
# MAGIC Validamos se os dados estão em formato adequado para cálculo das métricas.

# COMMAND ----------

# Validação dos dados simulados
is_valid, message = validate_metrics_data(
    df_simulado,
    y_col="demanda_real",
    yhat_col="demanda_prevista",
    required_cols=["CdFilial", "Cd_primario", "gemeos"]
)

if is_valid:
    print(f"✅ {message}")
else:
    print(f"❌ {message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cálculo de Métricas Agregadas
# MAGIC
# MAGIC Calculamos métricas agregadas para avaliar a qualidade geral das alocações.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Métricas Agregadas Globais
# MAGIC
# MAGIC Calculamos métricas para todo o conjunto de dados.

# COMMAND ----------

# Métricas agregadas globais
df_metrics_global = add_allocation_metrics(
    df=df_simulado,
    y_col="demanda_real",
    yhat_col="demanda_prevista"
)

print("📊 MÉTRICAS AGREGADAS GLOBAIS:")
print("=" * 50)
df_metrics_global.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Métricas Agregadas por CD Primário
# MAGIC
# MAGIC Calculamos métricas agregadas por centro de distribuição.

# COMMAND ----------

# Métricas agregadas por CD
df_metrics_por_cd = add_allocation_metrics(
    df=df_simulado,
    y_col="demanda_real",
    yhat_col="demanda_prevista",
    group_cols=["Cd_primario"]
)

print("📊 MÉTRICAS AGREGADAS POR CD:")
print("=" * 50)
df_metrics_por_cd.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Métricas Agregadas por CD e Gêmeo
# MAGIC
# MAGIC Calculamos métricas agregadas por combinação de CD e gêmeo.

# COMMAND ----------

# Métricas agregadas por CD e gêmeo
df_metrics_por_cd_gemeo = add_allocation_metrics(
    df=df_simulado,
    y_col="demanda_real",
    yhat_col="demanda_prevista",
    group_cols=["Cd_primario", "gemeos"]
)

print("📊 MÉTRICAS AGREGADAS POR CD E GÊMEO:")
print("=" * 50)
df_metrics_por_cd_gemeo.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cálculo de Métricas Linha a Linha
# MAGIC
# MAGIC Calculamos métricas detalhadas para cada linha individual.

# COMMAND ----------

# Métricas linha a linha
df_metrics_linha = calculate_line_metrics(
    df=df_simulado,
    y_col="demanda_real",
    yhat_col="demanda_prevista",
    group_cols=["Cd_primario", "gemeos"]
)

print("📊 MÉTRICAS LINHA A LINHA:")
print("=" * 50)
df_metrics_linha.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumo Estatístico das Métricas
# MAGIC
# MAGIC Geramos um resumo estatístico das métricas calculadas.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 Resumo Estatístico Global
# MAGIC
# MAGIC Resumo estatístico para todas as métricas calculadas.

# COMMAND ----------

# Resumo estatístico global
df_resumo_global = generate_metrics_summary(df_metrics_global)

print("📋 RESUMO ESTATÍSTICO GLOBAL:")
print("=" * 50)
df_resumo_global.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Resumo Estatístico por CD
# MAGIC
# MAGIC Resumo estatístico das métricas por centro de distribuição.

# COMMAND ----------

# Resumo estatístico por CD
df_resumo_por_cd = generate_metrics_summary(
    df_metrics_por_cd,
    group_cols=["Cd_primario"]
)

print("📋 RESUMO ESTATÍSTICO POR CD:")
print("=" * 50)
df_resumo_por_cd.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Análise Comparativa entre CDs
# MAGIC
# MAGIC Analisamos a performance comparativa entre diferentes centros de distribuição.

# COMMAND ----------

# Análise comparativa entre CDs
print("🔍 ANÁLISE COMPARATIVA ENTRE CDs:")
print("=" * 50)

# Comparar wMAPE entre CDs
df_comparacao_cds = df_metrics_por_cd.select("Cd_primario", "wMAPE_perc", "SE_pp", "Cross_entropy")
df_comparacao_cds.show()

# Ranking dos CDs por performance (menor wMAPE = melhor)
print("\n🏆 RANKING DOS CDs POR PERFORMANCE (wMAPE):")
df_ranking_cds = df_metrics_por_cd.orderBy("wMAPE_perc").select("Cd_primario", "wMAPE_perc")
df_ranking_cds.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Análise por Gêmeo
# MAGIC
# MAGIC Analisamos a performance por grupo de produtos similares.

# COMMAND ----------

# Análise por gêmeo
print("🔍 ANÁLISE POR GÊMEO:")
print("=" * 50)

# Métricas agregadas por gêmeo
df_metrics_por_gemeo = add_allocation_metrics(
    df=df_simulado,
    y_col="demanda_real",
    yhat_col="demanda_prevista",
    group_cols=["gemeos"]
)

df_metrics_por_gemeo.show()

# Ranking dos gêmeos por performance
print("\n🏆 RANKING DOS GÊMEOS POR PERFORMANCE (wMAPE):")
df_ranking_gemeos = df_metrics_por_gemeo.orderBy("wMAPE_perc").select("gemeos", "wMAPE_perc")
df_ranking_gemeos.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Exemplo com Dados Reais da Matriz de Merecimento
# MAGIC
# MAGIC Demonstramos como usar as métricas com dados reais da matriz de merecimento.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 Carregamento de Dados Reais (Exemplo)
# MAGIC
# MAGIC **Nota**: Este é um exemplo de como carregar dados reais. 
# MAGIC Os dados reais devem ser carregados conforme implementado no notebook principal.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Exemplo de carregamento de dados reais da matriz de merecimento
# MAGIC 
# MAGIC # 1. Carregar dados da matriz calculada
# MAGIC df_matriz_calculada = spark.table("matriz_merecimento_calculada")
# MAGIC 
# MAGIC # 2. Carregar dados reais de demanda
# MAGIC df_demanda_real = spark.table("dados_demanda_real")
# MAGIC 
# MAGIC # 3. Preparar dados para comparação
# MAGIC df_comparacao = (
# MAGIC     df_matriz_calculada
# MAGIC     .join(
# MAGIC         df_demanda_real,
# MAGIC         on=["CdFilial", "Cd_primario", "gemeos"],
# MAGIC         how="inner"
# MAGIC     )
# MAGIC )
# MAGIC 
# MAGIC # 4. Calcular métricas
# MAGIC df_metrics_reais = add_allocation_metrics(
# MAGIC     df=df_comparacao,
# MAGIC     y_col="demanda_real",
# MAGIC     yhat_col="merecimento_calculado",
# MAGIC     group_cols=["Cd_primario", "gemeos"]
# MAGIC )
# MAGIC 
# MAGIC print("✅ Métricas calculadas com dados reais!")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Interpretação das Métricas
# MAGIC
# MAGIC Explicamos como interpretar cada uma das métricas calculadas.

# COMMAND ----------

print("📚 INTERPRETAÇÃO DAS MÉTRICAS:")
print("=" * 50)

print("\n🔍 wMAPE (Weighted Mean Absolute Percentage Error):")
print("  • Mede o erro percentual absoluto ponderado pelo volume")
print("  • Quanto menor, melhor a precisão da matriz")
print("  • Interpretação: < 10% = Excelente, 10-20% = Bom, > 20% = Precisa melhorar")

print("\n🔍 SE (Share Error):")
print("  • Mede o erro na distribuição de participações entre filiais")
print("  • Quanto menor, melhor a distribuição")
print("  • Interpretação: < 5 pp = Excelente, 5-10 pp = Bom, > 10 pp = Precisa melhorar")

print("\n🔍 Cross Entropy:")
print("  • Mede a divergência entre distribuições reais e previstas")
print("  • Quanto menor, mais similares as distribuições")
print("  • Interpretação: < 0.1 = Excelente, 0.1-0.3 = Bom, > 0.3 = Precisa melhorar")

print("\n🔍 KL Divergence:")
print("  • Mede a divergência de Kullback-Leibler entre distribuições")
print("  • Quanto menor, mais similares as distribuições")
print("  • Interpretação: < 0.1 = Excelente, 0.1-0.3 = Bom, > 0.3 = Precisa melhorar")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Recomendações de Uso
# MAGIC
# MAGIC Apresentamos recomendações para o uso efetivo das métricas.

# COMMAND ----------

print("💡 RECOMENDAÇÕES DE USO:")
print("=" * 50)

print("\n1️⃣ **Monitoramento Contínuo**:")
print("  • Calcule as métricas regularmente (mensal/trimestral)")
print("  • Acompanhe a evolução ao longo do tempo")
print("  • Identifique tendências de melhoria ou deterioração")

print("\n2️⃣ **Análise por Segmento**:")
print("  • Analise métricas por CD, gêmeo, região")
print("  • Identifique padrões e outliers")
print("  • Foque esforços de melhoria nos segmentos com pior performance")

print("\n3️⃣ **Comparação de Abordagens**:")
print("  • Compare diferentes métodos de cálculo de demanda")
print("  • Avalie qual abordagem gera melhores resultados")
print("  • Use as métricas para otimizar parâmetros")

print("\n4️⃣ **Ações Corretivas**:")
print("  • Use as métricas para identificar problemas específicos")
print("  • Implemente melhorias baseadas nos resultados")
print("  • Monitore o impacto das mudanças implementadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Conclusão
# MAGIC
# MAGIC Este notebook demonstrou o uso prático das funções de métricas para avaliação
# MAGIC da matriz de merecimento, fornecendo ferramentas robustas para análise de qualidade.

# COMMAND ----------

print("✅ EXEMPLO DE USO DAS MÉTRICAS CONCLUÍDO!")
print("=" * 50)
print("\n📊 Funções demonstradas:")
print("  • add_allocation_metrics(): Métricas agregadas")
print("  • calculate_line_metrics(): Métricas linha a linha")
print("  • validate_metrics_data(): Validação de dados")
print("  • generate_metrics_summary(): Resumo estatístico")
print("  • compare_metrics_periods(): Comparação entre períodos")

print("\n🎯 Próximos passos:")
print("  • Aplique as métricas aos dados reais da matriz de merecimento")
print("  • Implemente monitoramento contínuo das métricas")
print("  • Use os resultados para otimização da matriz")
print("  • Compartilhe insights com stakeholders relevantes")
