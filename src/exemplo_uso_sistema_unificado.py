# Databricks notebook source
# MAGIC %md
# MAGIC # Exemplo de Uso do Sistema Unificado de Matriz de Merecimento
# MAGIC
# MAGIC Este notebook demonstra como usar o sistema unificado para calcular a matriz de merecimento
# MAGIC para diferentes categorias de produtos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importação do Sistema Unificado

# COMMAND ----------

# Importa o sistema unificado
from calculo_matriz_de_merecimento_unificado import (
    executar_calculo_matriz_merecimento,
    validar_resultados,
    REGRAS_AGRUPAMENTO,
    JANELAS_MOVEIS
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificação das Categorias Suportadas

# COMMAND ----------

print("🏢 CATEGORIAS SUPORTADAS PELO SISTEMA:")
print("=" * 60)

for categoria, config in REGRAS_AGRUPAMENTO.items():
    print(f"📋 {categoria}")
    print(f"   • Coluna de agrupamento: {config['coluna_grupo_necessidade']}")
    print(f"   • Tipo de agrupamento: {config['tipo_agrupamento']}")
    print(f"   • Descrição: {config['descricao']}")
    print()

print(f"📊 Janelas móveis disponíveis: {JANELAS_MOVEIS} dias")
print(f"🎯 Percentual de corte para médias aparadas: 10%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Como Funciona o Sistema de Agrupamento
# MAGIC
# MAGIC O sistema usa uma abstração inteligente chamada `grupo_de_necessidade` que se adapta automaticamente:
# MAGIC
# MAGIC **Exemplo 1 - DIRETORIA DE TELAS:**
# MAGIC - **Coluna origem**: `gemeos`
# MAGIC - **Valores copiados**: Todos os valores únicos da coluna `gemeos` (ex: "GRUPO_A", "GRUPO_B", "GRUPO_C")
# MAGIC - **Resultado**: `grupo_de_necessidade` contém os valores reais de `gemeos`
# MAGIC
# MAGIC **Exemplo 2 - DIRETORIA LINHA BRANCA:**
# MAGIC - **Coluna origem**: `NmEspecieGerencial`
# MAGIC - **Valores copiados**: Todos os valores únicos da coluna `NmEspecieGerencial` (ex: "GELADEIRA", "FOGÃO", "LAVADORA")
# MAGIC - **Resultado**: `grupo_de_necessidade` contém os valores reais de `NmEspecieGerencial`
# MAGIC
# MAGIC **Resumo**: `grupo_de_necessidade` = cópia dos valores da coluna especificada para cada categoria

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Configuração de Parâmetros Sigma para Outliers
# MAGIC
# MAGIC O sistema permite configurar diferentes níveis de sensibilidade para detecção de outliers:
# MAGIC
# MAGIC **Parâmetros Configuráveis**:
# MAGIC - **sigma_meses_atipicos**: Sensibilidade para meses atípicos (padrão: 3.0σ)
# MAGIC - **sigma_outliers_cd**: Sensibilidade para outliers a nível CD (padrão: 3.0σ)
# MAGIC - **sigma_outliers_loja**: Sensibilidade para outliers a nível loja (padrão: 3.0σ)
# MAGIC - **sigma_atacado_cd**: Sensibilidade para outliers CD em lojas de atacado (padrão: 1.5σ)
# MAGIC - **sigma_atacado_loja**: Sensibilidade para outliers loja em lojas de atacado (padrão: 1.5σ)
# MAGIC
# MAGIC **Guia de Sensibilidade**:
# MAGIC - **1.0σ - 2.0σ**: Muito restritivo (detecta muitos outliers)
# MAGIC - **2.0σ - 3.0σ**: Restritivo (detecção equilibrada)
# MAGIC - **3.0σ - 4.0σ**: Moderado (menos sensível)
# MAGIC - **4.0σ+**: Muito permissivo (poucos outliers detectados)

# COMMAND ----------

# Exemplos de configurações de sigma para diferentes cenários
print("🔧 EXEMPLOS DE CONFIGURAÇÕES DE SIGMA:")
print("=" * 60)

print("📊 CONFIGURAÇÃO PADRÃO (3.0σ):")
print("   • sigma_meses_atipicos: 3.0")
print("   • sigma_outliers_cd: 3.0")
print("   • sigma_outliers_loja: 3.0")
print("   • sigma_atacado_cd: 1.5")
print("   • sigma_atacado_loja: 1.5")

print("\n🎯 CONFIGURAÇÃO RESTRITIVA (2.0σ):")
print("   • sigma_meses_atipicos: 2.0")
print("   • sigma_outliers_cd: 2.0")
print("   • sigma_outliers_loja: 2.0")
print("   • sigma_atacado_cd: 1.0")
print("   • sigma_atacado_loja: 1.0")

print("\n🔍 CONFIGURAÇÃO PERMISSIVA (4.0σ):")
print("   • sigma_meses_atipicos: 4.0")
print("   • sigma_outliers_cd: 4.0")
print("   • sigma_outliers_loja: 4.0")
print("   • sigma_atacado_cd: 2.0")
print("   • sigma_atacado_loja: 2.0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Exemplo para DIRETORIA DE TELAS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execução para TELAS
# MAGIC 
# MAGIC Esta categoria usa `gemeos` como grupo de necessidade.

# COMMAND ----------

print("🖥️  EXECUTANDO PARA DIRETORIA DE TELAS")
print("=" * 50)

# Executa o cálculo para TELAS com parâmetros sigma padrão
df_telas = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")

# Exemplo com parâmetros sigma personalizados para TELAS
df_telas_personalizado = executar_calculo_matriz_merecimento(
    "DIRETORIA DE TELAS",
    sigma_meses_atipicos=2.5,      # Mais sensível a meses atípicos
    sigma_outliers_cd=2.8,         # Sensibilidade intermediária para CD
    sigma_outliers_loja=3.2,       # Menos sensível para lojas
    sigma_atacado_cd=1.2,          # Mais restritivo para atacado CD
    sigma_atacado_loja=1.8         # Menos restritivo para atacado loja
)

# Valida os resultados
validar_resultados(df_telas, "DIRETORIA DE TELAS")

# Exibe amostra dos resultados
print("\n📊 AMOSTRA DOS RESULTADOS (TELAS):")
display(df_telas.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Exemplo para DIRETORIA TELEFONIA CELULAR

# MAGIC %md
# MAGIC ### Execução para TELEFONIA
# MAGIC 
# MAGIC Esta categoria também usa `gemeos` como grupo de necessidade.

# COMMAND ----------

print("📱 EXECUTANDO PARA DIRETORIA TELEFONIA CELULAR")
print("=" * 50)

# Executa o cálculo para TELEFONIA com parâmetros sigma padrão
df_telefonia = executar_calculo_matriz_merecimento("DIRETORIA TELEFONIA CELULAR")

# Exemplo com parâmetros sigma personalizados para TELEFONIA
df_telefonia_personalizado = executar_calculo_matriz_merecimento(
    "DIRETORIA TELEFONIA CELULAR",
    sigma_meses_atipicos=3.5,      # Menos sensível a meses atípicos
    sigma_outliers_cd=3.0,         # Padrão para CD
    sigma_outliers_loja=2.5,       # Mais sensível para lojas
    sigma_atacado_cd=1.0,          # Muito restritivo para atacado CD
    sigma_atacado_loja=1.3         # Restritivo para atacado loja
)

# Valida os resultados
validar_resultados(df_telefonia, "DIRETORIA TELEFONIA CELULAR")

# Exibe amostra dos resultados
print("\n📊 AMOSTRA DOS RESULTADOS (TELEFONIA):")
display(df_telefonia.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exemplo para DIRETORIA LINHA BRANCA

# MAGIC %md
# MAGIC ### Execução para LINHA BRANCA
# MAGIC 
# MAGIC Esta categoria usa `NmEspecieGerencial` como grupo de necessidade.

# COMMAND ----------

print("🏠 EXECUTANDO PARA DIRETORIA LINHA BRANCA")
print("=" * 50)

# Executa o cálculo para LINHA BRANCA com parâmetros sigma padrão
df_linha_branca = executar_calculo_matriz_merecimento("DIRETORIA LINHA BRANCA")

# Exemplo com parâmetros sigma personalizados para LINHA BRANCA
df_linha_branca_personalizado = executar_calculo_matriz_merecimento(
    "DIRETORIA LINHA BRANCA",
    sigma_meses_atipicos=2.0,      # Muito sensível a meses atípicos
    sigma_outliers_cd=2.5,         # Sensível para CD
    sigma_outliers_loja=2.8,       # Sensível para lojas
    sigma_atacado_cd=1.5,          # Padrão para atacado CD
    sigma_atacado_loja=1.5         # Padrão para atacado loja
)

# Valida os resultados
validar_resultados(df_linha_branca, "DIRETORIA LINHA BRANCA")

# Exibe amostra dos resultados
print("\n📊 AMOSTRA DOS RESULTADOS (LINHA BRANCA):")
display(df_linha_branca.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Comparação entre Categorias

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise Comparativa
# MAGIC 
# MAGIC Vamos comparar os resultados entre as diferentes categorias para entender
# MAGIC como o sistema se comporta com diferentes regras de agrupamento.

# COMMAND ----------

def comparar_categorias(lista_dataframes, lista_categorias):
    """
    Compara os resultados entre diferentes categorias.
    
    Args:
        lista_dataframes: Lista de DataFrames com resultados
        lista_categorias: Lista de nomes das categorias
    """
    print("📊 COMPARAÇÃO ENTRE CATEGORIAS")
    print("=" * 60)
    
    for i, (df, categoria) in enumerate(zip(lista_dataframes, lista_categorias)):
        print(f"\n🔍 {categoria}:")
        print(f"   • Total de registros: {df.count():,}")
        print(f"   • SKUs únicos: {df.select('CdSku').distinct().count():,}")
        print(f"   • Lojas únicas: {df.select('CdFilial').distinct().count():,}")
        print(f"   • Grupos de necessidade: {df.select('grupo_de_necessidade').distinct().count():,}")
        
        # Verifica se tem colunas de gêmeos
        if 'gemeos' in df.columns:
            gemeos_unicos = df.select('gemeos').distinct().count()
            print(f"   • Gêmeos únicos: {gemeos_unicos:,}")
        
        # Verifica se tem colunas de espécie gerencial
        if 'NmEspecieGerencial' in df.columns:
            especies_unicas = df.select('NmEspecieGerencial').distinct().count()
            print(f"   • Espécies gerenciais únicas: {especies_unicas:,}")

# Lista de DataFrames e categorias para comparação
dataframes = [df_telas, df_telefonia, df_linha_branca]
categorias = ["DIRETORIA DE TELAS", "DIRETORIA TELEFONIA CELULAR", "DIRETORIA LINHA BRANCA"]

# Executa a comparação
comparar_categorias(dataframes, categorias)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Análise das Médias Aparadas

# MAGIC %md
# MAGIC ### Verificação das Médias Aparadas
# MAGIC 
# MAGIC Vamos analisar como as médias aparadas se comportam em comparação
# MAGIC com as médias normais e medianas.

# COMMAND ----------

def analisar_medias_aparadas(df: DataFrame, categoria: str):
    """
    Analisa o comportamento das médias aparadas.
    
    Args:
        df: DataFrame com os resultados
        categoria: Nome da categoria
    """
    print(f"📈 ANÁLISE DAS MÉDIAS APARADAS - {categoria}")
    print("=" * 60)
    
    # Seleciona apenas registros com valores válidos para análise
    df_analise = df.filter(
        (F.col("Media90_Qt_venda_sem_ruptura") > 0) &
        (F.col("Mediana90_Qt_venda_sem_ruptura") > 0) &
        (F.col("MediaAparada90_Qt_venda_sem_ruptura") > 0)
    )
    
    if df_analise.count() > 0:
        # Calcula estatísticas para cada tipo de média
        stats = df_analise.select([
            F.avg("Media90_Qt_venda_sem_ruptura").alias("media_aritmetica_90"),
            F.avg("Mediana90_Qt_venda_sem_ruptura").alias("mediana_90"),
            F.avg("MediaAparada90_Qt_venda_sem_ruptura").alias("media_aparada_90"),
            F.stddev("Media90_Qt_venda_sem_ruptura").alias("std_aritmetica_90"),
            F.stddev("Mediana90_Qt_venda_sem_ruptura").alias("std_mediana_90"),
            F.stddev("MediaAparada90_Qt_venda_sem_ruptura").alias("std_aparada_90")
        ]).collect()[0]
        
        print("📊 Estatísticas para janela de 90 dias:")
        print(f"   • Média aritmética: {stats['media_aritmetica_90']:.2f} ± {stats['std_aritmetica_90']:.2f}")
        print(f"   • Mediana: {stats['mediana_90']:.2f} ± {stats['std_mediana_90']:.2f}")
        print(f"   • Média aparada: {stats['media_aparada_90']:.2f} ± {stats['std_aparada_90']:.2f}")
        
        # Calcula correlações
        correlacoes = df_analise.select([
            F.corr("Media90_Qt_venda_sem_ruptura", "MediaAparada90_Qt_venda_sem_ruptura").alias("corr_aritmetica_aparada"),
            F.corr("Mediana90_Qt_venda_sem_ruptura", "MediaAparada90_Qt_venda_sem_ruptura").alias("corr_mediana_aparada")
        ]).collect()[0]
        
        print(f"\n🔗 Correlações:")
        print(f"   • Aritmética vs Aparada: {correlacoes['corr_aritmetica_aparada']:.3f}")
        print(f"   • Mediana vs Aparada: {correlacoes['corr_mediana_aparada']:.3f}")
        
    else:
        print("⚠️  Não há dados suficientes para análise das médias aparadas")

# Analisa as médias aparadas para cada categoria
for df, categoria in zip(dataframes, categorias):
    analisar_medias_aparadas(df, categoria)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Exportação dos Resultados

# MAGIC %md
# MAGIC ### Salvando os Resultados
# MAGIC 
# MAGIC Vamos salvar os resultados para uso posterior.

# COMMAND ----------

def salvar_resultados(df: DataFrame, categoria: str, formato: str = "delta"):
    """
    Salva os resultados em diferentes formatos.
    
    Args:
        df: DataFrame com os resultados
        categoria: Nome da categoria
        formato: Formato de saída (delta, parquet, csv)
    """
    print(f"💾 Salvando resultados para: {categoria}")
    
    # Normaliza o nome da categoria para uso em nomes de arquivo
    nome_arquivo = categoria.replace(" ", "_").replace("/", "_").lower()
    
    if formato == "delta":
        # Salva como tabela Delta
        df.write.mode("overwrite").format("delta").save(f"/tmp/matriz_merecimento_{nome_arquivo}")
        print(f"✅ Resultados salvos em Delta: /tmp/matriz_merecimento_{nome_arquivo}")
        
    elif formato == "parquet":
        # Salva como arquivo Parquet
        df.write.mode("overwrite").parquet(f"/tmp/matriz_merecimento_{nome_arquivo}.parquet")
        print(f"✅ Resultados salvos em Parquet: /tmp/matriz_merecimento_{nome_arquivo}.parquet")
        
    elif formato == "csv":
        # Salva como CSV (apenas para amostras pequenas)
        if df.count() <= 100000:  # Limite para CSV
            df.toPandas().to_csv(f"/tmp/matriz_merecimento_{nome_arquivo}.csv", index=False)
            print(f"✅ Resultados salvos em CSV: /tmp/matriz_merecimento_{nome_arquivo}.csv")
        else:
            print("⚠️  Dataset muito grande para CSV. Use Delta ou Parquet.")

# Salva os resultados em formato Delta
for df, categoria in zip(dataframes, categorias):
    salvar_resultados(df, categoria, "delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumo Final

# COMMAND ----------

print("🎯 RESUMO DA EXECUÇÃO")
print("=" * 60)
print("✅ Sistema unificado de matriz de merecimento executado com sucesso!")
print()
print("📊 Categorias processadas:")
for categoria in categorias:
    print(f"   • {categoria}")
print()
print("🔧 Funcionalidades implementadas:")
print("   • Abstração grupo_de_necessidade com regras específicas por categoria")
print("   • Médias móveis normais (90, 180, 270, 360 dias)")
print("   • Medianas móveis (90, 180, 270, 360 dias)")
print("   • Médias móveis aparadas (90, 180, 270, 360 dias)")
print("   • Detecção automática de outliers e meses atípicos")
print("   • Filtragem inteligente por grupo de necessidade")
print()
print("💡 Próximos passos:")
print("   • Ajustar parâmetros de outliers conforme necessário")
print("   • Implementar validações adicionais de negócio")
print("   • Criar dashboards de monitoramento")
print("   • Automatizar execução via DAGs")
