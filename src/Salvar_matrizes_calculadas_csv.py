# Databricks notebook source
# MAGIC %md
# MAGIC # Salvamento de Matrizes de Merecimento - Formato CSV para Sistema
# MAGIC
# MAGIC Este notebook implementa o salvamento unificado de matrizes de merecimento em formato CSV
# MAGIC com as seguintes especificações:
# MAGIC
# MAGIC **Formato de saída**: CSV sem index
# MAGIC **Colunas**: SKU, CANAL, LOJA, DATA FIM, PERCENTUAL, VERIFICAR, FASE DE VIDA
# MAGIC **Regras**:
# MAGIC - LOJA: formato 0021_0XXXX (5 dígitos, zeros à esquerda)
# MAGIC - DATA FIM: DATA_CALCULO + 60 dias (formato yyyyMMdd)
# MAGIC - PERCENTUAL: normalizado para 100% por CdSku+CANAL, ajuste no maior merecimento
# MAGIC - Máximo 250.000 linhas por arquivo
# MAGIC - Mesmo SKU-FILIAL sempre no mesmo arquivo (ambos canais)

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window as W
from datetime import datetime, timedelta
import os
from typing import List, Dict, Tuple

# Inicialização do Spark
spark = SparkSession.builder.appName("salvar_matrizes_merecimento_csv").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurações

# COMMAND ----------

# Data de cálculo e data fim
DATA_CALCULO = "2025-12-02"  # Ajustar conforme necessário
data_calculo_dt = datetime.strptime(DATA_CALCULO, "%Y-%m-%d")
data_fim_dt = data_calculo_dt + timedelta(days=60)
DATA_FIM_INT = int(data_fim_dt.strftime("%Y%m%d"))

print(f"📅 Data de cálculo: {DATA_CALCULO}")
print(f"📅 Data fim (+ 60 dias): {data_fim_dt.strftime('%Y-%m-%d')} → {DATA_FIM_INT}")

# Configuração das tabelas por categoria
TABELAS_MATRIZ_MERECIMENTO = {
    "DIRETORIA DE TELAS": {
        "offline": "databox.bcg_comum.supply_matriz_merecimento_de_telas_teste2509",
        "online": "databox.bcg_comum.supply_matriz_merecimento_de_telas_online_teste2609",
        "grupo_apelido": "telas"
    },
    "DIRETORIA TELEFONIA CELULAR": {
        "offline": "databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_teste1009",
        "online": "databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_online_teste0809",
        "grupo_apelido": "telefonia"
    },
    "DIRETORIA LINHA LEVE": {
        "offline": "databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_teste0110",
        "online": "databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_online_teste0110",
        "grupo_apelido": "linha_leve"
    },
}

# Configuração da pasta de saída
PASTA_OUTPUT = "/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/output"

# Configuração da coluna de merecimento por categoria
COLUNAS_MERECIMENTO = {
    "DIRETORIA DE TELAS": "Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura",
    "DIRETORIA TELEFONIA CELULAR": "Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura",
    "DIRETORIA LINHA LEVE": "Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura",
}

# Configuração de filtros por categoria
FILTROS_GRUPO_NECESSIDADE_REMOCAO = {
    "DIRETORIA DE TELAS": ["FORA DE LINHA", "SEM_GN"],
    "DIRETORIA TELEFONIA CELULAR": ["FORA DE LINHA", "SEM_GN"],
    "DIRETORIA LINHA LEVE": ["FORA DE LINHA", "SEM_GN"],
}

FLAG_SELECAO_REMOCAO = {
    "DIRETORIA DE TELAS": "REMOÇÃO",
    "DIRETORIA TELEFONIA CELULAR": "SELEÇÃO",
    "DIRETORIA LINHA LEVE": "REMOÇÃO",
}

FILTROS_GRUPO_NECESSIDADE_SELECAO = {
    "DIRETORIA DE TELAS": [],
    "DIRETORIA TELEFONIA CELULAR": ["Telef pp"],
    "DIRETORIA LINHA LEVE": [],
}

# Configuração de replicação de matrizes para novos produtos
CONFIGURACAO_REPLICACAO_MATRIZES = {
    "DIRETORIA TELEFONIA CELULAR": {
        "Telef pp": [
            5358744,  # CEL.DESB. SAMSUNG GALAXY A07 4G 256GB VIOLETA
            5358752,  # CEL.DESB. SAMSUNG GALAXY A07 4G 128GB PRETO
            5358760,  # CEL.DESB. SAMSUNG GALAXY A07 4G 256GB VERDE
            5358779,  # CEL.DESB. SAMSUNG GALAXY A07 4G 256GB PRETO
            5358787,  # CEL.DESB. SAMSUNG GALAXY A07 4G 128GB VERDE
            5358795   # CEL.DESB. SAMSUNG GALAXY A07 4G 128GB VIOLETA
        ]
    }
}

# Limite de linhas por arquivo CSV
MAX_LINHAS_POR_ARQUIVO = 250000

print("✅ Configurações carregadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções de Formatação

# COMMAND ----------

def formatar_codigo_loja(cdfilial: int) -> str:
    """
    Formata o código da loja no padrão 0021_0XXXX.
    
    Args:
        cdfilial: Código numérico da filial
        
    Returns:
        String formatada (ex: 0021_01234)
    """
    return f"0021_{cdfilial:05d}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Função de Processamento

# COMMAND ----------

def processar_matriz_merecimento(categoria: str, canal: str) -> DataFrame:
    """
    Processa a matriz de merecimento para uma categoria e canal específicos.
    
    Args:
        categoria: Categoria da diretoria
        canal: Canal (offline ou online)
        
    Returns:
        DataFrame processado com colunas finais
    """
    print(f"🔄 Processando matriz para: {categoria} - {canal}")
    
    # Configurações específicas
    tabela = TABELAS_MATRIZ_MERECIMENTO[categoria][canal]
    coluna_merecimento = COLUNAS_MERECIMENTO[categoria]
    flag_tipo = FLAG_SELECAO_REMOCAO.get(categoria, "REMOÇÃO")
    filtros_grupo_remocao = FILTROS_GRUPO_NECESSIDADE_REMOCAO[categoria]
    filtros_grupo_selecao = FILTROS_GRUPO_NECESSIDADE_SELECAO[categoria]
    
    print(f"  • Tabela: {tabela}")
    print(f"  • Tipo de filtro: {flag_tipo}")
    
    # Carregamento dos dados base
    df_base = (
        spark.table(tabela)
        .select(
            "CdFilial", "CdSku", "grupo_de_necessidade",
            (100 * F.col(coluna_merecimento)).alias("Merecimento_raw")
        )
    )
    
    # Aplicar filtro baseado no flag
    if flag_tipo == "SELEÇÃO":
        df_raw = df_base.filter(F.col("grupo_de_necessidade").isin(filtros_grupo_selecao))
        print(f"  • Aplicado filtro de SELEÇÃO: {filtros_grupo_selecao}")
    else:
        df_raw = df_base.filter(~F.col("grupo_de_necessidade").isin(filtros_grupo_remocao))
        print(f"  • Aplicado filtro de REMOÇÃO: {filtros_grupo_remocao}")
    
    # Regra especial para canal online: sobrescrever CdFilial 1401 → 14
    if canal == "online":
        df_raw = df_raw.withColumn("CdFilial", F.when(F.col("CdFilial") == 1401, 14).otherwise(F.col("CdFilial")))
        print("  • Aplicada regra especial: CdFilial 1401 → 14")
    
    # Agregação por CdSku + CdFilial (média de merecimento)
    df_agregado = (
        df_raw
        .groupBy("CdSku", "CdFilial")
        .agg(F.avg("Merecimento_raw").alias("Merecimento"))
    )
    
    print(f"✅ Matriz processada: {df_agregado.count():,} registros")
    
    return df_agregado

# COMMAND ----------

def replicar_skus_novos(df: DataFrame, categoria: str) -> DataFrame:
    """
    Replica merecimentos de grupos origem para SKUs novos.
    
    Args:
        df: DataFrame com matriz processada (CdSku, CdFilial, Merecimento)
        categoria: Categoria da diretoria
        
    Returns:
        DataFrame com SKUs replicados adicionados
    """
    if categoria not in CONFIGURACAO_REPLICACAO_MATRIZES:
        print(f"ℹ️ Nenhuma configuração de replicação para: {categoria}")
        return df
    
    config_categoria = CONFIGURACAO_REPLICACAO_MATRIZES[categoria]
    print(f"🔄 Replicando SKUs para: {categoria}")
    
    dfs_replicados = []
    
    for grupo_origem, skus_novos in config_categoria.items():
        print(f"  📋 Grupo: {grupo_origem} → {len(skus_novos)} SKUs novos")
        
        # Buscar um SKU representativo do grupo origem (qualquer SKU do grupo)
        # Como já agregamos por CdSku+CdFilial, precisamos buscar na matriz original
        # Vamos usar a primeira ocorrência
        
        # Criar merecimentos replicados para cada SKU novo
        for sku_novo in skus_novos:
            df_sku_replicado = (
                df
                .groupBy("CdFilial")
                .agg(F.avg("Merecimento").alias("Merecimento"))
                .withColumn("CdSku", F.lit(sku_novo))
                .select("CdSku", "CdFilial", "Merecimento")
            )
            dfs_replicados.append(df_sku_replicado)
    
    if dfs_replicados:
        from functools import reduce
        df_replicados_union = reduce(DataFrame.union, dfs_replicados)
        df_final = df.union(df_replicados_union)
        print(f"✅ Replicação concluída: +{df_replicados_union.count():,} registros")
        return df_final
    
    return df

# COMMAND ----------

def normalizar_merecimento_100(df: DataFrame, canal: str) -> DataFrame:
    """
    Normaliza o merecimento para somar 100% por CdSku+CANAL.
    Ajusta a diferença no maior merecimento de cada grupo.
    
    Args:
        df: DataFrame com CdSku, CdFilial, Merecimento, CANAL
        canal: Canal atual (para logging)
        
    Returns:
        DataFrame com merecimentos normalizados
    """
    print(f"🔄 Normalizando merecimentos para 100% - Canal: {canal}")
    
    # Calcular soma por CdSku + CANAL
    window_sku_canal = W.partitionBy("CdSku", "CANAL")
    
    df_normalizado = (
        df
        .withColumn("soma_sku_canal", F.sum("Merecimento").over(window_sku_canal))
        .withColumn(
            "Merecimento_proporcional",
            F.when(F.col("soma_sku_canal") > 0, (F.col("Merecimento") / F.col("soma_sku_canal")) * 100.0)
            .otherwise(0.0)
        )
    )
    
    # Identificar o maior merecimento por CdSku+CANAL
    window_maior = W.partitionBy("CdSku", "CANAL").orderBy(F.desc("Merecimento_proporcional"))
    
    df_com_rank = (
        df_normalizado
        .withColumn("rank", F.row_number().over(window_maior))
    )
    
    # Calcular diferença para 100%
    df_com_diferenca = (
        df_com_rank
        .withColumn("soma_proporcional", F.sum("Merecimento_proporcional").over(window_sku_canal))
        .withColumn("diferenca_100", 100.0 - F.col("soma_proporcional"))
    )
    
    # Ajustar apenas o maior merecimento (rank = 1)
    df_ajustado = (
        df_com_diferenca
        .withColumn(
            "PERCENTUAL",
            F.when(F.col("rank") == 1, F.col("Merecimento_proporcional") + F.col("diferenca_100"))
            .otherwise(F.col("Merecimento_proporcional"))
        )
        .drop("soma_sku_canal", "Merecimento_proporcional", "rank", "soma_proporcional", "diferenca_100", "Merecimento")
    )
    
    print(f"✅ Normalização concluída")
    
    return df_ajustado

# COMMAND ----------

def criar_dataframe_final(df: DataFrame, canal: str) -> DataFrame:
    """
    Cria o DataFrame final com todas as colunas no formato esperado.
    
    Args:
        df: DataFrame com CdSku, CdFilial, PERCENTUAL, CANAL
        canal: Canal (online/offline)
        
    Returns:
        DataFrame com colunas: SKU, CANAL, LOJA, DATA FIM, PERCENTUAL, VERIFICAR, FASE DE VIDA
    """
    print(f"🔄 Criando DataFrame final - Canal: {canal}")
    
    # UDF para formatar código da loja
    formatar_loja_udf = F.udf(formatar_codigo_loja, "string")
    
    df_final = (
        df
        .withColumn("SKU", F.col("CdSku").cast("string"))
        .withColumn("LOJA", formatar_loja_udf(F.col("CdFilial")))
        .withColumn("DATA FIM", F.lit(DATA_FIM_INT))
        .withColumn("VERIFICAR", F.lit(""))
        .withColumn("FASE DE VIDA", F.lit("SEM FASE"))
        .withColumn("PERCENTUAL", F.round(F.col("PERCENTUAL"), 3))
        .select("SKU", "CANAL", "LOJA", "DATA FIM", "PERCENTUAL", "VERIFICAR", "FASE DE VIDA")
    )
    
    print(f"✅ DataFrame final criado: {df_final.count():,} registros")
    
    return df_final

# COMMAND ----------

def dividir_em_arquivos(df: DataFrame, max_linhas: int = MAX_LINHAS_POR_ARQUIVO) -> List[DataFrame]:
    """
    Divide o DataFrame em múltiplos DataFrames garantindo que o mesmo SKU-LOJA
    (ambos canais) fique sempre no mesmo arquivo.
    
    Args:
        df: DataFrame completo
        max_linhas: Número máximo de linhas por arquivo
        
    Returns:
        Lista de DataFrames
    """
    print(f"🔄 Dividindo em arquivos (máx {max_linhas:,} linhas cada)")
    
    # Criar chave única por SKU-LOJA (ignorando canal)
    df_com_chave = df.withColumn("chave_particao", F.concat(F.col("SKU"), F.lit("_"), F.col("LOJA")))
    
    # Contar registros por chave (online + offline = 2 linhas por chave)
    df_contagem = (
        df_com_chave
        .groupBy("chave_particao")
        .agg(F.count("*").alias("qtd_registros"))
    )
    
    # Ordenar por chave e adicionar número de partição
    window_particao = W.orderBy("chave_particao").rowsBetween(W.unboundedPreceding, W.currentRow)
    
    df_com_particao = (
        df_contagem
        .withColumn("acumulado", F.sum("qtd_registros").over(window_particao))
        .withColumn("num_arquivo", (F.col("acumulado") / max_linhas).cast("int"))
    )
    
    # Join de volta para associar cada registro ao arquivo
    df_final = (
        df_com_chave
        .join(df_com_particao.select("chave_particao", "num_arquivo"), on="chave_particao", how="left")
        .drop("chave_particao")
    )
    
    # Separar em DataFrames
    num_arquivos = df_final.select(F.max("num_arquivo")).collect()[0][0] + 1
    print(f"  • Total de arquivos necessários: {num_arquivos}")
    
    dfs_separados = []
    for i in range(num_arquivos):
        df_arquivo = df_final.filter(F.col("num_arquivo") == i).drop("num_arquivo")
        qtd = df_arquivo.count()
        print(f"    - Arquivo {i+1}: {qtd:,} linhas")
        dfs_separados.append(df_arquivo)
    
    return dfs_separados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Função Principal de Exportação

# COMMAND ----------

def exportar_matriz_csv(categoria: str, data_exportacao: str = None) -> List[str]:
    """
    Exporta matriz de merecimento em formato CSV para uma categoria.
    
    Args:
        categoria: Categoria da diretoria
        data_exportacao: Data de exportação (padrão: DATA_CALCULO)
        
    Returns:
        Lista de caminhos dos arquivos CSV salvos
    """
    if data_exportacao is None:
        data_exportacao = DATA_CALCULO
    
    print(f"🚀 Iniciando exportação CSV para: {categoria}")
    print("=" * 80)
    
    grupo_apelido = TABELAS_MATRIZ_MERECIMENTO[categoria]["grupo_apelido"]
    
    # Criar pasta de saída
    pasta_data = f"{PASTA_OUTPUT}/{data_exportacao}"
    os.makedirs(pasta_data, exist_ok=True)
    
    # Processar canais
    dfs_canais = []
    
    for canal in ["offline", "online"]:
        print(f"\n📊 Processando canal: {canal.upper()}")
        df_canal = processar_matriz_merecimento(categoria, canal)
        df_canal = replicar_skus_novos(df_canal, categoria)
        df_canal = df_canal.withColumn("CANAL", F.lit(canal.upper()))
        dfs_canais.append(df_canal)
    
    # União dos canais
    print("\n🔗 Unindo canais...")
    df_union = dfs_canais[0].union(dfs_canais[1])
    
    # Normalizar para 100%
    df_normalizado = normalizar_merecimento_100(df_union, "ambos")
    
    # Criar DataFrame final
    df_final = criar_dataframe_final(df_normalizado, "ambos")
    
    # Dividir em arquivos
    dfs_arquivos = dividir_em_arquivos(df_final)
    
    # Salvar arquivos CSV
    print(f"\n💾 Salvando arquivos CSV...")
    arquivos_salvos = []
    
    for idx, df_arquivo in enumerate(dfs_arquivos, start=1):
        nome_arquivo = f"matriz_merecimento_{grupo_apelido}_{data_exportacao}_parte{idx}.csv"
        caminho_completo = f"{pasta_data}/{nome_arquivo}"
        
        # Converter para pandas e salvar
        df_pandas = df_arquivo.toPandas()
        df_pandas.to_csv(caminho_completo, index=False, sep=",", encoding="utf-8")
        
        print(f"  ✅ Arquivo {idx} salvo: {nome_arquivo} ({len(df_pandas):,} linhas)")
        arquivos_salvos.append(caminho_completo)
    
    print("\n" + "=" * 80)
    print(f"✅ Exportação concluída para {categoria}")
    print(f"📁 Total de arquivos: {len(arquivos_salvos)}")
    
    return arquivos_salvos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exportar Todas as Categorias

# COMMAND ----------

def exportar_todas_categorias(data_exportacao: str = None) -> Dict[str, List[str]]:
    """
    Exporta matrizes CSV para todas as categorias.
    
    Args:
        data_exportacao: Data de exportação (padrão: DATA_CALCULO)
        
    Returns:
        Dicionário com listas de arquivos por categoria
    """
    print("🚀 Iniciando exportação para TODAS as categorias")
    print("=" * 80)
    
    resultados = {}
    
    for categoria in TABELAS_MATRIZ_MERECIMENTO.keys():
        print(f"\n📊 Processando categoria: {categoria}")
        print("-" * 60)
        
        try:
            arquivos = exportar_matriz_csv(categoria, data_exportacao)
            resultados[categoria] = arquivos
            
        except Exception as e:
            print(f"❌ Erro ao processar {categoria}: {str(e)}")
            resultados[categoria] = []
    
    print("\n" + "=" * 80)
    print("📋 RESUMO FINAL:")
    print("=" * 80)
    
    for categoria, arquivos in resultados.items():
        if arquivos:
            print(f"✅ {categoria}: {len(arquivos)} arquivo(s)")
        else:
            print(f"❌ {categoria}: ERRO")
    
    return resultados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Execução

# COMMAND ----------

# Executar exportação para todas as categorias
resultados = exportar_todas_categorias()

# COMMAND ----------

# Exemplo: exportar apenas uma categoria específica
# arquivos = exportar_matriz_csv("DIRETORIA TELEFONIA CELULAR")
