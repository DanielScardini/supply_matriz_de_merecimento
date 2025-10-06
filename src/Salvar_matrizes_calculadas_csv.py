# Databricks notebook source
# MAGIC %md
# MAGIC # Salvamento de Matrizes de Merecimento - Formato Sistema de Abastecimento
# MAGIC
# MAGIC Este notebook implementa o salvamento de matrizes em formato CSV compatível com o sistema de abastecimento.
# MAGIC
# MAGIC **Especificações:**
# MAGIC - Formato: CSV sem index
# MAGIC - Colunas: SKU, CANAL, LOJA, DATA FIM, PERCENTUAL, VERIFICAR, FASE DE VIDA
# MAGIC - União de ONLINE e OFFLINE no mesmo arquivo
# MAGIC - Máximo 500.000 linhas por arquivo
# MAGIC - Mesmo SKU-FILIAL sempre no mesmo arquivo (ambos canais)
# MAGIC - Normalização para exatamente 100.00% por CdSku
# MAGIC - Ajuste de diferença no maior merecimento

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window as W
from datetime import datetime, timedelta
import os
import pandas as pd
from typing import List, Dict, Tuple

# Inicialização
spark = SparkSession.builder.appName("salvar_matrizes_csv_sistema").getOrCreate()

# Datas
DATA_ATUAL = datetime.now()
DATA_FIM = DATA_ATUAL + timedelta(days=60)
DATA_FIM_INT = int(DATA_FIM.strftime("%Y%m%d"))

print(f"📅 Data atual: {DATA_ATUAL.strftime('%Y-%m-%d')}")
print(f"📅 Data fim (+60 dias): {DATA_FIM.strftime('%Y-%m-%d')} → {DATA_FIM_INT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurações

# COMMAND ----------

dt_inicio = "2025-08-01"
dt_fim = "2025-10-01"

df_demanda = (
  spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
  .filter(F.col("NmSetorGerencial") == "PORTATEIS")
  .filter(F.col("DtAtual") >= dt_inicio)
  .filter(F.col("DtAtual") < dt_fim)
  .groupBy("NmEspecieGerencial")
  .agg(
    F.sum(F.col("QtMercadoria")).alias("QtDemanda"),
    F.sum(F.col("Receita")).alias("Receita")
  )

)

# calcular totais com window
w_total = W.rowsBetween(W.unboundedPreceding, W.unboundedFollowing)

# window para cumulativo
w_cum = W.orderBy(F.col("PercDemanda").desc()).rowsBetween(W.unboundedPreceding, 0)

df_demanda = (
    df_demanda
    .withColumn("TotalDemanda", F.sum("QtDemanda").over(w_total))
    .withColumn("TotalReceita", F.sum("Receita").over(w_total))
    .withColumn("PercDemanda", F.round((F.col("QtDemanda") / F.col("TotalDemanda")) * 100, 0))
    .withColumn("PercReceita", F.round((F.col("Receita") / F.col("TotalReceita")) * 100, 0))
    .drop("TotalDemanda", "TotalReceita")
    .withColumn("PercDemandaCumulativo", F.sum("PercDemanda").over(w_cum))
    .withColumn("PercReceitaCumulativo", F.sum("PercReceita").over(w_cum))

)

especies_top80 = (
    df_demanda
    .filter(F.col("PercDemandaCumulativo") <= 80)
    .select("NmEspecieGerencial")
    .rdd.flatMap(lambda x: x)
    .collect()
)


print(especies_top80)


especies_boas = [
    "LIQUIDIFICADORES 350 A 1000 W",
    "FERROS DE PASSAR A SECO",
    "LIQUIDIFICADORES ACIMA 1001 W.",
    "PANELAS ELETRICAS DE ARROZ",
    "FRITADEIRA ELETRICA (CAPSULA)",
    "FERROS PAS. ROUPA VAPOR/SPRAY",
    "CAFETEIRA ELETRICA (FILTRO)"
]

skus_especies_top80 = (
    spark.table('data_engineering_prd.app_venda.mercadoria')
    .select(
        F.col("CdSkuLoja").alias("CdSku"),
        F.col("NmEspecieGerencial")
    )
    .filter(F.col("NmEspecieGerencial").isin(especies_top80))
    .filter(F.col("CdSku") != -1)
    .select("CdSku")
    .rdd.flatMap(lambda x: x)
    .collect()
)

df_demanda.filter(F.col("NmEspecieGerencial").isin(especies_top80)).agg(F.sum("PercDemanda")).display()

# COMMAND ----------

# Tabelas por categoria
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
        "offline": "databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_teste0410",
        "online": "databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_online_teste0310",
        "grupo_apelido": "linha_leve"
    },
}

# Pasta de saída
PASTA_OUTPUT = "/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/output"

# Colunas de merecimento por categoria
COLUNAS_MERECIMENTO = {
    "DIRETORIA DE TELAS": "Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura",
    "DIRETORIA TELEFONIA CELULAR": "Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura",
    "DIRETORIA LINHA LEVE": "Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura",
}

# Filtros
FILTROS_GRUPO_REMOCAO = {
    "DIRETORIA DE TELAS": ["FORA DE LINHA", "SEM_GN"],
    "DIRETORIA TELEFONIA CELULAR": ["FORA DE LINHA", "SEM_GN"],
    "DIRETORIA LINHA LEVE": ["FORA DE LINHA", "SEM_GN"],
}

FLAG_SELECAO_REMOCAO = {
    "DIRETORIA DE TELAS": "REMOÇÃO",
    "DIRETORIA TELEFONIA CELULAR": "SELEÇÃO",
    "DIRETORIA LINHA LEVE": "REMOÇÃO",
}

FILTROS_GRUPO_SELECAO = {
    "DIRETORIA DE TELAS": [],
    "DIRETORIA TELEFONIA CELULAR": ["Telef pp"],
    "DIRETORIA LINHA LEVE": [],
}

# Limite de linhas por arquivo
MAX_LINHAS_POR_ARQUIVO = 500000

print("✅ Configurações carregadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções de Formatação

# COMMAND ----------

def formatar_codigo_loja(cdfilial: int, is_cd: bool) -> str:
    """
    Formata código da loja/CD no padrão 0021_0XXXX ou 0099_0XXXX.
    
    Regras:
    - Loja (is_cd=False): 0021_0XXXX (5 dígitos com zeros à esquerda)
    - CD (is_cd=True): 0099_0XXXX (5 dígitos com zeros à esquerda)
    
    Exemplos:
    - formatar_codigo_loja(1234, False) → "0021_01234"
    - formatar_codigo_loja(7, False) → "0021_00007"
    - formatar_codigo_loja(1401, True) → "0099_01401"
    """
    prefixo = "0099" if is_cd else "0021"
    return f"{prefixo}_{cdfilial:05d}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funções de Processamento

# COMMAND ----------

def diagnosticar_diferenca_canais(df_offline: DataFrame, df_online: DataFrame, categoria: str) -> None:
    """
    Diagnóstico de diferenças entre canais OFFLINE e ONLINE.
    
    Investiga granularidade, SKUs únicos, grupos de necessidade e filiais
    para identificar por que há diferenças significativas de volume.
    
    Args:
        df_offline: DataFrame do canal offline
        df_online: DataFrame do canal online
        categoria: Nome da categoria
    """
    print("\n" + "="*80)
    print(f"🔍 DIAGNÓSTICO DE DIFERENÇAS - {categoria}")
    print("="*80)
    
    # 1. Contagens básicas
    count_offline = df_offline.count()
    count_online = df_online.count()
    ratio = count_online / count_offline if count_offline > 0 else 0
    
    print(f"\n📊 VOLUMES:")
    print(f"  • OFFLINE: {count_offline:,} registros")
    print(f"  • ONLINE:  {count_online:,} registros")
    print(f"  • Razão:   {ratio:.1f}x {'🚨' if ratio > 2 else '✅'}")
    
    # 2. SKUs únicos
    skus_offline = df_offline.select("CdSku").distinct().count()
    skus_online = df_online.select("CdSku").distinct().count()
    
    print(f"\n🏷️  SKUs ÚNICOS:")
    print(f"  • OFFLINE: {skus_offline:,} SKUs")
    print(f"  • ONLINE:  {skus_online:,} SKUs")
    print(f"  • Diferença: {skus_online - skus_offline:+,} SKUs")
    
    # 3. Filiais únicas
    filiais_offline = df_offline.select("CdFilial").distinct().count()
    filiais_online = df_online.select("CdFilial").distinct().count()
    
    print(f"\n🏪 FILIAIS ÚNICAS:")
    print(f"  • OFFLINE: {filiais_offline:,} filiais")
    print(f"  • ONLINE:  {filiais_online:,} filiais")
    print(f"  • Diferença: {filiais_online - filiais_offline:+,} filiais")
    
    # 4. Grupos de necessidade únicos
    grupos_offline = df_offline.select("grupo_de_necessidade").distinct().count()
    grupos_online = df_online.select("grupo_de_necessidade").distinct().count()
    
    print(f"\n📦 GRUPOS DE NECESSIDADE:")
    print(f"  • OFFLINE: {grupos_offline:,} grupos")
    print(f"  • ONLINE:  {grupos_online:,} grupos")
    print(f"  • Diferença: {grupos_online - grupos_offline:+,} grupos")
    
    # 5. Granularidade média (registros por filial)
    registros_por_filial_offline = count_offline / filiais_offline if filiais_offline > 0 else 0
    registros_por_filial_online = count_online / filiais_online if filiais_online > 0 else 0
    
    print(f"\n📏 GRANULARIDADE (registros/filial):")
    print(f"  • OFFLINE: {registros_por_filial_offline:.1f} registros/filial")
    print(f"  • ONLINE:  {registros_por_filial_online:.1f} registros/filial")
    
    # 6. Análise de causa provável
    print(f"\n🔎 ANÁLISE:")
    if ratio > 5:
        print(f"  ⚠️  ALERTA: Diferença de {ratio:.1f}x é MUITO ALTA")
        if registros_por_filial_online > registros_por_filial_offline * 3:
            print(f"  💡 Causa provável: ONLINE tem granularidade muito mais fina")
            print(f"     (Possivelmente desagregado por SKU vs agregado por grupo)")
        if skus_online > skus_offline * 1.5:
            print(f"  💡 Causa provável: ONLINE tem {skus_online - skus_offline:,} SKUs a mais")
    elif ratio > 2:
        print(f"  ⚠️  Diferença de {ratio:.1f}x é ALTA mas pode ser aceitável")
        print(f"  💡 ONLINE tem mais filiais e/ou mais SKUs ativos")
    else:
        print(f"  ✅ Diferença de {ratio:.1f}x está dentro do esperado")
    
    print("="*80 + "\n")

# COMMAND ----------

def carregar_e_filtrar_matriz(categoria: str, canal: str) -> DataFrame:
    """
    Carrega matriz de merecimento e aplica filtros.
    
    Args:
        categoria: Nome da categoria
        canal: "offline" ou "online"
        
    Returns:
        DataFrame com CdSku, CdFilial, Merecimento_raw
    """
    print(f"🔄 Carregando matriz: {categoria} - {canal}")
    
    tabela = TABELAS_MATRIZ_MERECIMENTO[categoria][canal]
    coluna_merecimento = COLUNAS_MERECIMENTO[categoria]
    flag_tipo = FLAG_SELECAO_REMOCAO[categoria]
    filtros_remocao = FILTROS_GRUPO_REMOCAO[categoria]
    filtros_selecao = FILTROS_GRUPO_SELECAO[categoria]
    
    # Carregar dados base
    df_base = (
        spark.table(tabela)
        .select(
            "CdFilial", "CdSku", "grupo_de_necessidade",
            (100 * F.col(coluna_merecimento)).alias("Merecimento_raw")
        )
    )
    
    # Aplicar filtros
    if flag_tipo == "SELEÇÃO":
        df_filtrado = df_base.filter(F.col("grupo_de_necessidade").isin(filtros_selecao))
        print(f"  • Seleção: {filtros_selecao}")
    else:
        df_filtrado = df_base.filter(~F.col("grupo_de_necessidade").isin(filtros_remocao))
        print(f"  • Remoção: {filtros_remocao}")
    
    # Filtro especial para Linha Leve: apenas SKUs top 80% de PORTATEIS
    if categoria == "DIRETORIA LINHA LEVE":
        df_filtrado = df_filtrado.filter(F.col("CdSku").isin(skus_especies_top80))
        print(f"  • Filtro TOP 80% PORTATEIS: {len(skus_especies_top80)} SKUs")
    
    # Regra especial online: CdFilial 1401 → 14
    if canal == "online":
        df_filtrado = df_filtrado.withColumn(
            "CdFilial", 
            F.when(F.col("CdFilial") == 1401, 14).otherwise(F.col("CdFilial"))
        )
        print("  • CdFilial 1401 → 14")
    
    # Agregar por CdSku + CdFilial
    df_agregado = (
        df_filtrado
        .groupBy("CdSku", "CdFilial")
        .agg(F.avg("Merecimento_raw").alias("Merecimento"))
        .withColumn("CANAL", F.lit(canal.upper()))
    )
    
    print(f"  ✅ {df_agregado.count():,} registros carregados")
    
    return df_agregado

# COMMAND ----------

def normalizar_para_100_exato(df: DataFrame) -> DataFrame:
    """
    Normaliza merecimentos para somar EXATAMENTE 100.00 por CdSku + CANAL.
    Ajusta diferença no maior merecimento de cada grupo.
    
    Processo:
    1. Proporcionalizar para ~100%
    2. Calcular diferença real vs 100.00
    3. Adicionar diferença no maior merecimento
    
    Args:
        df: DataFrame com CdSku, CdFilial, Merecimento, CANAL
        
    Returns:
        DataFrame com PERCENTUAL normalizado para 100.00 exato
    """
    print("🔄 Normalizando para 100.00% exato...")
    
    # Janela por CdSku + CANAL
    window_sku_canal = W.partitionBy("CdSku", "CANAL")
    
    # 1. Proporcionalizar
    df_proporcional = (
        df
        .withColumn("soma_sku_canal", F.sum("Merecimento").over(window_sku_canal))
        .withColumn(
            "Merecimento_proporcional",
            F.when(F.col("soma_sku_canal") > 0, 
                   (F.col("Merecimento") / F.col("soma_sku_canal")) * 100.0)
            .otherwise(0.0)
        )
    )
    
    # 2. Identificar maior merecimento por CdSku + CANAL
    window_rank = W.partitionBy("CdSku", "CANAL").orderBy(F.desc("Merecimento_proporcional"))
    
    df_com_rank = (
        df_proporcional
        .withColumn("rank", F.row_number().over(window_rank))
    )
    
    # 3. Calcular diferença para 100.00
    df_com_diferenca = (
        df_com_rank
        .withColumn("soma_proporcional", F.sum("Merecimento_proporcional").over(window_sku_canal))
        .withColumn("diferenca_100", 100.0 - F.col("soma_proporcional"))
    )
    
    # 4. Ajustar apenas o maior merecimento (rank = 1)
    df_ajustado = (
        df_com_diferenca
        .withColumn(
            "PERCENTUAL",
            F.when(F.col("rank") == 1, 
                   F.col("Merecimento_proporcional") + F.col("diferenca_100"))
            .otherwise(F.col("Merecimento_proporcional"))
        )
        .select("CdSku", "CdFilial", "CANAL", "PERCENTUAL")
    )
    
    # Validação
    soma_validacao = (
        df_ajustado
        .groupBy("CdSku", "CANAL")
        .agg(F.sum("PERCENTUAL").alias("soma_total"))
    )
    
    nao_100 = soma_validacao.filter((F.col("soma_total") < 99.99) | (F.col("soma_total") > 100.01)).count()
    
    if nao_100 > 0:
        print(f"  ⚠️ ATENÇÃO: {nao_100} grupos não somam 100.00%")
    else:
        print(f"  ✅ Todos os grupos somam 100.00%")
    
    print(f"✅ Normalização concluída: {df_ajustado.count():,} registros")
    
    return df_ajustado

# COMMAND ----------

def adicionar_informacoes_filial(df: DataFrame) -> DataFrame:
    """
    Adiciona informações de filiais e cria coluna LOJA formatada.
    
    Lógica LOJA:
    - OFFLINE: sempre 0021_0XXXX
    - ONLINE + NmPorteLoja NULL: 0099_0XXXX (é CD)
    - ONLINE + NmPorteLoja NOT NULL: 0021_0XXXX (é loja)
    
    Args:
        df: DataFrame com CdFilial, CANAL
        
    Returns:
        DataFrame com coluna LOJA adicionada
    """
    print("🔄 Adicionando informações de filiais...")
    
    # Join com dados de filiais
    df_filiais = (
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica")
    )
    
    df_com_filiais = df.join(df_filiais, on="CdFilial", how="left")
    
    # Criar coluna is_cd
    df_com_tipo = (
        df_com_filiais
        .withColumn(
            "is_cd",
            F.when(
                (F.col("CANAL") == "ONLINE") & (F.col("NmPorteLoja").isNull()),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    )
    
    # UDF para formatar loja
    from pyspark.sql.types import StringType
    formatar_loja_udf = F.udf(
        lambda cdfilial, is_cd: formatar_codigo_loja(int(cdfilial), bool(is_cd)),
        StringType()
    )
    
    df_com_loja = (
        df_com_tipo
        .withColumn("LOJA", formatar_loja_udf(F.col("CdFilial"), F.col("is_cd")))
        .drop("is_cd", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica")
    )
    
    print(f"✅ Informações adicionadas: {df_com_loja.count():,} registros")
    
    return df_com_loja

# COMMAND ----------

def criar_dataframe_final(df: DataFrame) -> DataFrame:
    """
    Cria DataFrame final com todas as colunas no formato do sistema.
    
    Colunas finais: SKU, CANAL, LOJA, DATA FIM, PERCENTUAL
    
    Args:
        df: DataFrame com CdSku, CANAL, LOJA, PERCENTUAL
        
    Returns:
        DataFrame formatado
    """
    print("🔄 Criando DataFrame final...")
    
    df_final = (
        df
        .withColumn("SKU", F.col("CdSku").cast("string"))
        .withColumn("DATA FIM", F.lit(DATA_FIM_INT))
        .withColumn("PERCENTUAL", F.round(F.col("PERCENTUAL"), 3))
        .select("SKU", "CANAL", "LOJA", "DATA FIM", "PERCENTUAL")
        .orderBy("SKU", "LOJA", "CANAL")
    )
    
    print(f"✅ DataFrame final criado: {df_final.count():,} registros")
    
    return df_final

# COMMAND ----------

def dividir_em_arquivos(df: DataFrame, max_linhas: int = MAX_LINHAS_POR_ARQUIVO) -> List[DataFrame]:
    """
    Divide DataFrame em arquivos garantindo que SKU-LOJA fique junto (ambos canais).
    
    Regra: Cada SKU-LOJA tem 2 registros (ONLINE + OFFLINE) que devem ficar no mesmo arquivo.
    
    Args:
        df: DataFrame completo
        max_linhas: Máximo de linhas por arquivo
        
    Returns:
        Lista de DataFrames
    """
    print(f"🔄 Dividindo em arquivos (máx {max_linhas:,} linhas cada)...")
    
    # Criar chave única por SKU-LOJA
    df_com_chave = df.withColumn("chave_particao", F.concat(F.col("SKU"), F.lit("_"), F.col("LOJA")))
    
    # Contar registros por chave
    df_contagem = (
        df_com_chave
        .groupBy("chave_particao")
        .agg(F.count("*").alias("qtd_registros"))
    )
    
    # Calcular partições
    window_particao = W.orderBy("chave_particao").rowsBetween(W.unboundedPreceding, W.currentRow)
    
    df_com_particao = (
        df_contagem
        .withColumn("acumulado", F.sum("qtd_registros").over(window_particao))
        .withColumn("num_arquivo", (F.col("acumulado") / max_linhas).cast("int"))
    )
    
    # Join de volta
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
        print(f"    - Parte {i+1}: {qtd:,} linhas")
        dfs_separados.append(df_arquivo)
    
    return dfs_separados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Função Principal de Exportação

# COMMAND ----------

def exportar_matriz_csv(categoria: str, data_exportacao: str = None) -> List[str]:
    """
    Exporta matriz de merecimento em formato CSV para uma categoria.
    
    Processo completo:
    1. Carregar OFFLINE e ONLINE
    2. União dos canais
    3. Normalizar para 100.00% exato
    4. Adicionar informações de filiais
    5. Criar DataFrame final formatado
    6. Dividir em arquivos (max 500k linhas)
    7. Salvar CSVs
    
    Args:
        categoria: Nome da categoria
        data_exportacao: Data de exportação (padrão: hoje)
        
    Returns:
        Lista de caminhos dos arquivos salvos
    """
    if data_exportacao is None:
        data_exportacao = DATA_ATUAL.strftime("%Y-%m-%d")
    
    print(f"🚀 Iniciando exportação para: {categoria}")
    print("=" * 80)
    
    grupo_apelido = TABELAS_MATRIZ_MERECIMENTO[categoria]["grupo_apelido"]
    
    # Criar pasta
    pasta_data = f"{PASTA_OUTPUT}/{data_exportacao}"
    os.makedirs(pasta_data, exist_ok=True)
    
    # 1. Carregar canais
    df_offline = carregar_e_filtrar_matriz(categoria, "offline")
    df_online = carregar_e_filtrar_matriz(categoria, "online")
    
    # 1.5. Diagnóstico de diferenças
    diagnosticar_diferenca_canais(df_offline, df_online, categoria)
    
    # 2. União
    print("\n🔗 Unindo canais...")
    df_union = df_offline.union(df_online)
    print(f"  ✅ União: {df_union.count():,} registros")
    
    # 3. Normalizar para 100.00%
    print()
    df_normalizado = normalizar_para_100_exato(df_union)
    
    # 4. Adicionar informações
    print()
    df_com_filiais = adicionar_informacoes_filial(df_normalizado)
    
    # 5. Criar DataFrame final
    print()
    df_final = criar_dataframe_final(df_com_filiais)
    
    # 6. Dividir em arquivos
    print()
    dfs_arquivos = dividir_em_arquivos(df_final)
    
    # 7. Salvar CSVs
    print(f"\n💾 Salvando arquivos CSV...")
    arquivos_salvos = []
    
    for idx, df_arquivo in enumerate(dfs_arquivos, start=1):
        nome_arquivo = f"matriz_merecimento_{grupo_apelido}_{data_exportacao}_parte{idx}.csv"
        caminho_completo = f"{pasta_data}/{nome_arquivo}"
        
        # Salvar CSV
        df_pandas = df_arquivo.toPandas()
        df_pandas.to_csv(caminho_completo, index=False, sep=",", encoding="utf-8")
        
        print(f"  ✅ Parte {idx}: {nome_arquivo} ({len(df_pandas):,} linhas)")
        arquivos_salvos.append(caminho_completo)
        
        print("\n" + "=" * 80)
    print(f"✅ Exportação concluída: {categoria}")
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
        data_exportacao: Data de exportação (padrão: hoje)
        
    Returns:
        Dicionário com listas de arquivos por categoria
    """
    print("🚀 Iniciando exportação para TODAS as categorias")
    print("=" * 80)
    
    resultados = {}
    
    for categoria in TABELAS_MATRIZ_MERECIMENTO.keys():
        print(f"\n📊 Processando: {categoria}")
        print("-" * 60)
        
        try:
            arquivos = exportar_matriz_csv(categoria, data_exportacao)
            resultados[categoria] = arquivos
        except Exception as e:
            print(f"❌ Erro: {str(e)}")
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
# MAGIC ## 6. Exportação Excel para Validação (por Grupo de Necessidade)

# COMMAND ----------

def exportar_excel_validacao_grupo_necessidade(categoria: str, data_exportacao: str = None) -> str:
    """
    Exporta Excel de validação com ONLINE e OFFLINE lado a lado.
    
    Estrutura:
    - Uma linha por (grupo_de_necessidade, CdFilial) - DISTINCT
    - Merecimentos são SOMADOS por grupo + filial
    - Colunas: grupo_de_necessidade, CdFilial, Merecimento_OFFLINE, Merecimento_ONLINE
    - Fill com 0.00 em merecimentos faltantes
    - Exportado em pasta 'validacao'
    
    Args:
        categoria: Nome da categoria
        data_exportacao: Data de exportação (padrão: hoje)
        
    Returns:
        Caminho do arquivo Excel gerado
    """
    if data_exportacao is None:
        data_exportacao = DATA_ATUAL.strftime("%Y-%m-%d")
    
    print(f"📊 Exportando Excel de validação: {categoria}")
    print("=" * 80)
    
    grupo_apelido = TABELAS_MATRIZ_MERECIMENTO[categoria]["grupo_apelido"]
    
    # Criar pasta validacao
    pasta_validacao = f"{PASTA_OUTPUT}/{data_exportacao}/validacao"
    os.makedirs(pasta_validacao, exist_ok=True)
    
    # 1. Carregar dados OFFLINE com grupo_de_necessidade
    print("\n🔄 Carregando matriz OFFLINE...")
    tabela_offline = TABELAS_MATRIZ_MERECIMENTO[categoria]["offline"]
    coluna_merecimento = COLUNAS_MERECIMENTO[categoria]
    
    df_offline = (
        spark.table(tabela_offline)
        .select(
            "CdSku", "CdFilial", "grupo_de_necessidade",
            (100 * F.col(coluna_merecimento)).alias("Merecimento_OFFLINE")
        )
    )
    
    # Filtro especial para Linha Leve: apenas SKUs top 80% de PORTATEIS
    if categoria == "DIRETORIA LINHA LEVE":
        df_offline = df_offline.filter(F.col("CdSku").isin(skus_especies_top80))
        print(f"  ✅ OFFLINE (TOP 80%): {df_offline.count():,} registros | {len(skus_especies_top80)} SKUs")
    else:
        print(f"  ✅ OFFLINE: {df_offline.count():,} registros")
    
    # Agregar por grupo_de_necessidade + CdFilial (soma merecimentos)
    df_offline_agg = (
        df_offline
        .groupBy("grupo_de_necessidade", "CdFilial")
        .agg(F.sum("Merecimento_OFFLINE").alias("Merecimento_OFFLINE"))
    )
    print(f"  ✅ OFFLINE agregado: {df_offline_agg.count():,} registros (grupo + filial)")
    
    # 2. Carregar dados ONLINE com grupo_de_necessidade
    print("\n🔄 Carregando matriz ONLINE...")
    tabela_online = TABELAS_MATRIZ_MERECIMENTO[categoria]["online"]
    
    df_online = (
        spark.table(tabela_online)
        .select(
            "CdSku", "CdFilial", "grupo_de_necessidade",
            (100 * F.col(coluna_merecimento)).alias("Merecimento_ONLINE")
        )
        # Aplicar regra CdFilial 1401 → 14
        .withColumn(
            "CdFilial", 
            F.when(F.col("CdFilial") == 1401, 14).otherwise(F.col("CdFilial"))
        )
    )
    
    # Filtro especial para Linha Leve: apenas SKUs top 80% de PORTATEIS
    if categoria == "DIRETORIA LINHA LEVE":
        df_online = df_online.filter(F.col("CdSku").isin(skus_especies_top80))
        print(f"  ✅ ONLINE (TOP 80%): {df_online.count():,} registros | {len(skus_especies_top80)} SKUs")
    else:
        print(f"  ✅ ONLINE: {df_online.count():,} registros")
    
    # Agregar por grupo_de_necessidade + CdFilial (soma merecimentos)
    df_online_agg = (
        df_online
        .groupBy("grupo_de_necessidade", "CdFilial")
        .agg(F.sum("Merecimento_ONLINE").alias("Merecimento_ONLINE"))
    )
    print(f"  ✅ ONLINE agregado: {df_online_agg.count():,} registros (grupo + filial)")
    
    # 3. Fazer FULL OUTER JOIN
    print("\n🔗 Fazendo outer join...")
    df_joined = (
        df_offline_agg.join(
            df_online_agg,
            on=["grupo_de_necessidade", "CdFilial"],
            how="outer"
        )
        # Fill NULLs com 0.00
        .fillna(0.00, subset=["Merecimento_OFFLINE", "Merecimento_ONLINE"])
        # Ordenar
        .orderBy("grupo_de_necessidade", "CdFilial")
    )
    print(f"  ✅ Join: {df_joined.count():,} registros")
    
    # 4. Converter para Pandas e salvar Excel
    print("\n💾 Salvando Excel...")
    df_pandas = df_joined.toPandas()
    
    # Arredondar merecimentos para 3 casas decimais
    df_pandas["Merecimento_OFFLINE"] = df_pandas["Merecimento_OFFLINE"].round(3)
    df_pandas["Merecimento_ONLINE"] = df_pandas["Merecimento_ONLINE"].round(3)
    
    # Salvar
    nome_arquivo = f"validacao_{grupo_apelido}_{data_exportacao}.xlsx"
    caminho_completo = f"{pasta_validacao}/{nome_arquivo}"
    
    # Salvar diretamente (mais robusto para DataFrames grandes)
    try:
        df_pandas.to_excel(caminho_completo, sheet_name="Validacao", index=False, engine="openpyxl")
        print(f"  ✅ Arquivo salvo: {nome_arquivo}")
        print(f"  📁 Local: {pasta_validacao}")
        print(f"  📊 Total de linhas: {len(df_pandas):,}")
    except Exception as e:
        print(f"  ⚠️ Erro ao salvar Excel: {str(e)}")
        print(f"  💡 Tentando salvar como CSV...")
        caminho_csv = caminho_completo.replace(".xlsx", ".csv")
        df_pandas.to_csv(caminho_csv, index=False)
        print(f"  ✅ Arquivo CSV salvo: {caminho_csv}")
        caminho_completo = caminho_csv
    
    print("\n" + "=" * 80)
    print(f"✅ Exportação Excel de validação concluída: {categoria}")
    
    return caminho_completo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Exportar Validação para Todas as Categorias

# COMMAND ----------

def exportar_excel_validacao_todas_categorias(data_exportacao: str = None) -> Dict[str, str]:
    """
    Exporta Excel de validação para todas as categorias.
    
    Args:
        data_exportacao: Data de exportação (padrão: hoje)
        
    Returns:
        Dicionário com caminhos dos arquivos por categoria
    """
    print("🚀 Iniciando exportação Excel de validação para TODAS as categorias")
    print("=" * 80)
    
    resultados = {}
    
    for categoria in TABELAS_MATRIZ_MERECIMENTO.keys():
        print(f"\n📊 Processando: {categoria}")
        print("-" * 60)
        
        try:
            arquivo = exportar_excel_validacao_grupo_necessidade(categoria, data_exportacao)
            resultados[categoria] = arquivo
        except Exception as e:
            print(f"❌ Erro: {str(e)}")
            resultados[categoria] = None
    
    print("\n" + "=" * 80)
    print("📋 RESUMO FINAL - VALIDAÇÃO:")
    print("=" * 80)
    
    for categoria, arquivo in resultados.items():
        if arquivo:
            print(f"✅ {categoria}: {arquivo}")
        else:
            print(f"❌ {categoria}: ERRO")
    
    return resultados

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Execução

# COMMAND ----------

# Executar exportação para todas as categorias
resultados = exportar_todas_categorias()

# COMMAND ----------

# Exportar Excel de validação para todas as categorias
resultados_validacao = exportar_excel_validacao_todas_categorias()

# COMMAND ----------

# Exemplo: exportar apenas uma categoria
# arquivos = exportar_matriz_csv("DIRETORIA TELEFONIA CELULAR")

# Exemplo: exportar apenas validação de uma categoria
# arquivo_validacao = exportar_excel_validacao_grupo_necessidade("DIRETORIA TELEFONIA CELULAR")
