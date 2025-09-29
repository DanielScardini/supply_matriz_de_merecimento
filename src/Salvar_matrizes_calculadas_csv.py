# Databricks notebook source
# MAGIC %md
# MAGIC # Salvamento de Matrizes de Merecimento - Sistema Unificado
# MAGIC
# MAGIC Este notebook implementa o salvamento unificado de matrizes de merecimento para todas as categorias,
# MAGIC com tratamento automático para canais offline e online.
# MAGIC
# MAGIC **Formato de saída**: Excel (.xlsx) usando pandas
# MAGIC
# MAGIC **Estrutura de pastas**:
# MAGIC ```
# MAGIC PASTA_OUTPUT/
# MAGIC └── YYYY-MM-DD/
# MAGIC     ├── matriz_de_merecimento_{categoria}_{data}_offline.xlsx
# MAGIC     └── matriz_de_merecimento_{categoria}_{data}_online.xlsx
# MAGIC ```

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window as W
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any
import os

# Inicialização do Spark
spark = SparkSession.builder.appName("salvar_matrizes_merecimento_unificadas").getOrCreate()

hoje = datetime.now()
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração das Tabelas por Categoria

# COMMAND ----------

# Configuração das tabelas por categoria e canal
TABELAS_MATRIZ_MERECIMENTO = {
    "DIRETORIA DE TELAS": {
        "offline": "databox.bcg_comum.supply_matriz_merecimento_de_telas_teste2509",
        "online": "databox.bcg_comum.supply_matriz_merecimento_de_telas_online_teste2609",
        "grupo_apelido": "telas"
    },
    "DIRETORIA TELEFONIA CELULAR": {
        "offline": "databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_teste2509",
        "online": "databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_online_teste2609",
        "grupo_apelido": "telefonia"
    },
    # "DIRETORIA LINHA BRANCA": {
    #     "offline": "databox.bcg_comum.supply_matriz_merecimento_linha_branca_offline",
    #     "online": "databox.bcg_comum.supply_matriz_merecimento_linha_branca_online",
    #     "grupo_apelido": "linha_branca"
    # },
    "DIRETORIA LINHA LEVE": {
        "offline": "databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_teste1909_liq",
        "online": "databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_teste1909_liq",
        "grupo_apelido": "liquidificador"
    },
    # "DIRETORIA INFO/GAMES": {
    #     "offline": "databox.bcg_comum.supply_matriz_merecimento_info_games_offline",
    #     "online": "databox.bcg_comum.supply_matriz_merecimento_info_games_online",
    #     "grupo_apelido": "info_games"
    # }
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

# Configuração de filtros por categoria
FLAG_SELECAO_REMOCAO = {
    "DIRETORIA DE TELAS": "SELEÇÃO",
    "DIRETORIA TELEFONIA CELULAR": "SELEÇÃO",
    "DIRETORIA LINHA LEVE":  "SELEÇÃO",
}

FILTROS_GRUPO_NECESSIDADE_SELECAO = {
    "DIRETORIA DE TELAS": [
        # "TV 50 ALTO P", 
        # "TV 55 ALTO P", 
        "TV 43 PP", 
        # "TV 75 PP",
        # "TV 75 ALTO P"
        ],
    "DIRETORIA TELEFONIA CELULAR": [
        #"Telef pp", 
        "Telef Medio 128GB", 
        "Telef Medio 256GB", 
        "Telef Alto", 
        "LINHA PREMIUM"
        ],
    "DIRETORIA LINHA BRANCA": ["FORA DE LINHA", "SEM_GN"],
    "DIRETORIA LINHA LEVE": ["FORA DE LINHA", "SEM_GN"],
    "DIRETORIA INFO/GAMES": ["FORA DE LINHA", "SEM_GN"]
}

# SKUs temporários para telefonia celular (Samsung Galaxy A07)
SKUS_TEMPORARIOS_TELEFONIA = [
    5358744,  # CEL.DESB. SAMSUNG GALAXY A07 4G 256GB VIOLETA
    5358752,  # CEL.DESB. SAMSUNG GALAXY A07 4G 128GB PRETO
    5358760,  # CEL.DESB. SAMSUNG GALAXY A07 4G 256GB VERDE
    5358779,  # CEL.DESB. SAMSUNG GALAXY A07 4G 256GB PRETO
    5358787,  # CEL.DESB. SAMSUNG GALAXY A07 4G 128GB VERDE
    5358795   # CEL.DESB. SAMSUNG GALAXY A07 4G 128GB VIOLETA
]

print("✅ Configurações carregadas:")
print(f"  • Categorias suportadas: {list(TABELAS_MATRIZ_MERECIMENTO.keys())}")
print(f"  • Pasta de saída: {PASTA_OUTPUT}")
print(f"  • Data de exportação: {hoje_str}")
print(f"  • SKUs temporários telefonia: {len(SKUS_TEMPORARIOS_TELEFONIA)} SKUs")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funções de Tratamento

# COMMAND ----------

def processar_matriz_merecimento(categoria: str, canal: str) -> DataFrame:
    """
    Processa a matriz de merecimento para uma categoria e canal específicos.
    
    Args:
        categoria: Categoria da diretoria
        canal: Canal (offline ou online)
        
    Returns:
        DataFrame processado com merecimento normalizado
    """
    print(f"🔄 Processando matriz para: {categoria} - {canal}")
    
    # Validação dos parâmetros
    if categoria not in TABELAS_MATRIZ_MERECIMENTO:
        raise ValueError(f"Categoria '{categoria}' não suportada. Categorias válidas: {list(TABELAS_MATRIZ_MERECIMENTO.keys())}")
    
    if canal not in ["offline", "online"]:
        raise ValueError(f"Canal '{canal}' não suportado. Canais válidos: ['offline', 'online']")
    
    # Configurações específicas
    tabela = TABELAS_MATRIZ_MERECIMENTO[categoria][canal]
    coluna_merecimento = COLUNAS_MERECIMENTO[categoria]
    flag_tipo = FLAG_SELECAO_REMOCAO.get(categoria, "REMOÇÃO")  # Padrão é remoção
    filtros_grupo_remocao = FILTROS_GRUPO_NECESSIDADE_REMOCAO[categoria]
    filtros_grupo_selecao = FILTROS_GRUPO_NECESSIDADE_SELECAO[categoria]
    
    print(f"  • Tabela: {tabela}")
    print(f"  • Coluna merecimento: {coluna_merecimento}")
    print(f"  • Tipo de filtro: {flag_tipo}")
    print(f"  • Filtros grupo para remoção: {filtros_grupo_remocao}")
    print(f"  • Filtros grupo para seleção: {filtros_grupo_selecao}")
    
    # Carregamento dos dados base
    df_base = (
        spark.table(tabela)
        .select(
            "CdFilial", "CdSku", "grupo_de_necessidade",
            (100 * F.col(coluna_merecimento)).alias(f"Merecimento_Percentual_{canal}_raw")
        )
    )
    
    # Aplicar filtro baseado no flag
    if flag_tipo == "SELEÇÃO":
        # SELEÇÃO: manter apenas os grupos da lista de seleção
        df_raw = df_base.filter(F.col("grupo_de_necessidade").isin(filtros_grupo_selecao))
        print(f"  • Aplicado filtro de SELEÇÃO: mantendo apenas {filtros_grupo_selecao}")
    else:
        # REMOÇÃO: remover os grupos da lista de remoção
        df_raw = df_base.filter(~F.col("grupo_de_necessidade").isin(filtros_grupo_remocao))
        print(f"  • Aplicado filtro de REMOÇÃO: removendo {filtros_grupo_remocao}")
    
    # Join com dados de filiais
    df_raw = df_raw.join(
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica"),
        on="CdFilial", how="left"
    )
    
    # Normalização por SKU
    window_sku = W.partitionBy("CdSku")
    total_sku = F.sum(f"Merecimento_Percentual_{canal}_raw").over(window_sku)
    
    df_normalizado = (
        df_raw
        .withColumn(
            f"Merecimento_Percentual_{canal}",
            F.round(
                F.when(total_sku > 0, F.col(f"Merecimento_Percentual_{canal}_raw") * (100.0 / total_sku))
                .otherwise(0.0), 
                3
            )
        )
        .drop(f"Merecimento_Percentual_{canal}_raw")
    )
    
    print(f"✅ Matriz processada:")
    print(f"  • Total de registros: {df_normalizado.count():,}")
    print(f"  • SKUs únicos: {df_normalizado.select('CdSku').distinct().count():,}")
    print(f"  • Filiais únicas: {df_normalizado.select('CdFilial').distinct().count():,}")
    
    # Adicionar SKUs temporários para telefonia celular
    df_final = adicionar_skus_temporarios_telefonia(df_normalizado, categoria, canal)
    
    return df_final

# COMMAND ----------

def adicionar_skus_temporarios_telefonia(df: DataFrame, categoria: str, canal: str) -> DataFrame:
    """
    Adiciona SKUs temporários da Samsung Galaxy A07 para telefonia celular.
    Estes SKUs recebem o merecimento percentual de 'Telef pp' para todas as filiais.
    
    Args:
        df: DataFrame com a matriz processada
        categoria: Categoria da diretoria
        canal: Canal (offline ou online)
        
    Returns:
        DataFrame com SKUs temporários adicionados
    """
    if categoria != "DIRETORIA TELEFONIA CELULAR":
        print(f"ℹ️ SKUs temporários não aplicados para categoria: {categoria}")
        return df
    
    print(f"🔄 Adicionando SKUs temporários Samsung Galaxy A07 para {categoria} - {canal}")
    
    # Obter o merecimento de 'Telef pp' para cada filial
    df_telef_pp = df.filter(F.col("grupo_de_necessidade") == "Telef pp")
    
    if df_telef_pp.count() == 0:
        print("⚠️ Nenhum registro de 'Telef pp' encontrado. Pulando adição de SKUs temporários.")
        return df
    
    # Obter todas as filiais únicas
    filiais_unicas = df_telef_pp.select("CdFilial", "NmFilial", "NmPorteLoja", "NmRegiaoGeografica").distinct()
    
    # Criar registros temporários para cada SKU e filial
    registros_temporarios = []
    
    for filial_row in filiais_unicas.collect():
        for sku in SKUS_TEMPORARIOS_TELEFONIA:
            # Obter o merecimento de 'Telef pp' para esta filial
            merecimento_telef_pp = df_telef_pp.filter(F.col("CdFilial") == filial_row.CdFilial).select("Merecimento_Percentual_" + canal).collect()
            
            if merecimento_telef_pp:
                merecimento_valor = merecimento_telef_pp[0][0]
                
                registros_temporarios.append({
                    "CdFilial": filial_row.CdFilial,
                    "CdSku": sku,
                    "grupo_de_necessidade": "SAMSUNG_GALAXY_A07_TEMP",
                    f"Merecimento_Percentual_{canal}": merecimento_valor,
                    "NmFilial": filial_row.NmFilial,
                    "NmPorteLoja": filial_row.NmPorteLoja,
                    "NmRegiaoGeografica": filial_row.NmRegiaoGeografica
                })
    
    if registros_temporarios:
        # Criar DataFrame com registros temporários
        df_temporarios = spark.createDataFrame(registros_temporarios)
        
        # Unir com o DataFrame original
        df_com_temporarios = df.union(df_temporarios)
        
        print(f"✅ SKUs temporários adicionados:")
        print(f"  • SKUs adicionados: {len(SKUS_TEMPORARIOS_TELEFONIA)}")
        print(f"  • Filiais: {filiais_unicas.count()}")
        print(f"  • Total de registros temporários: {len(registros_temporarios)}")
        
        return df_com_temporarios
    else:
        print("⚠️ Nenhum registro temporário criado.")
        return df

# COMMAND ----------

def salvar_matriz_excel(df: DataFrame, categoria: str, canal: str, data_exportacao: str = None) -> str:
    """
    Salva a matriz de merecimento em arquivo Excel usando pandas.
    Cria estrutura de pastas: PASTA_OUTPUT/data_exportacao/
    
    Args:
        df: DataFrame com a matriz processada
        categoria: Categoria da diretoria
        canal: Canal (offline ou online)
        data_exportacao: Data de exportação (padrão: hoje)
        
    Returns:
        Caminho do arquivo salvo
    """
    if data_exportacao is None:
        data_exportacao = hoje_str
    
    # Configurações específicas
    grupo_apelido = TABELAS_MATRIZ_MERECIMENTO[categoria]["grupo_apelido"]
    
    # Criar estrutura de pastas: PASTA_OUTPUT/data_exportacao/
    pasta_data = f"{PASTA_OUTPUT}/{data_exportacao}"
    
    # Criar pasta se não existir
    os.makedirs(pasta_data, exist_ok=True)
    
    # Nome do arquivo
    nome_arquivo = f"matriz_de_merecimento_{grupo_apelido}_{data_exportacao}_{canal}.xlsx"
    caminho_completo = f"{pasta_data}/{nome_arquivo}"
    
    print(f"💾 Salvando matriz em Excel:")
    print(f"  • Arquivo: {nome_arquivo}")
    print(f"  • Pasta data: {pasta_data}")
    print(f"  • Caminho completo: {caminho_completo}")
    
    # Converter DataFrame do Spark para pandas
    df_pandas = df.toPandas()
    
    # Salvar como Excel usando pandas
    df_pandas.to_excel(caminho_completo, index=False, engine='openpyxl')
    
    print(f"✅ Arquivo salvo com sucesso!")
    
    return caminho_completo

# COMMAND ----------

def executar_exportacao_completa(categoria: str, data_exportacao: str = None) -> Dict[str, str]:
    """
    Executa a exportação completa para uma categoria (offline + online).
    
    Args:
        categoria: Categoria da diretoria
        data_exportacao: Data de exportação (padrão: hoje)
        
    Returns:
        Dicionário com caminhos dos arquivos salvos
    """
    print(f"🚀 Iniciando exportação completa para: {categoria}")
    print("=" * 80)
    
    arquivos_salvos = {}
    
    try:
        # Processar canal offline
        print("📊 Processando canal OFFLINE...")
        df_offline = processar_matriz_merecimento(categoria, "offline")
        caminho_offline = salvar_matriz_excel(df_offline, categoria, "offline", data_exportacao)
        arquivos_salvos["offline"] = caminho_offline
        
        # Processar canal online
        print("\n📊 Processando canal ONLINE...")
        df_online = processar_matriz_merecimento(categoria, "online")
        caminho_online = salvar_matriz_excel(df_online, categoria, "online", data_exportacao)
        arquivos_salvos["online"] = caminho_online
        
        print("\n" + "=" * 80)
        print("✅ Exportação completa finalizada!")
        print(f"📁 Arquivos salvos:")
        print(f"  • OFFLINE: {arquivos_salvos['offline']}")
        print(f"  • ONLINE: {arquivos_salvos['online']}")
        
        return arquivos_salvos
        
    except Exception as e:
        print(f"❌ Erro na exportação: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execução das Exportações

# COMMAND ----------

# Exemplo de uso para uma categoria específica
# categoria_teste = "DIRETORIA DE TELAS"
# arquivos = executar_exportacao_completa(categoria_teste)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Exportação para Todas as Categorias

# COMMAND ----------

!pip install openpyxl

def exportar_todas_categorias(data_exportacao: str = None) -> Dict[str, Dict[str, str]]:
    """
    Exporta matrizes para todas as categorias suportadas.
    
    Args:
        data_exportacao: Data de exportação (padrão: hoje)
        
    Returns:
        Dicionário com arquivos salvos por categoria e canal
    """
    print("🚀 Iniciando exportação para TODAS as categorias")
    print("=" * 80)
    
    resultados = {}
    
    for categoria in TABELAS_MATRIZ_MERECIMENTO.keys():
        print(f"\n📊 Processando categoria: {categoria}")
        print("-" * 60)
        
        try:
            arquivos_categoria = executar_exportacao_completa(categoria, data_exportacao)
            resultados[categoria] = arquivos_categoria
            
        except Exception as e:
            print(f"❌ Erro ao processar {categoria}: {str(e)}")
            resultados[categoria] = {"erro": str(e)}
    
    print("\n" + "=" * 80)
    print("📋 RESUMO FINAL:")
    print("=" * 80)
    
    for categoria, arquivos in resultados.items():
        if "erro" in arquivos:
            print(f"❌ {categoria}: ERRO - {arquivos['erro']}")
        else:
            print(f"✅ {categoria}:")
            print(f"   • OFFLINE: {arquivos['offline']}")
            print(f"   • ONLINE: {arquivos['online']}")
    
    return resultados

# COMMAND ----------

# Descomente para executar exportação para todas as categorias
resultados = exportar_todas_categorias()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

df = (
    processar_matriz_merecimento(categoria='DIRETORIA TELEFONIA CELULAR', canal='online')
    .withColumn(
        "Merecimento_Percentual_online",
        regexp_replace(col("Merecimento_Percentual_online").cast("string"), r"\.", ",")
    )
)

df.display()

