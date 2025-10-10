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
# MAGIC - Máximo 200.000 linhas por arquivo
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

!pip install openpyxl

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

# Calcular top 80% por ESPÉCIE (SKUs) - apenas PORTATEIS
df_demanda_especie = (
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

# TOP 80% POR ESPÉCIE (SKUs) - apenas PORTATEIS
df_demanda_especie = (
    df_demanda_especie
    .withColumn("TotalDemanda", F.sum("QtDemanda").over(w_total))
    .withColumn("TotalReceita", F.sum("Receita").over(w_total))
    .withColumn("PercDemanda", F.round((F.col("QtDemanda") / F.col("TotalDemanda")) * 100, 0))
    .withColumn("PercReceita", F.round((F.col("Receita") / F.col("TotalReceita")) * 100, 0))
    .drop("TotalDemanda", "TotalReceita")
    .withColumn("PercDemandaCumulativo", F.sum("PercDemanda").over(w_cum))
    .withColumn("PercReceitaCumulativo", F.sum("PercReceita").over(w_cum))
)

especies_top80 = (
    df_demanda_especie
    .filter(F.col("PercDemandaCumulativo") <= 80)
    .select("NmEspecieGerencial")
    .rdd.flatMap(lambda x: x)
    .collect()
)


print("🔝 ESPÉCIES TOP 80% PORTATEIS:")
print(especies_top80)
print(f"Total de espécies: {len(especies_top80)}")

# SKUs das espécies top 80%
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

print(f"\n📊 SKUs das espécies top 80%: {len(skus_especies_top80)} SKUs")

# Validação dos percentuais
print("\n📈 VALIDAÇÃO PERCENTUAIS:")
print("ESPÉCIES TOP 80%:")
df_demanda_especie.filter(F.col("NmEspecieGerencial").isin(especies_top80)).agg(F.sum("PercDemanda")).show()

# COMMAND ----------

# Tabelas por categoria
TABELAS_MATRIZ_MERECIMENTO = {
    "DIRETORIA DE TELAS": {
        "offline": "databox.bcg_comum.supply_matriz_merecimento_de_telas_teste0710",
        "online": "databox.bcg_comum.supply_matriz_merecimento_de_telas_online_teste0710",
        "grupo_apelido": "telas"
    },
    # "DIRETORIA TELEFONIA CELULAR": {
    #     "offline": "databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_teste0710",
    #     "online": "databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_online_teste0710",
    #     "grupo_apelido": "telefonia"
    # },
    # "DIRETORIA LINHA LEVE": {
    #     "offline": "databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_teste0410",
    #     "online": "databox.bcg_comum.supply_matriz_merecimento_LINHA_LEVE_online_teste0310",
    #     "grupo_apelido": "linha_leve"
    # },
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
    "DIRETORIA DE TELAS": ["FORA DE LINHA", 
                           "SEM_GN"
                            "TV 40 MEDIO P",
                            "TV 43 QLED ALTO",
                            "TV 50 ESP - QLED / MINI LED",
                            "TV 55 ESP MEDIO",
                            "TV 55 QLED / OLED ALTO",
                            "TV 55 QLED PP",
                            "TV 60 ALTO P",
                            "TV 65 MINI LED MEDIO",
                            "TV 65 NEO QLED ALTO",
                            "TV 65 QLED / OLED ALTO",
                            "TV 65 QLED / OLED PP",
                            "TV 65 QNED ALTO",
                            "TV 65 QNED MEDIO",
                            "TV 65 QNED PP",
                            "TV 70 ALTO P",
                            "TV 75 NEO QLED ALTO",
                            "TV 75 PP",
                            "TV 75 QLED / OLED ALTO",
                            "TV 75 QLED PP",
                            "TV 75 QNED ALTO",
                            "TV 75 QNED MEDIO",],
    
    "DIRETORIA TELEFONIA CELULAR": ["FORA DE LINHA", "SEM_GN", ">4000", "3001 a 3500"],
    "DIRETORIA LINHA LEVE": ["FORA DE LINHA", "SEM_GN", "ASPIRADOR DE PO_BIV"],
}

FLAG_SELECAO_REMOCAO = {
    "DIRETORIA DE TELAS": "REMOÇÃO",
    "DIRETORIA TELEFONIA CELULAR": "REMOÇÃO",
    "DIRETORIA LINHA LEVE": "REMOÇÃO",
}

FILTROS_GRUPO_SELECAO = {
    "DIRETORIA DE TELAS": [],
    "DIRETORIA TELEFONIA CELULAR": ["Telef pp"],
    "DIRETORIA LINHA LEVE": [],
}

# Limite de linhas por arquivo
MAX_LINHAS_POR_ARQUIVO = 150000

# Configurações de filtros de produtos por categoria
FILTROS_PRODUTOS = {
    "DIRETORIA DE TELAS": {
        "tipificacao_entrega": ["SL"],  # Apenas SL (Sai Loja)
        "marcas_excluidas": [],  # Excluir marca APPLE
        "aplicar_filtro": True
    },
    "DIRETORIA TELEFONIA CELULAR": {
        "tipificacao_entrega": ["SL"],  # Apenas SL (Sai Loja)
        "marcas_excluidas": ["APPLE"],  # Excluir marca APPLE
        "aplicar_filtro": True
    },
    "DIRETORIA LINHA LEVE": {
        "tipificacao_entrega": ["SL"],  # Apenas SL (Sai Loja)
        "marcas_excluidas": ["APPLE"],  # Excluir marca APPLE
        "aplicar_filtro": True
    },
    "DIRETORIA LINHA BRANCA": {
        "tipificacao_entrega": ["SL"],  # Apenas SL (Sai Loja)
        "marcas_excluidas": ["APPLE"],  # Excluir marca APPLE
        "aplicar_filtro": True
    }
}

# Configuração global de filtros de produtos (fallback)
FILTROS_PRODUTOS_GLOBAL = {
    "tipificacao_entrega": ["SL"],  # Apenas SL (Sai Loja)
    "marcas_excluidas": ["APPLE"],  # Excluir marca APPLE
    "aplicar_filtro": True
}

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
    - CD/Entreposto (is_cd=True): 0099_0XXXX (5 dígitos com zeros à esquerda)
    
    Exemplos:
    - formatar_codigo_loja(1234, False) → "0021_01234" (loja)
    - formatar_codigo_loja(7, False) → "0021_00007" (loja)
    - formatar_codigo_loja(1401, True) → "0099_01401" (CD)
    - formatar_codigo_loja(1501, True) → "0099_01501" (Entreposto)
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
    print(f"🔍 DIAGNÓSTICO COMPARATIVO - {categoria}")
    print("="*80)
    
    # 1. Contagens básicas
    count_offline = df_offline.count()
    count_online = df_online.count()
    ratio = count_online / count_offline if count_offline > 0 else 0
    
    print(f"\n📊 VOLUMES TOTAIS:")
    print(f"  • OFFLINE: {count_offline:,} registros")
    print(f"  • ONLINE:  {count_online:,} registros")
    print(f"  • Razão:   {ratio:.1f}x {'🚨 MUITO ALTO' if ratio > 5 else '⚠️  ALTO' if ratio > 2 else '✅ OK'}")
    
    # 2. SKUs únicos - ANÁLISE DETALHADA
    print(f"\n🏷️  ANÁLISE DE SKUs:")
    
    skus_offline_set = set([row.CdSku for row in df_offline.select("CdSku").distinct().collect()])
    skus_online_set = set([row.CdSku for row in df_online.select("CdSku").distinct().collect()])
    
    skus_apenas_offline = skus_offline_set - skus_online_set
    skus_apenas_online = skus_online_set - skus_offline_set
    skus_em_ambos = skus_offline_set & skus_online_set
    
    print(f"  • SKUs OFFLINE: {len(skus_offline_set):,} SKUs únicos")
    print(f"  • SKUs ONLINE:  {len(skus_online_set):,} SKUs únicos")
    print(f"  • SKUs em AMBOS: {len(skus_em_ambos):,} SKUs")
    print(f"  • SKUs APENAS OFFLINE: {len(skus_apenas_offline):,} SKUs")
    print(f"  • SKUs APENAS ONLINE:  {len(skus_apenas_online):,} SKUs")
    
    if len(skus_apenas_online) > 0:
        print(f"  💡 ONLINE tem {len(skus_apenas_online):,} SKUs exclusivos")
    if len(skus_apenas_offline) > 0:
        print(f"  ⚠️  OFFLINE tem {len(skus_apenas_offline):,} SKUs que não aparecem no ONLINE")
    
    # Validação do filtro TOP 80% para Linha Leve
    if categoria == "DIRETORIA LINHA LEVE":
        print(f"\n🔝 VALIDAÇÃO FILTRO TOP 80% ESPÉCIES:")
        print(f"  • Espécies top 80% definidas: {len(especies_top80)}")
        print(f"  • SKUs das espécies: {len(skus_especies_top80)}")
        
        skus_top80_em_offline = len(skus_offline_set & set(skus_especies_top80))
        skus_top80_em_online = len(skus_online_set & set(skus_especies_top80))
        
        print(f"  • SKUs top 80% presentes no OFFLINE: {skus_top80_em_offline:,} ({skus_top80_em_offline/len(skus_especies_top80)*100:.1f}%)")
        print(f"  • SKUs top 80% presentes no ONLINE:  {skus_top80_em_online:,} ({skus_top80_em_online/len(skus_especies_top80)*100:.1f}%)")
        
        if skus_top80_em_offline < len(skus_especies_top80):
            print(f"  ⚠️  {len(skus_especies_top80) - skus_top80_em_offline} SKUs top 80% ausentes no OFFLINE")
        if skus_top80_em_online < len(skus_especies_top80):
            print(f"  ⚠️  {len(skus_especies_top80) - skus_top80_em_online} SKUs top 80% ausentes no ONLINE")
    
    # 3. Filiais únicas
    filiais_offline = df_offline.select("CdFilial").distinct().count()
    filiais_online = df_online.select("CdFilial").distinct().count()
    
    print(f"\n🏪 FILIAIS ÚNICAS:")
    print(f"  • OFFLINE: {filiais_offline:,} filiais")
    print(f"  • ONLINE:  {filiais_online:,} filiais")
    print(f"  • Diferença: {filiais_online - filiais_offline:+,} filiais (+{(filiais_online/filiais_offline - 1)*100:.1f}%)")
    
    # 4. Granularidade média (registros por filial e por SKU)
    registros_por_filial_offline = count_offline / filiais_offline if filiais_offline > 0 else 0
    registros_por_filial_online = count_online / filiais_online if filiais_online > 0 else 0
    
    registros_por_sku_offline = count_offline / len(skus_offline_set) if len(skus_offline_set) > 0 else 0
    registros_por_sku_online = count_online / len(skus_online_set) if len(skus_online_set) > 0 else 0
    
    print(f"\n📏 GRANULARIDADE:")
    print(f"  • OFFLINE: {registros_por_filial_offline:.1f} registros/filial | {registros_por_sku_offline:.1f} registros/SKU")
    print(f"  • ONLINE:  {registros_por_filial_online:.1f} registros/filial | {registros_por_sku_online:.1f} registros/SKU")
    print(f"  • Razão registros/filial: {registros_por_filial_online / registros_por_filial_offline:.1f}x")
    
    # 5. Análise de causa provável
    print(f"\n🔎 DIAGNÓSTICO FINAL:")
    if ratio > 5:
        print(f"  🚨 ALERTA CRÍTICO: Diferença de {ratio:.1f}x é EXTREMAMENTE ALTA")
        print(f"  ")
        print(f"  📌 Causas identificadas:")
        if len(skus_apenas_online) > len(skus_online_set) * 0.3:
            print(f"     • ONLINE tem {len(skus_apenas_online):,} SKUs exclusivos ({len(skus_apenas_online)/len(skus_online_set)*100:.1f}% do total)")
        if registros_por_filial_online > registros_por_filial_offline * 3:
            print(f"     • Granularidade {registros_por_filial_online / registros_por_filial_offline:.1f}x maior no ONLINE")
            print(f"       (Possivelmente desagregado por SKU individual vs agregado por grupo)")
        if filiais_online > filiais_offline * 1.2:
            print(f"     • ONLINE tem {filiais_online - filiais_offline:,} filiais a mais ({(filiais_online/filiais_offline - 1)*100:.1f}%)")
    elif ratio > 2:
        print(f"  ⚠️  Diferença de {ratio:.1f}x é ALTA mas pode ser aceitável")
        print(f"  💡 Causas: ONLINE tem mais filiais ({filiais_online - filiais_offline:+,}) e/ou mais SKUs ({len(skus_online_set) - len(skus_offline_set):+,})")
    else:
        print(f"  ✅ Diferença de {ratio:.1f}x está dentro do esperado para operações Online/Offline")
    
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
    print(f"\n🔄 Carregando matriz: {categoria} - {canal.upper()}")
    print("-" * 80)
    
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
    
    # CHECKPOINT 1: Dados brutos
    skus_inicial = df_base.select("CdSku").distinct().count()
    registros_inicial = df_base.count()
    print(f"📦 DADOS BRUTOS DA TABELA:")
    print(f"  • Registros: {registros_inicial:,}")
    print(f"  • SKUs únicos: {skus_inicial:,}")
    
    # FILTRO DE PRODUTOS: Configurável por categoria
    filtros_produtos = FILTROS_PRODUTOS.get(categoria, FILTROS_PRODUTOS_GLOBAL)
    
    if filtros_produtos.get("aplicar_filtro", False):
        print(f"\n🏷️ FILTRO DE PRODUTOS:")
        print(f"  • Incluir apenas: {filtros_produtos['tipificacao_entrega']}")
        print(f"  • Excluir marcas: {filtros_produtos['marcas_excluidas']}")
        
        # Carregar informações de produtos da tabela mercadoria
        df_mercadoria = (
            spark.table('data_engineering_prd.app_venda.mercadoria')
            .select(
                F.col("CdSkuLoja").alias("CdSku"),
                "StTipificacaoEntrega", 
                "NmMarca"
            )
            .distinct()
        )
        
        # Log inicial da tabela mercadoria
        total_produtos_inicial = df_mercadoria.count()
        print(f"  📊 Produtos na tabela mercadoria: {total_produtos_inicial:,}")
        
        # Mostrar distribuição por tipificação de entrega
        print(f"  📋 Tipificações de entrega disponíveis:")
        tipificacoes_disponiveis = (
            df_mercadoria
            .groupBy("StTipificacaoEntrega")
            .count()
            .orderBy(F.desc("count"))
        )
        tipificacoes_disponiveis.show(10, truncate=False)
        
        # Aplicar filtros de produto
        df_produtos_filtrados = df_mercadoria
        
        # Filtro por tipificação de entrega
        if filtros_produtos["tipificacao_entrega"]:
            produtos_antes_tipificacao = df_produtos_filtrados.count()
            df_produtos_filtrados = df_produtos_filtrados.filter(
                F.col("StTipificacaoEntrega").isin(filtros_produtos["tipificacao_entrega"])
            )
            produtos_apos_tipificacao = df_produtos_filtrados.count()
            print(f"  ✅ Filtro tipificação: {produtos_antes_tipificacao:,} → {produtos_apos_tipificacao:,} (-{produtos_antes_tipificacao - produtos_apos_tipificacao:,})")
            
            # Verificar se filtro funcionou
            tipificacoes_restantes = (
                df_produtos_filtrados
                .select("StTipificacaoEntrega")
                .distinct()
                .rdd.flatMap(lambda x: x)
                .collect()
            )
            print(f"  🔍 Tipificações restantes: {sorted(tipificacoes_restantes)}")
        
        # Filtro por marcas excluídas
        if filtros_produtos["marcas_excluidas"]:
            produtos_antes_marca = df_produtos_filtrados.count()
            df_produtos_filtrados = df_produtos_filtrados.filter(
                ~F.col("NmMarca").isin(filtros_produtos["marcas_excluidas"])
            )
            produtos_apos_marca = df_produtos_filtrados.count()
            print(f"  ✅ Filtro marcas: {produtos_antes_marca:,} → {produtos_apos_marca:,} (-{produtos_antes_marca - produtos_apos_marca:,})")
            
            # Verificar se marcas excluídas foram removidas
            marcas_restantes = (
                df_produtos_filtrados
                .select("NmMarca")
                .distinct()
                .rdd.flatMap(lambda x: x)
                .collect()
            )
            marcas_excluidas_encontradas = [m for m in filtros_produtos["marcas_excluidas"] if m in marcas_restantes]
            if marcas_excluidas_encontradas:
                print(f"  ⚠️ ATENÇÃO: Marcas que deveriam ser excluídas ainda estão presentes: {marcas_excluidas_encontradas}")
            else:
                print(f"  ✅ Marcas excluídas removidas com sucesso: {filtros_produtos['marcas_excluidas']}")
        
        # Log final dos produtos filtrados
        total_produtos_final = df_produtos_filtrados.count()
        print(f"  📊 Produtos após filtros: {total_produtos_final:,} (-{total_produtos_inicial - total_produtos_final:,})")
    else:
        print(f"\n🏷️ FILTRO DE PRODUTOS:")
        print(f"  • Filtro desabilitado para {categoria}")
        df_produtos_filtrados = None
    
    # Aplicar filtro de produtos se habilitado
    if df_produtos_filtrados is not None:
        # Fazer join com dados base para aplicar filtro
        df_base_filtrado = (
            df_base
            .join(df_produtos_filtrados, on="CdSku", how="inner")
            .select("CdFilial", "CdSku", "grupo_de_necessidade", "Merecimento_raw")
        )
        
        # CHECKPOINT 2: Após filtro de produtos
        skus_pos_produto = df_base_filtrado.select("CdSku").distinct().count()
        registros_pos_produto = df_base_filtrado.count()
        print(f"  • SKUs após filtro de produtos: {skus_pos_produto:,} ({skus_pos_produto - skus_inicial:+,})")
        print(f"  • Registros após filtro de produtos: {registros_pos_produto:,} ({registros_pos_produto - registros_inicial:+,})")
        
        # Usar dados filtrados como base para próximos filtros
        df_base = df_base_filtrado
    else:
        print(f"  • Filtro de produtos não aplicado - usando dados originais")
    
    # Mostrar grupos disponíveis antes do filtro
    grupos_disponiveis = df_base.select("grupo_de_necessidade").distinct().rdd.flatMap(lambda x: x).collect()
    print(f"\n📋 GRUPOS DISPONÍVEIS:")
    print(f"  • Total: {len(grupos_disponiveis)} grupos")
    print(f"  • Lista: {sorted(grupos_disponiveis)}")
    
    # Aplicar filtros de grupo
    print(f"\n🎯 FILTRO DE GRUPOS DE NECESSIDADE:")
    if flag_tipo == "SELEÇÃO":
        df_filtrado = df_base.filter(F.col("grupo_de_necessidade").isin(filtros_selecao))
        print(f"  • Tipo: SELEÇÃO")
        print(f"  • Grupos selecionados: {len(filtros_selecao)}")
        print(f"  • Grupos solicitados: {filtros_selecao}")
    else:
        df_filtrado = df_base.filter(~F.col("grupo_de_necessidade").isin(filtros_remocao))
        print(f"  • Tipo: REMOÇÃO")
        print(f"  • Grupos removidos: {len(filtros_remocao)}")
        print(f"  • Grupos solicitados para remoção: {filtros_remocao}")
        
        # Verificar quais grupos solicitados realmente existem
        grupos_existentes = [g for g in filtros_remocao if g in grupos_disponiveis]
        grupos_inexistentes = [g for g in filtros_remocao if g not in grupos_disponiveis]
        
        if grupos_inexistentes:
            print(f"  ⚠️ Grupos solicitados mas NÃO EXISTENTES: {grupos_inexistentes}")
        if grupos_existentes:
            print(f"  ✅ Grupos que SERÃO removidos: {grupos_existentes}")
    
    # CHECKPOINT 2: Após filtro de grupos
    skus_pos_grupo = df_filtrado.select("CdSku").distinct().count()
    registros_pos_grupo = df_filtrado.count()
    print(f"  • SKUs após filtro: {skus_pos_grupo:,} ({skus_pos_grupo - skus_inicial:+,})")
    print(f"  • Registros após filtro: {registros_pos_grupo:,} ({registros_pos_grupo - registros_inicial:+,})")
    
    # Verificar grupos restantes após filtro
    grupos_restantes = df_filtrado.select("grupo_de_necessidade").distinct().rdd.flatMap(lambda x: x).collect()
    print(f"  • Grupos restantes após filtro: {len(grupos_restantes)}")
    print(f"  • Lista dos grupos restantes: {sorted(grupos_restantes)}")
    
    # Verificar se grupos solicitados para remoção ainda estão presentes
    grupos_nao_removidos = [g for g in filtros_remocao if g in grupos_restantes]
    if grupos_nao_removidos:
        print(f"  ❌ ERRO: Grupos solicitados para remoção ainda estão presentes: {grupos_nao_removidos}")
        for grupo in grupos_nao_removidos:
            registros_grupo = df_filtrado.filter(F.col("grupo_de_necessidade") == grupo).count()
            print(f"    • {grupo}: {registros_grupo:,} registros")
        print(f"  ⚠️ CORREÇÃO: Aplicando filtro adicional para remover grupos restantes...")
        df_filtrado = df_filtrado.filter(~F.col("grupo_de_necessidade").isin(grupos_nao_removidos))
        print(f"  ✅ Grupos removidos com filtro adicional")
    else:
        print(f"  ✅ Todos os grupos solicitados para remoção foram removidos com sucesso")
    
    # Filtro especial para Linha Leve: apenas SKUs das espécies top 80% de PORTATEIS
    if categoria == "DIRETORIA LINHA LEVE":
        print(f"\n🔝 FILTRO TOP 80% ESPÉCIES PORTATEIS:")
        print(f"  • Espécies top 80% definidas: {len(especies_top80)}")
        print(f"  • SKUs das espécies: {len(skus_especies_top80)}")
        
        skus_antes_top80 = df_filtrado.select("CdSku").distinct().count()
        df_filtrado = df_filtrado.filter(F.col("CdSku").isin(skus_especies_top80))
        skus_apos_top80 = df_filtrado.select("CdSku").distinct().count()
        registros_apos_top80 = df_filtrado.count()
        
        print(f"  • SKUs antes: {skus_antes_top80:,}")
        print(f"  • SKUs após: {skus_apos_top80:,} ({skus_apos_top80 - skus_antes_top80:+,})")
        print(f"  • Registros após: {registros_apos_top80:,}")
        
        if skus_apos_top80 != len(skus_especies_top80):
            print(f"  ⚠️  ATENÇÃO: {len(skus_especies_top80) - skus_apos_top80} SKUs top 80% não encontrados nos dados!")
    
    # Regra especial online: CdFilial 1401 → 14 (apenas para TELAS e TELEFONIA)
    if canal == "online" and categoria in ["DIRETORIA DE TELAS", "DIRETORIA TELEFONIA CELULAR"]:
        print(f"\n🔄 CONSOLIDAÇÃO DE FILIAIS:")
        filial_1401_count = df_filtrado.filter(F.col("CdFilial") == 1401).count()
        
        # Aplicar consolidação 1401 → 14
        df_filtrado = df_filtrado.withColumn(
            "CdFilial", 
            F.when(F.col("CdFilial") == 1401, 14).otherwise(F.col("CdFilial"))
        )
        
        print(f"  • CdFilial 1401 → 14 (apenas {categoria})")
        print(f"  • Registros consolidados: {filial_1401_count:,}")
        
        # Agregar por CdSku + CdFilial + grupo_de_necessidade para somar merecimentos
        print(f"  • Agregando merecimentos após consolidação...")
        df_filtrado = (
            df_filtrado
            .groupBy("CdSku", "CdFilial", "grupo_de_necessidade")
            .agg(F.sum("Merecimento_raw").alias("Merecimento_raw"))
        )
    
    # Agregar por CdSku + CdFilial (agregação final)
    print(f"\n📊 AGREGAÇÃO FINAL:")
    df_agregado = (
        df_filtrado
        .groupBy("CdSku", "CdFilial")
        .agg(F.avg("Merecimento_raw").alias("Merecimento"))
        .withColumn("CANAL", F.lit(canal.upper()))
    )
    
    registros_final = df_agregado.count()
    skus_final = df_agregado.select("CdSku").distinct().count()
    filiais_final = df_agregado.select("CdFilial").distinct().count()
    
    print(f"  • Registros finais: {registros_final:,}")
    print(f"  • SKUs finais: {skus_final:,}")
    print(f"  • Filiais finais: {filiais_final:,}")
    print(f"  • Granularidade: {registros_final / filiais_final:.1f} registros/filial")
    print("-" * 80)
    print(f"✅ Carregamento concluído: {canal.upper()}")
    
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
    
    # 4. Ajustar apenas o maior merecimento (rank = 1) para soma exata 100.000
    df_ajustado = (
        df_com_diferenca
        .withColumn(
            "PERCENTUAL",
            F.when(F.col("rank") == 1, 
                   F.col("Merecimento_proporcional") + F.col("diferenca_100"))
            .otherwise(F.col("Merecimento_proporcional"))
        )
        .withColumn("PERCENTUAL", F.round(F.col("PERCENTUAL"), 3))
        .select("CdSku", "CdFilial", "CANAL", "LOJA", "PERCENTUAL")
    )
    
    # Validação
    soma_validacao = (
        df_ajustado
        .groupBy("CdSku", "CANAL")
        .agg(F.sum("PERCENTUAL").alias("soma_total"))
    )
    
    nao_100 = soma_validacao.filter((F.col("soma_total") < 99.9999) | (F.col("soma_total") > 100.0001)).count()
    
    if nao_100 > 0:
        print(f"  ⚠️ ATENÇÃO: {nao_100} grupos não somam exatamente 100.000%")
        soma_validacao.filter((F.col("soma_total") < 99.9999) | (F.col("soma_total") > 100.0001)).show(5, truncate=False)
    else:
        print(f"  ✅ Todos os grupos somam exatamente 100.000%")
    
    # 5. Correção final para garantir exatidão matemática
    print("  🔧 Aplicando correção final para exatidão matemática...")
    df_final_corrigido = garantir_soma_exata_100(df_ajustado)
    
    print(f"✅ Normalização concluída: {df_final_corrigido.count():,} registros")
    
    return df_final_corrigido

def garantir_soma_exata_100(df: DataFrame) -> DataFrame:
    """
    Garante que todas as somas por SKU+CANAL sejam exatamente 100.000%.
    Aplica correção final no maior merecimento de cada grupo.
    """
    print("    🔧 Garantindo soma exata de 100.000%...")
    
    # Window para agrupar por SKU+CANAL
    window_sku_canal = W.partitionBy("CdSku", "CANAL")
    window_rank = W.partitionBy("CdSku", "CANAL").orderBy(F.desc("PERCENTUAL"))
    
    # Calcular soma atual e diferença exata
    df_com_soma = (
        df
        .withColumn("soma_atual", F.sum("PERCENTUAL").over(window_sku_canal))
        .withColumn("diferenca_exata", 100.0 - F.col("soma_atual"))
    )
    
    # Aplicar correção no maior merecimento de cada grupo
    df_com_rank = (
        df_com_soma
        .withColumn("rank", F.row_number().over(window_rank))
    )
    
    df_corrigido = (
        df_com_rank
        .withColumn(
            "PERCENTUAL",
            F.when(F.col("rank") == 1, 
                   F.col("PERCENTUAL") + F.col("diferenca_exata"))
            .otherwise(F.col("PERCENTUAL"))
        )
        .withColumn("PERCENTUAL", F.round(F.col("PERCENTUAL"), 3))
        .select("CdSku", "CdFilial", "CANAL", "LOJA", "PERCENTUAL")
    )
    
    # Validação final rigorosa
    validacao_final = (
        df_corrigido
        .groupBy("CdSku", "CANAL")
        .agg(F.sum("PERCENTUAL").alias("soma_final"))
    )
    
    nao_100_final = validacao_final.filter(F.abs(F.col("soma_final") - 100.0) > 0.0001).count()
    
    if nao_100_final > 0:
        print(f"    ❌ ERRO: {nao_100_final} grupos ainda não somam exatamente 100.000%")
        validacao_final.filter(F.abs(F.col("soma_final") - 100.0) > 0.0001).show(3, truncate=False)
        raise ValueError(f"{nao_100_final} grupos não somam exatamente 100.000% após correção")
    else:
        print(f"    ✅ Todos os grupos somam exatamente 100.000%")
    
    return df_corrigido

# COMMAND ----------

def adicionar_informacoes_filial(df: DataFrame) -> DataFrame:
    """
    Adiciona informações de filiais e cria coluna LOJA formatada.
    
    Nova lógica de preservação:
    1. CD 14: SEMPRE PRESERVAR (consolidado)
    2. CDs ativos: PRESERVAR
    3. Lojas ativas: PRESERVAR
    4. Outros: REMOVER com relatório detalhado
    
    Args:
        df: DataFrame com CdFilial, CANAL
        
    Returns:
        DataFrame com coluna LOJA adicionada e filiais não elegíveis removidas
    """
    print("🔄 Adicionando informações de filiais...")
    
    # Contar registros antes
    registros_antes = df.count()
    print(f"  📊 Registros antes do filtro: {registros_antes:,}")
    
    # Carregar tabelas de referência
    print("  📋 Carregando tabelas de referência...")
    
    # CDs ativos
    df_cds = (
        spark.table('databox.logistica_comum.roteirizacaocentrodistribuicao')
        .select("CdFilial", "NmFilial", "NmTipoFilial")
        .withColumn("tipo_filial", F.col("NmTipoFilial"))
    )
    
    # Lojas ativas
    df_lojas = (
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmFilial")
        .withColumn("NmTipoFilial", F.lit(None).cast("string"))
        .withColumn("tipo_filial", F.lit("LOJA"))
    )
    
    print(f"    • CDs ativos: {df_cds.count():,}")
    print(f"    • Lojas ativas: {df_lojas.count():,}")
    
    # Unir tabelas de referência
    df_referencia = df_cds.union(df_lojas)
    
    # Identificar filiais elegíveis (CD 14 + CDs ativos + lojas ativas)
    df_com_status = (
        df
        .join(df_referencia, on="CdFilial", how="left")
        .withColumn(
            "elegivel",
            F.when(F.col("CdFilial") == 14, F.lit(True))  # CD 14 sempre elegível
            .when(F.col("NmFilial").isNotNull(), F.lit(True))  # Está na referência
            .otherwise(F.lit(False))
        )
    )
    
    # Separar elegíveis e não elegíveis
    df_elegiveis = df_com_status.filter(F.col("elegivel") == True)
    df_removidos = df_com_status.filter(F.col("elegivel") == False)
    
    registros_elegiveis = df_elegiveis.count()
    registros_removidos = df_removidos.count()
    
    print(f"  ✅ Filiais elegíveis: {registros_elegiveis:,} registros")
    print(f"  ❌ Filiais removidas: {registros_removidos:,} registros")
    
    # Relatório detalhado dos removidos
    if registros_removidos > 0:
        print(f"\n  📋 RELATÓRIO DE FILIAIS REMOVIDAS:")
        
        # Filiais removidas
        filiais_removidas = (
            df_removidos
            .select("CdFilial")
            .distinct()
            .orderBy("CdFilial")
        )
        print(f"    • Filiais removidas: {filiais_removidas.count()}")
        print("    • Lista das filiais:")
        filiais_removidas.show(20, truncate=False)
        
        # SKUs com maior merecimento nos removidos
        skus_removidos = (
            df_removidos
            .groupBy("CdSku")
            .agg(F.sum("Merecimento").alias("total_merecimento"))
            .orderBy(F.desc("total_merecimento"))
            .limit(10)
        )
        print(f"    • Top 10 SKUs removidos:")
        skus_removidos.show(10, truncate=False)
    
    # Processar apenas os elegíveis
    df_com_tipo = (
        df_elegiveis
        .withColumn(
            "is_cd",
            F.when(F.col("CdFilial") == 14, F.lit(True))  # CD 14 é CD
            .when(F.col("tipo_filial").isin(["CD", "Entreposto", "TERMINAL"]), F.lit(True))
            .otherwise(F.lit(False))
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
        .drop("is_cd", "NmFilial", "NmTipoFilial", "tipo_filial", "elegivel")
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
        .withColumn("PERCENTUAL", F.round(F.col("PERCENTUAL"), 3).cast("double"))
        .select("SKU", "CANAL", "LOJA", "DATA FIM", "PERCENTUAL")
        .orderBy("SKU", "LOJA", "CANAL")
    )
    
    print(f"✅ DataFrame final criado: {df_final.count():,} registros")
    
    return df_final

# COMMAND ----------

def validar_integridade_dados_com_filtros(df: DataFrame, categoria: str) -> bool:
    """
    Valida integridade dos dados aplicando os mesmos filtros da exportação.
    
    Aplica os mesmos filtros de produtos que são usados na exportação para garantir
    que estamos validando exatamente o que será gerado.
    
    Args:
        df: DataFrame para validação
        categoria: Categoria sendo processada
        
    Returns:
        True se todas as validações passaram
    """
    print("🔍 Validando integridade dos dados com filtros aplicados...")
    
    # Aplicar os mesmos filtros de produtos da exportação
    filtros_produtos = FILTROS_PRODUTOS.get(categoria, FILTROS_PRODUTOS_GLOBAL)
    
    if filtros_produtos.get("aplicar_filtro", False):
        print(f"  🏷️ Aplicando filtros de produtos para validação:")
        print(f"    • Incluir apenas: {filtros_produtos['tipificacao_entrega']}")
        print(f"    • Excluir marcas: {filtros_produtos['marcas_excluidas']}")
        
        # Carregar informações de produtos da tabela mercadoria
        df_mercadoria = (
            spark.table('data_engineering_prd.app_venda.mercadoria')
            .select(
                F.col("CdSkuLoja").alias("CdSku"),
                "StTipificacaoEntrega", 
                "NmMarca"
            )
            .distinct()
        )
        
        # Aplicar filtros de produto
        df_produtos_filtrados = df_mercadoria
        
        # Filtro por tipificação de entrega
        if filtros_produtos["tipificacao_entrega"]:
            df_produtos_filtrados = df_produtos_filtrados.filter(
                F.col("StTipificacaoEntrega").isin(filtros_produtos["tipificacao_entrega"])
            )
        
        # Filtro por marcas excluídas
        if filtros_produtos["marcas_excluidas"]:
            df_produtos_filtrados = df_produtos_filtrados.filter(
                ~F.col("NmMarca").isin(filtros_produtos["marcas_excluidas"])
            )
        
        # Aplicar filtro ao DataFrame de validação
        df_filtrado = (
            df
            .join(df_produtos_filtrados, df.SKU == df_produtos_filtrados.CdSku, how="inner")
            .select("SKU", "CANAL", "LOJA", "PERCENTUAL")
        )
        
        registros_antes = df.count()
        registros_apos = df_filtrado.count()
        print(f"    • Registros antes do filtro: {registros_antes:,}")
        print(f"    • Registros após filtro: {registros_apos:,} (-{registros_antes - registros_apos:,})")
        
        # Usar DataFrame filtrado para validação
        df_validacao = df_filtrado
    else:
        print(f"  🏷️ Filtros de produtos desabilitados para {categoria}")
        df_validacao = df
    
    # Chamar validação original com DataFrame filtrado
    return validar_integridade_dados(df_validacao)

def validar_integridade_dados(df: DataFrame) -> bool:
    """
    Valida integridade dos dados antes de dividir em arquivos.
    
    Validações:
    1. Somas por SKU+CANAL = 100%
    2. Chaves SKU-LOJA-CANAL aparecem uma única vez
    3. Para cada SKU, ambos os canais estão presentes (em pelo menos uma LOJA)
    
    Args:
        df: DataFrame para validação
        
    Returns:
        True se todas as validações passaram
    """
    print("🔍 Validando integridade dos dados...")
    
    # 1. Validar somas por SKU+CANAL = 100%
    print("  📊 Validando somas por SKU+CANAL...")
    df_somas = (
        df
        .groupBy("SKU", "CANAL")
        .agg(F.sum("PERCENTUAL").alias("SomaPercentual"))
    )
    
    # Verificar se todas as somas são exatamente 100.000%
    somas_invalidas = df_somas.filter(F.abs(F.col("SomaPercentual") - 100.0) > 0.0001)
    qtd_somas_invalidas = somas_invalidas.count()
    
    if qtd_somas_invalidas > 0:
        print(f"  ❌ ERRO: {qtd_somas_invalidas} combinações SKU+CANAL não somam exatamente 100.000%")
        
        # Adicionar informações de filiais para diagnóstico
        print("  📋 Diagnóstico com informações de filiais:")
        df_lojas_info = (
            spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
            .select("CdFilial", "NmFilial", "NmPorteLoja")
        )
        
        # Extrair CdFilial da coluna LOJA (formato: 0021_0XXXX ou 0099_0XXXX)
        df_com_diagnostico = (
            somas_invalidas
            .join(df, on=["SKU", "CANAL"], how="inner")
            .withColumn("CdFilial", F.regexp_extract(F.col("LOJA"), r"(\d+)$", 1).cast("int"))
            .join(df_lojas_info, on="CdFilial", how="left")
            .select("SKU", "CANAL", "SomaPercentual", "CdFilial", "NmFilial", "NmPorteLoja", "LOJA")
            .distinct()
            .orderBy("SKU", "CANAL")
        )
        
        df_com_diagnostico.show(10, truncate=False)
        return False
    else:
        print(f"  ✅ Todas as {df_somas.count()} combinações SKU+CANAL somam exatamente 100.000%")
    
    # 2. Validar unicidade de chaves SKU-LOJA-CANAL
    print("  🔑 Validando unicidade de chaves SKU-LOJA-CANAL...")
    df_contagem = (
        df
        .groupBy("SKU", "LOJA", "CANAL")
        .agg(F.count("*").alias("QtdRegistros"))
    )
    
    chaves_duplicadas = df_contagem.filter(F.col("QtdRegistros") > 1)
    qtd_chaves_duplicadas = chaves_duplicadas.count()
    
    if qtd_chaves_duplicadas > 0:
        print(f"  ❌ ERRO: {qtd_chaves_duplicadas} chaves SKU-LOJA-CANAL duplicadas")
        chaves_duplicadas.show(10, truncate=False)
        return False
    else:
        print(f"  ✅ Todas as {df_contagem.count()} chaves SKU-LOJA-CANAL são únicas")
    
    # 3. Validar que para cada SKU, ambos os canais estão presentes
    print("  🔄 Validando presença de ambos os canais por SKU...")
    
    df_canais_por_sku = (
        df
        .groupBy("SKU")
        .agg(
            F.countDistinct("CANAL").alias("QtdCanais"),
            F.collect_list("CANAL").alias("Canais")
        )
    )
    
    skus_incompletos = df_canais_por_sku.filter(F.col("QtdCanais") != 2)
    qtd_skus_incompletos = skus_incompletos.count()
    
    if qtd_skus_incompletos > 0:
        print(f"  ❌ ERRO: {qtd_skus_incompletos} SKUs não têm ambos os canais")
        skus_incompletos.show(10, truncate=False)
        return False
    else:
        print(f"  ✅ Todos os {df_canais_por_sku.count()} SKUs têm ambos os canais")
    
    # 4. Validar que ambos os canais são ONLINE e OFFLINE
    print("  📋 Validando tipos de canais...")
    canais_unicos = df.select("CANAL").distinct().rdd.flatMap(lambda x: x).collect()
    canais_esperados = ["ONLINE", "OFFLINE"]
    
    if set(canais_unicos) != set(canais_esperados):
        print(f"  ❌ ERRO: Canais encontrados: {canais_unicos}, esperados: {canais_esperados}")
        return False
    else:
        print(f"  ✅ Canais corretos: {canais_unicos}")
    
    print("  ✅ Todas as validações passaram!")
    return True

def dividir_em_arquivos(df: DataFrame, categoria: str, max_linhas: int = MAX_LINHAS_POR_ARQUIVO) -> List[DataFrame]:
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
    
    # Validar integridade antes de dividir (com filtros aplicados)
    if not validar_integridade_dados_com_filtros(df, categoria):
        raise ValueError("❌ Validação de integridade falhou. Não é possível dividir os arquivos.")
    
    # Criar chave única por SKU (todos os registros do mesmo SKU ficam juntos)
    df_com_chave = df.withColumn("chave_particao", F.col("SKU"))
    
    # Contar registros por SKU
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
        
        # Validar que cada arquivo tem pares completos de canais
        validar_pares_canais_arquivo(df_arquivo, i)
        
        print(f"    - Parte {i+1}: {qtd:,} linhas")
        dfs_separados.append(df_arquivo)
    
    return dfs_separados

def validar_pares_canais_arquivo(df_arquivo: DataFrame, num_arquivo: int) -> None:
    """
    Valida que cada arquivo tem todos os registros de cada SKU (todas as LOJAs e canais).
    
    Args:
        df_arquivo: DataFrame do arquivo específico
        num_arquivo: Número do arquivo para logs
    """
    print(f"  🔍 Validando arquivo {num_arquivo + 1}...")
    
    # Contar registros por SKU no arquivo
    df_skus_arquivo = (
        df_arquivo
        .groupBy("SKU")
        .agg(F.count("*").alias("QtdRegistros"))
    )
    
    # Verificar se todos os SKUs têm registros completos (pelo menos 2 canais por LOJA)
    # Esta validação garante que não há SKUs "cortados" entre arquivos
    skus_incompletos = df_skus_arquivo.filter(F.col("QtdRegistros") < 2)
    qtd_incompletos = skus_incompletos.count()
    
    if qtd_incompletos > 0:
        print(f"    ❌ ERRO: Arquivo {num_arquivo + 1} tem {qtd_incompletos} SKUs com menos de 2 registros")
        skus_incompletos.show(5, truncate=False)
        raise ValueError(f"Arquivo {num_arquivo + 1} tem SKUs incompletos")
    else:
        print(f"    ✅ Arquivo {num_arquivo + 1}: Todos os SKUs têm registros completos")
    
    # Verificar se os canais são ONLINE e OFFLINE
    canais_arquivo = df_arquivo.select("CANAL").distinct().rdd.flatMap(lambda x: x).collect()
    canais_esperados = ["ONLINE", "OFFLINE"]
    
    if not all(canal in canais_arquivo for canal in canais_esperados):
        print(f"    ⚠️  AVISO: Arquivo {num_arquivo + 1} não tem ambos os canais: {canais_arquivo}")
    else:
        print(f"    ✅ Arquivo {num_arquivo + 1}: Canais corretos: {canais_arquivo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Função Principal de Exportação

# COMMAND ----------

def exportar_matriz_csv(categoria: str, data_exportacao: str = None, formato: str = "xlsx") -> List[str]:
    """
    Exporta matriz de merecimento em formato CSV ou XLSX para uma categoria.
    
    Processo completo:
    1. Carregar OFFLINE e ONLINE
    2. União dos canais
    3. Normalizar para 100.00% exato
    4. Adicionar informações de filiais
    5. Criar DataFrame final formatado
    6. Dividir em arquivos (max 200k linhas)
    7. Salvar arquivos no formato escolhido
    
    Args:
        categoria: Nome da categoria
        data_exportacao: Data de exportação (padrão: hoje)
        formato: Formato de exportação - "csv" ou "xlsx" (padrão: "xlsx")
        
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
    
    # 3. Adicionar informações de filiais (remover inativas)
    print()
    df_com_filiais = adicionar_informacoes_filial(df_union)
    
    # 4. Normalizar para 100.00% APÓS remoção de filiais
    print()
    df_normalizado = normalizar_para_100_exato(df_com_filiais)
    
    # 5. Criar DataFrame final
    print()
    df_final = criar_dataframe_final(df_normalizado)
    
    # 6. Dividir em arquivos
    print()
    dfs_arquivos = dividir_em_arquivos(df_final, categoria)
    
    # 7. Salvar arquivos no formato escolhido
    print(f"\n💾 Salvando arquivos {formato.upper()}...")
    arquivos_salvos = []
    
    for idx, df_arquivo in enumerate(dfs_arquivos, start=1):
        nome_base = f"matriz_merecimento_{grupo_apelido}_{data_exportacao}_parte{idx}"
        
        # Converter para Pandas
        df_pandas = df_arquivo.toPandas()
        
        # Garantir que PERCENTUAL seja float
        df_pandas["PERCENTUAL"] = df_pandas["PERCENTUAL"].astype(float)
        
        if formato.lower() == "csv":
            # Salvar CSV com vírgula como separador decimal
            caminho_arquivo = f"{pasta_data}/{nome_base}.csv"
            df_pandas.to_csv(caminho_arquivo, index=False, sep=";", decimal=",", encoding="utf-8")
        elif formato.lower() == "xlsx":
            # Salvar XLSX
            caminho_arquivo = f"{pasta_data}/{nome_base}.xlsx"
            df_pandas.to_excel(caminho_arquivo, index=False, engine="openpyxl")
        else:
            raise ValueError(f"Formato '{formato}' não suportado. Use 'csv' ou 'xlsx'.")
        
        print(f"  ✅ Parte {idx}: {nome_base}.{formato.lower()} ({len(df_pandas):,} linhas)")
        arquivos_salvos.append(caminho_arquivo)
        
        print("\n" + "=" * 80)
    print(f"✅ Exportação concluída: {categoria}")
    print(f"📁 Total de arquivos: {len(arquivos_salvos)}")
        
    return arquivos_salvos
        

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exportar Todas as Categorias

# COMMAND ----------

def exportar_todas_categorias(data_exportacao: str = None, formato: str = "xlsx") -> Dict[str, List[str]]:
    """
    Exporta matrizes para todas as categorias no formato escolhido.
    
    Args:
        data_exportacao: Data de exportação (padrão: hoje)
        formato: Formato de exportação - "csv" ou "xlsx" (padrão: "xlsx")
        
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
            arquivos = exportar_matriz_csv(categoria, data_exportacao, formato)
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
    
    # Filtro especial para Linha Leve: apenas SKUs das espécies top 80% de PORTATEIS
    if categoria == "DIRETORIA LINHA LEVE":
        df_offline = df_offline.filter(F.col("CdSku").isin(skus_especies_top80))
        print(f"  ✅ OFFLINE (TOP 80%): {df_offline.count():,} registros | {len(skus_especies_top80)} SKUs")
    else:
        print(f"  ✅ OFFLINE: {df_offline.count():,} registros")
    
    # Agregar por grupo_de_necessidade + CdFilial (first merecimento, não soma)
    df_offline_agg = (
        df_offline
        .groupBy("grupo_de_necessidade", "CdFilial")
        .agg(F.first("Merecimento_OFFLINE").alias("Merecimento_OFFLINE"))
    )
    print(f"  ✅ OFFLINE agregado: {df_offline_agg.count():,} registros (grupo + filial)")
    
    # 2. Carregar dados ONLINE com grupo_de_necessidade
    print("\n🔄 Carregando matriz ONLINE...")
    tabela_online = TABELAS_MATRIZ_MERECIMENTO[categoria]["online"]
    
    df_online = (
        spark.table(tabela_online)
        .select(
            "CdSku", "CdFilial", "grupo_de_necessidade", "NmPorteLoja",
            (100 * F.col(coluna_merecimento)).alias("Merecimento_ONLINE")
        )
        # Aplicar regra CdFilial 1401 → 14 (apenas para TELAS e TELEFONIA)
        .withColumn(
            "CdFilial", 
            F.when(
                (F.col("CdFilial") == 1401) & (F.lit(categoria).isin(["DIRETORIA DE TELAS", "DIRETORIA TELEFONIA CELULAR"])), 
                14
            ).otherwise(F.col("CdFilial"))
        )
        .drop("NmPorteLoja")
    )
    
    # Filtro especial para Linha Leve: apenas SKUs das espécies top 80% de PORTATEIS
    if categoria == "DIRETORIA LINHA LEVE":
        df_online = df_online.filter(F.col("CdSku").isin(skus_especies_top80))
        print(f"  ✅ ONLINE (TOP 80%): {df_online.count():,} registros | {len(skus_especies_top80)} SKUs")
    else:
        print(f"  ✅ ONLINE: {df_online.count():,} registros")
    
    # Agregar por grupo_de_necessidade + CdFilial (first merecimento, não soma)
    df_online_agg = (
        df_online
        .groupBy("grupo_de_necessidade", "CdFilial")
        .agg(F.first("Merecimento_ONLINE").alias("Merecimento_ONLINE"))
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
        # Fill NULLs com 0.00 para merecimentos
        .fillna(0.00, subset=["Merecimento_OFFLINE", "Merecimento_ONLINE"])
        .orderBy("grupo_de_necessidade", "CdFilial")
    )
    print(f"  ✅ Join: {df_joined.count():,} registros")
    
    # 4. Converter para Pandas e salvar Excel
    print("\n💾 Salvando Excel...")
    df_pandas = df_joined.toPandas()
    
    # Reordenar colunas para melhor visualização
    colunas_ordenadas = ["grupo_de_necessidade", "CdFilial", "Merecimento_OFFLINE", "Merecimento_ONLINE"]
    df_pandas = df_pandas[colunas_ordenadas]
    
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
