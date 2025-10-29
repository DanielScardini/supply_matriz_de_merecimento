# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any
import plotly.graph_objs as go
import os

# Inicialização do Spark
spark = SparkSession.builder.appName("calculo_ddv_futuro_expandido").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Global - Cálculo DDV Futuro Expandido
# MAGIC
# MAGIC Este notebook calcula DDV futuro para todas as categorias: Telas, Telefonia Celular e Linha Leve.
# MAGIC Utiliza merecimentos da versão parametrizável e proporções flexíveis on/off.

# COMMAND ----------

# Configurações globais
hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

data_inicio = datetime.now() - timedelta(days=30)
data_inicio_str = data_inicio.strftime("%Y-%m-%d")
data_inicio_int = int(data_inicio.strftime("%Y%m%d"))

# Parâmetro da versão do merecimento (flexível)
VERSAO_MERECIMENTO = {
    "TELAS":"0710",  # Pode ser alterado conforme necessário
    "TELEFONIA": "2410",
    "LEVES": "0710"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração de Grupos Teste por Categoria

# COMMAND ----------

# Grupos teste organizados por categoria
GRUPOS_TESTE_TELAS = [
    "TV 32 ALTO P",
    "TV 32 MEDIO", 
    "TV 32 PP",
    "TV 40 MEDIO P",
    "TV 43 ALTO P",
    "TV 43 MEDIO",
    "TV 43 PP",
    "TV 50 ALTO P",
    "TV 50 MEDIO",
    "TV 50 PP",
    "TV 55 ALTO P",
    "TV 55 MEDIO",
    "TV 58 PP",
    "TV 60 ALTO P",
    "TV 65 ALTO P",
    "TV 65 MEDIO"
]

GRUPOS_TESTE_TELEFONIA = [
    "1200 a 1600",
    "1601 a 2000", 
    "2001 a 2500",
    "2501 a 3000",
    "3001 a 3500",
    "<1099",
    "<799",
    ">4000"
]

GRUPOS_TESTE_LINHA_LEVE = [
    "APARADOR DE PELOS_110",
    "APARADOR DE PELOS_BIV",
    "ESCOVAS MODELADORAS_110",
    "ESCOVAS MODELADORAS_220",
    "ESCOVAS MODELADORAS_BIV",
    "SECADORES DE CABELO_",
    "SECADORES DE CABELO_110",
    "SECADORES DE CABELO_220",
    "SECADORES DE CABELO_BIV",
    "ASPIRADOR DE PO_110",
    "ASPIRADOR DE PO_220",
    "ASPIRADOR DE PO_BIV",
    "CAFETEIRA ELETRICA (FILTRO)_110",
    "CAFETEIRA ELETRICA (FILTRO)_220",
    "FERROS DE PASSAR A SECO_110",
    "FERROS DE PASSAR A SECO_220",
    "FERROS PAS. ROUPA VAPOR/SPRAY_110",
    "FERROS PAS. ROUPA VAPOR/SPRAY_220",
    "FRITADEIRA ELETRICA (CAPSULA)_110",
    "FRITADEIRA ELETRICA (CAPSULA)_220",
    "LIQUIDIFICADORES 350 A 1000 W_110",
    "LIQUIDIFICADORES 350 A 1000 W_220",
    "LIQUIDIFICADORES ACIMA 1001 W._110",
    "LIQUIDIFICADORES ACIMA 1001 W._220",
    "PANELAS ELETRICAS DE ARROZ_110",
    "PANELAS ELETRICAS DE ARROZ_220",
    "SANDUICHEIRAS_110",
    "SANDUICHEIRAS_220"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Flexível por Categoria

# COMMAND ----------

# Configuração flexível para cada categoria
CATEGORIAS_CONFIG = {
    "TELAS": {
        "grupos_teste": GRUPOS_TESTE_TELAS,
        "tabela_merecimento_off": f"databox.bcg_comum.supply_matriz_merecimento_de_telas_teste{VERSAO_MERECIMENTO['TELAS']}",
        "tabela_merecimento_on": f"databox.bcg_comum.supply_matriz_merecimento_de_telas_online_teste{VERSAO_MERECIMENTO['TELAS']}",
        "de_para": "databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia",
        "proporcao_on": 0.235,  # 23.5%
        "proporcao_off": 0.765,  # Complementar
        "tabela_base_off": "databox.bcg_comum.supply_base_merecimento_diario_v4",
        "tabela_base_on": "databox.bcg_comum.supply_base_merecimento_diario_v4_online"
    },
    "TELEFONIA": {
        "grupos_teste": GRUPOS_TESTE_TELEFONIA,
        "tabela_merecimento_off": f"databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_teste{VERSAO_MERECIMENTO['TELEFONIA']}",
        "tabela_merecimento_on": f"databox.bcg_comum.supply_matriz_merecimento_telefonia_celular_online_teste{VERSAO_MERECIMENTO['TELEFONIA']}",
        "de_para": "databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia",  # Mesmo de telas
        "proporcao_on": 0.235,  # 23.5%
        "proporcao_off": 0.765,  # Complementar
        "tabela_base_off": "databox.bcg_comum.supply_base_merecimento_diario_v4",
        "tabela_base_on": "databox.bcg_comum.supply_base_merecimento_diario_v4_online"
    },
    "LINHA_LEVE": {
        "grupos_teste": GRUPOS_TESTE_LINHA_LEVE,
        "tabela_merecimento_off": f"databox.bcg_comum.supply_matriz_merecimento_linha_leve_teste{VERSAO_MERECIMENTO['LEVES']}",
        "tabela_merecimento_on": f"databox.bcg_comum.supply_matriz_merecimento_linha_leve_online_teste{VERSAO_MERECIMENTO['LEVES']}",
        "de_para": "databox.bcg_comum.supply_grupo_de_necessidade_linha_leve",
        "proporcao_on": 0.235,  # 23.5%
        "proporcao_off": 0.765,  # Complementar
        "tabela_base_off": "databox.bcg_comum.supply_base_merecimento_diario_v4",
        "tabela_base_on": "databox.bcg_comum.supply_base_merecimento_diario_v4_online"
    }
}

print("🔧 CONFIGURAÇÃO CARREGADA:")
for categoria, config in CATEGORIAS_CONFIG.items():
    print(f"  • {categoria}: {len(config['grupos_teste'])} grupos teste")
    print("    - Proporções serão calculadas dinamicamente baseadas nos dados reais")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Função Genérica para Cálculo DDV

# COMMAND ----------

def calcular_ddv_categoria(categoria: str, tipo_dados: str) -> DataFrame:
    """
    Calcula DDV futuro para uma categoria específica (TELAS, TELEFONIA, LINHA_LEVE)
    e tipo de dados (offline ou online).
    
    Args:
        categoria: Nome da categoria (TELAS, TELEFONIA, LINHA_LEVE)
        tipo_dados: 'off' ou 'on'
    
    Returns:
        DataFrame com DDV calculado
    """
    if categoria not in CATEGORIAS_CONFIG:
        raise ValueError(f"Categoria '{categoria}' não encontrada. Categorias válidas: {list(CATEGORIAS_CONFIG.keys())}")
    
    if tipo_dados not in ['off', 'on']:
        raise ValueError(f"Tipo de dados '{tipo_dados}' inválido. Use 'off' ou 'on'")
    
    config = CATEGORIAS_CONFIG[categoria]
    grupos_teste = config['grupos_teste']
    
    print(f"🔍 Calculando DDV para {categoria} - {tipo_dados.upper()}")
    print(f"  • Grupos teste: {len(grupos_teste)}")
    
    # Determinar tabela de merecimento baseada no tipo
    if tipo_dados == 'off':
        tabela_merecimento = config['tabela_merecimento_off']
        tabela_base = config['tabela_base_off']
    else:
        tabela_merecimento = config['tabela_merecimento_on']
        tabela_base = config['tabela_base_on']
    
    print(f"  • Tabela base: {tabela_base}")
    print(f"  • Tabela merecimento: {tabela_merecimento}")
    
    df_base = (
        spark.table(tabela_base)
        .filter(F.col('DtAtual') >= data_inicio_str)
        .filter(F.col('DtAtual') <= hoje_str)
    )
    
    print(f"  • Dados base carregados: {df_base.count():,} registros")
    
    # Join com de-para (todos têm a mesma estrutura)
    df_de_para = spark.table(config['de_para'])
    
    df_com_grupos = df_base.join(
        df_de_para.filter(F.col('grupo_de_necessidade').isin(grupos_teste))
        .select('CdSku', 'grupo_de_necessidade'),
        how="inner",
        on="CdSku"
    )
    
    print(f"  • Após join com de-para: {df_com_grupos.count():,} registros")
    
    # FILTRO: Apenas lojas (não CDs)
    # Carregar tabela de lojas ativas para filtrar apenas lojas
    print(f"  📊 Carregando tabela de lojas ativas para filtrar apenas lojas (não CDs)...")
    df_lojas_ativas = spark.table("data_engineering_prd.app_operacoes_loja.roteirizacaolojaativa").select("CdFilial").distinct()
    total_lojas = df_lojas_ativas.count()
    print(f"  • Lojas ativas encontradas: {total_lojas:,}")
    
    # Filtrar df_com_grupos para incluir apenas lojas
    df_com_grupos = df_com_grupos.join(
        df_lojas_ativas,
        on="CdFilial",
        how="inner"
    )
    
    print(f"  • Após filtro de lojas: {df_com_grupos.count():,} registros (apenas lojas, sem CDs)")
    
    # LÓGICA CORRETA:
    # 1. Somar demanda TOTAL por SKU a nível CIA (apenas lojas, sem CDs) nos últimos N dias
    # 2. Diarizar essa demanda total (dividindo por dias úteis, excluindo domingos)
    # 3. Multiplicar demanda diarizada TOTAL pelo merecimento de cada filial por grupo
    #    para obter demanda diarizada POR FILIAL (apenas lojas)
    
    print(f"  📊 Calculando demanda TOTAL a nível CIA por grupo+SKU (apenas lojas)...")
    df_demanda = (
        df_com_grupos
        .groupBy("grupo_de_necessidade", "CdSku")  # SEM CdFilial - demanda TOTAL a nível CIA
        .agg(
            F.round(F.sum(F.col('QtMercadoria') + F.col("deltaRuptura")), 3).alias("demanda_total"),
            F.countDistinct("DtAtual").alias("dias"),
            F.round(F.col("dias")/7, 1).alias("n_domingos"),
            F.round(F.col("demanda_total")/(F.col("dias") - F.col("n_domingos")), 3).alias("demanda_diarizada")
        )
        .orderBy(F.desc("demanda_diarizada"))
    )
    
    # Validação: mostrar amostra da demanda calculada
    print(f"  • Demanda calculada: {df_demanda.count():,} registros (grupo+SKU)")
    print(f"  📋 Amostra de demanda diarizada TOTAL (top 5):")
    df_demanda.select("grupo_de_necessidade", "CdSku", "demanda_total", "dias", "demanda_diarizada").show(5, truncate=False)
    
    # Carregar matriz de merecimento
    # IMPORTANTE: merecimento é por filial+grupo+SKU e representa a proporção/distribuição
    # FILTRO: Apenas lojas (não CDs) no merecimento também
    print(f"  📊 Carregando matriz de merecimento (apenas lojas)...")
    df_merecimento = (
        spark.table(tabela_merecimento)
        .select(
            "grupo_de_necessidade",  # <- ADICIONADO para garantir unicidade
            "CdSku", 
            "CdFilial",
            F.col("Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura").alias("merecimento_final")
        )
        .join(
            df_lojas_ativas,
            on="CdFilial",
            how="inner"
        )
    )
    
    # Validar formato do merecimento (verificar se é percentual 0-100 ou decimal 0-1)
    print(f"  • Merecimentos carregados: {df_merecimento.count():,} registros")
    print(f"  📋 Amostra de merecimentos (top 5):")
    df_merecimento.select("grupo_de_necessidade", "CdSku", "CdFilial", "merecimento_final").show(5, truncate=False)
    
    # Verificar estatísticas do merecimento para entender se é percentual ou decimal
    stats_merecimento = df_merecimento.agg(
        F.min("merecimento_final").alias("min_merecimento"),
        F.max("merecimento_final").alias("max_merecimento"),
        F.avg("merecimento_final").alias("avg_merecimento")
    ).collect()[0]
    
    print(f"  📊 Estatísticas do merecimento:")
    print(f"     - Mínimo: {stats_merecimento['min_merecimento']}")
    print(f"     - Máximo: {stats_merecimento['max_merecimento']}")
    print(f"     - Média: {stats_merecimento['avg_merecimento']}")
    
    # Se o máximo for > 1, provavelmente é percentual (0-100), senão é decimal (0-1)
    # NORMALIZAR: se for percentual, dividir por 100 para converter em decimal (0-1)
    if stats_merecimento['max_merecimento'] > 1.0:
        print(f"  ⚠️ Merecimento parece estar em formato PERCENTUAL (0-100). Convertendo para decimal (0-1)...")
        df_merecimento = df_merecimento.withColumn(
            "merecimento_final", 
            F.round(F.col("merecimento_final") / 100.0, 6)
        )
        print(f"  ✅ Merecimento normalizado para decimal (0-1)")
    else:
        print(f"  ✅ Merecimento já está em formato decimal (0-1)")
    
    # Join: distribuir demanda diarizada TOTAL por filial via merecimento
    print(f"  📊 Distribuindo demanda diarizada TOTAL por filial via merecimento...")
    df_final = (
        df_demanda
        .join(
            df_merecimento, 
            on=["grupo_de_necessidade", "CdSku"],  # Join por grupo+SKU - distribui demanda total por filial
            how="inner"
        )
        .withColumn("DDV_futuro_filial",
                   F.round(F.col("demanda_diarizada") * F.col("merecimento_final"), 3))
    )
    
    # Validação: verificar se a soma dos DDVs por grupo+SKU é próxima da demanda diarizada original
    print(f"  ✅ Validando cálculo DDV...")
    df_validacao = (
        df_final
        .groupBy("grupo_de_necessidade", "CdSku", "demanda_diarizada")
        .agg(F.sum("DDV_futuro_filial").alias("soma_ddv_por_filial"))
        .withColumn("diferenca_pct", 
                   F.round(((F.col("soma_ddv_por_filial") - F.col("demanda_diarizada")) / F.col("demanda_diarizada")) * 100, 2))
    )
    
    print(f"  📋 Validação: Comparando demanda_diarizada TOTAL vs soma de DDV por filial (top 10):")
    df_validacao.select("grupo_de_necessidade", "CdSku", "demanda_diarizada", "soma_ddv_por_filial", "diferenca_pct").show(10, truncate=False)
    
    # Remover colunas temporárias de validação
    df_final = df_final.drop("demanda_total", "dias", "n_domingos")
    
    print(f"  • DDV calculado: {df_final.count():,} registros")
    
    return df_final

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executar Cálculo DDV para Todas as Categorias

# COMMAND ----------

print("🚀 INICIANDO CÁLCULO DDV PARA TODAS AS CATEGORIAS")
print("=" * 80)

resultados_ddv = {}

for categoria in CATEGORIAS_CONFIG.keys():
    print(f"\n{'='*20} {categoria} {'='*20}")
    
    try:
        # Calcular DDV offline
        df_off = calcular_ddv_categoria(categoria, 'off')
        
        # Calcular DDV online  
        df_on = calcular_ddv_categoria(categoria, 'on')
        
        print(f"  • Registros offline: {df_off.count():,}")
        print(f"  • Registros online: {df_on.count():,}")
        
        # Adicionar sufixos para distinguir offline/online
        chaves = ["grupo_de_necessidade", "CdSku", "CdFilial"]
        
        df_off_sufixo = df_off.toDF(
            *[c if c in chaves else f"{c}_off" for c in df_off.columns]
        )
        
        df_on_sufixo = df_on.toDF(
            *[c if c in chaves else f"{c}_on" for c in df_on.columns]
        )
        
        # Join offline + online usando full_outer para manter todos os registros
        # Se offline não tiver dados, manter apenas online (e vice-versa)
        df_consolidado = (
            df_off_sufixo
            .join(df_on_sufixo, on=["grupo_de_necessidade", "CdSku", "CdFilial"], how="full_outer")
        )
        
        # Identificar colunas numéricas para preencher nulos com 0
        colunas_off = [c for c in df_off_sufixo.columns if c not in chaves]
        colunas_on = [c for c in df_on_sufixo.columns if c not in chaves]
        colunas_para_preencher = colunas_off + colunas_on
        
        # Preencher valores nulos (quando um lado não tem dados) com 0
        if colunas_para_preencher:
            df_consolidado = df_consolidado.fillna(0.0, subset=colunas_para_preencher)
        
        # Calcular DDV total (off + on)
        df_consolidado = df_consolidado.withColumn(
            "DDV_futuro_filial_merecimento",
            F.round(
                F.coalesce(F.col("DDV_futuro_filial_off"), F.lit(0.0)) + 
                F.coalesce(F.col("DDV_futuro_filial_on"), F.lit(0.0)), 
                3
            )
        )
        
        resultados_ddv[categoria] = df_consolidado
        
        print(f"✅ {categoria} processada com sucesso!")
        print(f"  • Registros consolidados: {df_consolidado.count():,}")
        
        # Mostrar amostra dos dados
        print(f"\n📊 AMOSTRA DOS DADOS ({categoria}):")
        df_consolidado.select(
            "grupo_de_necessidade", "CdSku", "CdFilial",
            "DDV_futuro_filial_merecimento", "demanda_diarizada_off", "demanda_diarizada_on"
        ).show(5, truncate=False)
        
    except Exception as e:
        print(f"❌ Erro ao processar {categoria}: {str(e)}")
        continue

print(f"\n✅ PROCESSAMENTO CONCLUÍDO!")
print(f"📊 Categorias processadas: {len(resultados_ddv)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consolidação Final com Proporções Flexíveis

# COMMAND ----------

print("🔄 CONSOLIDANDO RESULTADOS FINAIS COM PROPORÇÕES FLEXÍVEIS")
print("=" * 80)

# Lista para armazenar DataFrames consolidados
dfs_consolidados = []

for categoria, df_resultado in resultados_ddv.items():
    print(f"\n📊 Processando {categoria}:")
    
    try:
        # Verificar se as colunas existem
        colunas_disponiveis = df_resultado.columns
        print(f"  • Colunas disponíveis: {', '.join(colunas_disponiveis[:10])}...")
        
        if "DDV_futuro_filial_on" not in colunas_disponiveis or "DDV_futuro_filial_off" not in colunas_disponiveis:
            print(f"  ⚠️ Colunas DDV_futuro_filial_on ou DDV_futuro_filial_off não encontradas!")
            print(f"     Colunas encontradas: {colunas_disponiveis}")
            continue
        
        # Calcular proporções reais baseadas nos dados das tabelas ON e OFF
        total_on = df_resultado.agg(F.sum("DDV_futuro_filial_on")).collect()[0][0] or 0.0
        total_off = df_resultado.agg(F.sum("DDV_futuro_filial_off")).collect()[0][0] or 0.0
        total_geral = total_on + total_off
        
        if total_geral > 0:
            proporcao_on_real = total_on / total_geral
            proporcao_off_real = total_off / total_geral
        else:
            proporcao_on_real = 0.0
            proporcao_off_real = 0.0
        
        print(f"  • Proporção ON real: {proporcao_on_real:.1%}")
        print(f"  • Proporção OFF real: {proporcao_off_real:.1%}")
        
        # CORREÇÃO: Não multiplicar DDVs pelas proporções - isso reduz os valores incorretamente
        # Os DDVs já estão calculados corretamente (demanda_diarizada * merecimento)
        # As proporções são apenas informativas para análise
        df_com_proporcoes = (
            df_resultado
            .withColumn("DDV_final_on", F.round(F.col("DDV_futuro_filial_on"), 3))
            .withColumn("DDV_final_off", F.round(F.col("DDV_futuro_filial_off"), 3))
            .withColumn("DDV_final_total", F.round(F.col("DDV_final_on") + F.col("DDV_final_off"), 3))
            .withColumn("categoria", F.lit(categoria))
            .withColumn("proporcao_on_real", F.lit(proporcao_on_real))
            .withColumn("proporcao_off_real", F.lit(proporcao_off_real))
            .select(
                "categoria",
                "grupo_de_necessidade", 
                "CdSku", 
                "CdFilial",
                "demanda_diarizada_off",
                "demanda_diarizada_on",
                "DDV_futuro_filial_off",
                "DDV_futuro_filial_on", 
                "DDV_final_on",
                "DDV_final_off",
                "DDV_final_total",
                "proporcao_on_real",
                "proporcao_off_real"
            )
        )
        
        dfs_consolidados.append(df_com_proporcoes)
        
        print(f"  • Registros com proporções: {df_com_proporcoes.count():,}")
    except Exception as e:
        print(f"  ❌ Erro ao processar {categoria} na consolidação: {str(e)}")
        import traceback
        traceback.print_exc()
        continue

print(f"\n📊 Total de categorias consolidadas: {len(dfs_consolidados)}")

# Unir todos os DataFrames
if dfs_consolidados:
    df_final_consolidado = dfs_consolidados[0]
    for df in dfs_consolidados[1:]:
        df_final_consolidado = df_final_consolidado.union(df)
    
    # IMPORTANTE: Consolidar duplicatas
    # demanda_diarizada é única por grupo+SKU (usa MAX)
    # DDVs podem ser somados se houver duplicatas
    registros_antes = df_final_consolidado.count()
    
    df_final_consolidado = (
        df_final_consolidado
        .groupBy("categoria", "grupo_de_necessidade", "CdSku", "CdFilial", "proporcao_on_real", "proporcao_off_real")
        .agg(
            # CORREÇÃO: demanda_diarizada é única por grupo+SKU, NÃO deve ser somada
            # Se houver duplicatas, usar MAX (ou FIRST) para pegar o valor único
            F.max("demanda_diarizada_off").alias("demanda_diarizada_off"),
            F.max("demanda_diarizada_on").alias("demanda_diarizada_on"),
            # DDVs devem ser SOMADOS se houver duplicatas
            F.sum("DDV_futuro_filial_off").alias("DDV_futuro_filial_off"),
            F.sum("DDV_futuro_filial_on").alias("DDV_futuro_filial_on"),
            F.sum("DDV_final_on").alias("DDV_final_on"),
            F.sum("DDV_final_off").alias("DDV_final_off"),
            F.sum("DDV_final_total").alias("DDV_final_total")
        )
    )
    
    registros_depois = df_final_consolidado.count()
    
    if registros_antes > registros_depois:
        print(f"  ⚠️ {registros_antes - registros_depois:,} duplicatas consolidadas (demanda_diarizada: MAX, DDVs: SUM)!")
    
    print(f"\n🎯 RESULTADO FINAL CONSOLIDADO:")
    print(f"  • Total de registros: {df_final_consolidado.count():,}")
    print(f"  • Categorias incluídas: {df_final_consolidado.select('categoria').distinct().count()}")
    
    # Mostrar resumo por categoria
    print(f"\n📈 RESUMO POR CATEGORIA:")
    resumo_categoria = (
        df_final_consolidado
        .groupBy("categoria")
        .agg(
            F.countDistinct("CdSku").alias("SKUs_unicos"),
            F.countDistinct("CdFilial").alias("Filiais_unicas"),
            F.countDistinct("grupo_de_necessidade").alias("Grupos_unicos"),
            F.sum("DDV_final_total").alias("DDV_total_soma"),
            F.avg("DDV_final_total").alias("DDV_medio")
        )
        .orderBy("categoria")
    )
    
    resumo_categoria.show(truncate=False)
    
else:
    print("❌ Nenhum resultado para consolidar!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregação Final por Chave (CdSku + CdFilial)

# COMMAND ----------

if 'df_final_consolidado' in locals():
    print("🔄 APLICANDO GROUPBY FINAL: SOMANDO DDV_final_total POR Chave, CdSku, CdFilial")
    print("=" * 80)
    
    # Criar coluna Chave (CdSku-CdFilial)
    df_final_consolidado = (
        df_final_consolidado
        .withColumn("Chave", 
                   F.concat(
                       F.col("CdSku").cast("string"),
                       F.lit("-"),
                       F.col("CdFilial").cast("string")
                   ))
    )
    
    # GroupBy por Chave, CdSku, CdFilial somando DDV_final_total
    registros_antes_agreg = df_final_consolidado.count()
    
    df_final_consolidado = (
        df_final_consolidado
        .groupBy("Chave", "CdSku", "CdFilial")
        .agg(
            F.sum("DDV_final_total").alias("DDV_final_total")
        )
    )
    
    registros_depois_agreg = df_final_consolidado.count()
    
    print(f"  • Registros antes da agregação: {registros_antes_agreg:,}")
    print(f"  • Registros depois da agregação: {registros_depois_agreg:,}")
    
    if registros_antes_agreg > registros_depois_agreg:
        print(f"  ✅ {registros_antes_agreg - registros_depois_agreg:,} registros consolidados (DDVs somados)")
    
    print(f"\n📊 AMOSTRA DO RESULTADO FINAL (top 10):")
    df_final_consolidado.orderBy(F.desc("DDV_final_total")).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar Resultados em CSV

# COMMAND ----------

if 'df_final_consolidado' in locals():
    print("💾 SALVANDO RESULTADOS EM CSV")
    print("=" * 50)
    
    # Validação final: garantir que não há duplicatas
    # Após o groupBy, temos apenas: Chave, CdSku, CdFilial, DDV_final_total
    registros_spark = df_final_consolidado.count()
    chaves_unicas_spark = df_final_consolidado.select("Chave", "CdSku", "CdFilial").distinct().count()
    
    if registros_spark != chaves_unicas_spark:
        print(f"  ⚠️ ATENÇÃO: {registros_spark - chaves_unicas_spark} duplicatas ainda presentes!")
        print(f"     Registros: {registros_spark}, Chaves únicas: {chaves_unicas_spark}")
    else:
        print(f"  ✅ Nenhuma duplicata encontrada!")
    
    # Converter para Pandas
    df_pandas = df_final_consolidado.toPandas()
    
    # Última garantia: remover duplicatas no pandas (caso ainda existam)
    registros_antes_pandas = len(df_pandas)
    df_pandas = df_pandas.drop_duplicates(subset=["Chave", "CdSku", "CdFilial"])
    registros_depois_pandas = len(df_pandas)
    
    if registros_antes_pandas > registros_depois_pandas:
        print(f"  ⚠️ {registros_antes_pandas - registros_depois_pandas:,} duplicatas removidas no pandas!")
    
    # Garantir que colunas numéricas sejam float
    colunas_numericas = ["DDV_final_total"]
    
    for col in colunas_numericas:
        if col in df_pandas.columns:
            df_pandas[col] = df_pandas[col].astype(float)
    
    # Salvar em CSV (mais eficiente para grandes volumes)
    output_dir = f"/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/output/{hoje_str}/ddv_futuro/"
    
    # Criar diretório se não existir
    os.makedirs(output_dir, exist_ok=True)
    
    # Instalar openpyxl se necessário
    import subprocess
    try:
        import openpyxl
    except ImportError:
        print("📦 Instalando openpyxl...")
        subprocess.check_call(["pip", "install", "openpyxl"])
    
    # Configuração para divisão em partes
    MAX_LINHAS_POR_ARQUIVO = 250000
    
    # Arquivo 1: DDV Final simplificado (dividido em partes se necessário)
    # A coluna Chave já foi criada no Spark antes da agregação
    df_simplificado = df_pandas[['Chave', 'CdSku', 'CdFilial', 'DDV_final_total']].copy()
    
    # Formatar DDV_final_total com vírgula como decimal
    df_simplificado['DDV_final_total'] = df_simplificado['DDV_final_total'].apply(lambda x: f"{x:.3f}".replace('.', ','))
    
    total_linhas_simplificado = len(df_simplificado)
    num_partes_simplificado = (total_linhas_simplificado + MAX_LINHAS_POR_ARQUIVO - 1) // MAX_LINHAS_POR_ARQUIVO
    
    print(f"📊 DDV Final simplificado: {total_linhas_simplificado:,} linhas")
    print(f"📦 Será dividido em {num_partes_simplificado} parte(s)")
    
    for i in range(num_partes_simplificado):
        inicio = i * MAX_LINHAS_POR_ARQUIVO
        fim = min((i + 1) * MAX_LINHAS_POR_ARQUIVO, total_linhas_simplificado)
        
        df_parte = df_simplificado.iloc[inicio:fim]
        
        if num_partes_simplificado == 1:
            nome_arquivo = f"ddv_futuro_final_{VERSAO_MERECIMENTO}_{hoje_str}.xlsx"
        else:
            nome_arquivo = f"ddv_futuro_final_{VERSAO_MERECIMENTO}_{hoje_str}_parte_{i+1:02d}.xlsx"
        
        output_path = f"{output_dir}{nome_arquivo}"
        df_parte.to_excel(output_path, index=False, engine='openpyxl')
        print(f"  ✅ Parte {i+1}: {len(df_parte):,} linhas -> {nome_arquivo}")
    
    # Arquivo 2: Memorial completo (dividido em partes se necessário)
    # Após o groupBy final, temos apenas: Chave, CdSku, CdFilial, DDV_final_total
    df_memorial_formatado = df_pandas.copy()
    colunas_para_formatar = ["DDV_final_total"]
    
    for col in colunas_para_formatar:
        if col in df_memorial_formatado.columns:
            df_memorial_formatado[col] = df_memorial_formatado[col].apply(lambda x: f"{x:.3f}".replace('.', ','))
    
    total_linhas_memorial = len(df_memorial_formatado)
    num_partes_memorial = (total_linhas_memorial + MAX_LINHAS_POR_ARQUIVO - 1) // MAX_LINHAS_POR_ARQUIVO
    
    print(f"\n📊 Memorial completo: {total_linhas_memorial:,} linhas")
    print(f"📦 Será dividido em {num_partes_memorial} parte(s)")
    
    for i in range(num_partes_memorial):
        inicio = i * MAX_LINHAS_POR_ARQUIVO
        fim = min((i + 1) * MAX_LINHAS_POR_ARQUIVO, total_linhas_memorial)
        
        df_parte = df_memorial_formatado.iloc[inicio:fim]
        
        if num_partes_memorial == 1:
            nome_arquivo = f"ddv_futuro_memorial_{VERSAO_MERECIMENTO}_{hoje_str}.xlsx"
        else:
            nome_arquivo = f"ddv_futuro_memorial_{VERSAO_MERECIMENTO}_{hoje_str}_parte_{i+1:02d}.xlsx"
        
        output_path = f"{output_dir}{nome_arquivo}"
        df_parte.to_excel(output_path, index=False, engine='openpyxl')
        print(f"  ✅ Parte {i+1}: {len(df_parte):,} linhas -> {nome_arquivo}")
    
    print(f"\n✅ Arquivos salvos em: {output_dir}")
    print(f"📊 Total de registros salvos: {len(df_pandas):,}")
    print(f"📋 Resumo:")
    print(f"  • DDV Final: {num_partes_simplificado} arquivo(s) XLSX")
    print(f"  • Memorial: {num_partes_memorial} arquivo(s) XLSX")
    print(f"  • Máximo por arquivo: {MAX_LINHAS_POR_ARQUIVO:,} linhas")
    print(f"  • Formato: Vírgula como separador decimal")
    
else:
    print("❌ Nenhum resultado para salvar!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validações Finais

# COMMAND ----------

if 'df_final_consolidado' in locals():
    print("🔍 VALIDAÇÕES FINAIS")
    print("=" * 50)
    
    # Após o groupBy final, temos apenas: Chave, CdSku, CdFilial, DDV_final_total
    # Validação 1: Verificar se DDV final é positivo
    ddv_negativos = df_final_consolidado.filter(F.col("DDV_final_total") < 0).count()
    print(f"📊 Validação 1 - DDV negativos: {ddv_negativos}")
    
    # Validação 2: Verificar unicidade de chaves (Chave, CdSku, CdFilial)
    print(f"\n📊 Validação 2 - Unicidade de chaves:")
    print("  Verificando se não há duplicação de chaves (Chave, CdSku, CdFilial)...")
    
    # Contar registros totais
    total_registros = df_final_consolidado.count()
    
    # Contar chaves únicas
    chaves_unicas = df_final_consolidado.select("Chave", "CdSku", "CdFilial").distinct().count()
    
    # Verificar duplicatas
    df_agrupado = (
        df_final_consolidado
        .groupBy("Chave", "CdSku", "CdFilial")
        .agg(F.count("*").alias("count_chave"))
        .filter(F.col("count_chave") > 1)
    )
    duplicatas = df_agrupado.count()
    
    print(f"  • Total de registros: {total_registros:,}")
    print(f"  • Chaves únicas esperadas: {chaves_unicas:,}")
    print(f"  • Duplicatas encontradas: {duplicatas:,}")
    
    if duplicatas == 0:
        print(f"  ✅ SEM DUPLICAÇÕES - Chaves únicas garantidas!")
    else:
        print(f"  ❌ ATENÇÃO: {duplicatas} chave(s) duplicada(s)")
        print(f"  📋 Amostra de chaves duplicadas:")
        df_agrupado.select("Chave", "CdSku", "CdFilial", "count_chave").show(10, truncate=False)
    
    # Validação 3: Estatísticas gerais
    print(f"\n📊 Validação 3 - Estatísticas gerais:")
    stats = df_final_consolidado.agg(
        F.sum("DDV_final_total").alias("total_ddv"),
        F.avg("DDV_final_total").alias("media_ddv"),
        F.min("DDV_final_total").alias("min_ddv"),
        F.max("DDV_final_total").alias("max_ddv"),
        F.countDistinct("CdSku").alias("skus_unicos"),
        F.countDistinct("CdFilial").alias("filiais_unicas")
    ).collect()[0]
    
    print(f"  • Total DDV: {stats['total_ddv']:,.2f}")
    print(f"  • Média DDV por chave: {stats['media_ddv']:,.2f}")
    print(f"  • Mínimo DDV: {stats['min_ddv']:,.2f}")
    print(f"  • Máximo DDV: {stats['max_ddv']:,.2f}")
    print(f"  • SKUs únicos: {stats['skus_unicos']:,}")
    print(f"  • Filiais únicas: {stats['filiais_unicas']:,}")
    
    print(f"\n✅ Validações concluídas!")
