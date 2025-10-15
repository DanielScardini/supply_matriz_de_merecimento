# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any
import plotly.graph_objs as go

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
VERSAO_MERECIMENTO = "0710"  # Pode ser alterado conforme necessário

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
        "tabela_merecimento_off": f"databox.bcg_comum.supply_matriz_merecimento_de_telas_teste{VERSAO_MERECIMENTO}",
        "tabela_merecimento_on": f"databox.bcg_comum.supply_matriz_merecimento_de_telas_online_teste{VERSAO_MERECIMENTO}",
        "de_para": "databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia",
        "proporcao_on": 0.235,  # 23.5%
        "proporcao_off": 0.765,  # Complementar
        "tabela_base": "databox.bcg_comum.supply_base_merecimento_diario_v4"
    },
    "TELEFONIA": {
        "grupos_teste": GRUPOS_TESTE_TELEFONIA,
        "tabela_merecimento_off": f"databox.bcg_comum.supply_matriz_merecimento_telefonia_teste{VERSAO_MERECIMENTO}",
        "tabela_merecimento_on": f"databox.bcg_comum.supply_matriz_merecimento_telefonia_online_teste{VERSAO_MERECIMENTO}",
        "de_para": "databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia",  # Mesmo de telas
        "proporcao_on": 0.235,  # 23.5%
        "proporcao_off": 0.765,  # Complementar
        "tabela_base": "databox.bcg_comum.supply_base_merecimento_diario_v4"
    },
    "LINHA_LEVE": {
        "grupos_teste": GRUPOS_TESTE_LINHA_LEVE,
        "tabela_merecimento_off": f"databox.bcg_comum.supply_matriz_merecimento_linha_leve_teste{VERSAO_MERECIMENTO}",
        "tabela_merecimento_on": f"databox.bcg_comum.supply_matriz_merecimento_linha_leve_online_teste{VERSAO_MERECIMENTO}",
        "de_para": "databox.bcg_comum.supply_grupo_de_necessidade_linha_leve",  # Mesma estrutura de colunas
        "proporcao_on": 0.235,  # 23.5%
        "proporcao_off": 0.765,  # Complementar
        "tabela_base": "databox.bcg_comum.supply_base_merecimento_diario_v4"
    }
}

print("🔧 CONFIGURAÇÃO CARREGADA:")
for categoria, config in CATEGORIAS_CONFIG.items():
    print(f"  • {categoria}: {len(config['grupos_teste'])} grupos teste")
    print(f"    - Proporção ON: {config['proporcao_on']:.1%}")
    print(f"    - Proporção OFF: {config['proporcao_off']:.1%}")

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
    else:
        tabela_merecimento = config['tabela_merecimento_on']
    
    # Carregar dados base
    df_base = (
        spark.table(config['tabela_base'])
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
    
    # Calcular demanda diarizada
    df_demanda = (
        df_com_grupos
        .groupBy("grupo_de_necessidade", "CdSku")
        .agg(
            F.round(F.sum(F.col('QtMercadoria') + F.col("deltaRuptura")), 3).alias("demanda_total"),
            F.countDistinct("DtAtual").alias("dias"),
            F.round(F.col("dias")/7, 1).alias("n_domingos"),
            F.round(F.col("demanda_total")/(F.col("dias") - F.col("n_domingos")), 3).alias("demanda_diarizada")
        )
        .orderBy(F.desc("demanda_diarizada"))
    )
    
    # Join com matriz de merecimento
    df_merecimento = spark.table(tabela_merecimento).select(
        "CdSku", "CdFilial",
        F.col("Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura").alias("merecimento_final")
    )
    
    df_final = (
        df_demanda
        .join(df_merecimento, on="CdSku", how="inner")
        .withColumn("DDV_futuro_filial",
                   F.round(F.col("demanda_diarizada") * F.col("merecimento_final"), 3))
    )
    
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
        
        # Adicionar sufixos para distinguir offline/online
        chaves = ["grupo_de_necessidade", "CdSku", "CdFilial"]
        
        df_off_sufixo = df_off.toDF(
            *[c if c in chaves else f"{c}_off" for c in df_off.columns]
        )
        
        df_on_sufixo = df_on.toDF(
            *[c if c in chaves else f"{c}_on" for c in df_on.columns]
        )
        
        # Join offline + online
        df_consolidado = (
            df_off_sufixo
            .join(df_on_sufixo, on=["grupo_de_necessidade", "CdSku", "CdFilial"], how="inner")
            .withColumn("DDV_futuro_filial_merecimento",
                       F.round(F.col("DDV_futuro_filial_off") + F.col("DDV_futuro_filial_on"), 3))
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
    config = CATEGORIAS_CONFIG[categoria]
    proporcao_on = config['proporcao_on']
    proporcao_off = config['proporcao_off']
    
    print(f"\n📊 Processando {categoria}:")
    print(f"  • Proporção ON: {proporcao_on:.1%}")
    print(f"  • Proporção OFF: {proporcao_off:.1%}")
    
    # Aplicar proporções flexíveis
    df_com_proporcoes = (
        df_resultado
        .withColumn("DDV_final_on", F.round(F.col("DDV_futuro_filial_on") * proporcao_on, 3))
        .withColumn("DDV_final_off", F.round(F.col("DDV_futuro_filial_off") * proporcao_off, 3))
        .withColumn("DDV_final_total", F.round(F.col("DDV_final_on") + F.col("DDV_final_off"), 3))
        .withColumn("categoria", F.lit(categoria))
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
            "DDV_final_total"
        )
    )
    
    dfs_consolidados.append(df_com_proporcoes)
    
    print(f"  • Registros com proporções: {df_com_proporcoes.count():,}")

# Unir todos os DataFrames
if dfs_consolidados:
    df_final_consolidado = dfs_consolidados[0]
    for df in dfs_consolidados[1:]:
        df_final_consolidado = df_final_consolidado.union(df)
    
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
# MAGIC ## Salvar Resultados em Excel

# COMMAND ----------

if 'df_final_consolidado' in locals():
    print("💾 SALVANDO RESULTADOS EM EXCEL")
    print("=" * 50)
    
    # Instalar openpyxl se necessário
    import subprocess
    try:
        import openpyxl
    except ImportError:
        print("📦 Instalando openpyxl...")
        subprocess.check_call(["pip", "install", "openpyxl"])
    
    # Converter para Pandas
    df_pandas = df_final_consolidado.toPandas()
    
    # Garantir que colunas numéricas sejam float
    colunas_numericas = [
        "demanda_diarizada_off", "demanda_diarizada_on",
        "DDV_futuro_filial_off", "DDV_futuro_filial_on",
        "DDV_final_on", "DDV_final_off", "DDV_final_total"
    ]
    
    for col in colunas_numericas:
        if col in df_pandas.columns:
            df_pandas[col] = df_pandas[col].astype(float)
    
    # Salvar em Excel
    output_path = f"/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/analysis/ddv_futuro_expandido_{VERSAO_MERECIMENTO}_{hoje_str}.xlsx"
    
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        # ABA 1: Planilha simplificada com apenas colunas essenciais
        df_simplificado = df_pandas[['CdSku', 'CdFilial', 'DDV_final_total']].copy()
        df_simplificado['Chave'] = df_simplificado['CdSku'].astype(str) + '-' + df_simplificado['CdFilial'].astype(str)
        df_simplificado = df_simplificado[['CdSku', 'CdFilial', 'Chave', 'DDV_final_total']]
        df_simplificado.to_excel(writer, sheet_name="DDV_Final", index=False)
        
        # ABA 2: Planilha completa com memorial de cálculo
        df_pandas.to_excel(writer, sheet_name="Memorial_Calculo", index=False)
    
    print(f"✅ Arquivo salvo em: {output_path}")
    print(f"📊 Total de registros salvos: {len(df_pandas):,}")
    print(f"📋 Estrutura do arquivo:")
    print(f"  • ABA 1 - 'DDV_Final': CdSku, CdFilial, Chave, DDV_final_total")
    print(f"  • ABA 2 - 'Memorial_Calculo': Todas as colunas com detalhes do cálculo")
    
else:
    print("❌ Nenhum resultado para salvar!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validações Finais

# COMMAND ----------

if 'df_final_consolidado' in locals():
    print("🔍 VALIDAÇÕES FINAIS")
    print("=" * 50)
    
    # Validação 1: Verificar se todas as categorias têm dados
    categorias_com_dados = df_final_consolidado.select("categoria").distinct().collect()
    categorias_esperadas = list(CATEGORIAS_CONFIG.keys())
    
    print(f"📊 Validação 1 - Categorias com dados:")
    for row in categorias_com_dados:
        print(f"  ✅ {row['categoria']}")
    
    categorias_faltando = set(categorias_esperadas) - {row['categoria'] for row in categorias_com_dados}
    if categorias_faltando:
        print(f"  ❌ Categorias faltando: {categorias_faltando}")
    
    # Validação 2: Verificar se DDV final é positivo
    ddv_negativos = df_final_consolidado.filter(F.col("DDV_final_total") < 0).count()
    print(f"\n📊 Validação 2 - DDV negativos: {ddv_negativos}")
    
    # Validação 3: Verificar se proporções estão corretas
    print(f"\n📊 Validação 3 - Verificação de proporções:")
    for categoria in categorias_esperadas:
        if categoria in [row['categoria'] for row in categorias_com_dados]:
            config = CATEGORIAS_CONFIG[categoria]
            df_cat = df_final_consolidado.filter(F.col("categoria") == categoria)
            
            # Calcular proporção real
            soma_on = df_cat.agg(F.sum("DDV_final_on")).collect()[0][0]
            soma_off = df_cat.agg(F.sum("DDV_final_off")).collect()[0][0]
            soma_total = soma_on + soma_off
            
            if soma_total > 0:
                prop_on_real = soma_on / soma_total
                prop_off_real = soma_off / soma_total
                
                print(f"  • {categoria}:")
                print(f"    - Proporção ON esperada: {config['proporcao_on']:.1%}")
                print(f"    - Proporção ON real: {prop_on_real:.1%}")
                print(f"    - Proporção OFF esperada: {config['proporcao_off']:.1%}")
                print(f"    - Proporção OFF real: {prop_off_real:.1%}")
    
    print(f"\n✅ Validações concluídas!")

# COMMAND ----------
