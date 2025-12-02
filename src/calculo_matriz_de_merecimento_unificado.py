# Databricks notebook source
# MAGIC %md
# MAGIC # Cálculo da Matriz de Merecimento - Solução Unificada para Todas as Categorias
# MAGIC
# MAGIC Este notebook implementa o cálculo da matriz de merecimento unificado para todas as categorias,
# MAGIC com abstração `grupo_de_necessidade` e implementação de médias aparadas.
# MAGIC
# MAGIC **Objetivo**: Calcular a matriz de merecimento otimizada em duas camadas:
# MAGIC 1. **Primeira camada**: Matriz a nível CD (grupo_de_necessidade)
# MAGIC 2. **Segunda camada**: Distribuição interna ao CD para as lojas
# MAGIC
# MAGIC **Regras de Agrupamento por Categoria**:
# MAGIC - **DIRETORIA DE TELAS**: Usa `gemeos` como grupo_de_necessidade
# MAGIC - **DIRETORIA TELEFONIA CELULAR**: Usa `gemeos` como grupo_de_necessidade  
# MAGIC - **DIRETORIA LINHA BRANCA**: Usa `NmEspecieGerencial + "_" + DsVoltagem` como grupo_de_necessidade (DsVoltagem nulls preenchidos com "")
# MAGIC - **DIRETORIA LINHA LEVE**: Usa `NmEspecieGerencial + "_" + DsVoltagem` como grupo_de_necessidade (DsVoltagem nulls preenchidos com "")
# MAGIC - **DIRETORIA INFO/GAMES**: Usa `NmEspecieGerencial` como grupo_de_necessidade

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports e Configuração Inicial

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, Window
from datetime import datetime, timedelta, date
import pandas as pd
from typing import List, Optional, Dict, Any

%pip install openpyxl

# Inicialização do Spark
# ✅ OTIMIZAÇÃO: Configurar número padrão de partições baseado em cores
spark = (
    SparkSession.builder
    .appName("calculo_matriz_merecimento_unificado")
    .config("spark.sql.shuffle.partitions", "200")  # Será sobrescrito dinamicamente
    .getOrCreate()
)

# ✅ OTIMIZAÇÃO: Ajustar número padrão de partições do Spark dinamicamente
# Isso evita que o Spark use 200 partições hardcoded em operações automáticas
def configurar_particoes_padrao_spark():
    """
    Configura o número padrão de partições do Spark para usar o valor ideal calculado.
    Isso evita que o Spark use 200 partições hardcoded em operações automáticas.
    """
    try:
        # Calcular número ideal de partições
        num_cores = spark.sparkContext.defaultParallelism
        num_cores = min(num_cores, 24)  # Limitar a 24 cores
        num_particoes_ideal = max(24, int(num_cores * 1.5))
        
        # Configurar spark.sql.shuffle.partitions
        spark.conf.set("spark.sql.shuffle.partitions", str(num_particoes_ideal))
        
        print(f"✅ Configuração Spark: spark.sql.shuffle.partitions = {num_particoes_ideal}")
        print(f"   ℹ️  Isso evita uso de 200 partições hardcoded pelo Spark")
    except Exception as e:
        print(f"⚠️ Erro ao configurar partições padrão do Spark: {e}")

# Configurar antes de calcular NUM_PARTICOES_IDEAL
configurar_particoes_padrao_spark()

# ✅ OTIMIZAÇÃO: Calcular número ideal de partições baseado no número de cores
# Nota: spark.sql.shuffle.partitions já foi configurado acima
def calcular_num_particoes_ideal(multiplier: float = 1.5, max_cores: int = 24) -> int:
    """
    Calcula o número ideal de partições baseado no número de cores disponíveis.
    
    Melhores práticas:
    - Número de partições = 1.5x o número de cores (padrão: 1.5x)
    - Máximo de 24 cores conforme especificação do cluster
    - Evita muitas partições pequenas (overhead) ou poucas partições grandes (gargalo)
    - Ideal: 36 partições para 24 cores (1.5x) - balanceamento otimizado
    
    Args:
        multiplier: Multiplicador para número de cores (padrão: 1.5)
        max_cores: Número máximo de cores a considerar (padrão: 24)
        
    Returns:
        Número ideal de partições
    """
    try:
        # Obter número de cores do Spark Context
        num_cores = spark.sparkContext.defaultParallelism
        # Limitar ao máximo especificado
        num_cores = min(num_cores, max_cores)
        # Calcular partições ideais (mínimo 24 para evitar partições muito pequenas)
        # Usar 1.5x para evitar partições excessivas e overhead
        num_particoes = max(24, int(num_cores * multiplier))
        print(f"📊 Configuração de particionamento: {num_cores} cores × {multiplier} = {num_particoes} partições ideais")
        print(f"   ℹ️  Otimizado para evitar partições pequenas por local (50+ partições)")
        return num_particoes
    except Exception as e:
        print(f"⚠️ Erro ao calcular número de partições, usando padrão: {e}")
        # Fallback: usar 36 partições (1.5x 24 cores)
        return 36

# ✅ OTIMIZAÇÃO: Função auxiliar para coalesce inteligente
def coalesce_inteligente(df: DataFrame, max_particoes: int = None) -> DataFrame:
    """
    Aplica coalesce de forma inteligente, reduzindo partições pequenas.
    
    Args:
        df: DataFrame a ser otimizado
        max_particoes: Número máximo de partições desejado (padrão: NUM_PARTICOES_IDEAL)
        
    Returns:
        DataFrame com número otimizado de partições
    """
    if max_particoes is None:
        max_particoes = NUM_PARTICOES_IDEAL
    
    num_particoes_atual = df.rdd.getNumPartitions()
    
    if num_particoes_atual > max_particoes:
        print(f"  🔧 Coalesce: {num_particoes_atual} → {max_particoes} partições")
        return df.coalesce(max_particoes)
    else:
        return df

NUM_PARTICOES_IDEAL = calcular_num_particoes_ideal(multiplier=1.5, max_cores=24)

hoje = datetime.now() - timedelta(days=1)
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_int = int(hoje.strftime("%Y%m%d"))

FILIAIS_OUTLET = [2528, 3604]

# Flag para escolher fonte do de-para
USAR_DE_PARA_EXCEL = True  # True = Excel, False = CSV antigo

def criar_tabela_de_para_grupo_necessidade_direto(hoje: datetime, usar_excel: bool = True) -> int:
    """
    Lê diretamente do arquivo de de-para, trata os dados e salva na tabela.
    
    Args:
        hoje: Data/hora atual para timestamp
        usar_excel: True para Excel, False para CSV antigo
        
    Returns:
        int: Número de registros salvos
    """
    
    try:
        if usar_excel:
            # Carregar do Excel
            print("📁 Carregando de-para do Excel (de_para_gemeos_tecnologia.xlsx)...")
            de_para_df = pd.read_excel(
                "/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/de_para_gemeos_tecnologia.xlsx",
                sheet_name="de_para"
            )
        else:
            # Carregar do CSV antigo
            print("📁 Carregando de-para do CSV antigo (ITENS_GEMEOS 2.csv)...")
            de_para_df = pd.read_csv(
                "/dbfs/mnt/datalake/bcg_comum/ITENS_GEMEOS 2.csv",
                sep=";",
                encoding="utf-8"
            )
        
        print(f"  ✅ Arquivo carregado: {len(de_para_df)} registros")
        
        # Padronizar nomes das colunas
        de_para_df.columns = (
            de_para_df.columns
            .str.lower()
            .str.replace(r"[^\w]+", "_", regex=True)
            .str.strip("_")
        )
        
        # Mapear colunas para o formato esperado
        if 'sku' in de_para_df.columns and 'gemeos' in de_para_df.columns:
            de_para_df = de_para_df.rename(columns={'sku': 'CdSku', 'gemeos': 'grupo_de_necessidade'})
        elif 'cdsku' in de_para_df.columns and 'gemeos' in de_para_df.columns:
            de_para_df = de_para_df.rename(columns={'cdsku': 'CdSku', 'gemeos': 'grupo_de_necessidade'})
        elif 'sku' in de_para_df.columns and 'gemeos' in de_para_df.columns:
            de_para_df = de_para_df.rename(columns={'sku': 'CdSku', 'gemeos': 'grupo_de_necessidade'})
        else:
            raise ValueError(f"Colunas não encontradas. Disponíveis: {list(de_para_df.columns)}")
        
        # Garantir que CdSku seja string
        de_para_df['CdSku'] = de_para_df['CdSku'].astype(str)
        
        # Remover duplicatas e valores nulos
        de_para_df = de_para_df.dropna(subset=['CdSku', 'grupo_de_necessidade'])
        de_para_df = de_para_df.drop_duplicates(subset=['CdSku'])
        
        # Adicionar timestamp
        de_para_df['DtAtualizacao'] = hoje
        
        # Converter para Spark DataFrame
        df_spark = spark.createDataFrame(de_para_df)
        
        # Salvar tabela em modo overwrite
        df_spark.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia")
        
        count_registros = len(de_para_df)
        print(f"✅ Tabela supply_de_para_modelos_gemeos_tecnologia atualizada com {count_registros} registros")
        
        return count_registros
        
    except Exception as e:
        print(f"❌ Erro ao criar tabela de de-para: {str(e)}")
        raise

# COMMAND ----------

def carregar_de_para_gemeos_tecnologia(flag_excel=True) -> pd.DataFrame:
    """
    Carrega o de-para de gêmeos tecnologia baseado no flag USAR_DE_PARA_EXCEL.
    
    Returns:
        DataFrame com colunas: CdSku, gemeos
    """
    if flag_excel:
        print("📋 Carregando de-para do Excel (de_para_gemeos_tecnologia.xlsx)...")
        try:
            de_para_df = pd.read_excel(
                '/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/de_para_gemeos_tecnologia.xlsx',
                sheet_name='de_para'
            )
            
            # Padronizar nomes das colunas
            de_para_df.columns = (
                de_para_df.columns
                .str.strip()
                .str.lower()
                .str.replace(r"[^\w]+", "_", regex=True)
                .str.strip("_")
            )
            
            # Mapear colunas para o formato esperado
            if 'sku' in de_para_df.columns and 'gemeos' in de_para_df.columns:
                de_para_df = de_para_df.rename(columns={'sku': 'CdSku'})
            elif 'cdsku' in de_para_df.columns and 'gemeos' in de_para_df.columns:
                de_para_df = de_para_df.rename(columns={'cdsku': 'CdSku'})
            elif 'SKU' in de_para_df.columns and 'gemeos' in de_para_df.columns:
                de_para_df = de_para_df.rename(columns={'SKU': 'CdSku'})
            else:
                raise ValueError(f"Colunas não encontradas. Disponíveis: {list(de_para_df.columns)}")
            
            # Garantir que CdSku seja string
            de_para_df['CdSku'] = de_para_df['CdSku'].astype(str)
            
            # Remover duplicatas
            de_para_df = de_para_df.drop_duplicates()
            
            print(f"  ✅ Excel carregado: {len(de_para_df):,} registros")
            print(f"  📊 Colunas: {list(de_para_df.columns)}")
            
            return de_para_df
            
        except Exception as e:
            print(f"  ❌ Erro ao carregar Excel: {e}")
            print("  🔄 Tentando CSV como fallback...")
            USAR_DE_PARA_EXCEL = False
    
    if not USAR_DE_PARA_EXCEL:
        print("📋 Carregando de-para do CSV (ITENS_GEMEOS 2.csv)...")
        try:
            de_para_df = pd.read_csv(
                'dados_analise/ITENS_GEMEOS 2.csv',
                delimiter=";",
                encoding='iso-8859-1'
            )
            
            # Padronizar nomes das colunas
            de_para_df.columns = (
                de_para_df.columns
                .str.strip()
                .str.lower()
                .str.replace(r"[^\w]+", "_", regex=True)
                .str.strip("_")
            )
            
            # Remover duplicatas
            de_para_df = de_para_df.drop_duplicates()
            
            print(f"  ✅ CSV carregado: {len(de_para_df):,} registros")
            print(f"  📊 Colunas: {list(de_para_df.columns)}")
            
            return de_para_df
            
        except Exception as e:
            print(f"  ❌ Erro ao carregar CSV: {e}")
            raise ValueError("Não foi possível carregar o de-para de nenhuma fonte")

# ✅ DE-PARA CONSOLIDAÇÃO DE CDs - MESMO DO ONLINE
DE_PARA_CONSOLIDACAO_CDS = {
  "14"  : "1401",
  "1635": "1200",
  "1500": "1200",
  "1640": "1401",
  "1088": "1200",
  "4760": "1760",
  "4400": "1400",
  "4887": "1887",
  "4475": "1475",
  "4445": "1445",
  "2200": "1200",
  "1736": "1887",
  "1792": "1887",
  "1875": "1887",
  "1999": "1887",
  "1887": "1887",
  "1200": "1200",
  "1400": "1400",
  "1401": "1401",
  "1445": "1445",
  "1475": "1475",
  "1760": "1760"
}

data_m_menos_1 = hoje - timedelta(days=30)
data_m_menos_1 = data_m_menos_1.strftime("%Y-%m-%d")

# ✅ PARAMETRIZAÇÃO: Widgets do Databricks para configuração
# Remover widgets existentes se houver (evita erros ao recriar)
try:
    dbutils.widgets.remove("data_calculo")
    dbutils.widgets.remove("sufixo_tabela")
    dbutils.widgets.remove("diretorias")
except:
    pass

dbutils.widgets.text("data_calculo", "2025-11-30", "📅 Data de Cálculo (YYYY-MM-DD)")
dbutils.widgets.text("sufixo_tabela", "teste0112", "🏷️ Sufixo da Tabela (ex: teste0112)")

# Widget multiselect: name, defaultValue (string separada por vírgulas), choices (lista), label
# Nota: defaultValue deve ser uma string com valores separados por vírgulas que estão em choices
dbutils.widgets.multiselect(
    "diretorias",
    "DIRETORIA DE LINHA BRANCA,DIRETORIA INFO/PERIFERICOS",  # Valores padrão como string separada por vírgulas
    ["DIRETORIA DE TELAS", "DIRETORIA TELEFONIA CELULAR", "DIRETORIA DE LINHA BRANCA", "DIRETORIA LINHA LEVE", "DIRETORIA INFO/PERIFERICOS"],  # Opções disponíveis
    "📋 Diretorias para Processar"
)

# Obter valores dos widgets
DATA_CALCULO = dbutils.widgets.get("data_calculo")
SUFIXO_TABELA = dbutils.widgets.get("sufixo_tabela")
DIRETORIAS_SELECIONADAS = dbutils.widgets.get("diretorias").split(",") if dbutils.widgets.get("diretorias") else []

# Validar data de cálculo
try:
    datetime.strptime(DATA_CALCULO, "%Y-%m-%d")
    print(f"✅ Data de cálculo configurada: {DATA_CALCULO}")
except ValueError:
    print(f"⚠️ Data inválida '{DATA_CALCULO}', usando data padrão")
    DATA_CALCULO = (hoje - timedelta(days=1)).strftime("%Y-%m-%d")

# Validar diretorias selecionadas
if not DIRETORIAS_SELECIONADAS:
    print("⚠️ Nenhuma diretoria selecionada, usando padrão")
    DIRETORIAS_SELECIONADAS = ["DIRETORIA DE LINHA BRANCA", "DIRETORIA INFO/PERIFERICOS"]

print(f"✅ Configurações dos widgets:")
print(f"  📅 Data de cálculo: {DATA_CALCULO}")
print(f"  🏷️ Sufixo da tabela: {SUFIXO_TABELA}")
print(f"  📋 Diretorias selecionadas: {DIRETORIAS_SELECIONADAS}")

# COMMAND ----------

def get_data_inicio(min_meses: int = 18, hoje: datetime | None = None) -> datetime:
    """
    Retorna 1º de janeiro mais recente que esteja a pelo menos `min_meses` meses de 'hoje'.
    """
    if hoje is None:
        hoje_d = date.today()
    else:
        hoje_d = hoje.date() if isinstance(hoje, datetime) else hoje

    ano = hoje_d.year
    while True:
        jan = date(ano, 1, 1)
        diff_meses = (hoje_d.year - jan.year) * 12 + (hoje_d.month - jan.month)
        if diff_meses >= min_meses:
            return datetime(jan.year, jan.month, jan.day)
        ano -= 1

data_inicio = get_data_inicio()
data_inicio_int = int(data_inicio.strftime("%Y%m%d"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuração das Regras por Categoria

# COMMAND ----------

# Configuração das regras de agrupamento por categoria
REGRAS_AGRUPAMENTO = {
    "DIRETORIA DE TELAS": {
        "coluna_grupo_necessidade": "gemeos",
        "tipo_agrupamento": "gêmeos",
        "descricao": "Agrupamento por produtos similares (gêmeos)"
    },
    "DIRETORIA TELEFONIA CELULAR": {
        "coluna_grupo_necessidade": "gemeos", 
        "tipo_agrupamento": "gêmeos",
        "descricao": "Agrupamento por produtos similares (gêmeos)"
    },
    "DIRETORIA DE LINHA BRANCA": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial",
        "descricao": "Agrupamento por espécie gerencial + voltagem (DsVoltagem)"
    },
    "DIRETORIA LINHA LEVE": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial", 
        "descricao": "Agrupamento por espécie gerencial + voltagem (DsVoltagem)"
    },
    "DIRETORIA INFO/PERIFERICOS": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial",
        "descricao": "Agrupamento por espécie gerencial"
    }
}

# Configuração de parâmetros para detecção de outliers
PARAMETROS_OUTLIERS = {
    "desvios_meses_atipicos": 2,  # Desvios para meses atípicos
    "desvios_historico_cd": 3,     # Desvios para outliers históricos a nível CD
    "desvios_historico_loja": 3,   # Desvios para outliers históricos a nível loja
    "desvios_atacado_cd": 3,     # Desvios para outliers CD em lojas de atacado
    "desvios_atacado_loja": 1.5    # Desvios para outliers loja em lojas de atacado
}

# Configuração das janelas móveis para médias aparadas
JANELAS_MOVEIS_APARADAS = [90, 180, 270, 360]

# Configuração específica para merecimento CD (sempre 90 dias)
JANELA_CD_MERECIMENTO = 90

# Configuração das médias aparadas (percentual de corte)
PERCENTUAL_CORTE_MEDIAS_APARADAS = 0.01  # 1% de corte superior e inferior

# Parâmetros de amortização de demanda
PERCENTUAL_MAX_DEMANDA_SUPRIMIDA = 0.30  # 30% do QtMercadoria

FILIAIS_ATACADO = [
    1671,     # Petrolina - PE
    17,       # Norte Shopping
    1778,     # Shop Tacaruna - PE
    293,      # Varginha - MG
    1003,     # Shop Guarulhos - SP
    1949,     # São Mateus - ES
    1717,     # Fortaleza - CE
    2383,     # Pacajus - CE
    1634,     # Shop Pantanal - MT
    590,      # Contagem - MG
    1485,     # Sorocaba - SP
    2103,     # Caruaru 2 - PE
    2059,     # Arcoverde - PE
    520,      # Shop Bangu - RJ
    4000,     # Berrini - SP
    1157,     # Feira de Santana - BA
    1764,     # Shop Moxuara - ES
    1158,     # Catete - RJ
    376,      # Ponte Nova - MG
    242,      # Montes Claros - MG
    2038,     # Caruarua - PE
    1639,     # Vitoria - ES
    445,      # Ubá - MG
    1679,     # Fortaleza 2 - CE
    1697,     # Mossoró - RN
    39,       # Copacabana 4 - RJ
    1811,     # Shop da Ilha - MA
    461,      # Barra Shopping - RJ
]

print("✅ Configurações carregadas:")
print(f"  • Categorias suportadas: {list(REGRAS_AGRUPAMENTO.keys())}")
print(f"  • Janelas móveis aparadas: {JANELAS_MOVEIS_APARADAS} dias")
print(f"  • Janela CD merecimento: {JANELA_CD_MERECIMENTO} dias")
print(f"  • Percentual de corte para médias aparadas: {PERCENTUAL_CORTE_MEDIAS_APARADAS*100}% (total 2%)")
print(f"  • Percentual máximo demanda suprimida: {PERCENTUAL_MAX_DEMANDA_SUPRIMIDA*100:.0f}%")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funções Essenciais para Cálculo

# COMMAND ----------

def determinar_grupo_necessidade(categoria: str, df: DataFrame) -> DataFrame:
    """
    Determina o grupo de necessidade baseado na categoria e aplica a regra correspondente.
    """
    if categoria not in REGRAS_AGRUPAMENTO:
        raise ValueError(f"Categoria '{categoria}' não suportada. Categorias válidas: {list(REGRAS_AGRUPAMENTO.keys())}")
    
    regra = REGRAS_AGRUPAMENTO[categoria]
    coluna_origem = regra["coluna_grupo_necessidade"]

    colunas_df = df.columns
    if coluna_origem not in colunas_df:
        raise ValueError(f"Coluna '{coluna_origem}' não encontrada no DataFrame. Colunas disponíveis: {colunas_df}")
    
    # Verifica se é LINHA BRANCA ou LINHA LEVE para aplicar agrupamento especial
    if categoria in ["DIRETORIA DE LINHA BRANCA", "DIRETORIA LINHA LEVE"]:
        # Verifica se DsVoltagem existe no DataFrame
        if "DsVoltagem" not in colunas_df:
            raise ValueError(f"Coluna 'DsVoltagem' não encontrada no DataFrame para categoria '{categoria}'. Colunas disponíveis: {colunas_df}")
        
        # Cria grupo de necessidade combinando NmEspecieGerencial + "_" + DsVoltagem (nulls preenchidos com "")
        df_com_grupo = df.withColumn(
            "DsVoltagem_filled",
            F.substring(F.coalesce(F.col("DsVoltagem"), F.lit("")), 1, 3)
        ).withColumn(
            "grupo_de_necessidade",
            F.concat(
                F.coalesce(F.col(coluna_origem), F.lit("SEM_GN")),
                F.lit("_"),
                F.col("DsVoltagem_filled")
            )
        ).withColumn(
            "tipo_agrupamento",
            F.lit(regra["tipo_agrupamento"])
        ).drop("DsVoltagem_filled")
        
        print(f"✅ Grupo de necessidade definido para '{categoria}' (com DsVoltagem):")
        print(f"  • Coluna origem: {coluna_origem} + DsVoltagem")
        print(f"  • Valores copiados: {df_com_grupo.select('grupo_de_necessidade').distinct().count()} grupos únicos")
        
        # Mostrar amostra dos grupos de necessidade
        grupos_amostra = df_com_grupo.select("grupo_de_necessidade").distinct().limit(10).collect()
        grupos_lista = [row.grupo_de_necessidade for row in grupos_amostra]
        print(f"  • Amostra dos grupos: {grupos_lista}")
        
    else:
        # Para outras categorias, mantém o comportamento original
        df_com_grupo = df.withColumn(
            "grupo_de_necessidade",
            F.coalesce(F.col(coluna_origem), F.lit("SEM_GN"))
        ).withColumn(
            "tipo_agrupamento",
            F.lit(regra["tipo_agrupamento"])
        )
        
        print(f"✅ Grupo de necessidade definido para '{categoria}':")
        print(f"  • Coluna origem: {coluna_origem}")
        print(f"  • Valores copiados: {df_com_grupo.select('grupo_de_necessidade').distinct().count()} grupos únicos")
        
        # Mostrar amostra dos grupos de necessidade
        grupos_amostra = df_com_grupo.select("grupo_de_necessidade").distinct().limit(10).collect()
        grupos_lista = [row.grupo_de_necessidade for row in grupos_amostra]
        print(f"  • Amostra dos grupos: {grupos_lista}")
    
    return df_com_grupo

# COMMAND ----------

def consolidar_telas_especiais_em_tv_esp(df: DataFrame, categoria: str) -> DataFrame:
    """
    Consolida todos os grupos de necessidade de telas especiais em um único grupo "TV ESP".
    
    Agrupa gêmeos que contenham tecnologias especiais:
    - ESP (Especial)
    - QLED
    - MINI LED
    - QNED
    - OLED
    
    Aplica apenas para categoria "DIRETORIA DE TELAS".
    
    Args:
        df: DataFrame com coluna grupo_de_necessidade já definida
        categoria: Categoria sendo processada
        
    Returns:
        DataFrame com grupos de necessidade consolidados
    """
    if categoria != "DIRETORIA DE TELAS":
        print(f"ℹ️  Consolidação de telas especiais não aplicada para categoria: {categoria}")
        return df
    
    print("🔄 Consolidando telas especiais em grupo único 'TV ESP'...")
    
    # Lista de tecnologias especiais a serem identificadas (case-insensitive)
    tecnologias_especiais = ["ESP", "QLED", "MINI LED", "QNED", "OLED"]
    
    # Criar condição para identificar grupos que contenham qualquer tecnologia especial
    # Usar upper() para case-insensitive
    condicao_tela_especial = F.lit(False)
    for tecnologia in tecnologias_especiais:
        condicao_tela_especial = condicao_tela_especial | (
            F.upper(F.col("grupo_de_necessidade")).contains(F.upper(F.lit(tecnologia)))
        )
    
    # Contar grupos antes da consolidação
    grupos_antes = df.select("grupo_de_necessidade").distinct().count()
    grupos_especiais = (
        df
        .filter(condicao_tela_especial)
        .select("grupo_de_necessidade")
        .distinct()
        .count()
    )
    
    print(f"  📊 Grupos antes da consolidação: {grupos_antes}")
    print(f"  📊 Grupos especiais identificados: {grupos_especiais}")
    
    # Aplicar consolidação: substituir grupos especiais por "TV ESP"
    df_consolidado = df.withColumn(
        "grupo_de_necessidade",
        F.when(
            condicao_tela_especial,
            F.lit("TV ESP")
        ).otherwise(F.col("grupo_de_necessidade"))
    )
    
    # Contar grupos depois da consolidação
    grupos_depois = df_consolidado.select("grupo_de_necessidade").distinct().count()
    
    # Mostrar exemplos de grupos consolidados
    grupos_consolidados = (
        df
        .filter(condicao_tela_especial)
        .select("grupo_de_necessidade")
        .distinct()
        .limit(10)
        .collect()
    )
    grupos_lista = [row.grupo_de_necessidade for row in grupos_consolidados]
    
    print(f"  ✅ Grupos após consolidação: {grupos_depois}")
    print(f"  📉 Redução: {grupos_antes - grupos_depois} grupos consolidados em 'TV ESP'")
    print(f"  📋 Exemplos de grupos consolidados: {grupos_lista}")
    
    return df_consolidado

# COMMAND ----------

def carregar_dados_base(categoria: str, data_inicio: str = "2024-07-01") -> DataFrame:
    """
    Carrega os dados base para a categoria especificada.
    """
    print(f"🔄 Carregando dados para categoria: {categoria}")
    
    df_base = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
        .filter(F.col("NmAgrupamentoDiretoriaSetor") == categoria)
        .filter(F.col("DtAtual") >= data_inicio)
        .withColumn(
            "year_month",
            F.date_format(F.col("DtAtual"), "yyyyMM").cast("int")
        )
        .fillna(0, subset=["Receita", "QtMercadoria", "TeveVenda", "deltaRuptura"])
    )
    
    print(f"✅ Dados carregados para '{categoria}':")
    print(f"  • Total de registros: {df_base.count():,}")
    
    return df_base

# COMMAND ----------

def carregar_de_para_espelhamento() -> DataFrame:
    """
    Carrega o de-para de espelhamento de filiais do arquivo Excel.
    
    Returns:
        DataFrame com colunas: CdFilial_referencia, CdFilial_espelhada
    """
    print("🔄 Carregando de-para de espelhamento de filiais...")

    # pip install openpyxl
    
    try:
        # Carrega o arquivo Excel usando pandas
        df_pandas = pd.read_excel(
            "/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/planilha_governanca/governanca_supply_inputs_matriz_merecimento.xlsx",
            sheet_name="espelhamento_lojas"
        )
        
        # Verifica se o DataFrame não está vazio
        if df_pandas.empty:
            print("ℹ️ Aba 'espelhamento_lojas' está vazia")
            return spark.createDataFrame([], "CdFilial_referencia INT, CdFilial_espelhada INT")
        
        # Renomeia as colunas para padronizar
        df_pandas = df_pandas.rename(columns={
            "CdFilial_referência": "CdFilial_referencia",
            "CdFilial_espelhada": "CdFilial_espelhada"
        })
        
        # Remove linhas com valores nulos
        df_pandas = df_pandas.dropna(subset=["CdFilial_referencia", "CdFilial_espelhada"])
        
        # Converte para DataFrame do Spark
        df_espelhamento = spark.createDataFrame(df_pandas)
        
        print(f"✅ De-para de espelhamento carregado:")
        print(f"  • Total de mapeamentos: {df_espelhamento.count():,}")
        
        if df_espelhamento.count() > 0:
            print("  • Exemplos de espelhamento:")
            df_espelhamento.show(5, truncate=False)
        
        return df_espelhamento
        
    except FileNotFoundError:
        print("⚠️ Arquivo 'governanca_supply_inputs_matriz_merecimento.xlsx' não encontrado")
        print("  • Continuando sem espelhamento...")
        return spark.createDataFrame([], "CdFilial_referencia INT, CdFilial_espelhada INT")
        
    except Exception as e:
        print(f"⚠️ Erro ao carregar de-para de espelhamento: {str(e)}")
        print("  • Continuando sem espelhamento...")
        return spark.createDataFrame([], "CdFilial_referencia INT, CdFilial_espelhada INT")

# COMMAND ----------

def aplicar_espelhamento_filiais(df_base: DataFrame, df_espelhamento: DataFrame) -> DataFrame:
    """
    Aplica o espelhamento de filiais nos dados base.
    
    Para cada filial espelhada, remove os dados existentes e substitui pelos dados 
    da filial de referência.
    
    Args:
        df_base: DataFrame com dados base
        df_espelhamento: DataFrame com de-para de espelhamento
        
    Returns:
        DataFrame com dados espelhados aplicados
    """
    if df_espelhamento.count() == 0:
        print("ℹ️ Nenhum espelhamento para aplicar")
        return df_base
    
    print("🔄 Aplicando espelhamento de filiais...")
    
    # Contar registros antes do espelhamento
    registros_antes = df_base.count()
    
    # Obter lista de filiais que serão espelhadas
    filiais_espelhadas = [row.CdFilial_espelhada for row in df_espelhamento.select("CdFilial_espelhada").distinct().collect()]
    
    print(f"  • Filiais que serão espelhadas: {filiais_espelhadas}")
    
    # Remover dados existentes das filiais que serão espelhadas
    df_sem_espelhadas = df_base.filter(~F.col("CdFilial").isin(filiais_espelhadas))
    
    registros_removidos = registros_antes - df_sem_espelhadas.count()
    print(f"  • Registros removidos das filiais espelhadas: {registros_removidos:,}")
    
    # ✅ OTIMIZAÇÃO: Repartition ambos DataFrames nas chaves de join
    df_base_opt = df_base.repartition(NUM_PARTICOES_IDEAL, "CdFilial")
    df_espelhamento_opt = df_espelhamento.repartition(NUM_PARTICOES_IDEAL, "CdFilial_referencia")
    
    # Criar dados espelhados (copiando da filial de referência)
    df_espelhados = (
        df_base_opt
        .join(
            df_espelhamento_opt,
            df_base_opt.CdFilial == df_espelhamento_opt.CdFilial_referencia,
            "inner"
        )
        .select(
            df_espelhamento.CdFilial_espelhada.alias("CdFilial"),
            *[col for col in df_base.columns if col != "CdFilial"]
        )
    )
    
    # Unir dados sem as filiais espelhadas com os novos dados espelhados
    df_com_espelhamento = df_sem_espelhadas.union(df_espelhados)
    
    # Contar registros após espelhamento
    registros_depois = df_com_espelhamento.count()
    registros_espelhados = df_espelhados.count()
    
    print(f"✅ Espelhamento aplicado:")
    print(f"  • Registros antes: {registros_antes:,}")
    print(f"  • Registros removidos: {registros_removidos:,}")
    print(f"  • Registros espelhados adicionados: {registros_espelhados:,}")
    print(f"  • Registros após: {registros_depois:,}")
    
    # Mostrar exemplos de filiais espelhadas
    if registros_espelhados > 0:
        print("  • Exemplos de filiais espelhadas:")
        df_espelhados.select("CdFilial").distinct().show(5, truncate=False)
    
    return df_com_espelhamento

# COMMAND ----------

def carregar_mapeamentos_produtos(categoria: str) -> tuple:
    """
    Carrega os arquivos de mapeamento de produtos para a categoria específica.
    """
    print("🔄 Carregando mapeamentos de produtos...")
    
    de_para_modelos_tecnologia = (
        pd.read_csv('/Workspace/Users/daniel.scardini-ext@viavarejo.com.br/supply/supply_matriz_de_merecimento/src/dados_analise/MODELOS_AJUSTE (1).csv', 
                    delimiter=';')
        .drop_duplicates()
    )
    
    de_para_modelos_tecnologia.columns = (
        de_para_modelos_tecnologia.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
        .str.strip("_")
    )
    
    try:
        # Usar a nova função para carregar de-para
        de_para_gemeos_tecnologia = carregar_de_para_gemeos_tecnologia()
        
        print("✅ Mapeamento de gêmeos carregado")
    except FileNotFoundError:
        print("⚠️  Arquivo de mapeamento de gêmeos não encontrado")
        de_para_gemeos_tecnologia = None
    
    return (
        de_para_modelos_tecnologia.rename(columns={"codigo_item": "CdSku"})[['CdSku', 'modelos']], 
        de_para_gemeos_tecnologia[['CdSku', 'gemeos']] if de_para_gemeos_tecnologia is not None else None
    )

# COMMAND ----------

def aplicar_mapeamentos_produtos(df: DataFrame, categoria: str, 
                                de_para_modelos: pd.DataFrame, 
                                de_para_gemeos: pd.DataFrame = None) -> DataFrame:
    """
    Aplica os mapeamentos de produtos ao DataFrame base.
    """
    print(f"🔄 Aplicando mapeamentos para categoria: {categoria}")
    
    df_modelos_spark = spark.createDataFrame(de_para_modelos)
    
    # ✅ OTIMIZAÇÃO: Repartition ambos DataFrames nas chaves de join
    df_opt = df.repartition(NUM_PARTICOES_IDEAL, "CdSku")
    df_modelos_spark_opt = df_modelos_spark.repartition(NUM_PARTICOES_IDEAL, "CdSku")
    
    df_com_modelos = df_opt.join(
        df_modelos_spark_opt,
        on="CdSku",
        how="left"
    )
    
    if (de_para_gemeos is not None and 
        REGRAS_AGRUPAMENTO[categoria]["coluna_grupo_necessidade"] == "gemeos"):
        
        df_gemeos_spark = spark.createDataFrame(de_para_gemeos)
        # ✅ OTIMIZAÇÃO: Repartition para o segundo join também
        df_com_modelos_opt = df_com_modelos.repartition(NUM_PARTICOES_IDEAL, "CdSku")
        df_gemeos_spark_opt = df_gemeos_spark.repartition(NUM_PARTICOES_IDEAL, "CdSku")
        
        df_com_mapeamentos = df_com_modelos_opt.join(
            df_gemeos_spark_opt,
            on="CdSku",
            how="left"
        )
        df_com_mapeamentos = (
            df_com_mapeamentos
            .withColumn("gemeos",
                        F.when(F.col("gemeos") == '-', F.col("modelos"))
                        .otherwise(F.col("gemeos"))
            )
        )
        print("✅ Mapeamento de gêmeos aplicado")

        print("ℹ️  Mapeamento de gêmeos aplicado: ", df_com_mapeamentos.count())       

    else:
        df_com_mapeamentos = df_com_modelos
        print("ℹ️  Mapeamento de gêmeos não aplicado")
    
    return df_com_mapeamentos

# COMMAND ----------

def remover_outliers_series_historicas(df: DataFrame, 
                                     coluna_valor: str = "QtMercadoria",
                                     n_sigmas_padrao: float = 3.0,
                                     n_sigmas_atacado: float = 1.5,
                                     filiais_atacado: list = None) -> DataFrame:
    """
    Remove outliers das séries históricas (grupo de necessidade x filial) usando dois métodos:
    
    1. Método padrão: n desvios padrão (3 sigmas por padrão)
    2. Método atacado: n desvios padrão específico para filiais na watchlist de atacado
    
    Os outliers são saturados para exatamente o threshold (média + n*sigmas).
    
    Args:
        df: DataFrame com dados históricos
        coluna_valor: Nome da coluna com os valores a serem tratados
        n_sigmas_padrao: Número de desvios padrão para método padrão
        n_sigmas_atacado: Número de desvios padrão para filiais de atacado
        filiais_atacado: Lista de filiais consideradas de atacado
        
    Returns:
        DataFrame com outliers removidos (saturados no threshold)
    """
    print(f"🔄 Removendo outliers das séries históricas...")
    print(f"  • Coluna valor: {coluna_valor}")
    print(f"  • N sigmas padrão: {n_sigmas_padrao}")
    print(f"  • N sigmas atacado: {n_sigmas_atacado}")
    print(f"  • Filiais atacado: {filiais_atacado if filiais_atacado else 'Não definidas'}")
    
    # Se não há filiais de atacado definidas, usa método padrão para todas
    if not filiais_atacado:
        filiais_atacado = []
    
    # Janela para calcular estatísticas por grupo_de_necessidade x filial
    w_grupo_filial = Window.partitionBy("grupo_de_necessidade", "CdFilial")
    
    # Calcular estatísticas por grupo_de_necessidade x filial
    df_com_stats = (
        df
        .withColumn("media_grupo_filial", F.avg(F.col(coluna_valor)).over(w_grupo_filial))
        .withColumn("desvio_grupo_filial", F.stddev(F.col(coluna_valor)).over(w_grupo_filial))
        .withColumn("is_atacado", F.col("CdFilial").isin(filiais_atacado))
    )
    
    # Calcular thresholds baseado no tipo de filial
    df_com_thresholds = (
        df_com_stats
        .withColumn(
            "n_sigmas_aplicado",
            F.when(F.col("is_atacado"), F.lit(n_sigmas_atacado))
            .otherwise(F.lit(n_sigmas_padrao))
        )
        .withColumn(
            "threshold_superior",
            F.col("media_grupo_filial") + (F.col("n_sigmas_aplicado") * F.col("desvio_grupo_filial"))
        )
        .withColumn(
            "threshold_inferior",
            F.greatest(
                F.col("media_grupo_filial") - (F.col("n_sigmas_aplicado") * F.col("desvio_grupo_filial")),
                F.lit(0)  # Não permite valores negativos
            )
        )
    )
    
    # Aplicar saturação dos outliers
    df_sem_outliers = (
        df_com_thresholds
        .withColumn(
            f"{coluna_valor}_original",
            F.col(coluna_valor)
        )
        .withColumn(
            coluna_valor,
            F.when(
                F.col(coluna_valor) > F.col("threshold_superior"),
                F.col("threshold_superior")
            )
            .when(
                F.col(coluna_valor) < F.col("threshold_inferior"),
                F.col("threshold_inferior")
            )
            .otherwise(F.col(f"{coluna_valor}_original"))
        )
        .withColumn(
            "flag_outlier_removido",
            F.when(
                (F.col(f"{coluna_valor}_original") != F.col(coluna_valor)),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .drop(
            "media_grupo_filial", "desvio_grupo_filial", "is_atacado", 
            "n_sigmas_aplicado", "threshold_superior", "threshold_inferior"
        )
    )
    
    # Calcular estatísticas de remoção
    total_registros = df_sem_outliers.count()
    outliers_removidos = (
        df_sem_outliers
        .filter(F.col("flag_outlier_removido") == 1)
        .count()
    )
    
    print(f"✅ Outliers removidos: {outliers_removidos:,} de {total_registros:,} registros ({outliers_removidos/total_registros*100:.2f}%)")
    
    return df_sem_outliers

# COMMAND ----------

def detectar_outliers_meses_atipicos(df: DataFrame, categoria: str) -> tuple:
    """
    Detecta outliers e meses atípicos baseado no grupo_de_necessidade.
    """
    print(f"🔄 Detectando outliers para categoria: {categoria}")
    
    df_stats_por_grupo_mes = (
        df.groupBy("grupo_de_necessidade", "year_month")
        .agg(
            F.sum("QtMercadoria").alias("QtMercadoria_total"),
            F.count("*").alias("total_registros")
        )
    )
    
    w_stats_grupo = Window.partitionBy("grupo_de_necessidade")
    
    df_stats_grupo = (
        df_stats_por_grupo_mes
        .withColumn(
            "media_qt_mercadoria",
            F.avg("QtMercadoria_total").over(w_stats_grupo)
        )
        .withColumn(
            "desvio_padrao_qt_mercadoria",
            F.stddev("QtMercadoria_total").over(w_stats_grupo)
        )
        .withColumn(
            "limite_superior_nsigma",
            F.col("media_qt_mercadoria") + (F.lit(PARAMETROS_OUTLIERS["desvios_meses_atipicos"]) * F.col("desvio_padrao_qt_mercadoria"))
        )
        .withColumn(
            "limite_inferior_nsigma",
            F.greatest(
                F.col("media_qt_mercadoria") - (F.lit(PARAMETROS_OUTLIERS["desvios_meses_atipicos"]) * F.col("desvio_padrao_qt_mercadoria")),
                F.lit(0)
            )
        )
        .withColumn(
            "flag_mes_atipico",
            F.when(
                (F.col("QtMercadoria_total") > F.col("limite_superior_nsigma")) |
                (F.col("QtMercadoria_total") < F.col("limite_inferior_nsigma")),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )
    
    df_meses_atipicos = (
        df_stats_grupo
        .filter(F.col("flag_mes_atipico") == 1)
        .select("grupo_de_necessidade", "year_month")
    )
    
    print(f"✅ Detecção de outliers concluída: {df_meses_atipicos.count()} meses atípicos")
    
    return df_stats_grupo, df_meses_atipicos

# COMMAND ----------

def filtrar_meses_atipicos(df: DataFrame, df_meses_atipicos: DataFrame, data_calculo: str = None) -> DataFrame:
    """
    Filtra os meses atípicos do DataFrame principal.
    PROTEGE a data_calculo de ser removida.
    """
    print("🔄 Aplicando filtro de meses atípicos...")
    
    # Se data_calculo foi fornecida, remover ela da lista de meses atípicos
    if data_calculo:
        year_month_calculo = int(data_calculo.replace("-", "")[:6])  # "2025-09-25" -> 202509
        print(f"🛡️ Protegendo year_month {year_month_calculo} da remoção (DATA_CALCULO)")
        
        df_meses_atipicos_filtrado = df_meses_atipicos.filter(
            F.col("year_month") != year_month_calculo
        )
        
        meses_antes = df_meses_atipicos.count()
        meses_depois = df_meses_atipicos_filtrado.count()
        print(f"  • Meses atípicos: {meses_antes} → {meses_depois} (removido {meses_antes - meses_depois} mês protegido)")
    else:
        df_meses_atipicos_filtrado = df_meses_atipicos
    
    # ✅ OTIMIZAÇÃO: Repartition ambos DataFrames nas chaves de join
    df_opt = df.repartition(NUM_PARTICOES_IDEAL, "grupo_de_necessidade", "year_month")
    df_meses_atipicos_opt = df_meses_atipicos_filtrado.withColumn("flag_remover", F.lit(1)).repartition(NUM_PARTICOES_IDEAL, "grupo_de_necessidade", "year_month")
    
    df_filtrado = (
        df_opt
        .join(
            df_meses_atipicos_opt,
            on=["grupo_de_necessidade", "year_month"],
            how="left"
        )
        .filter(F.col("flag_remover").isNull())
        .drop("flag_remover")
    )
    
    registros_antes = df.count()
    registros_depois = df_filtrado.count()
    
    print(f"✅ Filtro aplicado: {registros_antes:,} → {registros_depois:,} registros")
    
    return df_filtrado

# COMMAND ----------

def add_media_aparada_rolling(df, janelas, col_val="demanda_robusta", col_ord="DtAtual", 
                              grupos=("CdSku","CdFilial"), alpha=0.10, min_obs=10):
    """
    Adiciona médias aparadas rolling com proteção completa contra NULLs.
    """
    out = df
    
    # ✅ JANELA DE BACKUP 360d para casos extremos
    window_360_backup = Window.partitionBy(*grupos).orderBy(F.col(col_ord)).rowsBetween(-360, 0)
    backup_360_mean = F.avg(F.when(F.col(col_val).isNotNull(), F.col(col_val))).over(window_360_backup)
    
    for dias in janelas:
        w = Window.partitionBy(*grupos).orderBy(F.col(col_ord)).rowsBetween(-dias, 0)

        # Percentis com proteção contra janelas vazias
        ql = F.percentile_approx(F.col(col_val), F.lit(alpha)).over(w)
        qh = F.percentile_approx(F.col(col_val), F.lit(1 - alpha)).over(w)

        out = (
            out
            .withColumn(f"_ql_{dias}", F.coalesce(ql, F.lit(0)))  # ✅ Proteção percentis
            .withColumn(f"_qh_{dias}", F.coalesce(qh, F.lit(float('inf'))))  # ✅ Proteção percentis
        )

        cnt = F.count(F.col(col_val)).over(w)
        cond = (F.col(col_val) >= F.col(f"_ql_{dias}")) & (F.col(col_val) <= F.col(f"_qh_{dias}"))
        sum_trim = F.sum(F.when(cond, F.col(col_val)).otherwise(F.lit(0))).over(w)
        cnt_trim = F.sum(F.when(cond, F.lit(1)).otherwise(F.lit(0))).over(w)

        # ✅ PROTEÇÃO: Média simples com fallback
        mean_simple = F.coalesce(
            F.avg(F.col(col_val)).over(w),  # Média da janela
            backup_360_mean,  # Backup 360d
            F.lit(0)  # Último recurso
        )
        
        # ✅ PROTEÇÃO: Média aparada com fallback para média simples
        mean_trim = F.when(
            cnt_trim > 0, 
            sum_trim / cnt_trim
        ).otherwise(mean_simple)  # Fallback para média simples protegida

        # ✅ PROTEÇÃO: Lógica final com múltiplos fallbacks
        out = out.withColumn(
            f"MediaAparada{dias}_Qt_venda_sem_ruptura",
            F.when(
                cnt >= F.lit(min_obs), 
                F.coalesce(mean_trim, mean_simple, backup_360_mean, F.lit(0))
            )
            .otherwise(
                F.coalesce(mean_simple, backup_360_mean, F.lit(0))
            )
        ).drop(f"_ql_{dias}", f"_qh_{dias}")

    return out

# COMMAND ----------

def calcular_medidas_centrais_com_medias_aparadas(df: DataFrame) -> DataFrame:
    """
    Calcula medidas centrais com médias aparadas e proteção completa contra NULLs.
    """
    print("🔄 Calculando medidas centrais com médias aparadas (protegido)...")
    
    # Aplicar lógica de amortização: deltaRuptura saturado ao máximo de 30% do QtMercadoria
    df_com_amortizacao = (
        df
        .withColumn(
            "demandaSuprimida",  # deltaRuptura saturado ao máximo de 30% do QtMercadoria
            F.least(
                F.col("deltaRuptura"),
                F.col("QtMercadoria") * PERCENTUAL_MAX_DEMANDA_SUPRIMIDA
            )
        )
    )
    
    df_sem_ruptura = (
        df_com_amortizacao
        .withColumn("demanda_robusta",
                    F.col("QtMercadoria") + F.col("demandaSuprimida"))
        .withColumn("demanda_robusta",
                    F.when(
                        F.col("CdFilial").isin(FILIAIS_OUTLET), F.lit(0)
                        )
                    .otherwise(F.col("demanda_robusta"))
                    )
        # ✅ HIERARQUIA INTELIGENTE: demanda_robusta → QtMercadoria → demandaSuprimida → 0
        .withColumn("demanda_robusta", 
                    F.coalesce(
                        F.col("demanda_robusta"),  # Primeiro: demanda robusta calculada
                        F.col("QtMercadoria"),     # Segundo: apenas vendas
                        F.col("demandaSuprimida"), # Terceiro: apenas demanda suprimida
                        F.lit(0)                   # Último: zero
                    ))
    )

    lista = ", ".join(str(f) for f in FILIAIS_OUTLET)
    print(f"🏬 Zerando a demanda das filiais [{lista}] ⚠️ pois não são abastecidas via CD normalmente.")
    
    # Estatísticas da amortização
    casos_com_ruptura = df_com_amortizacao.filter(F.col("deltaRuptura") > 0).count()
    casos_amortizados = df_com_amortizacao.filter(F.col("demandaSuprimida") > 0).count()
    if casos_com_ruptura > 0:
        demanda_suprimida_total = df_com_amortizacao.agg(F.sum("demandaSuprimida")).collect()[0][0]
        print(f"🔧 Amortização aplicada: {casos_amortizados:,} casos de {casos_com_ruptura:,} com ruptura")
        print(f"📉 Demanda total suprimida: {demanda_suprimida_total:,.0f}")
    else:
        print(f"✅ Nenhum caso com ruptura encontrado - amortização não necessária")
    
     # Aplicar apenas médias aparadas
    df_com_medias_aparadas = (
        add_media_aparada_rolling(
            df_sem_ruptura,
            janelas=JANELAS_MOVEIS_APARADAS,
            col_val="demanda_robusta",
            col_ord="DtAtual",
            grupos=("grupo_de_necessidade","CdFilial"),
            alpha=PERCENTUAL_CORTE_MEDIAS_APARADAS,
            min_obs=10
        )
    )
    
    print("✅ Medidas centrais calculadas (apenas médias aparadas)")
    return df_com_medias_aparadas

# COMMAND ----------

def consolidar_medidas(df: DataFrame) -> DataFrame:
    """
    Consolida todas as medidas calculadas em uma base única.
    """
    print("🔄 Consolidando medidas (apenas médias aparadas)...")
    
    colunas_medias_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS_APARADAS]
    
    df_consolidado = (
        df.select(
            "DtAtual", "CdFilial", "grupo_de_necessidade", "year_month",
            "QtMercadoria",  "deltaRuptura", "tipo_agrupamento",
            *colunas_medias_aparadas
        )
        .fillna(0, subset=colunas_medias_aparadas)
    )
    
    print("✅ Medidas consolidadas (apenas médias aparadas)")
    return df_consolidado

# COMMAND ----------

def criar_de_para_filial_cd() -> DataFrame:
    """
    Cria o mapeamento filial → CD usando dados da tabela base com consolidação.
    Aplica a mesma lógica do online para evitar distorções.
    Usa a data mais recente disponível (max DtAtual) em vez de data exata.
    """
    print("🔄 Criando de-para filial → CD com consolidação...")
    
    # ✅ Buscar a data mais recente disponível
    max_dt_atual = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
        .select(F.max("DtAtual").alias("max_dt"))
        .collect()[0]["max_dt"]
    )
    
    print(f"✅ Data mais recente na base: {max_dt_atual}")
    
    df_base = (
        spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
        .filter(F.col("DtAtual") == max_dt_atual)
        .filter(F.col("CdSku").isNotNull())
    )

    print(f"✅ De-para filial usando registros de {max_dt_atual}")

    # ✅ NORMALIZAÇÃO: None → 0 para depois filtrar
    dict_norm = {int(k): (int(v) if v is not None else 0) for k, v in DE_PARA_CONSOLIDACAO_CDS.items()}

    # ✅ CONSTRÓI EXPRESSÃO DE MAPEAMENTO
    mapping_expr = F.create_map([F.lit(x) for kv in dict_norm.items() for x in kv])

    de_para_filial_cd = (
        df_base
        .select(
            F.col("cdfilial").alias("CdFilial"),  # ✅ Renomear para CamelCase
            "cd_secundario"
        )
        .distinct()
        .filter(F.col("CdFilial").isNotNull())
        .withColumn(
            "cd_vinculo",
            F.coalesce(F.col("cd_secundario"), F.lit("SEM_CD"))
        )
        .drop("cd_secundario")
        # ✅ APLICA SUBSTITUIÇÃO DE CONSOLIDAÇÃO
        .withColumn(
            "cd_vinculo",
            F.coalesce(mapping_expr.getItem(F.col("cd_vinculo").cast("int")), F.col("cd_vinculo"))
        )
        # ✅ FILTRA CDs ZERADOS (None no dict)
        .filter(F.col("cd_vinculo") != F.lit(0))
        .fillna("SEM_CD", subset="cd_vinculo")
    )
    
    # ✅ DIAGNÓSTICO: Verificar distribuição de CDs
    print("🔍 Diagnóstico do mapeamento CD:")
    
    # Contar filiais por CD
    distribuicao_cd = (
        de_para_filial_cd
        .groupBy("cd_vinculo")
        .agg(F.count("*").alias("qtd_filiais"))
        .orderBy(F.desc("qtd_filiais"))
    )
    
    print("  📊 Distribuição de filiais por CD:")
    for row in distribuicao_cd.collect():
        print(f"    CD {row['cd_vinculo']}: {row['qtd_filiais']} filiais")
    
    # Verificar filiais sem CD
    filiais_sem_cd = de_para_filial_cd.filter(F.col("cd_vinculo") == "SEM_CD").count()
    if filiais_sem_cd > 0:
        print(f"  ⚠️ ATENÇÃO: {filiais_sem_cd} filiais sem CD mapeado!")
    else:
        print("  ✅ Todas as filiais têm CD mapeado")
    
    print(f"✅ De-para filial → CD criado: {de_para_filial_cd.count():,} filiais")
    return de_para_filial_cd

# COMMAND ----------

def calcular_merecimento_cd(df: DataFrame, data_calculo: str, categoria: str) -> DataFrame:
    """
    Calcula o merecimento a nível CD por grupo de necessidade usando APENAS média aparada 90 dias.
    Retorna o percentual que cada CD representa dentro da Cia.
    """
    print(f"🔄 Calculando merecimento CD para categoria: {categoria} (média aparada 90 dias)")
    
    df_data_calculo = df.filter(F.col("DtAtual") == data_calculo)

    df_data_calculo = (
        df_data_calculo
        .orderBy('CdFilial', 'grupo_de_necessidade')
        .dropDuplicates(subset=['CdFilial', 'grupo_de_necessidade'])  # ✅ Manter granularidade por grupo
    )
    
    # Usar apenas média aparada 90 dias para merecimento CD
    medida_cd = f"MediaAparada{JANELA_CD_MERECIMENTO}_Qt_venda_sem_ruptura"
    
    de_para_filial_cd = criar_de_para_filial_cd()
    df_com_cd = df_data_calculo.join(de_para_filial_cd, on="CdFilial", how="left")  # ✅ CamelCase
    
    # ✅ AGREGAÇÃO COM PROTEÇÃO DUPLA:
    df_merecimento_cd = (
        df_com_cd
        .groupBy("cd_vinculo", "grupo_de_necessidade")
        .agg(
            # F.sum() já ignora NULLs, mas garantimos com coalesce
            F.sum(F.coalesce(F.col(medida_cd), F.lit(0))).alias(f"Total_{medida_cd}"),
            F.count("*").alias("qtd_filiais_cd")  # ✅ Contar registros (filial + grupo)
        )
    )
    
    # ✅ OTIMIZAÇÃO: Coalesce inteligente após groupBy para reduzir partições
    df_merecimento_cd = coalesce_inteligente(df_merecimento_cd)
    
    # Calcular percentual do CD dentro da Cia
    w_total_cia = Window.partitionBy("grupo_de_necessidade")
    
    df_merecimento_cd = df_merecimento_cd.withColumn(
        f"Total_Cia_{medida_cd}",
        F.sum(F.col(f"Total_{medida_cd}")).over(w_total_cia)
    )
    
    df_merecimento_cd = df_merecimento_cd.withColumn(
        f"Merecimento_CD_{medida_cd}",
        F.when(F.col(f"Total_Cia_{medida_cd}") > 0,
            F.col(f"Total_{medida_cd}") / F.col(f"Total_Cia_{medida_cd}"))
        .otherwise(0)
    )

    df_merecimento_cd = (
        df_merecimento_cd
        .orderBy('cd_vinculo', 'grupo_de_necessidade')
        .dropDuplicates(subset=['cd_vinculo', 'grupo_de_necessidade'])
    )
    
    # ✅ DIAGNÓSTICO FINAL: Verificar distribuição de merecimento
    print("🔍 Diagnóstico do merecimento por CD:")
    
    # Somar merecimento por CD (todos os grupos)
    merecimento_por_cd = (
        df_merecimento_cd
        .groupBy("cd_vinculo")
        .agg(
            F.sum(f"Merecimento_CD_{medida_cd}").alias("merecimento_total_cd"),
            F.count("*").alias("qtd_grupos")
        )
        .orderBy(F.desc("merecimento_total_cd"))
    )
    
    print("  📊 Merecimento total por CD:")
    for row in merecimento_por_cd.collect():
        print(f"    CD {row['cd_vinculo']}: {row['merecimento_total_cd']:.3f} ({row['qtd_grupos']} grupos)")
    
    # Verificar se soma fecha 100% por grupo
    soma_por_grupo = (
        df_merecimento_cd
        .groupBy("grupo_de_necessidade")
        .agg(F.sum(f"Merecimento_CD_{medida_cd}").alias("soma_grupo"))
        .filter(F.abs(F.col("soma_grupo") - 1.0) > 0.01)  # Tolerância de 1%
    )
    
    grupos_problema = soma_por_grupo.count()
    if grupos_problema > 0:
        print(f"  ⚠️ ATENÇÃO: {grupos_problema} grupos não somam 100%")
    else:
        print("  ✅ Todos os grupos somam 100% (tolerância 1%)")
    
    print(f"✅ Merecimento CD calculado: {df_merecimento_cd.count():,} registros (média aparada 90 dias)")
    return df_merecimento_cd


# COMMAND ----------

def calcular_merecimento_interno_cd(df: DataFrame, data_calculo: str, categoria: str) -> DataFrame:
    """
    Calcula a proporção interna de cada loja dentro do CD por grupo de necessidade.
    Usa médias aparadas de 90 a 360 dias.
    Mantém colunas: Total_<medida> e Proporcao_Interna_<medida>.
    """
    print(f"🔄 Calculando merecimento interno CD para categoria: {categoria} (médias aparadas 90-360 dias)")
    
    # Filtro pela data
    df_data_calculo = df.filter(F.col("DtAtual") == data_calculo)
    
    # Lista de medidas aparadas disponíveis
    medidas_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS_APARADAS]
    medidas = [m for m in medidas_aparadas if m in df_data_calculo.columns]
    
    print(f"  📊 Medidas disponíveis: {medidas}")
    
    # Join com de-para filial-CD
    de_para_filial_cd = criar_de_para_filial_cd()
    
    # ✅ OTIMIZAÇÃO: Repartition ambos DataFrames nas chaves de join
    df_data_calculo_opt = df_data_calculo.repartition(NUM_PARTICOES_IDEAL, "CdFilial")
    de_para_filial_cd_opt = de_para_filial_cd.repartition(NUM_PARTICOES_IDEAL, "CdFilial")
    
    df_com_cd = df_data_calculo_opt.join(de_para_filial_cd_opt, on="CdFilial", how="left")  # ✅ CamelCase
    
    # Agregar no nível filial × grupo_de_necessidade (somando os SKUs)
    aggs = [F.sum(F.coalesce(F.col(m), F.lit(0))).alias(m) for m in medidas]
    df_filial = (
        df_com_cd
        .groupBy("CdFilial", "cd_vinculo", "grupo_de_necessidade")
        .agg(*aggs)
    )
    
    # ✅ OTIMIZAÇÃO: Coalesce inteligente após groupBy
    df_filial = coalesce_inteligente(df_filial)
    
    # Janela no nível cd_vinculo × grupo_de_necessidade
    w_cd_grp = Window.partitionBy("cd_vinculo", "grupo_de_necessidade")
    df_out = df_filial
    for m in medidas:
        df_out = (
            df_out
            .withColumn(f"Total_{m}", F.sum(F.col(m)).over(w_cd_grp))
            .withColumn(
                f"Proporcao_Interna_{m}",
                F.when(F.col(f"Total_{m}") > 0, F.col(m) / F.col(f"Total_{m}")).otherwise(F.lit(0.0))
            )
        )

    print(f"✅ Merecimento interno CD calculado: {df_out.count():,} registros (médias aparadas 90-360 dias)")
    return df_out


# COMMAND ----------

def calcular_merecimento_final(df_merecimento_cd: DataFrame, 
                              df_merecimento_interno: DataFrame) -> DataFrame:
    """
    Calcula o merecimento final: Merecimento_CD × Proporcao_Interna
    Usa apenas médias aparadas de 90 a 360 dias.
    Retorna apenas CdFilial x grupo_de_necessidade com os merecimentos finais
    """
    print("🔄 Calculando merecimento final (médias aparadas 90-360 dias)...")
    
    # Medidas disponíveis (apenas médias aparadas)
    medidas_aparadas = [f"MediaAparada{dias}_Qt_venda_sem_ruptura" for dias in JANELAS_MOVEIS_APARADAS]
    
    # 1. Preparar dados do merecimento CD (cd_vinculo x grupo_de_necessidade)
    # CD usa apenas média aparada 90 dias
    medida_cd = f"MediaAparada{JANELA_CD_MERECIMENTO}_Qt_venda_sem_ruptura"
    colunas_cd = ["cd_vinculo", "grupo_de_necessidade", f"Merecimento_CD_{medida_cd}"]
    
    df_merecimento_cd_limpo = df_merecimento_cd.select(*colunas_cd)
    
    # 2. Adicionar cd_vinculo ao merecimento interno
    de_para_filial_cd = criar_de_para_filial_cd()
    df_merecimento_interno_com_cd = (
        df_merecimento_interno
        .join(de_para_filial_cd, on="CdFilial", how="left")
        .withColumn("cd_vinculo_final", F.coalesce(de_para_filial_cd["cd_vinculo"], F.lit("SEM_CD")))
        .drop("cd_vinculo")  # Remove a coluna ambígua
        .withColumnRenamed("cd_vinculo_final", "cd_vinculo")  # Renomeia para o nome final
    )
    
    # ✅ OTIMIZAÇÃO: Repartition ambos DataFrames nas chaves de join
    df_merecimento_interno_opt = (
        df_merecimento_interno_com_cd
        .orderBy("CdFilial", "cd_vinculo", "grupo_de_necessidade")
        .dropDuplicates(subset=["CdFilial", "cd_vinculo", "grupo_de_necessidade"])
        .repartition(NUM_PARTICOES_IDEAL, "cd_vinculo", "grupo_de_necessidade")
    )
    df_merecimento_cd_opt = df_merecimento_cd_limpo.repartition(NUM_PARTICOES_IDEAL, "cd_vinculo", "grupo_de_necessidade")
    
    # 3. Join entre merecimento CD e merecimento interno
    df_merecimento_final = (
        df_merecimento_interno_opt
        .join(
            df_merecimento_cd_opt,
            on=["cd_vinculo", "grupo_de_necessidade"],
            how="left"
        )
    )
    
    # ✅ OTIMIZAÇÃO: Coalesce inteligente após join
    df_merecimento_final = coalesce_inteligente(df_merecimento_final)
    
    # 4. Calcular merecimento final (multiplicação)
    # Para cada medida aparada, multiplicar pelo merecimento CD (90 dias)
    for medida in medidas_aparadas:
        if f"Proporcao_Interna_{medida}" in df_merecimento_final.columns:
            df_merecimento_final = df_merecimento_final.withColumn(
                f"Merecimento_Final_{medida}",
                F.col(f"Merecimento_CD_{medida_cd}") * F.col(f"Proporcao_Interna_{medida}")
            )
    
    # 5. Selecionar apenas colunas finais: CdFilial x grupo_de_necessidade
    colunas_finais = ["CdFilial", "grupo_de_necessidade"]
    for medida in medidas_aparadas:
        coluna_final = f"Merecimento_Final_{medida}"
        if coluna_final in df_merecimento_final.columns:
            colunas_finais.append(coluna_final)
    
    df_merecimento_final_limpo = df_merecimento_final.select(*colunas_finais)
    
    print(f"✅ Merecimento final calculado: {df_merecimento_final_limpo.count():,} registros")
    print(f"📊 Colunas finais: {colunas_finais}")
    
    # VALIDAÇÃO: Verificar se a multiplicação ainda soma 100% por grupo de necessidade
    print("🔍 Validando se a multiplicação dos dois níveis ainda soma 100%...")
    
    for medida in medidas_aparadas:
        coluna_final = f"Merecimento_Final_{medida}"
        if coluna_final in df_merecimento_final_limpo.columns:
            print(f"  Verificando medida: {medida}")
            
            # Calcular soma por grupo de necessidade
            df_validacao = (
                df_merecimento_final_limpo
                .groupBy("grupo_de_necessidade")
                .agg(F.sum(coluna_final).alias("soma_merecimento_final"))
                .withColumn("diferenca_100", F.abs(F.col("soma_merecimento_final") - 1.0))
                .orderBy(F.desc("diferenca_100"))
            )

    
    return df_merecimento_final_limpo

# COMMAND ----------

def criar_esqueleto_matriz_completa(df_com_grupo: DataFrame, data_calculo: str = "2025-09-15") -> DataFrame:
    """
    Cria esqueleto completo da matriz com cross join entre todas as filiais e SKUs.
    
    Processo:
    1. Pega todas as filiais de roteirizacaolojaativa
    2. Pega todos os SKUs que existem em df_base no dia especificado
    3. Faz cross join entre filiais e SKUs
    4. Adiciona grupo_de_necessidade para cada SKU
    5. Retorna esqueleto pronto para join com merecimento final
    
    Args:
        data_calculo: Data para buscar os SKUs (formato YYYY-MM-DD)
        
    Returns:
        DataFrame com CdFilial, CdSku, grupo_de_necessidade
    """
    print(f"🚀 Criando esqueleto da matriz completa para data: {data_calculo}")
    print("=" * 80)
    
    # 1. Carregar todas as filiais ativas
    print("📊 Passo 1: Carregando todas as filiais ativas...")
    df_filiais = (
        spark.table('data_engineering_prd.app_operacoesloja.roteirizacaolojaativa')
        .select("CdFilial", "NmRegiaoGeografica", "NmPorteLoja")
        .distinct()
        .filter(F.col("CdFilial").isNotNull())
    )
    
    filiais_count = df_filiais.count()
    print(f"  ✅ {filiais_count:,} filiais carregadas")

    df_gdn = df_com_grupo.select("CdSku", "grupo_de_necessidade").distinct()
    
    # 2. Carregar todos os SKUs que existem na data especificada
    # ✅ Buscar a data mais recente disponível na tabela
    max_dt_estoque = (
        spark.table('dev_logistica_ds.estoquegerencial')
        .select(F.max("dtatual").alias("max_dt"))
        .collect()[0]["max_dt"]
    )
    
    print(f"📊 Passo 2: Carregando SKUs existentes...")
    print(f"  • Data solicitada: {data_calculo}")
    print(f"  • Data mais recente na tabela: {max_dt_estoque}")
    print(f"  • Usando data: {max_dt_estoque}")
    
    df_skus_data = (
        spark.table('dev_logistica_ds.estoquegerencial')
        .select(
            F.col("cdfilial").cast("int").alias("CdFilial"),
            F.col("CdSku").cast("string").alias("CdSku"),
            F.col("dtatual").cast("date").alias("DtAtual"),
            F.col("DsObrigatorio").alias("DsObrigatorio"),
            F.col("Cluster_Sugestao").alias('Cluster_Sugestao')
        )
        .filter(F.col("DtAtual") == max_dt_estoque)  # ✅ Usar data mais recente
        .filter(F.col("CdSku").isNotNull())
        .filter(
            (F.col("DsObrigatorio") == 'S') | 
            (F.col("Cluster_Sugestao") == 1)
        )
        .select("CdSku")
        .distinct()
    )
    
    # ✅ OTIMIZAÇÃO: Repartition ambos DataFrames nas chaves de join
    df_skus_opt = df_skus_data.repartition(NUM_PARTICOES_IDEAL, "CdSku")
    df_gdn_opt = df_gdn.repartition(NUM_PARTICOES_IDEAL, "CdSku")
    
    df_skus_data = (
        df_skus_opt
        .join(df_gdn_opt, on="CdSku", how="inner")
        .filter(F.col("grupo_de_necessidade").isNotNull())
    )
    
    skus_count = df_skus_data.count()
    print(f"  ✅ {skus_count:,} SKUs únicos encontrados")
    
    # 3. Cross join entre filiais e SKUs
    print("📊 Passo 3: Criando cross join filiais × SKUs...")
    df_esqueleto = (
        df_filiais
        .crossJoin(df_skus_data)
    )
    
    esqueleto_count = df_esqueleto.count()
    print(f"  ✅ Cross join criado: {esqueleto_count:,} combinações (filiais × SKUs)")
    
    # 4. Adicionar informações adicionais das filiais
    print("📊 Passo 4: Adicionando informações das filiais...")
    df_esqueleto_final = df_esqueleto.select(
        "CdFilial",
        "CdSku", 
        "grupo_de_necessidade",
        "NmRegiaoGeografica",
        "NmPorteLoja"
    )


    return df_esqueleto_final


# COMMAND ----------

def garantir_integridade_dados_pre_merecimento(df: DataFrame) -> DataFrame:
    """
    Garante integridade dos dados ANTES de qualquer cálculo de merecimento.
    Preenche NULLs com média 360d da própria combinação grupo+filial.
    """
    print("🛡️ Garantindo integridade dos dados pré-merecimento...")
    
    # Identificar colunas de médias aparadas
    colunas_medias_aparadas = [col for col in df.columns 
                              if col.startswith("MediaAparada") and col.endswith("_Qt_venda_sem_ruptura")]
    
    if not colunas_medias_aparadas:
        print("  ⚠️ Nenhuma coluna de média aparada encontrada")
        return df
    
    print(f"  📊 Tratando: {colunas_medias_aparadas}")
    
    # Janela de 360 dias para backup
    window_360 = Window.partitionBy("grupo_de_necessidade", "CdFilial").orderBy("DtAtual").rowsBetween(-360, 0)
    
    df_tratado = df
    
    for coluna in colunas_medias_aparadas:
        backup_col = f"{coluna}_backup360"
        
        # Contar NULLs antes
        nulls_antes = df_tratado.filter(F.col(coluna).isNull()).count()
        
        df_tratado = (
            df_tratado
            # Calcular média 360d apenas de valores não-nulos da própria combinação
            .withColumn(
                backup_col,
                F.avg(F.when(F.col(coluna).isNotNull(), F.col(coluna))).over(window_360)
            )
            # Preencher NULL APENAS se há histórico válido da própria combinação
            .withColumn(
                coluna,
                F.when(
                    F.col(coluna).isNull() & F.col(backup_col).isNotNull(), 
                    F.col(backup_col)
                )
                .otherwise(F.col(coluna))  # Mantém NULL se não há histórico próprio
            )
            .drop(backup_col)
        )
        
        # Contar NULLs depois
        nulls_depois = df_tratado.filter(F.col(coluna).isNull()).count()
        nulls_preenchidos = nulls_antes - nulls_depois
        
        print(f"    ✅ {coluna}: {nulls_preenchidos:,} NULLs preenchidos | {nulls_depois:,} restantes")
    
    return df_tratado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Função Principal de Execução

# COMMAND ----------

def executar_calculo_matriz_merecimento_completo(categoria: str, 
                                                data_inicio: str = "2024-07-01",
                                                data_calculo: str = DATA_CALCULO) -> DataFrame:
    """
    Função principal que executa todo o fluxo da matriz de merecimento.
    """
    print(f"🚀 Iniciando cálculo da matriz de merecimento para: {categoria}")
    print("=" * 80)
    
    try:
        # 1. Carregamento dos dados base
        df_base = carregar_dados_base(categoria, data_inicio)
        df_base.cache()

        # 2. Carregamento e aplicação do espelhamento de filiais
        df_espelhamento = carregar_de_para_espelhamento()
        df_base_com_espelhamento = aplicar_espelhamento_filiais(df_base, df_espelhamento)
        df_base_com_espelhamento.cache()

        # 3. Carregamento dos mapeamentos
        de_para_modelos, de_para_gemeos = carregar_mapeamentos_produtos(categoria)  

        # 4. Aplicação dos mapeamentos
        df_com_mapeamentos = aplicar_mapeamentos_produtos(
            df_base_com_espelhamento, categoria, de_para_modelos, de_para_gemeos
        )
        
        # 5. Definição do grupo_de_necessidade
        df_com_grupo = determinar_grupo_necessidade(categoria, df_com_mapeamentos)
        
        # 5.1. Consolidação de telas especiais em "TV ESP" (apenas para DIRETORIA DE TELAS)
        df_com_grupo = consolidar_telas_especiais_em_tv_esp(df_com_grupo, categoria)
        
        df_com_grupo.cache()

        # 5.0. Criar tabela de de-para grupo de necessidade
        count_registros = criar_tabela_de_para_grupo_necessidade_direto(hoje, usar_excel=USAR_DE_PARA_EXCEL)

        # ✅ OTIMIZAÇÃO: Repartition antes de groupBy para melhor distribuição
        df_com_grupo_opt = df_com_grupo.repartition(NUM_PARTICOES_IDEAL, "grupo_de_necessidade", "CdFilial")
        
        df_agregado = (
            df_com_grupo_opt
            .groupBy("grupo_de_necessidade", "CdFilial", "DtAtual", "year_month")
            .agg(
                F.sum("QtMercadoria").alias("QtMercadoria"),
                F.sum("deltaRuptura").alias("deltaRuptura"),
                F.first("tipo_agrupamento").alias("tipo_agrupamento")
            )
        )
        
        # ✅ OTIMIZAÇÃO: Coalesce inteligente após groupBy para reduzir partições pequenas
        df_agregado = coalesce_inteligente(df_agregado)
        
        # 6. Detecção de outliers
        df_stats, df_meses_atipicos = detectar_outliers_meses_atipicos(df_agregado, categoria)
        
        # 7. Filtragem de meses atípicos
        df_filtrado = filtrar_meses_atipicos(df_agregado, df_meses_atipicos, DATA_CALCULO)
        
        # ✅ OTIMIZAÇÃO: Repartition antes de operações de Window pesadas
        df_filtrado = df_filtrado.repartition(NUM_PARTICOES_IDEAL, "grupo_de_necessidade", "CdFilial")
        
        # 8. Remoção de outliers das séries históricas
        print("=" * 80)
        print("🔄 Aplicando remoção de outliers das séries históricas...")
        
        # Definir filiais de atacado (exemplo - ajustar conforme necessário)
        filiais_atacado = FILIAIS_ATACADO  # Lista de filiais consideradas de atacado
        
        df_sem_outliers = remover_outliers_series_historicas(
            df_filtrado,
            coluna_valor="QtMercadoria",
            n_sigmas_padrao=PARAMETROS_OUTLIERS["desvios_historico_loja"],
            n_sigmas_atacado=PARAMETROS_OUTLIERS["desvios_atacado_loja"],
            filiais_atacado=filiais_atacado
        )
        
        # 9. Cálculo das medidas centrais
        df_com_medidas = calcular_medidas_centrais_com_medias_aparadas(df_sem_outliers)
        
        # ✅ OTIMIZAÇÃO: Coalesce inteligente após cálculos de Window para reduzir partições
        df_com_medidas = coalesce_inteligente(df_com_medidas)
        
        # 10. Consolidação final
        df_final = consolidar_medidas(df_com_medidas)
        
        # ✅ OTIMIZAÇÃO: Repartition antes de joins e cálculos de merecimento
        df_final = df_final.repartition(NUM_PARTICOES_IDEAL, "grupo_de_necessidade", "CdFilial")
        
        # ✅ 10.1 NOVO: Garantir integridade dos dados pré-merecimento
        df_final = garantir_integridade_dados_pre_merecimento(df_final)
        
        # 11. Cálculo de merecimento por CD e filial
        print("=" * 80)
        print("🔄 Iniciando cálculo de merecimento...")
        
        # 11.1 Merecimento a nível CD
        df_merecimento_cd = calcular_merecimento_cd(df_final, data_calculo, categoria)
        
        # 11.2 Merecimento interno ao CD
        df_merecimento_interno = calcular_merecimento_interno_cd(df_final, data_calculo, categoria)
        
        # 11.3 Merecimento final
        df_merecimento_final = calcular_merecimento_final(df_merecimento_cd, df_merecimento_interno)

        # Criar o esqueleto
        df_esqueleto = criar_esqueleto_matriz_completa(df_com_grupo, data_m_menos_1)
        
        # ✅ OTIMIZAÇÃO: Repartition esqueleto antes do join final
        df_esqueleto = df_esqueleto.repartition(NUM_PARTICOES_IDEAL, "grupo_de_necessidade", "CdFilial")

        # Primeiro, identificar todas as colunas de merecimento final
        colunas_merecimento_final = [col for col in df_merecimento_final.columns 
                                if col.startswith('Merecimento_Final_')]

        # ✅ OTIMIZAÇÃO: Preparar e repartition merecimento final antes do join
        df_merecimento_final_join = (
            df_merecimento_final
            .select('grupo_de_necessidade', 'CdFilial', *colunas_merecimento_final)
            .dropDuplicates(subset=['grupo_de_necessidade', 'CdFilial'])
            .repartition(NUM_PARTICOES_IDEAL, "grupo_de_necessidade", "CdFilial")
        )

        # Criar dicionário de fillna
        fillna_dict = {col: 0.0 for col in colunas_merecimento_final}

        df_merecimento_sku_filial = (
            df_esqueleto
            .join(
                df_merecimento_final_join, 
                on=['grupo_de_necessidade', 'CdFilial'], 
                how='left'
            )
            .fillna(fillna_dict)
        )
        
        # ✅ OTIMIZAÇÃO: Coalesce inteligente final antes de retornar
        df_merecimento_sku_filial = coalesce_inteligente(df_merecimento_sku_filial)
        
        print("=" * 80)
        print(f"✅ Cálculo da matriz de merecimento concluído para: {categoria}")
        print(f"📊 Total de registros finais: {df_merecimento_sku_filial.count():,}")
        
        return df_merecimento_sku_filial
        
    except Exception as e:
        print(f"❌ Erro durante o cálculo: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execução Final - Cálculo e Salvamento de Todas as Categorias

# COMMAND ----------

# EXECUTAR CÁLCULO DA MATRIZ DE MERECIMENTO PARA TODAS AS CATEGORIAS
print("🚀 EXECUÇÃO FINAL - Calculando matriz de merecimento para todas as categorias...")
print("=" * 80)

# ✅ PARAMETRIZAÇÃO: Usar diretorias selecionadas via widget
categorias = [d.strip() for d in DIRETORIAS_SELECIONADAS if d.strip() in REGRAS_AGRUPAMENTO.keys()]

if not categorias:
    raise ValueError(f"❌ Nenhuma diretoria válida selecionada. Diretorias disponíveis: {list(REGRAS_AGRUPAMENTO.keys())}")

print(f"📋 Processando {len(categorias)} diretorias: {categorias}")


resultados_finais = {}

for categoria in categorias:
    print(f"\n🔄 Processando: {categoria}")
    print("-" * 60)
    
    try:
        # Executa cálculo da matriz de merecimento
        df_matriz_final = executar_calculo_matriz_merecimento_completo(
            categoria=categoria,
            data_inicio="2024-07-01",
            data_calculo=DATA_CALCULO
        )
        
        # Salva em tabela específica da categoria
        categoria_normalizada = (
            categoria
            .replace("DIRETORIA ", "")
            .replace(" ", "_")
            .replace("/", "_")  # ✅ Corrigir: substituir / por _ para evitar erro SQL
            .upper()
        )
        
        nome_tabela = f"databox.bcg_comum.supply_matriz_merecimento_{categoria_normalizada}_{SUFIXO_TABELA}"

        
        print(f"💾 Salvando matriz de merecimento para: {categoria}")
        print(f"📊 Tabela: {nome_tabela}")
        
        (
            df_matriz_final
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(nome_tabela)
        )
        
        # Armazena resultado
        resultados_finais[categoria] = {
            "matriz": df_matriz_final,
            "tabela": nome_tabela,
            "status": "SUCESSO",
            "total_registros": df_matriz_final.count()
        }
        
        print(f"✅ {categoria} - Matriz calculada e salva com sucesso!")
        print(f"📊 Total de registros: {df_matriz_final.count():,}")
        
    except Exception as e:
        print(f"❌ {categoria} - Erro: {str(e)}")
        resultados_finais[categoria] = {
            "status": "ERRO",
            "erro": str(e)
        }

print("\n" + "=" * 80)
print("🎉 CÁLCULO DAS MATRIZES DE MERECIMENTO CONCLUÍDO!")
print("=" * 80)

# Exibe resumo dos resultados
print("📊 RESUMO DOS RESULTADOS:")
for categoria, resultado in resultados_finais.items():
    if resultado["status"] == "SUCESSO":
        print(f"  ✅ {categoria}: {resultado['total_registros']:,} registros → {resultado['tabela']}")
    else:
        print(f"  ❌ {categoria}: {resultado['erro']}")

print("\n" + "=" * 80)
print("🎯 SCRIPT DE CÁLCULO CONCLUÍDO!")
print("📋 Próximo passo: Executar script de análise de factual e comparações")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 RESUMO FINAL DO SCRIPT DE CÁLCULO
# MAGIC
# MAGIC ### **O que este script faz:**
# MAGIC 1. **Calcula matriz de merecimento** para todas as categorias
# MAGIC 2. **Salva em tabelas específicas** por categoria
# MAGIC 3. **PARA AQUI** - Não faz análise de factual nem comparações
# MAGIC
# MAGIC ### **Tabelas criadas:**
# MAGIC - `supply_matriz_merecimento_TELAS`
# MAGIC - `supply_matriz_merecimento_TELEFONIA_CELULAR`
# MAGIC - `supply_matriz_merecimento_LINHA_BRANCA`
# MAGIC - `supply_matriz_merecimento_LINHA_LEVE`
# MAGIC - `supply_matriz_merecimento_INFO_GAMES`
# MAGIC
# MAGIC ### **Próximo passo:**
# MAGIC Executar o script `analise_factual_comparacao_matrizes.py` para:
# MAGIC - Análise de factual
# MAGIC - Cálculo de sMAPE e WMAPE
# MAGIC - Comparação com matriz DRP geral
# MAGIC - Identificação de distorções
# MAGIC
# MAGIC **Este script está completo e finalizado!** 🎉
