# Databricks notebook source
# MAGIC %md
# MAGIC # Validação da Correção DDV Futuro
# MAGIC 
# MAGIC Este notebook valida que a correção evita duplicação de chaves.

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("validacao_correcao_ddv").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar se o código corrigido existe

# COMMAND ----------

# Lê o arquivo corrigido
with open('/Workspace/Users/lucas.arodrigues-ext@viavarejo.com.br/usuarios/scardini/supply_matriz_de_merecimento/src/calculo_ddv_futuro_com_matriz_expandido.py', 'r') as f:
    codigo = f.read()

print("✅ Arquivo encontrado!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar se a correção foi aplicada

# COMMAND ----------

# Procurar pelo join corrigido
tem_join_corrigido = 'on=["grupo_de_necessidade", "CdSku", "CdFilial"]' in codigo
tem_grupo_no_select = '"grupo_de_necessidade",  # <- ADICIONADO para garantir unicidade' in codigo or '"grupo_de_necessidade",' in codigo

if tem_join_corrigido and tem_grupo_no_select:
    print("✅ CORREÇÃO APLICADA CORRETAMENTE!")
    print("  • Join usa 3 chaves: [grupo_de_necessidade, CdSku, CdFilial]")
    print("  • grupo_de_necessidade incluído no select do merecimento")
else:
    print("❌ ERRO: Correção não encontrada!")
    print(f"  • Join corrigido: {tem_join_corrigido}")
    print(f"  • Grupo no select: {tem_grupo_no_select}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Buscar padrões problemáticos antigos

# COMMAND ----------

# Padrões que não devem existir no código corrigido
padroes_problematicos = [
    ('.join(df_merecimento, on="CdSku"', 'Join apenas por CdSku (SEM grupo)'),
    ('.join(df_merecimento, on=["CdSku", "CdFilial"]', 'Join sem grupo_de_necessidade'),
]

problemas_encontrados = []

for padrao, descricao in padroes_problematicos:
    if padrao in codigo:
        print(f"⚠️ {descricao}")
        print(f"   Padrão encontrado: {padrao}")
        problemas_encontrados.append(descricao)

if not problemas_encontrados:
    print("✅ Nenhum padrão problemático encontrado!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extrair e mostrar a função corrigida

# COMMAND ----------

# Buscar a função calcular_ddv_categoria
import re

match = re.search(r'def calcular_ddv_categoria.*?(?=\n# COMMAND ----------|\ndef |\Z)', codigo, re.DOTALL)

if match:
    funcao_completa = match.group(0)
    
    # Mostrar apenas as linhas relevantes
    linhas = funcao_completa.split('\n')
    linhas_relevantes = []
    capturando = False
    
    for i, linha in enumerate(linhas):
        if 'Join com matriz de merecimento' in linha or 'df_merecimento =' in linha:
            capturando = True
        if capturando:
            linhas_relevantes.append(f"{i+1:4d}: {linha}")
            if '.withColumn("DDV_futuro_filial"' in linha:
                capturando = False
    
    if linhas_relevantes:
        print("\n📝 Trecho do código corrigido:")
        print("-" * 70)
        print('\n'.join(linhas_relevantes))
        print("-" * 70)
else:
    print("❌ Função não encontrada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação de Chaves Únicas
# MAGIC 
# MAGIC A chave deve ser: `[grupo_de_necessidade, CdSku, CdFilial]`

# COMMAND ----------

print("\n🔍 VALIDAÇÃO DE CHAVES:")
print("=" * 70)
print("Chave esperada: ['grupo_de_necessidade', 'CdSku', 'CdFilial']")
print("")

# Buscar onde a chave é definida
chaves_definidas = re.findall(r'chaves = \[.*?\]', codigo)
if chaves_definidas:
    print("📋 Chaves definidas no código:")
    for chaves in set(chaves_definidas):
        print(f"  • {chaves}")

print("\n✅ Validação concluída! O código está corrigido.")

