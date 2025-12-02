# 📋 Análise de Parametrização - Script Salvar Matrizes CSV

## 🎯 Objetivo
Avaliar e propor alternativas para parametrizar o script `Salvar_matrizes_calculadas_csv.py` usando widgets do Databricks.

---

## 📊 Parâmetros Identificados para Parametrização

### **1. PARÂMETROS DE DATA E TEMPO**

#### 1.1 Data de Exportação
- **Atual**: `DATA_ATUAL = datetime.now()`
- **Uso**: Define pasta de saída e nome dos arquivos
- **Proposta**: Widget `text` para data customizada

#### 1.2 Data Fim (DATA FIM)
- **Atual**: `DATA_ATUAL + timedelta(days=60)` (hardcoded 60 dias)
- **Uso**: Coluna "DATA FIM" nos arquivos CSV
- **Proposta**: Widget `text` ou `dropdown` para dias (30, 60, 90, 120)


---

### **2. PARÂMETROS DE SELEÇÃO DE CATEGORIAS**

#### 2.1 Categorias/Diretoria para Processar
- **Atual**: `TABELAS_MATRIZ_MERECIMENTO` (dicionário hardcoded)
- **Uso**: Define quais diretorias serão exportadas
- **Proposta**: Widget `multiselect` (similar ao script de cálculo)
  - Opções: Todas as diretorias disponíveis
  - Padrão: Categorias atualmente ativas

#### 2.2 Tabelas de Origem (Offline/Online)
- **Atual**: Hardcoded no dicionário `TABELAS_MATRIZ_MERECIMENTO`
- **Uso**: Define tabelas de origem para cada categoria
- **Proposta**: 
  - **Opção A**: Widget `text` para sufixo de tabela (ex: `teste0112`)
  - **Opção B**: Widget `text` para tabela completa (mais flexível)
  - **Opção C**: Manter dicionário, mas usar sufixo parametrizado

---

### **3. PARÂMETROS DE CONFIGURAÇÃO DE ARQUIVO**

#### 3.1 Formato de Exportação
- **Atual**: `formato: str = "xlsx"` (hardcoded na função)
- **Uso**: Define se salva CSV ou XLSX
- **Proposta**: Widget `dropdown` com opções: `["csv", "xlsx"]`

#### 3.2 Limite de Linhas por Arquivo
- **Atual**: `MAX_LINHAS_POR_ARQUIVO = 150000`
- **Uso**: Define quando dividir em múltiplos arquivos
- **Proposta**: Widget `text` ou `dropdown` (100k, 150k, 200k, 500k)

#### 3.3 Pasta de Saída
- **Atual**: `PASTA_OUTPUT = "/Workspace/Users/.../output"`
- **Uso**: Define onde salvar os arquivos
- **Proposta**: Widget `text` para caminho customizado (com validação)

---

### **4. PARÂMETROS DE FILTROS**

#### 4.1 Filtros de Grupos de Necessidade
- **Atual**: `FILTROS_GRUPO_REMOCAO` e `FILTROS_GRUPO_SELECAO` (dicionários hardcoded)
- **Uso**: Define quais grupos incluir/excluir
- **Proposta**: 
  - **Opção A**: Manter dicionários, mas permitir ativar/desativar por categoria
  - **Opção B**: Widget `multiselect` para grupos a remover (mais complexo)
  - **Opção C**: Widget `dropdown` para modo: `["REMOÇÃO", "SELEÇÃO"]`

#### 4.2 Filtros de Produtos (Tipificação e Marcas)
- **Atual**: `FILTROS_PRODUTOS` (dicionário hardcoded por categoria)
- **Uso**: Filtra produtos por tipificação de entrega e marcas
- **Proposta**: 
  - Widget `multiselect` para tipificações permitidas
  - Widget `multiselect` para marcas excluídas
  - Widget `dropdown` para ativar/desativar filtro

---

### **5. PARÂMETROS DE VALIDAÇÃO**

#### 5.1 Exportar Excel de Validação
- **Atual**: Sempre executa `exportar_excel_validacao_todas_categorias()`
- **Uso**: Gera arquivo Excel para validação
- **Proposta**: Widget `dropdown` para `["Sim", "Não"]`

---

## 🎨 ALTERNATIVAS DE IMPLEMENTAÇÃO

### **ALTERNATIVA 1: Parametrização Mínima (Recomendada para início)**

**Widgets propostos:**
1. `data_exportacao` (text) - Data de exportação
2. `dias_data_fim` (dropdown) - Dias para DATA FIM (30, 60, 90, 120)
3. `categorias` (multiselect) - Seleção de diretorias
4. `sufixo_tabela` (text) - Sufixo das tabelas (ex: `teste0112`)
5. `formato` (dropdown) - Formato de exportação (csv, xlsx)
6. `exportar_validacao` (dropdown) - Exportar Excel de validação (Sim/Não)

**Vantagens:**
- ✅ Implementação rápida
- ✅ Cobre os casos de uso mais comuns
- ✅ Fácil de usar e entender
- ✅ Mantém configurações complexas no código

**Desvantagens:**
- ❌ Não permite alterar filtros sem editar código
- ❌ Não permite alterar limite de linhas facilmente

---

### **ALTERNATIVA 2: Parametrização Intermediária**

**Widgets propostos:**
1. Todos da Alternativa 1 +
2. `max_linhas_arquivo` (dropdown) - Limite de linhas (100k, 150k, 200k, 500k)
3. `pasta_output` (text) - Pasta de saída customizada
4. `aplicar_filtros_produtos` (dropdown) - Aplicar filtros de produtos (Sim/Não)

**Vantagens:**
- ✅ Mais flexibilidade
- ✅ Permite ajustar limites e pastas
- ✅ Permite controlar filtros de produtos

**Desvantagens:**
- ⚠️ Mais widgets para gerenciar
- ⚠️ Pode ser confuso para usuários menos técnicos

---

### **ALTERNATIVA 3: Parametrização Completa**

**Widgets propostos:**
1. Todos da Alternativa 2 +
2. `tipificacoes_permitidas` (multiselect) - Tipificações de entrega
3. `marcas_excluidas` (multiselect) - Marcas a excluir
4. `modo_filtro_grupos` (dropdown) - Modo: REMOÇÃO ou SELEÇÃO
5. `grupos_remover` (multiselect) - Grupos a remover (se modo = REMOÇÃO)
6. `grupos_selecionar` (multiselect) - Grupos a selecionar (se modo = SELEÇÃO)

**Vantagens:**
- ✅ Máxima flexibilidade
- ✅ Permite alterar qualquer configuração sem editar código
- ✅ Ideal para testes e validações

**Desvantagens:**
- ❌ Muitos widgets (pode ser confuso)
- ❌ Mais complexo de implementar
- ❌ Risco de configurações inválidas

---

## 💡 RECOMENDAÇÃO

### **Recomendação: Alternativa 1 + Extensões Seletivas**

**Implementar primeiro:**
1. ✅ **Alternativa 1** (parametrização mínima)
2. ✅ Adicionar `max_linhas_arquivo` (útil para testes)

**Adicionar depois (se necessário):**
- Widget para pasta de saída (se houver necessidade de múltiplas pastas)
- Widgets de filtros (apenas se houver necessidade frequente de alterar)

**Manter no código:**
- Filtros de grupos (mudam raramente)
- Configurações de produtos por categoria (mudam raramente)

---

## 📝 ESTRUTURA PROPOSTA (Alternativa 1 + Extensões)

```python
# ✅ PARAMETRIZAÇÃO: Widgets do Databricks
# Remover widgets existentes se houver
try:
    dbutils.widgets.removeAll()
except:
    pass

# 1. Data e Tempo
dbutils.widgets.text("data_exportacao", datetime.now().strftime("%Y-%m-%d"), "📅 Data de Exportação (YYYY-MM-DD)")
dbutils.widgets.dropdown("dias_data_fim", "60", ["30", "60", "90", "120"], "📆 Dias para DATA FIM")

# 2. Seleção de Categorias
dbutils.widgets.multiselect(
    "categorias",
    "DIRETORIA TELEFONIA CELULAR",  # Padrão
    ["DIRETORIA DE TELAS", "DIRETORIA TELEFONIA CELULAR", "DIRETORIA DE LINHA BRANCA", "DIRETORIA LINHA LEVE", "DIRETORIA INFO/PERIFERICOS"],
    "📋 Diretorias para Exportar"
)

# 3. Configuração de Tabelas
dbutils.widgets.text("sufixo_tabela_offline", "teste0112", "🏷️ Sufixo Tabela Offline")
dbutils.widgets.text("sufixo_tabela_online", "online_teste1411", "🏷️ Sufixo Tabela Online")

# 4. Formato e Limites
dbutils.widgets.dropdown("formato", "xlsx", ["csv", "xlsx"], "📄 Formato de Exportação")
dbutils.widgets.dropdown("max_linhas_arquivo", "150000", ["100000", "150000", "200000", "500000"], "📊 Máx. Linhas por Arquivo")

# 5. Validação
dbutils.widgets.dropdown("exportar_validacao", "Sim", ["Sim", "Não"], "✅ Exportar Excel de Validação")


# Obter valores
DATA_EXPORTACAO = dbutils.widgets.get("data_exportacao")
DIAS_DATA_FIM = int(dbutils.widgets.get("dias_data_fim"))
CATEGORIAS_SELECIONADAS = dbutils.widgets.get("categorias").split(",") if dbutils.widgets.get("categorias") else []
SUFIXO_TABELA_OFFLINE = dbutils.widgets.get("sufixo_tabela_offline")
SUFIXO_TABELA_ONLINE = dbutils.widgets.get("sufixo_tabela_online")
FORMATO = dbutils.widgets.get("formato")
MAX_LINHAS = int(dbutils.widgets.get("max_linhas_arquivo"))
EXPORTAR_VALIDACAO = dbutils.widgets.get("exportar_validacao") == "Sim"

# Validar e aplicar
DATA_FIM = datetime.strptime(DATA_EXPORTACAO, "%Y-%m-%d") + timedelta(days=DIAS_DATA_FIM)
DATA_FIM_INT = int(DATA_FIM.strftime("%Y%m%d"))

# Construir dicionário de tabelas dinamicamente
TABELAS_MATRIZ_MERECIMENTO = {}
for categoria in CATEGORIAS_SELECIONADAS:
    categoria_normalizada = categoria.replace("DIRETORIA ", "").replace(" ", "_").replace("/", "_").upper()
    TABELAS_MATRIZ_MERECIMENTO[categoria] = {
        "offline": f"databox.bcg_comum.supply_matriz_merecimento_{categoria_normalizada.lower()}_{SUFIXO_TABELA_OFFLINE}",
        "online": f"databox.bcg_comum.supply_matriz_merecimento_{categoria_normalizada.lower()}_{SUFIXO_TABELA_ONLINE}",
        "grupo_apelido": categoria_normalizada.lower().replace("_", "")
    }

MAX_LINHAS_POR_ARQUIVO = MAX_LINHAS

print(f"✅ Configurações dos widgets:")
print(f"  📅 Data exportação: {DATA_EXPORTACAO}")
print(f"  📆 Data fim (+{DIAS_DATA_FIM} dias): {DATA_FIM.strftime('%Y-%m-%d')} → {DATA_FIM_INT}")
print(f"  📋 Categorias: {CATEGORIAS_SELECIONADAS}")
print(f"  🏷️ Sufixo offline: {SUFIXO_TABELA_OFFLINE}")
print(f"  🏷️ Sufixo online: {SUFIXO_TABELA_ONLINE}")
print(f"  📄 Formato: {FORMATO}")
print(f"  📊 Máx. linhas: {MAX_LINHAS:,}")
print(f"  ✅ Exportar validação: {EXPORTAR_VALIDACAO}")
```

---

## 🔄 PRÓXIMOS PASSOS

1. **Revisar proposta** com o time
2. **Escolher alternativa** (recomendado: Alternativa 1 + Extensões)
3. **Implementar widgets** no script
4. **Testar** com diferentes configurações
5. **Documentar** uso dos widgets

---

## ❓ PERGUNTAS PARA DECISÃO

1. **Frequência de mudança de filtros?**
   - Se rara → manter no código
   - Se frequente → adicionar widgets

2. **Necessidade de múltiplas pastas de saída?**
   - Se sim → adicionar widget de pasta
   - Se não → manter padrão

3. **Testes frequentes com diferentes limites?**
   - Se sim → adicionar widget de limite
   - Se não → manter padrão

4. **Múltiplos usuários com configurações diferentes?**
   - Se sim → parametrização mais completa
   - Se não → parametrização mínima

