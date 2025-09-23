# 📊 Sistema de Cálculo da Matriz de Merecimento

## 🎯 Visão Geral

Esta pasta contém o **núcleo do sistema de cálculo da matriz de merecimento**, implementando uma arquitetura unificada que processa dados de vendas históricas para calcular alocações otimizadas de produtos por loja, considerando diferentes categorias de produtos e suas especificidades.

## 🏗️ Arquitetura do Sistema

### 🔄 **Fluxo Principal de Cálculo**

```
1. 📋 PREPARAÇÃO DE DADOS
   ↓
2. 🔄 ESPELHAMENTO DE FILIAIS
   ↓
3. 🔍 DETECÇÃO DE OUTLIERS
   ↓
4. 📊 CÁLCULO DE MEDIDAS CENTRAIS
   ↓
5. 🏢 MEREIMENTO A NÍVEL CD
   ↓
6. 🏪 MEREIMENTO INTERNO AO CD
   ↓
7. 🎯 MEREIMENTO FINAL
```

### 📁 **Componentes Principais**

#### **🔄 Cálculo de Merecimento**
- **`calculo_matriz_de_merecimento_unificado.py`**: Sistema principal OFFLINE
- **`calculo_matriz_de_merecimento_online.py`**: Sistema principal ONLINE

#### **🔧 Preparação de Dados**
- **`Preparacao_tabelas_Matriz_merecimento.py`**: Preparação OFFLINE
- **`Preparacao_tabelas_Matriz_merecimento_online.py`**: Preparação ONLINE

#### **💾 Exportação**
- **`Salvar_matrizes_calculadas_csv.py`**: Exportação de resultados

## 🔗 Interconexões no Cálculo da Matriz

### 1️⃣ **Preparação de Dados** → **Cálculo de Merecimento**

```python
# Preparação cria tabelas base
df_base = carregar_dados_base()  # Dados históricos de vendas
df_mapeamentos = carregar_mapeamentos_produtos()  # SKU → Grupo de Necessidade

# Cálculo utiliza tabelas preparadas
df_com_mapeamentos = aplicar_mapeamentos_produtos(df_base, df_mapeamentos)
```

### 2️⃣ **Detecção de Outliers** → **Medidas Centrais**

```python
# Outliers são detectados por grupo de necessidade
outliers_meses = detectar_outliers_meses_atipicos(df_com_mapeamentos)

# Medidas são calculadas SEM os outliers detectados
df_sem_outliers = filtrar_meses_atipicos(df_com_mapeamentos, outliers_meses)
medidas = calcular_medidas_centrais_com_medias_aparadas(df_sem_outliers)
```

### 3️⃣ **Medidas Centrais** → **Merecimento em Duas Camadas**

```python
# Primeira camada: Merecimento a nível CD
merecimento_cd = calcular_merecimento_cd(medidas)

# Segunda camada: Participação interna ao CD
participacao_interna = calcular_merecimento_interno_cd(medidas)

# Cálculo final: CD × Participação Interna
merecimento_final = calcular_merecimento_final(merecimento_cd, participacao_interna)
```

### 4️⃣ **Espelhamento de Filiais** → **Dados Base**

```python
# Carrega de-para de espelhamento do Excel
df_espelhamento = carregar_de_para_espelhamento()

# Aplica espelhamento nos dados base
df_base_com_espelhamento = aplicar_espelhamento_filiais(df_base, df_espelhamento)
```

### 5️⃣ **Sistema Offline** ↔ **Sistema Online**

```python
# OFFLINE: Processamento completo para análises
df_offline = executar_calculo_matriz_merecimento(
    categoria="DIRETORIA DE TELAS",
    modo="offline"  # Processamento completo
)

# ONLINE: Processamento incremental para operações
df_online = executar_calculo_matriz_merecimento(
    categoria="DIRETORIA DE TELAS", 
    modo="online"   # Processamento incremental
)
```

## ⚙️ Constantes Estabelecidas e Motivos

### 🎯 **Regras de Agrupamento por Categoria**

```python
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
    "DIRETORIA LINHA BRANCA": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial",
        "descricao": "Agrupamento por espécie gerencial + voltagem"
    },
    "DIRETORIA LINHA LEVE": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial", 
        "descricao": "Agrupamento por espécie gerencial + voltagem"
    },
    "DIRETORIA INFO/GAMES": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial",
        "descricao": "Agrupamento por espécie gerencial"
    }
}
```

**🎯 Motivos das Regras**:
- **TELAS/TELEFONIA**: Produtos similares (gêmeos) têm padrões de demanda correlacionados
- **LINHA BRANCA/LEVE**: Voltagem é fator crítico para distribuição geográfica
- **INFO/GAMES**: Espécie gerencial é suficiente para agrupamento eficaz

### 📊 **Parâmetros de Detecção de Outliers**

```python
PARAMETROS_OUTLIERS = {
    "sigma_meses_atipicos": {
        "DIRETORIA DE TELAS": 2.0,           # Mais sensível (produtos sazonais)
        "DIRETORIA TELEFONIA CELULAR": 2.0,  # Mais sensível (lançamentos frequentes)
        "DIRETORIA LINHA BRANCA": 3.0,       # Menos sensível (demanda estável)
        "DIRETORIA LINHA LEVE": 3.0,         # Menos sensível (demanda estável)
        "DIRETORIA INFO/GAMES": 3.0          # Menos sensível (demanda estável)
    },
    "sigma_outliers_cd": 2.0,      # Sensibilidade para outliers a nível CD
    "sigma_outliers_loja": 2.5,    # Sensibilidade para outliers a nível loja
    "sigma_atacado_cd": 1.5,      # Sensibilidade para vendas atacado CD
    "sigma_atacado_loja": 1.5     # Sensibilidade para vendas atacado loja
}
```

**🎯 Motivos dos Parâmetros**:
- **Sigma 2.0**: Para categorias com alta volatilidade (lançamentos, sazonalidade)
- **Sigma 3.0**: Para categorias com demanda mais estável e previsível
- **Sigma 1.5**: Para vendas atacado (mais sensível a outliers por volume)

### 📅 **Janelas Móveis de Cálculo**

```python
JANELAS_MOVEIS = [90, 180, 270, 360]  # dias
```

**🎯 Motivos das Janelas**:
- **90 dias**: Tendência de curto prazo (3 meses)
- **180 dias**: Tendência de médio prazo (6 meses)
- **270 dias**: Tendência de longo prazo (9 meses)
- **360 dias**: Tendência anual (12 meses)

### 🏢 **Filiais Outlet**

```python
FILIAIS_OUTLET = [2528, 3604]
```

**🎯 Motivo**: Filiais outlet têm padrões de demanda diferentes e devem ser tratadas separadamente.

### 📊 **Tipos de Medidas Calculadas**

```python
TIPOS_MEDIDAS = [
    "Media",      # Média tradicional (sensível a outliers)
    "Mediana",    # Mediana (robusta a outliers)
    "MediaAparada" # Média aparada (equilibra robustez e informação)
]
```

**🎯 Motivos dos Tipos**:
- **Média**: Captura tendências gerais, mas sensível a outliers
- **Mediana**: Robusta a outliers, mas pode perder informação
- **Média Aparada**: Equilibra robustez e informação (remove extremos)

### 🔄 **Espelhamento de Filiais**

```python
# Arquivo de configuração
ARQUIVO_ESPELHAMENTO = "/mnt/datalake/governanca_supply_inputs_matriz_merecimento.xlsx"
ABA_ESPELHAMENTO = "espelhamento_lojas"

# Colunas do de-para
COLUNAS_ESPELHAMENTO = {
    "referencia": "CdFilial_referência",
    "espelhada": "CdFilial_espelhada"
}
```

**🎯 Motivo do Espelhamento**:
- **Filiais Novas**: Não possuem histórico de vendas suficiente
- **Espelhamento**: Copia dados de uma filial de referência similar
- **Aplicação**: Antes do cálculo de merecimento para incluir demanda estimada
- **Fonte**: Arquivo Excel de governança para flexibilidade de configuração

## 🔄 Modos de Operação

### 📊 **Modo OFFLINE**
- **Uso**: Análises históricas, validações, testes
- **Características**:
  - Processamento completo de dados históricos
  - Validação de qualidade e consistência
  - Geração de relatórios detalhados
  - Análise de tendências e sazonalidade

### ⚡ **Modo ONLINE**
- **Uso**: Operações diárias, atualizações incrementais
- **Características**:
  - Processamento incremental de dados
  - Atualização rápida da matriz operacional
  - Otimizado para performance
  - Integração com sistemas de produção

## 🎯 Resultado Final

O sistema produz uma **matriz de merecimento** com:

### 📋 **Estrutura do Resultado**
- **Granularidade**: SKU × Loja × Grupo de Necessidade
- **Medidas**: 12 tipos de merecimento (4 janelas × 3 tipos de medida)
- **Camadas**: Merecimento CD + Participação Interna + Merecimento Final

### 📊 **Colunas de Saída**
```python
# Identificação
['cdfilial', 'cd_primario', 'grupo_de_necessidade']

# Merecimento CD (4 janelas × 3 tipos)
['Total_CD_Media90_Qt_venda_sem_ruptura', ...]

# Participação Interna (4 janelas × 3 tipos)  
['Percentual_Media90_Qt_venda_sem_ruptura', ...]

# Merecimento Final (4 janelas × 3 tipos)
['Merecimento_Final_Media90_Qt_venda_sem_ruptura', ...]
```

## 🚀 Como Usar

### **Execução Básica**
```python
# Para qualquer categoria
df_resultado = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")
```

### **Execução com Parâmetros**
```python
df_resultado = executar_calculo_matriz_merecimento(
    categoria="DIRETORIA DE TELAS",
    data_inicio="2024-01-01",
    data_calculo="2025-06-30",
    sigma_meses_atipicos=2.0
)
```

## 🔧 Dependências

- **PySpark**: Processamento distribuído de dados
- **Pandas**: Manipulação de dados
- **Datetime**: Manipulação de datas
- **Databricks**: Plataforma de execução

## 📚 Próximos Passos

1. **Integração**: Conectar com sistemas de produção
2. **Automação**: Implementar agendamento automático
3. **Monitoramento**: Adicionar alertas de qualidade
4. **Otimização**: Melhorar performance para grandes volumes

---

**Versão**: 1.1.0  
**Última Atualização**: Janeiro 2025  
**Mantenedor**: Equipe de Supply Chain Analytics
