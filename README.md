# Supply Matriz de Merecimento - Sistema Unificado

## 🎯 Visão Geral

Este repositório implementa um **sistema unificado e automatizado para cálculo da matriz de merecimento SKU-loja** utilizando Databricks, substituindo o processo manual atual por uma solução robusta, governada e configurável para todas as categorias de produtos.

### 🏗️ Arquitetura do Sistema

O sistema é composto por **três componentes principais**:

1. **📊 Cálculo de Merecimento** (`src/`)
   - **Offline**: Processamento em lote para análises históricas e validações
   - **Online**: Processamento em tempo real para operações diárias

2. **🔧 Preparação de Tabelas** (`src/`)
   - **Offline**: Preparação de dados históricos e tabelas de referência
   - **Online**: Atualização incremental de dados operacionais

3. **📈 Análises e Monitoramento** (`src/analysis/`)
   - Notebooks especializados para análise de efetividade
   - Monitoramento de performance e qualidade da matriz
   - Comparações factuais entre diferentes abordagens

## Contexto do Negócio

### Processo de Abastecimento de Lojas

O abastecimento de lojas é definido por uma série de processos sequenciais:

1. **Planejamento de Demanda por Canal** (Por SKUxCanal)
   - Projeção de demanda por SKU para canais online e offline
   - Foco atual: Produtos "Sai Loja" das diretorias Tech (Telefonia, Telas, Info e Games) e Linha Leve
   - Base para distribuição posterior por loja

2. **Matriz de Merecimento** (Por SKUxLoja)
   - **Matriz Fixa**: 95% da receita - definida manualmente pelo time de abastecimento
   - **Matriz Automática (Neogrid)**: 5% da receita - cálculo automático baseado em vendas recentes
   - **Modelo de Rateio (Escassez)**: Prioriza CDs e lojas com maior risco de ruptura

3. **Previsão de Demanda na Loja** (Por SKUxLoja)
   - Demanda diária baseada em concentração de vendas
   - Verificação de parâmetros (cluster, voltagem)
   - Cálculo do envio considerando múltiplos fatores

## 🚀 Componentes do Sistema

### 📊 **Cálculo de Merecimento**

#### **🔄 Modo Offline** (`calculo_matriz_de_merecimento_unificado.py`)
- **Propósito**: Processamento em lote para análises históricas e validações
- **Uso**: Execução manual ou agendada para períodos específicos
- **Características**:
  - Processamento completo de dados históricos
  - Validação de qualidade e consistência
  - Geração de relatórios detalhados
  - Análise de tendências e sazonalidade

#### **⚡ Modo Online** (`calculo_matriz_de_merecimento_online.py`)
- **Propósito**: Processamento em tempo real para operações diárias
- **Uso**: Execução automática diária para atualização da matriz
- **Características**:
  - Processamento incremental de dados
  - Atualização rápida da matriz operacional
  - Otimizado para performance
  - Integração com sistemas de produção

### 🔧 **Preparação de Tabelas**

#### **📋 Preparação Offline** (`Preparacao_tabelas_Matriz_merecimento.py`)
- **Propósito**: Preparação de dados históricos e tabelas de referência
- **Funcionalidades**:
  - Carregamento e limpeza de dados históricos
  - Criação de tabelas de mapeamento (SKU → Grupo de Necessidade)
  - Validação de integridade dos dados
  - Preparação de dados para análises estatísticas

#### **🔄 Preparação Online** (`Preparacao_tabelas_Matriz_merecimento_online.py`)
- **Propósito**: Atualização incremental de dados operacionais
- **Funcionalidades**:
  - Atualização diária de dados de vendas
  - Sincronização com sistemas operacionais
  - Manutenção de tabelas de referência
  - Preparação de dados para cálculo online

### 📈 **Análises e Monitoramento** (`src/analysis/`)

#### **📊 Análises de Efetividade**
- **Análise de Demanda**: Avaliação da precisão da matriz de merecimento
- **Comparação Factual**: Comparação entre matriz antiga e nova
- **Análise de Elasticidade**: Avaliação de impacto de eventos e promoções

#### **🔍 Monitoramento**
- **Dashboard de Estoque**: Monitoramento de estoque por CD e loja
- **Métricas de Performance**: Cálculo de métricas de qualidade (wMAPE, SE, Cross Entropy)
- **Alertas de Anomalias**: Detecção automática de problemas na matriz

## Sistema Unificado de Merecimento

### 🚀 **Arquitetura Unificada para Todas as Categorias**

O novo sistema unifica o cálculo de merecimento para **todas as diretorias** através de uma arquitetura flexível e configurável:

#### **📊 Categorias Suportadas**
- **DIRETORIA DE TELAS** → Agrupamento por gêmeos (produtos similares)
- **DIRETORIA TELEFONIA CELULAR** → Agrupamento por gêmeos (produtos similares)
- **DIRETORIA LINHA BRANCA** → Agrupamento por espécie gerencial
- **DIRETORIA LINHA LEVE** → Agrupamento por espécie gerencial
- **DIRETORIA INFO/GAMES** → Agrupamento por espécie gerencial

#### **🎯 Abstração de Grupo de Necessidade**
O sistema implementa uma **abstração inteligente** que se adapta automaticamente a cada categoria:

```python
REGRAS_AGRUPAMENTO = {
    "DIRETORIA DE TELAS": {
        "coluna_grupo_necessidade": "gemeos",
        "tipo_agrupamento": "gêmeos",
        "descricao": "Agrupamento por produtos similares (gêmeos)"
    },
    "DIRETORIA LINHA BRANCA": {
        "coluna_grupo_necessidade": "NmEspecieGerencial",
        "tipo_agrupamento": "espécie_gerencial",
        "descricao": "Agrupamento por espécie gerencial"
    }
    # ... outras categorias
}
```

#### **⚙️ Parâmetros Configuráveis**
- **Sigma para outliers**: Configurável por categoria (padrão: 3.0σ)
- **Data de cálculo**: Configurável (padrão: 2025-06-30)
- **Janelas móveis**: 90, 180, 270, 360 dias
- **Tipos de medida**: Médias, medianas e médias aparadas

### 🔄 **Fluxo de Execução Unificado**

```
1. Carregamento de dados base (sem grupo_de_necessidade)
2. Carregamento de mapeamentos de produtos
3. Aplicação de mapeamentos (joins com tabelas de referência)
4. Definição de grupo_de_necessidade (APÓS os mapeamentos)
5. Detecção de outliers usando grupo_de_necessidade
6. Filtragem de meses atípicos
7. Cálculo de medidas centrais (médias, medianas, aparadas)
8. Consolidação final
9. Cálculo de merecimento CD (por grupo de necessidade)
10. Cálculo de merecimento interno CD (percentual filial)
11. Cálculo de merecimento final (CD × Interno CD)
```

### 📈 **Cálculo de Merecimento em Duas Camadas**

#### **🏗️ Primeira Camada: Merecimento a Nível CD**
- **Agregação**: Por CD + grupo_de_necessidade
- **Base**: Medidas calculadas (médias, medianas, médias aparadas)
- **Resultado**: Total de demanda por CD para cada grupo de necessidade

#### **🏪 Segunda Camada: Merecimento Interno ao CD**
- **Agregação**: Por filial dentro de cada CD + grupo_de_necessidade
- **Base**: Mesma medida usada na primeira camada
- **Resultado**: Percentual de participação de cada loja dentro do CD

#### **🎯 Cálculo Final**
```
Merecimento Final = Merecimento CD × Percentual Interno CD
```

### 🔧 **Funcionalidades Técnicas**

#### **📊 Medidas Calculadas**
- **Médias móveis normais**: 90, 180, 270, 360 dias
- **Medianas móveis**: 90, 180, 270, 360 dias (robustas a outliers)
- **Médias móveis aparadas**: 90, 180, 270, 360 dias (equilibra robustez e informação)

#### **🚨 Detecção de Outliers**
- **Meses atípicos**: Identificação automática por grupo_de_necessidade
- **Parâmetros sigma configuráveis**: Diferentes níveis de sensibilidade por categoria
- **Filtragem inteligente**: Remove apenas meses atípicos identificados

#### **🔄 Mapeamento Filial → CD**
- **De-para automático**: Criado a partir da tabela base
- **Distinct**: cdfilial + cd_primario
- **Join inteligente**: Evita referências circulares

## Problemas do Método Atual

### Processos Manuais por Categoria

O método atual envolve **4 grandes etapas manuais** que apresentam diversas limitações:

1. **Agrupamento de Produtos**
   - Filtragem por grupo de necessidade (gêmeos)
   - ⚠️ **Problema**: Processo demorado, time evita atualizações sem demanda clara

2. **Dados Históricos de Vendas**
   - Remoção de meses com demanda anormal (eventos)
   - ⚠️ **Problema**: Racional robusto, mas execução muito sujeita a erros

3. **Cálculo de Demanda**
   - Média das médias móveis 3M, 6M, 9M, 12M
   - ⚠️ **Problema**: "Falsa robustez analítica", pouco responsivo a tendências

4. **Divisão entre Lojas**
   - Percentual da demanda em cada loja vs. total da empresa
   - ⚠️ **Problema**: Demandas pontuais atribuem percentual a lojas que quase nunca vendem

## Solução Proposta

### Ferramenta Automatizada no Databricks

A nova ferramenta traz **maior robustez e melhor governança** através de:

#### 🔧 **Unificação do Racional de Cálculo**
- Padronização para todas as categorias
- Alinhamento com cada área através de mapeamento SKU x Grupo de Necessidade
- Governança para atualizações e revisões periódicas

#### ⏰ **Automação e Agendamento**
- Rodadas de cálculo automáticas agendadas
- Atualizações periódicas garantidas
- **Benefício**: Desonera a equipe e evita erros de execução

#### 📊 **Gestão Flexível de Grupos de Necessidade**
- Planilha para input manual de SKU x grupo de necessidade
- Flexibilidade para diferentes categorias de produtos

#### 🔍 **Checagem de Anomalias**
- Geração de alertas estruturados para distorções relevantes
- Pós-processamento dos resultados com validações
- Tratamento robusto de outliers e vendas B2B

#### 🛠️ **Sustentação e Suporte Internos**
- Suporte dedicado ao time de supply
- Infraestrutura interna para manutenção e evolução

## Benefícios Esperados

### Robustez Analítica
- Métodos estatísticos robustos a outliers
- Cálculo de demanda robusto a rupturas
- Uso de demanda média quando havia estoque disponível

### Governança
- Processos estruturados e documentados
- Verificações automatizadas de anomalias
- Rotina estabelecida de cálculo e revisão

### Eficiência Operacional
- Eliminação de processos manuais demorados
- Atualizações automáticas seguindo evolução das vendas
- Redução de erros de execução

## Tecnologias

- **Databricks**: Plataforma principal para processamento e automação
- **PySpark**: Framework para processamento distribuído de dados
- **Python**: Linguagem principal para implementação dos algoritmos
- **SQL**: Consultas para extração e transformação de dados
- **Agendamento**: Automação de rodadas de cálculo
- **Alertas**: Sistema de notificações para anomalias

## Métricas de Avaliação da Matriz de Merecimento

### Visão Geral

Implementamos um sistema robusto de métricas para avaliar a qualidade das alocações da matriz de merecimento, permitindo comparação entre valores previstos e reais de demanda.

### Métricas Implementadas

#### 🔍 **wMAPE (Weighted Mean Absolute Percentage Error)**
- **Descrição**: Erro percentual absoluto ponderado pelo volume
- **Interpretação**: Quanto menor, melhor a precisão da matriz
- **Escala**: < 10% = Excelente, 10-20% = Bom, > 20% = Precisa melhorar

#### 🔍 **SE (Share Error)**
- **Descrição**: Erro na distribuição de participações entre filiais
- **Interpretação**: Quanto menor, melhor a distribuição
- **Escala**: < 5 pp = Excelente, 5-10 pp = Bom, > 10 pp = Precisa melhorar

#### 🔍 **Cross Entropy**
- **Descrição**: Medida de divergência entre distribuições reais e previstas
- **Interpretação**: Quanto menor, mais similares as distribuições
- **Escala**: < 0.1 = Excelente, 0.1-0.3 = Bom, > 0.3 = Precisa melhorar

#### 🔍 **KL Divergence**
- **Descrição**: Divergência de Kullback-Leibler para comparação de distribuições
- **Interpretação**: Quanto menor, mais similares as distribuições
- **Escala**: < 0.1 = Excelente, 0.1-0.3 = Bom, > 0.3 = Precisa melhorar

### Funcionalidades do Sistema de Métricas

#### 📊 **Cálculo Automático**
- Métricas calculadas para todas as 8 abordagens de demanda (4 médias + 4 medianas)
- Agregação por diferentes níveis: global, CD, gêmeo, CD-gêmeo
- Validação automática de dados antes do cálculo

#### 📈 **Análise Comparativa**
- Comparação entre diferentes métodos de cálculo de demanda
- Ranking de performance por CD e gêmeo
- Identificação de oportunidades de melhoria

#### 📋 **Resumos Estatísticos**
- Estatísticas descritivas das métricas (média, desvio padrão, min, max, mediana)
- Agrupamento por diferentes dimensões de análise
- Exportação para tabelas Delta para análise posterior

### Arquivos de Implementação

- **`metricas_matriz_merecimento.py`**: Módulo principal com todas as funções de métricas
- **`exemplo_uso_metricas.py`**: Notebook de demonstração com exemplos práticos
- **`calculo_matriz_de_merecimento.py`**: Integração das métricas no cálculo principal

### Uso das Métricas

#### 🔧 **Implementação Automática**
As métricas são calculadas automaticamente após o cálculo da matriz de merecimento, comparando:
- **Valores previstos**: Merecimentos calculados pela matriz
- **Valores reais**: Demandas observadas (médias móveis e medianas)

#### 📊 **Análise e Monitoramento**
- **Monitoramento contínuo**: Cálculo regular das métricas para acompanhamento da evolução
- **Análise por segmento**: Identificação de padrões por CD, gêmeo, região
- **Ações corretivas**: Uso dos resultados para otimização da matriz

#### 🎯 **Benefícios para o Negócio**
- **Qualidade da matriz**: Avaliação objetiva da precisão das alocações
- **Identificação de gaps**: Detecção de problemas específicos por segmento
- **Otimização contínua**: Base para melhorias iterativas da matriz

## 📁 Estrutura do Projeto

```
supply_matriz_de_merecimento/
├── README.md                                    # 📖 Documentação principal do sistema
├── src/                                         # 💻 Código fonte Python
│   ├── calculo_matriz_de_merecimento_unificado.py    # 🔄 Sistema unificado OFFLINE
│   ├── calculo_matriz_de_merecimento_online.py        # ⚡ Sistema unificado ONLINE
│   ├── Preparacao_tabelas_Matriz_merecimento.py      # 📋 Preparação de tabelas OFFLINE
│   ├── Preparacao_tabelas_Matriz_merecimento_online.py # 🔄 Preparação de tabelas ONLINE
│   ├── Salvar_matrizes_calculadas_csv.py             # 💾 Exportação de resultados
│   └── analysis/                                # 📊 Notebooks de análise e monitoramento
│       ├── Analise_demanda_matriz_telas.py           # 📈 Análise de efetividade - Telas
│       ├── Analise_demanda_matriz_antiga.py          # 📈 Análise de efetividade - Sistema anterior
│       ├── analise_factual_comparacao_matrizes.py    # 🔍 Comparação factual entre matrizes
│       ├── analise_elasticidade_demanda.py           # 📊 Análise de elasticidade de demanda
│       ├── analise_elasticidade_eventos.py           # 📊 Análise de elasticidade de eventos
│       ├── dashboard_estoque_cd_loja.py             # 📊 Dashboard de estoque por CD/loja
│       ├── monitoramento_teste_matriz.py             # 🔍 Monitoramento de testes da matriz
│       └── README.md                                # 📖 Documentação das análises
├── docs/                                        # 📚 Documentação técnica adicional
├── tests/                                        # 🧪 Testes unitários
└── README_SISTEMA_UNIFICADO.md                  # 📖 Documentação técnica detalhada
```

### 🎯 **Arquivos Principais**

#### **🔄 Cálculo de Merecimento**
- **`calculo_matriz_de_merecimento_unificado.py`**: Sistema principal OFFLINE para análises históricas
- **`calculo_matriz_de_merecimento_online.py`**: Sistema principal ONLINE para operações diárias

#### **🔧 Preparação de Dados**
- **`Preparacao_tabelas_Matriz_merecimento.py`**: Preparação OFFLINE de dados históricos
- **`Preparacao_tabelas_Matriz_merecimento_online.py`**: Preparação ONLINE de dados operacionais

#### **💾 Exportação**
- **`Salvar_matrizes_calculadas_csv.py`**: Exportação de matrizes calculadas para CSV

#### **📊 Análises** (`src/analysis/`)
- **Análises de Efetividade**: Avaliação da precisão da matriz
- **Comparações Factuais**: Comparação entre diferentes abordagens
- **Análises de Elasticidade**: Impacto de eventos e promoções
- **Dashboards**: Visualizações interativas de estoque e performance
- **Monitoramento**: Alertas e métricas de qualidade

## Arquitetura do Código

### 🔧 **Sistema Unificado Principal**

O **`calculo_matriz_de_merecimento_unificado.py`** é o núcleo do sistema, implementando uma arquitetura modular e configurável:

#### **1. Configuração e Regras de Negócio**
- `REGRAS_AGRUPAMENTO`: Dicionário com regras específicas por categoria
- `PARAMETROS_OUTLIERS`: Configurações sigma para detecção de outliers
- `JANELAS_MOVEIS`: Períodos para cálculo de médias móveis (90, 180, 270, 360 dias)

#### **2. Carregamento e Preparação de Dados**
- `carregar_dados_base()`: Dados base da tabela supply_base_merecimento_diario
- `carregar_mapeamentos_produtos()`: Mapeamentos de modelos e gêmeos
- `aplicar_mapeamentos_produtos()`: Aplicação de mapeamentos via joins

#### **3. Definição de Grupos de Necessidade**
- `determinar_grupo_necessidade()`: Criação dinâmica de grupo_de_necessidade
- **TELAS/TELEFONIA**: Usa coluna `gemeos` (produtos similares)
- **LINHA BRANCA/LEVE/INFO**: Usa coluna `NmEspecieGerencial`

#### **4. Detecção e Tratamento de Outliers**
- `detectar_outliers_meses_atipicos()`: Identificação de meses atípicos por grupo
- `filtrar_meses_atipicos()`: Remoção seletiva de outliers identificados
- **Parâmetros configuráveis**: Sigma para diferentes tipos de outlier

#### **5. Cálculo de Medidas Centrais**
- `calcular_medidas_centrais_com_medias_aparadas()`: Médias, medianas e aparadas
- **Médias móveis normais**: Tradicionais, sensíveis a outliers
- **Medianas móveis**: Robustas a outliers
- **Médias móveis aparadas**: Equilibram robustez e informação

#### **6. Sistema de Merecimento em Duas Camadas**
- `criar_de_para_filial_cd()`: Mapeamento automático filial → CD
- `calcular_merecimento_cd()`: **Primeira camada**: Merecimento a nível CD
- `calcular_merecimento_interno_cd()`: **Segunda camada**: Participação interna ao CD
- `calcular_merecimento_final()`: **Cálculo final**: CD × Participação interna

#### **7. Orquestração Principal**
- `executar_calculo_matriz_merecimento()`: Função principal que coordena todo o fluxo
- **Parâmetros configuráveis**: Categoria, datas, sensibilidade sigma
- **Resultado limpo**: SKU x loja x gêmeo com todos os merecimentos

### 📚 **Padrões de Qualidade**

- **Type Hints**: Tipagem completa para todos os parâmetros e retornos
- **Docstrings**: Documentação detalhada seguindo padrão Google
- **Funções Modulares**: Cada função com responsabilidade única
- **Nomenclatura Consistente**: Padrões de nomenclatura Python
- **Tratamento de Erros**: Validações e verificações robustas
- **Evita Referências Circulares**: Ordem de execução otimizada

### 🚀 **Funcionalidades Principais**

1. **Sistema Unificado**: Uma função para todas as categorias
2. **Agrupamento Inteligente**: Adapta-se automaticamente por categoria
3. **Detecção de Outliers**: Configurável e robusta
4. **Múltiplas Medidas**: Médias, medianas e aparadas para diferentes períodos
5. **Merecimento em Duas Camadas**: CD + Participação interna
6. **Resultado Limpo**: Dados agregados, sem granularidade desnecessária



## Como Usar o Sistema Unificado

### **🚀 Execução Simplificada**

O sistema unificado pode ser executado com **uma única linha de código** para qualquer categoria:

```python
# DIRETORIA DE TELAS
df_telas = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")

# DIRETORIA TELEFONIA CELULAR
df_telefonia = executar_calculo_matriz_merecimento("DIRETORIA TELEFONIA CELULAR")

# DIRETORIA LINHA BRANCA
df_linha_branca = executar_calculo_matriz_merecimento("DIRETORIA LINHA BRANCA")

# DIRETORIA LINHA LEVE
df_linha_leve = executar_calculo_matriz_merecimento("DIRETORIA LINHA LEVE")

# DIRETORIA INFO/GAMES
df_info_games = executar_calculo_matriz_merecimento("DIRETORIA INFO/GAMES")
```

### **⚙️ Configuração Avançada**

#### **Parâmetros Personalizáveis**
```python
df_resultado = executar_calculo_matriz_merecimento(
    categoria="DIRETORIA DE TELAS",
    data_inicio="2024-01-01",           # Período de análise
    data_calculo="2025-06-30",          # Data para cálculo de merecimento
    sigma_meses_atipicos=2.5,           # Sensibilidade para meses atípicos
    sigma_outliers_cd=2.0,              # Sensibilidade para outliers CD
    sigma_outliers_loja=2.5,            # Sensibilidade para outliers loja
    sigma_atacado_cd=1.5,               # Sensibilidade para outliers atacado CD
    sigma_atacado_loja=1.5              # Sensibilidade para outliers atacado loja
)
```

#### **Configurações por Categoria**
```python
# Categorias com agrupamento por gêmeos (mais sensíveis a outliers)
"DIRETORIA DE TELAS" → sigma_meses_atipicos=2.0
"DIRETORIA TELEFONIA CELULAR" → sigma_meses_atipicos=2.0

# Categorias com agrupamento por espécie gerencial (mais robustas)
"DIRETORIA LINHA BRANCA" → sigma_meses_atipicos=3.0
"DIRETORIA LINHA LEVE" → sigma_meses_atipicos=3.0
"DIRETORIA INFO/GAMES" → sigma_meses_atipicos=3.0
```

### **📊 Estrutura do Resultado**

O sistema retorna um DataFrame com **uma linha por SKU + loja + gêmeo** contendo:

#### **Colunas de Identificação**
- `cdfilial`: Código da filial/loja
- `cd_primario`: Código do Centro de Distribuição
- `grupo_de_necessidade`: Gêmeo ou espécie gerencial

#### **Colunas de Merecimento CD**
- `Total_CD_Media90_Qt_venda_sem_ruptura`: Total CD para média 90 dias
- `Total_CD_Media180_Qt_venda_sem_ruptura`: Total CD para média 180 dias
- `Total_CD_Media270_Qt_venda_sem_ruptura`: Total CD para média 270 dias
- `Total_CD_Media360_Qt_venda_sem_ruptura`: Total CD para média 360 dias
- `Total_CD_Mediana90_Qt_venda_sem_ruptura`: Total CD para mediana 90 dias
- `Total_CD_Mediana180_Qt_venda_sem_ruptura`: Total CD para mediana 180 dias
- `Total_CD_Mediana270_Qt_venda_sem_ruptura`: Total CD para mediana 270 dias
- `Total_CD_Mediana360_Qt_venda_sem_ruptura`: Total CD para mediana 360 dias
- `Total_CD_MediaAparada90_Qt_venda_sem_ruptura`: Total CD para média aparada 90 dias
- `Total_CD_MediaAparada180_Qt_venda_sem_ruptura`: Total CD para média aparada 180 dias
- `Total_CD_MediaAparada270_Qt_venda_sem_ruptura`: Total CD para média aparada 270 dias
- `Total_CD_MediaAparada360_Qt_venda_sem_ruptura`: Total CD para média aparada 360 dias

#### **Colunas de Participação Interna**
- `Percentual_Media90_Qt_venda_sem_ruptura`: Participação da loja no CD (90 dias)
- `Percentual_Media180_Qt_venda_sem_ruptura`: Participação da loja no CD (180 dias)
- `Percentual_Media270_Qt_venda_sem_ruptura`: Participação da loja no CD (270 dias)
- `Percentual_Media360_Qt_venda_sem_ruptura`: Participação da loja no CD (360 dias)
- `Percentual_Mediana90_Qt_venda_sem_ruptura`: Participação da loja no CD (mediana 90 dias)
- `Percentual_Mediana180_Qt_venda_sem_ruptura`: Participação da loja no CD (mediana 180 dias)
- `Percentual_Mediana270_Qt_venda_sem_ruptura`: Participação da loja no CD (mediana 270 dias)
- `Percentual_Mediana360_Qt_venda_sem_ruptura`: Participação da loja no CD (mediana 360 dias)
- `Percentual_MediaAparada90_Qt_venda_sem_ruptura`: Participação da loja no CD (média aparada 90 dias)
- `Percentual_MediaAparada180_Qt_venda_sem_ruptura`: Participação da loja no CD (média aparada 180 dias)
- `Percentual_MediaAparada270_Qt_venda_sem_ruptura`: Participação da loja no CD (média aparada 270 dias)
- `Percentual_MediaAparada360_Qt_venda_sem_ruptura`: Participação da loja no CD (média aparada 360 dias)

#### **Colunas de Merecimento Final**
- `Merecimento_Final_Media90_Qt_venda_sem_ruptura`: Merecimento final (90 dias)
- `Merecimento_Final_Media180_Qt_venda_sem_ruptura`: Merecimento final (180 dias)
- `Merecimento_Final_Media270_Qt_venda_sem_ruptura`: Merecimento final (270 dias)
- `Merecimento_Final_Media360_Qt_venda_sem_ruptura`: Merecimento final (360 dias)
- `Merecimento_Final_Mediana90_Qt_venda_sem_ruptura`: Merecimento final (mediana 90 dias)
- `Merecimento_Final_Mediana180_Qt_venda_sem_ruptura`: Merecimento final (mediana 180 dias)
- `Merecimento_Final_Mediana270_Qt_venda_sem_ruptura`: Merecimento final (mediana 270 dias)
- `Merecimento_Final_Mediana360_Qt_venda_sem_ruptura`: Merecimento final (mediana 360 dias)
- `Merecimento_Final_MediaAparada90_Qt_venda_sem_ruptura`: Merecimento final (média aparada 90 dias)
- `Merecimento_Final_MediaAparada180_Qt_venda_sem_ruptura`: Merecimento final (média aparada 180 dias)
- `Merecimento_Final_MediaAparada270_Qt_venda_sem_ruptura`: Merecimento final (média aparada 270 dias)
- `Merecimento_Final_MediaAparada360_Qt_venda_sem_ruptura`: Merecimento final (média aparada 360 dias)

### **🔍 Análise dos Resultados**

#### **Exemplo de Uso para Análise**
```python
# Análise por CD
df_por_cd = df_resultado.groupBy("cd_primario").agg(
    F.sum("Merecimento_Final_Media90_Qt_venda_sem_ruptura").alias("Total_Merecimento_90dias"),
    F.avg("Percentual_Media90_Qt_venda_sem_ruptura").alias("Participacao_Media_90dias")
)

# Análise por grupo de necessidade
df_por_grupo = df_resultado.groupBy("grupo_de_necessidade").agg(
    F.countDistinct("cdfilial").alias("Total_Lojas"),
    F.sum("Total_CD_Media90_Qt_venda_sem_ruptura").alias("Total_Demanda_CD_90dias")
)

# Análise por loja
df_por_loja = df_resultado.groupBy("cdfilial").agg(
    F.sum("Merecimento_Final_Media90_Qt_venda_sem_ruptura").alias("Merecimento_Total_90dias"),
    F.countDistinct("grupo_de_necessidade").alias("Total_Grupos_Necessidade")
)
```

### **Pré-requisitos**
- Databricks workspace configurado
- Acesso às tabelas de dados necessárias
- Permissões para criação de tabelas Delta

### **Configuração**
- Ajustar datas de início e fim conforme necessário
- Configurar parâmetros de filtro para diferentes categorias
- Personalizar regras de negócio específicas

## Status do Projeto

✅ **Sistema Unificado Implementado** - Cálculo de merecimento para todas as categorias
✅ **Arquitetura Robusta** - Detecção de outliers, médias aparadas, merecimento em duas camadas
✅ **Código Refatorado e Documentado** - Estrutura modular implementada
🚧 **Em Produção** - Sistema funcional para uso operacional

### **Últimas Atualizações**
- **🆕 Sistema unificado** implementado para todas as diretorias
- **🆕 Cálculo de merecimento** em duas camadas (CD + participação interna)
- **🆕 Detecção de outliers** configurável por categoria
- **🆕 Médias aparadas** para robustez estatística
- **🆕 Resultado limpo** (SKU x loja x gêmeo) sem dados granulares
- **Refatoração completa** do código para funções modulares
- **Documentação completa** com docstrings e type hints
- **Organização estrutural** seguindo melhores práticas Python/PySpark
- **Padrões de qualidade** implementados para manutenibilidade

---

*Desenvolvido para o time de Supply do Grupo Casas Bahia*

**Autor**: Scardini  
**Data**: 2025  
**Versão**: 1.1 - Sistema Unificado de Merecimento Implementado
