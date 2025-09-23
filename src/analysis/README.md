# 📊 Análises e Monitoramento da Matriz de Merecimento

## 🎯 Visão Geral

Esta pasta contém **notebooks especializados** para análise de efetividade, monitoramento e comparação da matriz de merecimento. Os notebooks são organizados por funcionalidade e categoria de produto, permitindo análises detalhadas e comparações factuais entre diferentes abordagens.

## 📁 Estrutura dos Notebooks

### 🔍 **Análises de Efetividade**

#### **📈 Análise de Demanda**
- **`Analise_demanda_matriz_telas.py`**: Análise específica para produtos de telas
- **`Analise_demanda_matriz_antiga.py`**: Análise do sistema anterior para comparação

**🎯 Propósito**: Avaliar a precisão e efetividade da matriz de merecimento comparando valores previstos vs. reais.

### 🔍 **Comparações Factuais**

#### **📊 Comparação por Categoria**
- **`(telas) analise_factual_comparacao_matrizes_offline.py`**: Comparação factual - Telas (Offline)
- **`(telas) analise_factual_comparacao_matrizes_online.py`**: Comparação factual - Telas (Online)
- **`(telefonia) analise_factual_comparacao_matrizes_offline.py`**: Comparação factual - Telefonia (Offline)
- **`(telefonia) analise_factual_comparacao_matrizes_online.py`**: Comparação factual - Telefonia (Online)
- **`(Leves) analise_factual_comparacao_matrizes_offline.py`**: Comparação factual - Linha Leve (Offline)
- **`(Leves) analise_factual_comparacao_matrizes_online.py`**: Comparação factual - Linha Leve (Online)

**🎯 Propósito**: Comparar diferentes abordagens de cálculo (offline vs. online) para identificar a melhor estratégia por categoria.

#### **📋 Análise Geral**
- **`analise_factual_comparacao_matrizes.py`**: Comparação geral entre matrizes
- **`analise_resultados_factuais.py`**: Análise de resultados factuais (Offline)
- **`analise_resultados_factuais_online.py`**: Análise de resultados factuais (Online)

**🎯 Propósito**: Análise consolidada de performance e identificação de padrões gerais.

### 📊 **Análises de Elasticidade**

#### **🎯 Elasticidade de Demanda**
- **`analise_elasticidade_demanda.py`**: Análise de elasticidade de demanda
- **`analise_elasticidade_eventos.py`**: Análise de elasticidade de eventos

**🎯 Propósito**: Avaliar como a demanda responde a mudanças de preço e eventos promocionais.

### 📊 **Dashboards e Monitoramento**

#### **📈 Dashboards Operacionais**
- **`dashboard_estoque_cd_loja.py`**: Dashboard de estoque por CD e loja
- **`monitoramento_teste_matriz.py`**: Monitoramento de testes da matriz

**🎯 Propósito**: Visualizações interativas para monitoramento operacional e acompanhamento de testes.

### 🧪 **Análises Experimentais**

#### **🔬 Testes e Experimentos**
- **`análise_teste_matriz_scale_up.py`**: Análise de teste de escala da matriz
- **`analises_setembro25.py`**: Análises específicas de setembro 2025
- **`analise_atacado.py`**: Análise específica para vendas atacado

**🎯 Propósito**: Análises experimentais e testes de novas abordagens.

## 🎯 Agrupamento por Funcionalidade

### 📊 **Grupo 1: Análises de Efetividade**
**Notebooks**: `Analise_demanda_matriz_telas.py`, `Analise_demanda_matriz_antiga.py`

**Funcionalidade**: Avaliar precisão da matriz comparando valores previstos vs. reais
- Cálculo de métricas de erro (wMAPE, SE, Cross Entropy)
- Análise de tendências e sazonalidade
- Identificação de gaps de performance

### 🔍 **Grupo 2: Comparações Factuais**
**Notebooks**: `(categoria) analise_factual_comparacao_matrizes_(offline/online).py`, `analise_factual_comparacao_matrizes.py`

**Funcionalidade**: Comparar diferentes abordagens de cálculo
- Comparação offline vs. online por categoria
- Análise de performance relativa
- Identificação da melhor estratégia por segmento

### 📈 **Grupo 3: Análises de Elasticidade**
**Notebooks**: `analise_elasticidade_demanda.py`, `analise_elasticidade_eventos.py`

**Funcionalidade**: Avaliar resposta da demanda a mudanças
- Elasticidade-preço da demanda
- Impacto de eventos promocionais
- Análise de sazonalidade e tendências

### 📊 **Grupo 4: Dashboards e Monitoramento**
**Notebooks**: `dashboard_estoque_cd_loja.py`, `monitoramento_teste_matriz.py`

**Funcionalidade**: Visualizações e monitoramento operacional
- Dashboards interativos de estoque
- Monitoramento de testes em produção
- Alertas de anomalias

### 🧪 **Grupo 5: Análises Experimentais**
**Notebooks**: `análise_teste_matriz_scale_up.py`, `analises_setembro25.py`, `analise_atacado.py`

**Funcionalidade**: Testes e análises específicas
- Testes de escala e performance
- Análises pontuais por período
- Análises específicas por canal (atacado)

## 🚀 Como Usar

### **Para Análises de Efetividade**
```python
# Execute para avaliar precisão da matriz
%run Analise_demanda_matriz_telas.py
%run Analise_demanda_matriz_antiga.py
```

### **Para Comparações Factuais**
```python
# Execute para comparar abordagens por categoria
%run "(telas) analise_factual_comparacao_matrizes_offline.py"
%run "(telefonia) analise_factual_comparacao_matrizes_online.py"
```

### **Para Análises de Elasticidade**
```python
# Execute para analisar elasticidade
%run analise_elasticidade_demanda.py
%run analise_elasticidade_eventos.py
```

### **Para Monitoramento**
```python
# Execute para dashboards e monitoramento
%run dashboard_estoque_cd_loja.py
%run monitoramento_teste_matriz.py
```

## 📊 Tabelas Utilizadas

### **Tabelas Principais**
- **`databox.bcg_comum.supply_base_merecimento_diario`**: Base principal de dados
- **`app_venda.mercadoria`**: Dados de mercadorias
- **`app_venda.vendafaturadarateada`**: Vendas faturadas rateadas
- **`app_venda.filial`**: Dados de filiais

### **Tabelas de Análise**
- **Tabelas de matriz calculada**: Resultados do sistema unificado
- **Tabelas de métricas**: Métricas de qualidade calculadas
- **Tabelas de comparação**: Comparações entre diferentes abordagens

## 🔗 Integração com Sistema Unificado

### **Fluxo de Dados**
```
Sistema Unificado (src/)
    ↓ Gera matrizes calculadas
    ↓
Análises (analysis/)
    ↓ Consome matrizes
    ↓
Relatórios e Dashboards
```

### **Reutilização de Funções**
Os notebooks podem reutilizar funções do sistema unificado:
```python
# Importar funções do sistema principal
from calculo_matriz_de_merecimento_unificado import executar_calculo_matriz_merecimento

# Executar cálculo para análise
df_matriz = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")
```

## 📈 Métricas Calculadas

### **Métricas de Qualidade**
- **wMAPE**: Erro percentual absoluto ponderado
- **SE**: Erro de participação entre filiais
- **Cross Entropy**: Divergência entre distribuições
- **KL Divergence**: Divergência de Kullback-Leibler

### **Métricas de Performance**
- **Precisão**: Acerto nas previsões de demanda
- **Estabilidade**: Consistência ao longo do tempo
- **Responsividade**: Adaptação a mudanças de tendência

## 🎯 Próximos Passos

### **Melhorias Planejadas**
1. **Automação**: Criar pipelines automatizados de análise
2. **Dashboards**: Desenvolver visualizações interativas
3. **Alertas**: Implementar sistema de alertas para anomalias
4. **Integração**: Conectar com sistemas de BI corporativo

### **Novas Análises**
1. **Análise de Sazonalidade**: Padrões sazonais por categoria
2. **Análise de Ruptura**: Impacto de rupturas na matriz
3. **Análise de Clusters**: Agrupamento de lojas por padrão de demanda
4. **Análise de Elasticidade**: Resposta a mudanças de preço

---

**Versão**: 1.1.0  
**Última Atualização**: Janeiro 2025  
**Mantenedor**: Equipe de Supply Chain Analytics
