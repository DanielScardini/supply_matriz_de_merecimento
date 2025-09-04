# Analysis Notebooks - Supply Chain Matrix

Esta pasta contém notebooks de análise e utilitários para o sistema de matriz de merecimento, implementados especificamente para o ambiente Databricks.

## 📁 Estrutura dos Arquivos

### 🔍 **Análises de Demanda e Efetividade**
- **`Analise_demanda_matriz_telas.py`**: Análise de efetividade da matriz de merecimento para produtos de telas
- **`Analise_demanda_matriz_antiga.py`**: Análise da matriz de merecimento antiga para comparação

### 📊 **Análise de Elasticidade**
- **`analise_elasticidade_demanda.py`**: Análise de elasticidade de demanda por produtos gêmeos
- **`analise_elasticidade_eventos.py`**: Análise de impacto de eventos na demanda

### 🔍 **Análise Factual e Comparação**
- **`analise_factual_comparacao_matrizes.py`**: Comparação entre matrizes calculadas e matriz DRP geral
- **`analise_resultados_factuais.py`**: Análise de resultados factuais vs. previstos

### 📚 **Documentação Específica**
- **`README_ELASTICIDADE_DATABRICKS.md`**: Documentação específica para análise de elasticidade no Databricks

## 🎯 **Funcionalidades Principais**

### **Análise de Efetividade da Matriz**
- **Métricas de qualidade**: wMAPE, SE, Cross Entropy, KL Divergence
- **Comparação temporal**: Análise de performance ao longo do tempo
- **Validação por categoria**: Análise específica para cada diretoria
- **Identificação de distorções**: Detecção de alocações problemáticas

### **Análise de Elasticidade de Demanda**
- **Identificação automática** dos top 5 gêmeos por diretoria
- **Análise temporal** com quebras por porte de loja e região geográfica
- **Visualizações profissionais** com gráficos de barras empilhadas
- **Duas versões de análise**: apenas porte de loja e porte + região

### **Análise Factual e Comparação**
- **Comparação com matriz DRP**: Análise de diferenças entre matrizes
- **Cálculo de métricas**: sMAPE e WMAPE para avaliação de qualidade
- **Análise de distorções**: Identificação de alocações problemáticas
- **Validação de resultados**: Verificação de consistência dos cálculos

## 🚀 **Como Usar**

### **Para Análises de Efetividade**
```python
# Execute o notebook Analise_demanda_matriz_telas.py para analisar produtos de telas
# Execute o notebook Analise_demanda_matriz_antiga.py para análise comparativa
```

### **Para Análise de Elasticidade**
```python
# Execute analise_elasticidade_demanda.py para análise de elasticidade por gêmeos
# Execute analise_elasticidade_eventos.py para análise de impacto de eventos
```

### **Para Análise Factual**
```python
# Execute analise_factual_comparacao_matrizes.py para comparação com DRP
# Execute analise_resultados_factuais.py para análise de resultados factuais
```

## 🔧 **Configuração e Pré-requisitos**

### **Ambiente Databricks**
- **Cluster**: Configurado com PySpark 3.x+
- **Bibliotecas**: Pandas, Plotly, OpenPyXL
- **Permissões**: Acesso às tabelas do sistema

### **Tabelas Necessárias**
- **`databox.bcg_comum.supply_base_merecimento_diario`**: Base principal de dados
- **`databox.bcg_comum.supply_base_merecimento_diario_v2`**: Versão atualizada da base
- **`databox.bcg_comum.supply_base_merecimento_diario_v3`**: Versão mais recente
- **`data_engineering_prd.app_operacoesloja.roteirizacaolojaativa`**: Dados de lojas e regiões

### **Arquivos de Mapeamento**
- **`MODELOS_AJUSTE (1).csv`**: Mapeamento de modelos de produtos
- **`ITENS_GEMEOS 2.csv`**: Mapeamento de produtos gêmeos
- **`(DRP)_MATRIZ_*.csv`**: Matrizes DRP para comparação

## 📊 **Métricas Implementadas**

### **Métricas de Qualidade da Matriz**
- **wMAPE**: Weighted Mean Absolute Percentage Error
- **SE**: Share Error (erro na distribuição de participações)
- **Cross Entropy**: Divergência entre distribuições reais e previstas
- **KL Divergence**: Divergência de Kullback-Leibler

### **Métricas de Elasticidade**
- **Análise temporal**: Variação de demanda ao longo do tempo
- **Quebra por porte**: Análise por tamanho da loja
- **Quebra por região**: Análise geográfica
- **Identificação de padrões**: Detecção de sazonalidade e tendências

### **Métricas de Comparação**
- **sMAPE**: Symmetric Mean Absolute Percentage Error
- **WMAPE**: Weighted Mean Absolute Percentage Error
- **Correlação**: Entre matrizes calculadas e DRP
- **Distribuição de erros**: Análise de padrões de erro

## 🎨 **Visualizações**

### **Gráficos de Elasticidade**
- **Barras empilhadas**: Vendas mensais por porte de loja
- **Gráficos de proporção**: Distribuição percentual por categoria
- **Cores padronizadas**: Gradiente de azuis para hierarquia
- **Formato profissional**: Otimizado para apresentações

### **Gráficos de Análise**
- **Scatter plots**: Comparação entre matrizes
- **Box plots**: Distribuição de métricas por categoria
- **Heatmaps**: Matrizes de correlação
- **Time series**: Evolução temporal das métricas

## 🔗 **Integração com Sistema Unificado**

### **Compatibilidade**
- **Paths corrigidos**: Todos os caminhos ajustados para execução no Databricks
- **Funções reutilizáveis**: Código modular para integração
- **Padrões consistentes**: Seguindo as mesmas convenções do sistema principal

### **Reutilização de Código**
```python
# Importar funções do sistema principal
from calculo_matriz_de_merecimento_unificado import (
    executar_calculo_matriz_merecimento,
    carregar_mapeamentos_produtos
)

# Usar em análises
df_matriz = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")
```

## 📈 **Exemplos de Uso**

### **Análise de Efetividade para Telas**
```python
# Executar análise completa para telas
# 1. Carregar dados base
# 2. Aplicar mapeamentos
# 3. Calcular métricas de qualidade
# 4. Gerar visualizações
# 5. Exportar resultados
```

### **Análise de Elasticidade por Gêmeos**
```python
# Executar análise de elasticidade
# 1. Identificar top 5 gêmeos por diretoria
# 2. Preparar dados para visualização
# 3. Criar gráficos por porte de loja
# 4. Criar gráficos por porte + região
# 5. Salvar visualizações
```

### **Comparação com Matriz DRP**
```python
# Executar comparação factual
# 1. Carregar matrizes calculadas
# 2. Carregar matriz DRP geral
# 3. Calcular métricas de comparação
# 4. Identificar distorções
# 5. Gerar relatório de diferenças
```

## ⚡ **Performance e Otimizações**

### **Estratégias de Cache**
- **Cache automático**: DataFrames intermediários em cache
- **Particionamento**: Por SKU e loja para otimização
- **Filtros precoces**: Redução de volume de dados

### **Otimizações PySpark**
- **Joins otimizados**: Usando broadcast joins quando apropriado
- **Agregações eficientes**: Window functions para cálculos complexos
- **Filtros inteligentes**: Aplicação de filtros antes de operações custosas

## 🛠️ **Manutenção e Extensibilidade**

### **Adicionar Nova Análise**
1. Criar novo notebook seguindo padrões existentes
2. Implementar funções modulares
3. Adicionar documentação
4. Testar com dados de exemplo

### **Modificar Análises Existentes**
1. Identificar função a ser modificada
2. Fazer alterações mantendo compatibilidade
3. Atualizar documentação
4. Testar com dados reais

## 📚 **Documentação Adicional**

### **Arquivos de Referência**
- **`README_ELASTICIDADE_DATABRICKS.md`**: Documentação específica para análise de elasticidade
- **`README.md`** (pasta principal): Documentação geral do sistema
- **Docstrings**: Documentação inline em todas as funções

### **Exemplos e Tutoriais**
- **Notebooks de exemplo**: Cada análise inclui exemplos de uso
- **Comentários detalhados**: Explicação de cada etapa do processo
- **Logs de progresso**: Acompanhamento da execução

## 🚀 **Próximos Passos**

### **Melhorias Planejadas**
1. **Dashboards interativos**: Interface visual para análise
2. **Automação**: Integração com pipelines de dados
3. **Alertas**: Notificações para anomalias detectadas
4. **Histórico**: Versionamento de análises e resultados

### **Extensões Técnicas**
1. **Métricas avançadas**: Coeficientes de variação, assimetria
2. **Análise temporal**: Sazonalidade, tendências
3. **Machine Learning**: Detecção automática de padrões
4. **API REST**: Interface para integração externa

## 🤝 **Contribuição**

### **Padrões de Código**
- **Seguir PEP 8**: Para Python
- **Documentar funções**: Docstrings completas
- **Usar type hints**: Para melhor legibilidade
- **Implementar testes**: Para validação

### **Processo de Desenvolvimento**
1. **Criar branch**: Para nova funcionalidade
2. **Implementar com testes**: Validação completa
3. **Validar em ambiente**: Teste com dados reais
4. **Pull request**: Com revisão de código
5. **Merge**: Após aprovação

## 📞 **Suporte**

Para dúvidas ou problemas:

1. **Verificar documentação**: Consultar READMEs e docstrings
2. **Revisar exemplos**: Usar notebooks de exemplo
3. **Consultar logs**: Verificar mensagens de erro
4. **Contatar equipe**: Para suporte técnico

---

**Versão**: 2.0.0  
**Última Atualização**: Janeiro 2025  
**Mantenedor**: Equipe de Supply Chain Analytics