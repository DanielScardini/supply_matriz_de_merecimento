# Analysis Notebooks - Supply Chain Matrix

Esta pasta contém notebooks de análise e utilitários para o sistema de matriz de merecimento.

## 📁 Estrutura dos Arquivos

### 🔍 **Análises de Demanda**
- **`Analise_demanda_matriz_telas.py`**: Análise de efetividade da matriz de merecimento para produtos de telas
- **`Analise_demanda_matriz_antiga.py`**: Análise da matriz de merecimento antiga para comparação

### 📊 **Métricas e Utilitários**
- **`metricas_matriz_merecimento.py`**: Cálculo de métricas para avaliação da matriz
- **`exemplo_uso_metricas.py`**: Exemplos práticos de uso das métricas
- **`Preparacao_tabelas_Matriz_merecimento.py`**: Preparação e limpeza de tabelas para o cálculo

## 🎯 **Como Usar**

### **Para Análises de Demanda**
```python
# Execute o notebook Analise_demanda_matriz_telas.py para analisar produtos de telas
# Execute o notebook Analise_demanda_matriz_antiga.py para análise comparativa
```

### **Para Cálculo de Métricas**
```python
# Use metricas_matriz_merecimento.py para calcular métricas de qualidade
# Consulte exemplo_uso_metricas.py para exemplos práticos
```

### **Para Preparação de Dados**
```python
# Execute Preparacao_tabelas_Matriz_merecimento.py para preparar tabelas base
```

## 🔗 **Integração com Sistema Unificado**

Os notebooks de análise podem ser integrados com o sistema unificado:

```python
# Import do sistema unificado
from ..calculo_matriz_de_merecimento_unificado import (
    executar_calculo_matriz_merecimento,
    validar_resultados
)

# Uso das funções
df_resultado = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")
```

## 📊 **Tabelas Utilizadas**

- **`databox.bcg_comum.supply_base_merecimento_diario`**: Base principal de dados
- **`app_venda.mercadoria`**: Dados de mercadorias
- **`app_venda.vendafaturadarateada`**: Vendas faturadas rateadas
- **`app_venda.filial`**: Dados de filiais

## 🚀 **Próximos Passos**

1. **Integração**: Conectar notebooks de análise com sistema unificado
2. **Automação**: Criar pipelines automatizados de análise
3. **Dashboards**: Desenvolver visualizações interativas
4. **Alertas**: Implementar sistema de alertas para anomalias

---

**Versão**: 1.1.0  
**Última Atualização**: Dezembro 2024  
**Mantenedor**: Equipe de Supply Chain Analytics
