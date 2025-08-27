# Sistema Unificado de Matriz de Merecimento

## 📋 Visão Geral

Este sistema unifica o cálculo da matriz de merecimento para todas as categorias de produtos, implementando uma arquitetura flexível com abstração `grupo_de_necessidade` e médias aparadas robustas.

## 🎯 Objetivos

- **Unificação**: Um único sistema para todas as categorias
- **Flexibilidade**: Regras específicas por categoria via configuração
- **Robustez**: Médias aparadas para melhor resistência a outliers
- **Manutenibilidade**: Código modular e bem documentado
- **Escalabilidade**: Suporte a grandes volumes de dados via PySpark

## 🏗️ Arquitetura

### Abstração `grupo_de_necessidade`

O sistema usa uma abstração chamada `grupo_de_necessidade` que se adapta automaticamente conforme a categoria:

| Categoria | Coluna de Agrupamento | Tipo | Descrição |
|-----------|----------------------|------|-----------|
| **DIRETORIA DE TELAS** | `gemeos` | gêmeos | Produtos similares |
| **DIRETORIA TELEFONIA CELULAR** | `gemeos` | gêmeos | Produtos similares |
| **DIRETORIA LINHA BRANCA** | `NmEspecieGerencial` | espécie_gerencial | Categoria gerencial |
| **DIRETORIA LINHA LEVE** | `NmEspecieGerencial` | espécie_gerencial | Categoria gerencial |
| **DIRETORIA INFO/GAMES** | `NmEspecieGerencial` | espécie_gerencial | Categoria gerencial |

### Fluxo de Processamento

```
1. Carregamento de Dados Base
   ↓
2. Aplicação da Regra de Agrupamento
   ↓
3. Carregamento de Mapeamentos
   ↓
4. Detecção de Outliers
   ↓
5. Filtragem de Meses Atípicos
   ↓
6. Cálculo de Medidas Centrais
   ↓
7. Consolidação Final
```

## 🔧 Funcionalidades

### Medidas Centrais Calculadas

Para cada janela móvel (90, 180, 270, 360 dias):

- **Médias Móveis Normais**: Média aritmética tradicional
- **Medianas Móveis**: Mediana para robustez a outliers
- **Médias Móveis Aparadas**: Média excluindo valores extremos (10% superior e inferior)

### Detecção de Outliers

- **Meses Atípicos**: Remove meses com QtMercadoria > 3σ da média
- **Outliers Históricos CD**: Remove registros > 3σ por grupo_de_necessidade
- **Outliers Históricos Loja**: Remove registros > 3σ por grupo_de_necessidade-loja
- **Parâmetros Configuráveis**: Desvios padrão ajustáveis por categoria

### Filtros Inteligentes

- **Filtro de Ruptura**: Considera apenas dias sem ruptura para cálculo de demanda
- **Filtro por Gêmeo**: Remove meses atípicos apenas do grupo específico
- **Cache Inteligente**: Otimiza performance com cache estratégico

## 📁 Estrutura de Arquivos

```
src/
├── calculo_matriz_de_merecimento_unificado.py    # Sistema principal
├── exemplo_uso_sistema_unificado.py              # Exemplos de uso
├── calculo_matriz_de_merecimento_telas.py        # Implementação específica (legado)
├── calculo_matriz_de_merecimento_telefonia.py    # Implementação específica (legado)
└── ...                                           # Outros arquivos
```

## 🚀 Como Usar

### Uso Básico

```python
from calculo_matriz_de_merecimento_unificado import (
    executar_calculo_matriz_merecimento,
    validar_resultados
)

# Executar para qualquer categoria
df_resultado = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")

# Validar resultados
validar_resultados(df_resultado, "DIRETORIA DE TELAS")
```

### Exemplos por Categoria

```python
# TELAS (usa gêmeos)
df_telas = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")

# TELEFONIA (usa gêmeos)
df_telefonia = executar_calculo_matriz_merecimento("DIRETORIA TELEFONIA CELULAR")

# LINHA BRANCA (usa espécie gerencial)
df_linha_branca = executar_calculo_matriz_merecimento("DIRETORIA LINHA BRANCA")
```

### Parâmetros Configuráveis

```python
# Configurações de outliers
PARAMETROS_OUTLIERS = {
    "desvios_meses_atipicos": 3,      # Desvios para meses atípicos
    "desvios_historico_cd": 3,         # Desvios para outliers CD
    "desvios_historico_loja": 3,       # Desvios para outliers loja
    "desvios_atacado_cd": 1.5,         # Desvios para atacado CD
    "desvios_atacado_loja": 1.5        # Desvios para atacado loja
}

# Janelas móveis
JANELAS_MOVEIS = [90, 180, 270, 360]

# Percentual de corte para médias aparadas
PERCENTUAL_CORTE_MEDIAS_APARADAS = 0.10  # 10%
```

## 📊 Saída do Sistema

### Colunas Principais

- **Identificação**: `DtAtual`, `CdSku`, `CdFilial`, `grupo_de_necessidade`
- **Dados Base**: `QtMercadoria`, `Receita`, `FlagRuptura`
- **Agrupamento**: `tipo_agrupamento`, `year_month`

### Medidas Calculadas

Para cada janela móvel (exemplo para 90 dias):

```
Media90_Qt_venda_sem_ruptura          # Média móvel normal
Mediana90_Qt_venda_sem_ruptura        # Mediana móvel
MediaAparada90_Qt_venda_sem_ruptura   # Média móvel aparada
```

## 🔍 Validação e Monitoramento

### Função de Validação

```python
def validar_resultados(df: DataFrame, categoria: str):
    # Estatísticas gerais
    # Verificação de valores nulos
    # Validação de integridade
```

### Métricas de Qualidade

- Total de registros processados
- Contagem de SKUs e lojas únicos
- Verificação de valores nulos nas medidas
- Correlações entre diferentes tipos de média

## ⚡ Performance e Otimizações

### Estratégias de Cache

- Cache automático de DataFrames intermediários
- Cache de mapeamentos de produtos
- Cache de estatísticas por grupo

### Otimizações PySpark

- Uso eficiente de janelas móveis
- Particionamento inteligente por SKU-loja
- Agregações otimizadas para grandes volumes

## 🛠️ Manutenção e Extensibilidade

### Adicionar Nova Categoria

1. Adicionar entrada em `REGRAS_AGRUPAMENTO`
2. Definir coluna de agrupamento
3. Especificar tipo e descrição
4. Sistema se adapta automaticamente

### Modificar Parâmetros

- Ajustar `PARAMETROS_OUTLIERS` para diferentes sensibilidades
- Modificar `JANELAS_MOVEIS` para novos períodos
- Alterar `PERCENTUAL_CORTE_MEDIAS_APARADAS` para diferentes níveis de robustez

## 📈 Análise e Comparação

### Comparação entre Categorias

```python
def comparar_categorias(lista_dataframes, lista_categorias):
    # Estatísticas comparativas
    # Análise de distribuição
    # Verificação de qualidade
```

### Análise das Médias Aparadas

```python
def analisar_medias_aparadas(df: DataFrame, categoria: str):
    # Estatísticas descritivas
    # Correlações entre tipos de média
    # Análise de robustez
```

## 💾 Exportação de Resultados

### Formatos Suportados

- **Delta**: Formato nativo do Databricks (recomendado)
- **Parquet**: Formato otimizado para análise
- **CSV**: Para datasets pequenos ou análise externa

### Estrutura de Nomenclatura

```
/tmp/matriz_merecimento_diretoria_de_telas/
/tmp/matriz_merecimento_diretoria_telefonia_celular/
/tmp/matriz_merecimento_diretoria_linha_branca/
```

## 🔮 Próximos Passos

### Melhorias Planejadas

1. **Validações de Negócio**: Regras específicas por categoria
2. **Dashboards**: Interface visual para monitoramento
3. **Automação**: Integração com DAGs e pipelines
4. **Alertas**: Notificações para anomalias detectadas
5. **Histórico**: Versionamento de parâmetros e resultados

### Extensões Técnicas

1. **Métricas Avançadas**: Coeficientes de variação, assimetria
2. **Análise Temporal**: Sazonalidade, tendências
3. **Machine Learning**: Detecção automática de padrões
4. **API REST**: Interface para integração externa

## 📚 Referências e Documentação

### Arquivos Relacionados

- `exemplo_uso_sistema_unificado.py`: Exemplos práticos
- `metricas_matriz_merecimento.py`: Métricas de avaliação
- `Preparacao_tabelas_Matriz_merecimento.py`: Preparação de dados

### Dependências

- PySpark 3.x+
- Pandas
- Databricks Runtime

## 🤝 Contribuição

### Padrões de Código

- Seguir PEP 8 para Python
- Documentar todas as funções públicas
- Usar type hints
- Implementar testes unitários

### Processo de Desenvolvimento

1. Criar branch para nova funcionalidade
2. Implementar com testes
3. Validar em ambiente de desenvolvimento
4. Pull request com revisão de código
5. Merge após aprovação

## 📞 Suporte

Para dúvidas ou problemas:

1. Verificar documentação existente
2. Consultar exemplos de uso
3. Revisar logs de execução
4. Contatar equipe de desenvolvimento

---

**Versão**: 1.0.0  
**Última Atualização**: Dezembro 2024  
**Mantenedor**: Equipe de Supply Chain Analytics
