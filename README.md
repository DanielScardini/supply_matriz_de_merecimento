# Supply Matriz de Merecimento

## Visão Geral

Este projeto implementa uma **ferramenta automatizada para cálculo da matriz de merecimento SKU-loja** utilizando Databricks, substituindo o processo manual atual por um sistema robusto e governado.

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

## Estrutura do Projeto

```
supply_matriz_de_merecimento/
├── README.md
├── src/                           # Código fonte Python
│   └── Preparação de tabelas - Matriz de merecimento.py
├── .cursor/                       # Regras e configurações do Cursor
│   └── rules/
│       ├── python.mdc            # Regras para Python
│       ├── typescript.mdc        # Regras para TypeScript
│       └── pyspark.mdc           # Regras para PySpark
└── docs/                         # Documentação técnica
```

## Arquitetura do Código

### 🔧 **Módulos Principais**

O código está organizado em funções modulares e bem documentadas:

#### **1. Carregamento de Dados**
- `load_estoque_loja_data()`: Dados de estoque das lojas ativas
- `load_mercadoria_data()`: Dados de mercadorias e classificações
- `build_sales_view()`: Visão unificada e agregada de vendas

#### **2. Processamento de Dados**
- `create_base_merecimento()`: União de estoque, vendas e mercadorias
- `add_rolling_90_metrics()`: Cálculo de métricas de média móvel de 90 dias
- `create_analysis_with_rupture_flags()`: Análise com flags de ruptura

#### **3. Mapeamento de Abastecimento**
- `load_cd_characteristics()`: Características dos Centros de Distribuição
- `load_store_characteristics()`: Características das lojas ativas
- `load_supply_plan_mapping()`: Mapeamento de plano de abastecimento
- `create_complete_supply_mapping()`: Mapeamento completo de abastecimento

#### **4. Finalização e Persistência**
- `create_final_merecimento_base()`: Base final de merecimento
- `save_merecimento_table()`: Salvamento como tabela Delta

### 📚 **Padrões de Qualidade**

- **Type Hints**: Tipagem completa para todos os parâmetros e retornos
- **Docstrings**: Documentação detalhada seguindo padrão Google
- **Funções Modulares**: Cada função com responsabilidade única
- **Nomenclatura Consistente**: Padrões de nomenclatura Python
- **Tratamento de Erros**: Validações e verificações robustas

### 🚀 **Funcionalidades Principais**

1. **Cálculo de Médias Móveis**: Métricas de 90 dias para receita e quantidade
2. **Análise de Ruptura**: Identificação e cálculo de impacto de rupturas
3. **Mapeamento de Supply Chain**: Relacionamento entre CDs, lojas e rotas
4. **Normalização de Dados**: Padronização de IDs e formatos
5. **Persistência Delta**: Salvamento otimizado para análise posterior

## Como Usar

### **Pré-requisitos**
- Databricks workspace configurado
- Acesso às tabelas de dados necessárias
- Permissões para criação de tabelas Delta

### **Execução**
```python
# O código está estruturado para execução direta no Databricks
# Cada seção pode ser executada independentemente
# A tabela final será salva em: databox.bcg_comum.supply_base_merecimento_diario
```

### **Configuração**
- Ajustar datas de início e fim conforme necessário
- Configurar parâmetros de filtro para diferentes categorias
- Personalizar regras de negócio específicas

## Status do Projeto

✅ **Código Refatorado e Documentado** - Estrutura modular implementada
🚧 **Em Desenvolvimento** - Ferramenta para discussão e implementação

### **Últimas Atualizações**
- **Refatoração completa** do código para funções modulares
- **Documentação completa** com docstrings e type hints
- **Organização estrutural** seguindo melhores práticas Python/PySpark
- **Padrões de qualidade** implementados para manutenibilidade

---

*Desenvolvido para o time de Supply do Grupo Casas Bahia*

**Autor**: Scardini  
**Data**: 2025  
**Versão**: 2.0 - Refatorado e Documentado
