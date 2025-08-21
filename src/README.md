# Estrutura do Código Fonte

## Visão Geral

Este diretório contém a implementação do pipeline de matriz de merecimento, organizado em três camadas principais seguindo princípios de arquitetura limpa e separação de responsabilidades.

## Arquitetura do Pipeline

```
src/
├── domain/           # Camada de dados brutos
├── processed/        # Camada de dados processados
├── feature/          # Camada de engenharia de features
├── config.py         # Configurações centralizadas
├── pipeline_orchestrator.py  # Orquestrador principal
└── main.py           # Script de execução
```

## Camadas do Pipeline

### 1. Domain Layer (`domain/`)

**Responsabilidade**: Carregamento e validação de dados brutos do sistema.

**Componentes**:
- `data_loader.py`: Carregador de dados brutos (vendas, produtos, lojas, grupos de necessidade)
- `__init__.py`: Documentação da camada

**Funcionalidades**:
- Carregamento de dados de vendas históricas
- Validação de qualidade dos dados
- Configuração de caminhos de dados
- Tratamento de erros de carregamento

### 2. Processed Layer (`processed/`)

**Responsabilidade**: Limpeza, processamento e preparação de dados para análise.

**Componentes**:
- `data_processor.py`: Processador de dados com limpeza e agregação
- `__init__.py`: Documentação da camada

**Funcionalidades**:
- Remoção de outliers e anomalias
- Filtros de qualidade de dados
- Agregação temporal (mensal, semanal)
- Cálculo de médias móveis (3M, 6M, 9M, 12M)
- Filtros para períodos anormais (eventos)

### 3. Feature Layer (`feature/`)

**Responsabilidade**: Engenharia de features para o modelo de merecimento.

**Componentes**:
- `feature_engineer.py`: Criador de features temporais, de demanda e de risco
- `__init__.py`: Documentação da camada

**Funcionalidades**:
- Features temporais (tendências, sazonalidade, volatilidade)
- Features de demanda (média, estabilidade, consistência)
- Features de risco (ruptura, padrões de demanda)
- Features de loja (região, cluster, tamanho)
- Features de produto (categoria, grupo de necessidade, preço)
- Normalização e codificação de variáveis categóricas

## Componentes Principais

### PipelineOrchestrator

**Arquivo**: `pipeline_orchestrator.py`

**Responsabilidade**: Coordenação e execução do pipeline completo.

**Funcionalidades**:
- Orquestração das três camadas
- Gerenciamento de configurações
- Logging e monitoramento
- Validação de qualidade dos dados
- Geração de relatórios de execução

### Configurações

**Arquivo**: `config.py`

**Responsabilidade**: Centralização de todas as configurações do pipeline.

**Seções**:
- `DataPaths`: Caminhos para dados e arquivos
- `ProcessingParameters`: Parâmetros de processamento
- `FeatureParameters`: Parâmetros de engenharia de features
- `ModelParameters`: Parâmetros do modelo
- `LoggingConfig`: Configurações de logging
- `PipelineConfig`: Configuração geral unificada

### Script Principal

**Arquivo**: `main.py`

**Responsabilidade**: Interface de linha de comando para execução do pipeline.

**Funcionalidades**:
- Parse de argumentos da linha de comando
- Configuração dinâmica do pipeline
- Execução com tratamento de erros
- Logging configurável
- Validação de parâmetros

## Execução do Pipeline

### Execução Básica

```bash
# Execução com configurações padrão
python src/main.py

# Execução com período personalizado
python src/main.py --start-date 2023-01-01 --end-date 2023-12-31

# Execução com parâmetros personalizados
python src/main.py \
    --start-date 2023-01-01 \
    --end-date 2023-12-31 \
    --outlier-threshold 2.5 \
    --min-sales-threshold 5 \
    --max-sales-threshold 5000 \
    --lookback-periods 1 3 6 \
    --normalize-features \
    --log-level DEBUG
```

### Parâmetros Disponíveis

- `--start-date`: Data de início para análise
- `--end-date`: Data de fim para análise
- `--group-columns`: Colunas para agrupamento (padrão: sku store_id)
- `--outlier-threshold`: Threshold para detecção de outliers
- `--min-sales-threshold`: Threshold mínimo de vendas
- `--max-sales-threshold`: Threshold máximo de vendas
- `--lookback-periods`: Períodos para lookback em meses
- `--normalize-features`: Normalizar features (padrão: True)
- `--log-level`: Nível de logging (DEBUG, INFO, WARNING, ERROR)

## Fluxo de Dados

```
1. Domain Layer
   ↓ Carrega dados brutos
   ↓ Valida qualidade
   
2. Processed Layer
   ↓ Limpa e processa dados
   ↓ Remove outliers e anomalias
   ↓ Calcula médias móveis
   ↓ Filtra períodos anormais
   
3. Feature Layer
   ↓ Cria features temporais
   ↓ Cria features de demanda
   ↓ Cria features de risco
   ↓ Cria features de loja/produto
   ↓ Normaliza features
   
4. Output
   ↓ Dataset final com features
   ↓ Relatórios de qualidade
   ↓ Logs de execução
```

## Configuração de Dados

### Estrutura de Diretórios

```
data/
├── raw/              # Dados brutos
│   ├── sales.csv     # Dados de vendas
│   ├── products.csv  # Catálogo de produtos
│   ├── stores.csv    # Dados de lojas
│   └── need_groups.csv # Grupos de necessidade
├── processed/        # Dados processados
├── features/         # Features criadas
└── output/           # Resultados finais

logs/                 # Logs de execução
```

### Formato dos Dados

#### Dados de Vendas (`sales.csv`)
- `sku`: Código do produto
- `store_id`: ID da loja
- `sales_date`: Data da venda
- `sales_amount`: Valor da venda
- `sales_quantity`: Quantidade vendida

#### Catálogo de Produtos (`products.csv`)
- `sku`: Código do produto
- `category`: Categoria do produto
- `need_group`: Grupo de necessidade
- `price`: Preço do produto

#### Dados de Lojas (`stores.csv`)
- `store_id`: ID da loja
- `region`: Região da loja
- `cluster`: Cluster da loja
- `store_size`: Tamanho da loja

## Logging e Monitoramento

### Níveis de Log

- **DEBUG**: Informações detalhadas para desenvolvimento
- **INFO**: Informações gerais de execução
- **WARNING**: Avisos sobre possíveis problemas
- **ERROR**: Erros que impedem a execução

### Arquivos de Log

- Logs são salvos em `logs/pipeline_YYYYMMDD_HHMMSS.log`
- Rotação automática de logs (10MB por arquivo, máximo 5 arquivos)
- Logs também são exibidos no console

## Tratamento de Erros

### Validações Implementadas

- Verificação de dados vazios
- Validação de qualidade dos dados
- Tratamento de outliers e anomalias
- Verificação de integridade dos dados
- Validação de parâmetros de entrada

### Recuperação de Erros

- Logging detalhado de erros
- Continuação da execução quando possível
- Fallbacks para valores padrão
- Notificações de problemas críticos

## Extensibilidade

### Adicionando Novas Features

1. Crie novos métodos em `FeatureEngineer`
2. Adicione configurações em `FeatureParameters`
3. Integre no pipeline em `PipelineOrchestrator`
4. Atualize documentação

### Adicionando Novos Processadores

1. Crie novos métodos em `DataProcessor`
2. Adicione configurações em `ProcessingParameters`
3. Integre no pipeline em `PipelineOrchestrator`
4. Atualize documentação

### Adicionando Novas Fontes de Dados

1. Crie novos métodos em `DomainDataLoader`
2. Adicione configurações em `DataPaths`
3. Integre no pipeline em `PipelineOrchestrator`
4. Atualize documentação

## Dependências

### Principais

- `pandas`: Manipulação de dados
- `numpy`: Computação numérica
- `scikit-learn`: Machine learning e pré-processamento

### Desenvolvimento

- `pytest`: Testes unitários
- `black`: Formatação de código
- `flake8`: Linting
- `jupyter`: Desenvolvimento interativo

## Próximos Passos

### Implementações Pendentes

- [ ] Salvamento de resultados intermediários
- [ ] Carregamento de configurações de arquivo
- [ ] Validação avançada de dados
- [ ] Testes unitários
- [ ] Documentação Sphinx
- [ ] Integração com Databricks
- [ ] Sistema de alertas
- [ ] Dashboard de monitoramento

### Melhorias Planejadas

- [ ] Cache de dados processados
- [ ] Processamento paralelo
- [ ] Métricas de performance
- [ ] Versionamento de dados
- [ ] Pipeline incremental
- [ ] Integração com sistemas externos
