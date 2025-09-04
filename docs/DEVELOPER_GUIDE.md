# Developer Guide - Supply Matriz de Merecimento

## 📋 Visão Geral

Este guia técnico fornece informações detalhadas para desenvolvedores que trabalham com o sistema de matriz de merecimento, incluindo arquitetura, padrões de código, e melhores práticas.

## 🏗️ Arquitetura do Sistema

### **Estrutura Modular**

O sistema é construído com uma arquitetura modular que separa responsabilidades:

```
src/
├── calculo_matriz_de_merecimento_unificado.py  # Núcleo do sistema
├── Preparacao_tabelas_Matriz_merecimento.py    # Preparação de dados
├── Salvar_matrizes_calculadas_csv.py           # Exportação
└── analysis/                                   # Análises e utilitários
    ├── Analise_demanda_matriz_telas.py
    ├── Analise_demanda_matriz_antiga.py
    ├── analise_elasticidade_demanda.py
    ├── analise_elasticidade_eventos.py
    ├── analise_factual_comparacao_matrizes.py
    └── analise_resultados_factuais.py
```

### **Padrões de Design**

#### **1. Funções Modulares**
- **Responsabilidade única**: Cada função tem um propósito específico
- **Reutilização**: Funções podem ser reutilizadas em diferentes contextos
- **Testabilidade**: Funções isoladas são mais fáceis de testar

#### **2. Configuração Centralizada**
```python
REGRAS_AGRUPAMENTO = {
    "DIRETORIA DE TELAS": {
        "coluna_grupo_necessidade": "gemeos",
        "tipo_agrupamento": "gêmeos",
        "descricao": "Agrupamento por produtos similares (gêmeos)"
    }
    # ... outras categorias
}
```

#### **3. Tratamento de Erros**
- **Validação de entrada**: Verificação de parâmetros
- **Logs informativos**: Mensagens claras para debugging
- **Fallbacks**: Valores padrão quando apropriado

## 🔧 Padrões de Código

### **Python Standards**

#### **Type Hints**
```python
def executar_calculo_matriz_merecimento(
    categoria: str,
    data_inicio: str = "2024-01-01",
    sigma_meses_atipicos: float = 3.0
) -> DataFrame:
    """Executa o cálculo da matriz de merecimento."""
    pass
```

#### **Docstrings**
```python
def calcular_medidas_centrais_com_medias_aparadas(
    df: DataFrame,
    janelas_moveis: List[int],
    percentual_corte: float = 0.10
) -> DataFrame:
    """
    Calcula medidas centrais incluindo médias aparadas.
    
    Args:
        df: DataFrame com dados de vendas
        janelas_moveis: Lista de janelas móveis em dias
        percentual_corte: Percentual de corte para médias aparadas
        
    Returns:
        DataFrame com medidas centrais calculadas
    """
    pass
```

#### **Nomenclatura**
- **Variáveis**: `snake_case` (ex: `df_resultado`, `total_vendas`)
- **Funções**: `snake_case` (ex: `calcular_merecimento_cd`)
- **Constantes**: `UPPER_CASE` (ex: `JANELAS_MOVEIS`)
- **Classes**: `PascalCase` (ex: `MatrizMerecimento`)

### **PySpark Standards**

#### **DataFrame Operations**
```python
# Usar method chaining quando apropriado
df_resultado = (
    df_base
    .filter(F.col("categoria") == categoria)
    .groupBy("grupo_necessidade")
    .agg(F.sum("quantidade").alias("total"))
    .orderBy(F.desc("total"))
)
```

#### **Window Functions**
```python
# Definir janelas para cálculos móveis
window_spec = Window.partitionBy("grupo_necessidade").orderBy("data").rowsBetween(-89, 0)

df_com_media = df_base.withColumn(
    "media_90_dias",
    F.avg("quantidade").over(window_spec)
)
```

#### **Cache Strategy**
```python
# Cache DataFrames que são reutilizados
df_base.cache()
df_mapeamentos.cache()

# Unpersist quando não precisar mais
df_base.unpersist()
```

## 🧪 Testing

### **Estrutura de Testes**

```
tests/
├── __init__.py
├── test_calculo_matriz.py
├── test_medidas_centrais.py
├── test_deteccao_outliers.py
└── test_analise_elasticidade.py
```

### **Testes Unitários**

```python
import unittest
from pyspark.sql import SparkSession
from src.calculo_matriz_de_merecimento_unificado import calcular_medidas_centrais

class TestMedidasCentrais(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("test").getOrCreate()
        
    def test_calculo_media_90_dias(self):
        # Arrange
        df_teste = self.spark.createDataFrame([
            (1, "2024-01-01", 10),
            (1, "2024-01-02", 20)
        ], ["sku", "data", "quantidade"])
        
        # Act
        resultado = calcular_medidas_centrais(df_teste, [90])
        
        # Assert
        self.assertEqual(resultado.count(), 2)
        
    def tearDown(self):
        self.spark.stop()
```

### **Testes de Integração**

```python
def test_fluxo_completo_matriz_telas():
    """Testa o fluxo completo para categoria de telas."""
    # Arrange
    categoria = "DIRETORIA DE TELAS"
    
    # Act
    resultado = executar_calculo_matriz_merecimento(categoria)
    
    # Assert
    assert resultado.count() > 0
    assert "grupo_de_necessidade" in resultado.columns
    assert "Merecimento_Final_Media90_Qt_venda_sem_ruptura" in resultado.columns
```

## 📊 Performance

### **Otimizações PySpark**

#### **1. Particionamento**
```python
# Particionar por colunas frequentemente usadas em joins
df_base = df_base.repartition("cdfilial", "grupo_necessidade")
```

#### **2. Broadcast Joins**
```python
# Para tabelas pequenas de mapeamento
df_mapeamentos = broadcast(df_mapeamentos)
df_resultado = df_base.join(df_mapeamentos, "sku", "left")
```

#### **3. Filtros Precoces**
```python
# Aplicar filtros antes de operações custosas
df_filtrado = (
    df_base
    .filter(F.col("data") >= data_inicio)
    .filter(F.col("categoria") == categoria)
)
```

### **Monitoramento de Performance**

#### **Logs de Performance**
```python
import time

def executar_com_timing(func, *args, **kwargs):
    """Executa função com medição de tempo."""
    inicio = time.time()
    resultado = func(*args, **kwargs)
    fim = time.time()
    
    print(f"Execução de {func.__name__}: {fim - inicio:.2f} segundos")
    return resultado
```

#### **Métricas de Cluster**
```python
# Monitorar uso de recursos
def monitorar_recursos():
    """Monitora uso de recursos do cluster."""
    print(f"Executors ativos: {spark.sparkContext.statusTracker().getExecutorInfos()}")
    print(f"Jobs ativos: {spark.sparkContext.statusTracker().getJobIdsForGroup()}")
```

## 🔍 Debugging

### **Logs Estruturados**

```python
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def carregar_dados_base(categoria: str) -> DataFrame:
    """Carrega dados base com logging."""
    logger.info(f"Carregando dados base para categoria: {categoria}")
    
    try:
        df = spark.table("databox.bcg_comum.supply_base_merecimento_diario")
        logger.info(f"Dados carregados: {df.count()} registros")
        return df
    except Exception as e:
        logger.error(f"Erro ao carregar dados: {str(e)}")
        raise
```

### **Validação de Dados**

```python
def validar_dataframe(df: DataFrame, nome: str) -> None:
    """Valida DataFrame com verificações básicas."""
    logger.info(f"Validando DataFrame: {nome}")
    
    # Verificar se tem dados
    if df.count() == 0:
        raise ValueError(f"DataFrame {nome} está vazio")
    
    # Verificar colunas essenciais
    colunas_essenciais = ["cdfilial", "cd_primario", "grupo_necessidade"]
    for col in colunas_essenciais:
        if col not in df.columns:
            raise ValueError(f"Coluna {col} não encontrada em {nome}")
    
    logger.info(f"DataFrame {nome} validado com sucesso")
```

### **Debugging de Queries**

```python
# Habilitar explain para debugging
df.explain(True)

# Usar show() para inspecionar dados
df.show(20, truncate=False)

# Verificar schema
df.printSchema()
```

## 🚀 Deployment

### **Ambiente Databricks**

#### **Configuração do Cluster**
```python
# Configurações recomendadas
cluster_config = {
    "num_workers": 4,
    "node_type_id": "i3.xlarge",
    "spark_version": "13.3.x-scala2.12",
    "spark_conf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    }
}
```

#### **Bibliotecas Necessárias**
```python
# Bibliotecas Python
libraries = [
    "pandas>=1.5.0",
    "plotly>=5.0.0",
    "openpyxl>=3.0.0"
]
```

### **Pipeline de CI/CD**

#### **GitHub Actions**
```yaml
name: Test and Deploy
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: python -m pytest tests/
```

## 📚 Documentação

### **Padrões de Documentação**

#### **README Files**
- **README.md**: Documentação principal do projeto
- **README.md** (pasta analysis): Documentação específica da pasta
- **README_ELASTICIDADE_DATABRICKS.md**: Documentação específica para Databricks

#### **Docstrings**
```python
def calcular_merecimento_cd(
    df: DataFrame,
    grupo_necessidade: str,
    medida: str
) -> DataFrame:
    """
    Calcula merecimento a nível de Centro de Distribuição.
    
    Esta função implementa a primeira camada do cálculo de merecimento,
    agregando demanda por CD e grupo de necessidade.
    
    Args:
        df: DataFrame com dados de vendas por loja
        grupo_necessidade: Nome da coluna de agrupamento
        medida: Nome da coluna de medida (ex: 'Media90_Qt_venda_sem_ruptura')
        
    Returns:
        DataFrame com merecimento agregado por CD
        
    Raises:
        ValueError: Se grupo_necessidade não for encontrado
        KeyError: Se medida não for encontrada
        
    Example:
        >>> df_resultado = calcular_merecimento_cd(df_vendas, 'gemeos', 'Media90_Qt_venda_sem_ruptura')
        >>> df_resultado.show()
    """
    pass
```

### **Comentários no Código**

```python
# Configuração de parâmetros para detecção de outliers
# Valores baseados em análise estatística de dados históricos
PARAMETROS_OUTLIERS = {
    "desvios_meses_atipicos": 3,  # 3σ para meses atípicos
    "desvios_historico_cd": 3,     # 3σ para outliers CD
    "desvios_historico_loja": 3,   # 3σ para outliers loja
    "desvios_atacado_cd": 1.5,     # 1.5σ para atacado CD (mais restritivo)
    "desvios_atacado_loja": 1.5    # 1.5σ para atacado loja (mais restritivo)
}
```

## 🔧 Manutenção

### **Versionamento**

#### **Semantic Versioning**
- **MAJOR**: Mudanças incompatíveis na API
- **MINOR**: Novas funcionalidades compatíveis
- **PATCH**: Correções de bugs

#### **Changelog**
```markdown
## [2.0.0] - 2025-01-XX

### Added
- Sistema unificado para todas as categorias
- Análise de elasticidade de demanda
- Métricas de qualidade da matriz

### Changed
- Refatoração completa da arquitetura
- Melhoria na detecção de outliers
- Otimização de performance

### Fixed
- Correção de paths para execução no Databricks
- Correção de bugs na detecção de outliers
- Melhoria na validação de dados
```

### **Monitoramento**

#### **Métricas de Sistema**
```python
def coletar_metricas_sistema():
    """Coleta métricas do sistema para monitoramento."""
    return {
        "timestamp": datetime.now().isoformat(),
        "total_skus": df_skus.count(),
        "total_lojas": df_lojas.count(),
        "total_grupos": df_grupos.count(),
        "tempo_execucao": tempo_execucao,
        "memoria_utilizada": memoria_utilizada
    }
```

## 🤝 Contribuição

### **Processo de Contribuição**

1. **Fork** do repositório
2. **Criar branch** para feature/fix
3. **Implementar** com testes
4. **Documentar** mudanças
5. **Pull request** com descrição detalhada
6. **Code review** obrigatório
7. **Merge** após aprovação

### **Code Review Checklist**

- [ ] Código segue padrões estabelecidos
- [ ] Testes passam
- [ ] Documentação atualizada
- [ ] Performance considerada
- [ ] Tratamento de erros implementado
- [ ] Logs apropriados adicionados

## 📞 Suporte

### **Canais de Suporte**

1. **Documentação**: Consultar READMEs e docstrings
2. **Issues**: GitHub Issues para bugs e features
3. **Discussões**: GitHub Discussions para dúvidas
4. **Email**: Contato direto com equipe

### **Informações para Suporte**

Ao solicitar suporte, incluir:
- **Versão** do sistema
- **Ambiente** (Databricks, local)
- **Logs** de erro
- **Dados** de exemplo (se aplicável)
- **Passos** para reproduzir o problema

---

**Versão**: 2.0.0  
**Última Atualização**: Janeiro 2025  
**Mantenedor**: Equipe de Supply Chain Analytics
