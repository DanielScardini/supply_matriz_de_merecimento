# API Reference - Supply Matriz de Merecimento

## 📋 Visão Geral

Esta documentação fornece referência completa da API do sistema de matriz de merecimento, incluindo funções, parâmetros, retornos e exemplos de uso.

## 🚀 Funções Principais

### **`executar_calculo_matriz_merecimento`**

Função principal para execução do cálculo da matriz de merecimento.

#### **Assinatura**
```python
def executar_calculo_matriz_merecimento(
    categoria: str,
    data_inicio: str = "2024-01-01",
    data_calculo: str = "2025-06-30",
    sigma_meses_atipicos: float = 3.0,
    sigma_outliers_cd: float = 3.0,
    sigma_outliers_loja: float = 3.0,
    sigma_atacado_cd: float = 1.5,
    sigma_atacado_loja: float = 1.5
) -> DataFrame
```

#### **Parâmetros**
- **`categoria`** (str): Categoria da diretoria
  - Valores aceitos: `"DIRETORIA DE TELAS"`, `"DIRETORIA TELEFONIA CELULAR"`, `"DIRETORIA LINHA BRANCA"`, `"DIRETORIA LINHA LEVE"`, `"DIRETORIA INFO/GAMES"`
- **`data_inicio`** (str): Data de início do período de análise (formato: "YYYY-MM-DD")
- **`data_calculo`** (str): Data para cálculo de merecimento (formato: "YYYY-MM-DD")
- **`sigma_meses_atipicos`** (float): Sensibilidade para detecção de meses atípicos (padrão: 3.0)
- **`sigma_outliers_cd`** (float): Sensibilidade para outliers a nível CD (padrão: 3.0)
- **`sigma_outliers_loja`** (float): Sensibilidade para outliers a nível loja (padrão: 3.0)
- **`sigma_atacado_cd`** (float): Sensibilidade para outliers atacado CD (padrão: 1.5)
- **`sigma_atacado_loja`** (float): Sensibilidade para outliers atacado loja (padrão: 1.5)

#### **Retorno**
- **`DataFrame`**: DataFrame com matriz de merecimento calculada

#### **Exemplo de Uso**
```python
# Execução básica
df_telas = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")

# Execução com parâmetros personalizados
df_telefonia = executar_calculo_matriz_merecimento(
    categoria="DIRETORIA TELEFONIA CELULAR",
    data_inicio="2024-01-01",
    data_calculo="2025-06-30",
    sigma_meses_atipicos=2.5,
    sigma_outliers_cd=2.8
)
```

### **`carregar_dados_base`**

Carrega dados base da tabela de merecimento.

#### **Assinatura**
```python
def carregar_dados_base(
    data_inicio: str,
    data_fim: str,
    categoria: str
) -> DataFrame
```

#### **Parâmetros**
- **`data_inicio`** (str): Data de início do período
- **`data_fim`** (str): Data de fim do período
- **`categoria`** (str): Categoria da diretoria

#### **Retorno**
- **`DataFrame`**: DataFrame com dados base carregados

#### **Exemplo de Uso**
```python
df_base = carregar_dados_base(
    data_inicio="2024-01-01",
    data_fim="2025-06-30",
    categoria="DIRETORIA DE TELAS"
)
```

### **`carregar_mapeamentos_produtos`**

Carrega mapeamentos de produtos (modelos e gêmeos).

#### **Assinatura**
```python
def carregar_mapeamentos_produtos() -> Tuple[DataFrame, DataFrame]
```

#### **Retorno**
- **`Tuple[DataFrame, DataFrame]`**: Tupla com DataFrames de modelos e gêmeos

#### **Exemplo de Uso**
```python
df_modelos, df_gemeos = carregar_mapeamentos_produtos()
```

### **`aplicar_mapeamentos_produtos`**

Aplica mapeamentos de produtos aos dados base.

#### **Assinatura**
```python
def aplicar_mapeamentos_produtos(
    df_base: DataFrame,
    df_modelos: DataFrame,
    df_gemeos: DataFrame
) -> DataFrame
```

#### **Parâmetros**
- **`df_base`** (DataFrame): DataFrame base
- **`df_modelos`** (DataFrame): DataFrame de mapeamento de modelos
- **`df_gemeos`** (DataFrame): DataFrame de mapeamento de gêmeos

#### **Retorno**
- **`DataFrame`**: DataFrame com mapeamentos aplicados

#### **Exemplo de Uso**
```python
df_mapeado = aplicar_mapeamentos_produtos(df_base, df_modelos, df_gemeos)
```

### **`determinar_grupo_necessidade`**

Determina grupo de necessidade baseado na categoria.

#### **Assinatura**
```python
def determinar_grupo_necessidade(
    df: DataFrame,
    categoria: str
) -> DataFrame
```

#### **Parâmetros**
- **`df`** (DataFrame): DataFrame com dados mapeados
- **`categoria`** (str): Categoria da diretoria

#### **Retorno**
- **`DataFrame`**: DataFrame com grupo de necessidade definido

#### **Exemplo de Uso**
```python
df_grupo = determinar_grupo_necessidade(df_mapeado, "DIRETORIA DE TELAS")
```

### **`detectar_outliers_meses_atipicos`**

Detecta meses atípicos baseado em desvios padrão.

#### **Assinatura**
```python
def detectar_outliers_meses_atipicos(
    df: DataFrame,
    sigma: float = 3.0
) -> DataFrame
```

#### **Parâmetros**
- **`df`** (DataFrame): DataFrame com dados
- **`sigma`** (float): Número de desvios padrão para detecção

#### **Retorno**
- **`DataFrame`**: DataFrame com outliers identificados

#### **Exemplo de Uso**
```python
df_outliers = detectar_outliers_meses_atipicos(df_grupo, sigma=2.5)
```

### **`filtrar_meses_atipicos`**

Filtra meses atípicos identificados.

#### **Assinatura**
```python
def filtrar_meses_atipicos(
    df: DataFrame,
    df_outliers: DataFrame
) -> DataFrame
```

#### **Parâmetros**
- **`df`** (DataFrame): DataFrame original
- **`df_outliers`** (DataFrame): DataFrame com outliers identificados

#### **Retorno**
- **`DataFrame`**: DataFrame com meses atípicos removidos

#### **Exemplo de Uso**
```python
df_filtrado = filtrar_meses_atipicos(df_grupo, df_outliers)
```

### **`calcular_medidas_centrais_com_medias_aparadas`**

Calcula medidas centrais incluindo médias aparadas.

#### **Assinatura**
```python
def calcular_medidas_centrais_com_medias_aparadas(
    df: DataFrame,
    janelas_moveis: List[int] = [90, 180, 270, 360],
    percentual_corte: float = 0.10
) -> DataFrame
```

#### **Parâmetros**
- **`df`** (DataFrame): DataFrame com dados filtrados
- **`janelas_moveis`** (List[int]): Lista de janelas móveis em dias
- **`percentual_corte`** (float): Percentual de corte para médias aparadas

#### **Retorno**
- **`DataFrame`**: DataFrame com medidas centrais calculadas

#### **Exemplo de Uso**
```python
df_medidas = calcular_medidas_centrais_com_medias_aparadas(
    df_filtrado,
    janelas_moveis=[90, 180, 270, 360],
    percentual_corte=0.10
)
```

### **`calcular_merecimento_cd`**

Calcula merecimento a nível de Centro de Distribuição.

#### **Assinatura**
```python
def calcular_merecimento_cd(
    df: DataFrame,
    grupo_necessidade: str
) -> DataFrame
```

#### **Parâmetros**
- **`df`** (DataFrame): DataFrame com medidas calculadas
- **`grupo_necessidade`** (str): Nome da coluna de grupo de necessidade

#### **Retorno**
- **`DataFrame`**: DataFrame com merecimento CD calculado

#### **Exemplo de Uso**
```python
df_merecimento_cd = calcular_merecimento_cd(df_medidas, "grupo_de_necessidade")
```

### **`calcular_merecimento_interno_cd`**

Calcula participação interna ao CD.

#### **Assinatura**
```python
def calcular_merecimento_interno_cd(
    df: DataFrame,
    grupo_necessidade: str
) -> DataFrame
```

#### **Parâmetros**
- **`df`** (DataFrame): DataFrame com medidas calculadas
- **`grupo_necessidade`** (str): Nome da coluna de grupo de necessidade

#### **Retorno**
- **`DataFrame`**: DataFrame com participação interna calculada

#### **Exemplo de Uso**
```python
df_participacao = calcular_merecimento_interno_cd(df_medidas, "grupo_de_necessidade")
```

### **`calcular_merecimento_final`**

Calcula merecimento final combinando CD e participação interna.

#### **Assinatura**
```python
def calcular_merecimento_final(
    df_merecimento_cd: DataFrame,
    df_participacao: DataFrame
) -> DataFrame
```

#### **Parâmetros**
- **`df_merecimento_cd`** (DataFrame): DataFrame com merecimento CD
- **`df_participacao`** (DataFrame): DataFrame com participação interna

#### **Retorno**
- **`DataFrame`**: DataFrame com merecimento final calculado

#### **Exemplo de Uso**
```python
df_final = calcular_merecimento_final(df_merecimento_cd, df_participacao)
```

## 📊 Funções de Análise

### **`executar_analise_elasticidade_demanda`**

Executa análise de elasticidade de demanda por gêmeos.

#### **Assinatura**
```python
def executar_analise_elasticidade_demanda() -> None
```

#### **Retorno**
- **`None`**: Função executa análise e salva resultados

#### **Exemplo de Uso**
```python
executar_analise_elasticidade_demanda()
```

### **`executar_analise_factual_comparacao_matrizes`**

Executa análise factual comparando matrizes calculadas com DRP.

#### **Assinatura**
```python
def executar_analise_factual_comparacao_matrizes() -> None
```

#### **Retorno**
- **`None`**: Função executa análise e salva resultados

#### **Exemplo de Uso**
```python
executar_analise_factual_comparacao_matrizes()
```

### **`executar_analise_resultados_factuais`**

Executa análise de resultados factuais vs. previstos.

#### **Assinatura**
```python
def executar_analise_resultados_factuais() -> None
```

#### **Retorno**
- **`None`**: Função executa análise e salva resultados

#### **Exemplo de Uso**
```python
executar_analise_resultados_factuais()
```

## 🔧 Funções de Utilitários

### **`validar_resultados`**

Valida resultados da matriz de merecimento.

#### **Assinatura**
```python
def validar_resultados(
    df: DataFrame,
    categoria: str
) -> Dict[str, Any]
```

#### **Parâmetros**
- **`df`** (DataFrame): DataFrame com resultados
- **`categoria`** (str): Categoria da diretoria

#### **Retorno**
- **`Dict[str, Any]`**: Dicionário com métricas de validação

#### **Exemplo de Uso**
```python
metricas = validar_resultados(df_final, "DIRETORIA DE TELAS")
print(f"Total de registros: {metricas['total_registros']}")
```

### **`salvar_matriz_csv`**

Salva matriz de merecimento em formato CSV.

#### **Assinatura**
```python
def salvar_matriz_csv(
    df: DataFrame,
    categoria: str,
    caminho: str = "/dbfs/resultados/"
) -> None
```

#### **Parâmetros**
- **`df`** (DataFrame): DataFrame com matriz
- **`categoria`** (str): Categoria da diretoria
- **`caminho`** (str): Caminho para salvar arquivo

#### **Retorno**
- **`None`**: Função salva arquivo CSV

#### **Exemplo de Uso**
```python
salvar_matriz_csv(df_final, "DIRETORIA DE TELAS", "/dbfs/resultados/")
```

### **`carregar_matriz_csv`**

Carrega matriz de merecimento de arquivo CSV.

#### **Assinatura**
```python
def carregar_matriz_csv(
    categoria: str,
    caminho: str = "/dbfs/resultados/"
) -> DataFrame
```

#### **Parâmetros**
- **`categoria`** (str): Categoria da diretoria
- **`caminho`** (str): Caminho do arquivo

#### **Retorno**
- **`DataFrame`**: DataFrame com matriz carregada

#### **Exemplo de Uso**
```python
df_carregado = carregar_matriz_csv("DIRETORIA DE TELAS", "/dbfs/resultados/")
```

## 📈 Funções de Métricas

### **`calcular_wmape`**

Calcula Weighted Mean Absolute Percentage Error.

#### **Assinatura**
```python
def calcular_wmape(
    y_real: Series,
    y_pred: Series
) -> float
```

#### **Parâmetros**
- **`y_real`** (Series): Valores reais
- **`y_pred`** (Series): Valores previstos

#### **Retorno**
- **`float`**: Valor do wMAPE

#### **Exemplo de Uso**
```python
wmape = calcular_wmape(df['real'], df['previsto'])
print(f"wMAPE: {wmape:.2f}%")
```

### **`calcular_share_error`**

Calcula Share Error (erro na distribuição de participações).

#### **Assinatura**
```python
def calcular_share_error(
    y_real: Series,
    y_pred: Series
) -> float
```

#### **Parâmetros**
- **`y_real`** (Series): Valores reais
- **`y_pred`** (Series): Valores previstos

#### **Retorno**
- **`float`**: Valor do Share Error

#### **Exemplo de Uso**
```python
se = calcular_share_error(df['real'], df['previsto'])
print(f"Share Error: {se:.2f} pp")
```

### **`calcular_cross_entropy`**

Calcula Cross Entropy entre distribuições.

#### **Assinatura**
```python
def calcular_cross_entropy(
    y_real: Series,
    y_pred: Series
) -> float
```

#### **Parâmetros**
- **`y_real`** (Series): Valores reais
- **`y_pred`** (Series): Valores previstos

#### **Retorno**
- **`float`**: Valor do Cross Entropy

#### **Exemplo de Uso**
```python
ce = calcular_cross_entropy(df['real'], df['previsto'])
print(f"Cross Entropy: {ce:.4f}")
```

### **`calcular_kl_divergence`**

Calcula KL Divergence entre distribuições.

#### **Assinatura**
```python
def calcular_kl_divergence(
    y_real: Series,
    y_pred: Series
) -> float
```

#### **Parâmetros**
- **`y_real`** (Series): Valores reais
- **`y_pred`** (Series): Valores previstos

#### **Retorno**
- **`float`**: Valor do KL Divergence

#### **Exemplo de Uso**
```python
kl = calcular_kl_divergence(df['real'], df['previsto'])
print(f"KL Divergence: {kl:.4f}")
```

## 🎨 Funções de Visualização

### **`criar_grafico_elasticidade`**

Cria gráfico de elasticidade de demanda.

#### **Assinatura**
```python
def criar_grafico_elasticidade(
    df: DataFrame,
    titulo: str,
    tipo: str = "porte"
) -> Figure
```

#### **Parâmetros**
- **`df`** (DataFrame): DataFrame com dados de elasticidade
- **`titulo`** (str): Título do gráfico
- **`tipo`** (str): Tipo de análise ("porte" ou "porte_regiao")

#### **Retorno**
- **`Figure`**: Objeto Figure do Plotly

#### **Exemplo de Uso**
```python
fig = criar_grafico_elasticidade(df_elasticidade, "Análise de Elasticidade - Telas")
fig.show()
```

### **`criar_grafico_comparacao_matrizes`**

Cria gráfico de comparação entre matrizes.

#### **Assinatura**
```python
def criar_grafico_comparacao_matrizes(
    df: DataFrame,
    titulo: str
) -> Figure
```

#### **Parâmetros**
- **`df`** (DataFrame): DataFrame com dados de comparação
- **`titulo`** (str): Título do gráfico

#### **Retorno**
- **`Figure`**: Objeto Figure do Plotly

#### **Exemplo de Uso**
```python
fig = criar_grafico_comparacao_matrizes(df_comparacao, "Comparação Matrizes")
fig.show()
```

## 🔍 Exemplos de Uso Completos

### **Exemplo 1: Cálculo Básico**
```python
# Executar cálculo para telas
df_telas = executar_calculo_matriz_merecimento("DIRETORIA DE TELAS")

# Validar resultados
metricas = validar_resultados(df_telas, "DIRETORIA DE TELAS")
print(f"Total de registros: {metricas['total_registros']}")
print(f"SKUs únicos: {metricas['skus_unicos']}")
print(f"Lojas únicas: {metricas['lojas_unicas']}")

# Salvar resultado
salvar_matriz_csv(df_telas, "DIRETORIA DE TELAS")
```

### **Exemplo 2: Cálculo com Parâmetros Personalizados**
```python
# Executar com parâmetros personalizados
df_telefonia = executar_calculo_matriz_merecimento(
    categoria="DIRETORIA TELEFONIA CELULAR",
    data_inicio="2024-01-01",
    data_calculo="2025-06-30",
    sigma_meses_atipicos=2.5,
    sigma_outliers_cd=2.8,
    sigma_outliers_loja=3.2
)

# Análise de elasticidade
executar_analise_elasticidade_demanda()

# Análise factual
executar_analise_factual_comparacao_matrizes()
```

### **Exemplo 3: Análise de Métricas**
```python
# Carregar matriz calculada
df_matriz = carregar_matriz_csv("DIRETORIA DE TELAS")

# Calcular métricas de qualidade
wmape = calcular_wmape(df_matriz['real'], df_matriz['previsto'])
se = calcular_share_error(df_matriz['real'], df_matriz['previsto'])
ce = calcular_cross_entropy(df_matriz['real'], df_matriz['previsto'])
kl = calcular_kl_divergence(df_matriz['real'], df_matriz['previsto'])

print(f"Métricas de Qualidade:")
print(f"wMAPE: {wmape:.2f}%")
print(f"Share Error: {se:.2f} pp")
print(f"Cross Entropy: {ce:.4f}")
print(f"KL Divergence: {kl:.4f}")
```

### **Exemplo 4: Visualizações**
```python
# Criar gráfico de elasticidade
fig_elasticidade = criar_grafico_elasticidade(
    df_elasticidade,
    "Análise de Elasticidade - Telas",
    tipo="porte"
)
fig_elasticidade.show()

# Criar gráfico de comparação
fig_comparacao = criar_grafico_comparacao_matrizes(
    df_comparacao,
    "Comparação Matrizes - Telas"
)
fig_comparacao.show()
```

## ⚠️ Tratamento de Erros

### **Exceções Comuns**

#### **`ValueError`**
```python
try:
    df = executar_calculo_matriz_merecimento("CATEGORIA_INVALIDA")
except ValueError as e:
    print(f"Erro de categoria: {e}")
```

#### **`KeyError`**
```python
try:
    df = carregar_dados_base("2024-01-01", "2024-12-31", "DIRETORIA DE TELAS")
except KeyError as e:
    print(f"Erro de coluna: {e}")
```

#### **`FileNotFoundError`**
```python
try:
    df = carregar_matriz_csv("DIRETORIA DE TELAS", "/caminho/inexistente/")
except FileNotFoundError as e:
    print(f"Arquivo não encontrado: {e}")
```

## 📚 Referências

### **Dependências**
- **PySpark**: 3.0+
- **Pandas**: 1.5+
- **Plotly**: 5.0+
- **OpenPyXL**: 3.0+

### **Tabelas Necessárias**
- `databox.bcg_comum.supply_base_merecimento_diario_v3`
- `data_engineering_prd.app_operacoesloja.roteirizacaolojaativa`

### **Arquivos de Mapeamento**
- `MODELOS_AJUSTE (1).csv`
- `ITENS_GEMEOS 2.csv`

---

**Versão**: 2.0.0  
**Última Atualização**: Janeiro 2025  
**Mantenedor**: Equipe de Supply Chain Analytics
