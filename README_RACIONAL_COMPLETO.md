# 📊 Racional Completo - Sistema de Matriz de Merecimento

## 🎯 Visão Geral

Este documento apresenta o **racional completo e detalhado** do sistema de cálculo da matriz de merecimento, desde a preparação das tabelas base até a exportação final para o time de supply. O sistema foi projetado para automatizar e padronizar o processo de distribuição de produtos entre lojas, substituindo métodos manuais por uma solução robusta, governada e configurável.

---

## 📋 Índice

1. [Preparação de Tabelas Base](#1-preparação-de-tabelas-base)
2. [Cálculo da Matriz de Merecimento](#2-cálculo-da-matriz-de-merecimento)
3. [Salvamento e Exportação](#3-salvamento-e-exportação)
4. [Validação e Checagem](#4-validação-e-checagem)
5. [Envio para o Time de Supply](#5-envio-para-o-time-de-supply)

---

## 1. Preparação de Tabelas Base

### 1.1. Racional da Agregação da Tabela Base

A preparação das tabelas base é o **fundamento** de todo o sistema de merecimento. Esta etapa consolida dados de múltiplas fontes em uma única tabela unificada que servirá como base para todos os cálculos subsequentes.

#### **Arquivos Principais:**
- **`Preparacao_tabelas_Matriz_merecimento.py`**: Preparação para dados OFFLINE (lojas físicas)
- **`Preparacao_tabelas_Matriz_merecimento_online.py`**: Preparação para dados ONLINE (e-commerce)

#### **Por que Agregar em uma Tabela Única?**

1. **Consistência de Dados**
   - Garante que todos os cálculos usam a mesma fonte de verdade
   - Elimina discrepâncias entre diferentes sistemas
   - Facilita auditoria e rastreabilidade

2. **Performance**
   - Evita joins repetidos em múltiplas execuções
   - Permite otimização de partições e índices
   - Reduz tempo de processamento nos cálculos

3. **Manutenibilidade**
   - Centraliza lógica de transformação de dados
   - Facilita atualizações e correções
   - Simplifica testes e validações

4. **Histórico Completo**
   - Mantém dados históricos para análises de tendências
   - Permite reprocessamento de períodos específicos
   - Suporta análises de sazonalidade

### 1.2. Estrutura da Tabela Base

A tabela `supply_base_merecimento_diario_v4` (offline) e `supply_base_merecimento_diario_v4_online` (online) contém:

#### **Dimensões (Chaves):**
- `DtAtual`: Data do registro (granularidade diária)
- `CdFilial`: Código da filial/loja/CD
- `CdSku`: Código do produto (SKU)
- `year_month`: Ano-mês para agregações mensais

#### **Métricas de Estoque:**
- `EstoqueLoja`: Quantidade de estoque na loja
- `QtdEstoqueCDVinculado`: Estoque disponível no CD vinculado
- `DDE`: Dias de Estoque (Days of Supply)
- `ClassificacaoDDE`: Classificação do DDE (baixo, médio, alto)

#### **Métricas de Vendas:**
- `Receita`: Receita total do dia
- `QtMercadoria`: Quantidade vendida
- `TeveVenda`: Flag binária (1 = teve venda, 0 = sem venda)
- `Custo`: Custo contábil

#### **Métricas de Ruptura:**
- `FlagRuptura`: Indica se há ruptura (1) ou não (0)
- `deltaRuptura`: Diferença entre demanda média e estoque disponível
- `ReceitaPerdidaRuptura`: Receita perdida devido à ruptura

#### **Métricas de Média Móvel (90 dias):**
- `Media90_Receita_venda_estq`: Média de receita dos últimos 90 dias (apenas dias com estoque)
- `Media90_Qt_venda_estq`: Média de quantidade vendida dos últimos 90 dias (apenas dias com estoque)
- `PrecoMedio90`: Preço médio dos últimos 90 dias

#### **Características de Produto:**
- `DsSku`: Descrição do SKU
- `DsSetor`: Setor do produto
- `DsCurva`: Curva ABC do produto
- `DsCurvaAbcLoja`: Curva ABC específica da loja
- `StLinha`: Status da linha (ativa/inativa)
- `DsObrigatorio`: Indica se é obrigatório (S/N)
- `DsVoltagem`: Voltagem do produto
- `TipoEntrega`: Tipo de entrega (SL, CD, etc.)

#### **Características de Filial:**
- `BandeiraLoja`: Bandeira da loja
- `NmLoja`: Nome da loja
- `NmCidadeLoja`: Cidade da loja
- `NmUFLoja`: UF da loja
- `NmPorteLoja`: Porte da loja (pequeno, médio, grande)
- `TipoLoja`: Tipo da loja
- `LatitudeLoja`, `LongitudeLoja`: Coordenadas geográficas

#### **Mapeamento de Abastecimento:**
- `CD_primario`: CD principal de abastecimento
- `CD_secundario`: CD secundário (backup)
- `LeadTime`: Tempo de viagem em dias
- `QtdCargasDia`: Quantidade de cargas por dia
- `DsCubagemCaminhao`: Cubagem do caminhão
- `DsGrupoHorario`: Grupo de horário de entrega
- `QtdSegunda`, `QtdTerca`, ..., `QtdDomingo`: Cargas por dia da semana

#### **Classificação Gerencial:**
- `NmAgrupamentoDiretoriaSetor`: Diretoria (ex: DIRETORIA DE TELAS)
- `NmSetorGerencial`: Setor gerencial
- `NmClasseGerencial`: Classe gerencial
- `NmEspecieGerencial`: Espécie gerencial

### 1.3. Processo de Construção da Tabela Base

#### **Etapa 1: Carregamento de Estoque**

```python
def load_estoque_loja_data(spark: SparkSession) -> DataFrame:
    """
    Carrega dados de estoque das lojas ativas.
    
    Filtros aplicados:
    - StLoja == "ATIVA": Apenas lojas ativas
    - DsEstoqueLojaDeposito == "L": Apenas estoque de loja (não depósito)
    - DtAtual >= data_inicio: Período mínimo de 14 meses (offline) ou 3 meses (online)
    
    Transformações:
    - Calcula DDE = VrTotalVv / VrVndCmv
    - Normaliza formato de data
    - Remove duplicatas por (DtAtual, CdSku, CdFilial)
    """
```

**Racional:**
- **14 meses (offline)**: Histórico suficiente para análises de sazonalidade e tendências
- **3 meses (online)**: Dados mais recentes, foco em operações diárias
- **Apenas lojas ativas**: Evita cálculos para lojas fechadas
- **Apenas estoque de loja**: Foca em estoque disponível para venda

#### **Etapa 2: Carregamento de Vendas**

```python
def build_sales_view(spark: SparkSession, start_date: int, end_date: int) -> DataFrame:
    """
    Constrói visão unificada de vendas.
    
    Fontes:
    - vendafaturadarateada: Vendas rateadas (online)
    - vendafaturadanaorateada: Quantidades não rateadas
    
    Filtros:
    - NmEstadoMercadoria != '1 - SALDO': Exclui saldos
    - NmTipoNegocio == 'LOJA FISICA' (offline) ou != 'LOJA FISICA' (online)
    - VrOperacao >= 0: Apenas vendas válidas
    - VrCustoContabilFilialSku >= 0: Apenas custos válidos
    
    Transformações:
    - Cria grade completa (Dt x Filial x SKU) com zeros para dias sem venda
    - Agrega receita e quantidade por dia
    - Adiciona flag TeveVenda (1 = teve venda, 0 = sem venda)
    """
```

**Racional:**
- **Grade completa**: Garante que todos os dias tenham registro (mesmo sem venda)
- **Zeros para dias sem venda**: Permite cálculo correto de médias móveis
- **União de tabelas rateadas e não rateadas**: Captura todas as vendas
- **Flag TeveVenda**: Facilita análises de frequência de vendas

#### **Etapa 3: Cálculo de Médias Móveis de 90 Dias**

```python
def add_rolling_90_metrics(df: DataFrame) -> DataFrame:
    """
    Adiciona médias móveis de 90 dias.
    
    Condição de inclusão:
    - EstoqueLoja >= 1: Apenas dias com estoque disponível
    
    Janela:
    - Window.partitionBy("CdFilial", "CdSku")
    - Window.orderBy("DayIdx")
    - Window.rangeBetween(-89, 0): Últimos 90 dias (incluindo dia atual)
    
    Métricas calculadas:
    - Media90_Receita_venda_estq: Média de receita
    - Media90_Qt_venda_estq: Média de quantidade
    """
```

**Racional:**
- **90 dias**: Período que equilibra estabilidade e responsividade
- **Apenas dias com estoque**: Evita distorções de demanda suprimida por ruptura
- **Janela por filial+SKU**: Mantém contexto local
- **Preenchimento com 0.0**: Garante que todos os registros tenham valor

#### **Etapa 4: Análise de Ruptura**

```python
def create_analysis_with_rupture_flags(df: DataFrame) -> DataFrame:
    """
    Cria flags e métricas de ruptura.
    
    FlagRuptura = 1 quando:
    - Media90_Qt_venda_estq > EstoqueLoja: Demanda média maior que estoque
    - DsObrigatorio == 'S': Apenas produtos obrigatórios
    
    Métricas calculadas:
    - deltaRuptura: Diferença entre demanda e estoque
    - PrecoMedio90: Preço médio dos últimos 90 dias
    - ReceitaPerdidaRuptura: Receita perdida = deltaRuptura × PrecoMedio90
    """
```

**Racional:**
- **Apenas produtos obrigatórios**: Foca em produtos críticos para o negócio
- **Demanda média vs estoque**: Identifica risco de ruptura
- **Receita perdida**: Quantifica impacto financeiro da ruptura

#### **Etapa 5: Mapeamento de Abastecimento**

```python
def create_complete_supply_mapping(spark: SparkSession, current_date: datetime) -> DataFrame:
    """
    Cria mapeamento completo de abastecimento.
    
    Fontes:
    - roteirizacaocentrodistribuicao: Características dos CDs
    - roteirizacaolojaativa: Características das lojas
    - PlanoAbastecimento: Mapeamento CD → Loja
    
    Transformações:
    - Normaliza IDs (remove zeros à esquerda)
    - Adiciona características do CD primário e secundário
    - Mantém informações de lead time e frequência de entrega
    """
```

**Racional:**
- **Mapeamento CD → Loja**: Essencial para cálculo de merecimento por CD
- **CD primário e secundário**: Permite estratégias de backup
- **Lead time**: Importante para planejamento de abastecimento
- **Frequência de entrega**: Impacta na distribuição de estoque

#### **Etapa 6: Consolidação de CDs (Apenas Online)**

```python
def consolidar_CD_dentro_de_outro(df: DataFrame, dict_CDs: dict) -> DataFrame:
    """
    Consolida CDs menores em CDs maiores.
    
    Exemplo:
    - CD 14 → CD 1401
    - CD 1635 → CD 1200
    
    Agregações:
    - Numéricos: soma (QtdEstoqueCDVinculado, EstoqueLoja)
    - DDE: média (mantém proporcionalidade)
    - Categorias: preferência do CD posterior
    """
```

**Racional:**
- **Consolidação**: Simplifica gestão de múltiplos CDs pequenos
- **Agregação inteligente**: Mantém propriedades estatísticas corretas
- **Preferência do CD posterior**: Preserva informações mais relevantes

### 1.4. Processamento Incremental

#### **Por que Processamento Incremental?**

1. **Eficiência**
   - Processa apenas novos dados
   - Reduz tempo de execução
   - Economiza recursos computacionais

2. **Gestão de Memória**
   - Processa em lotes de 3 meses
   - Libera memória entre lotes
   - Evita estouro de memória

3. **Atualização Contínua**
   - Permite atualização diária
   - Mantém dados sempre atualizados
   - Facilita reprocessamento de períodos específicos

#### **Estratégia de Processamento:**

```python
def process_incremental_from_start_date(
    spark: SparkSession,
    start_date: datetime,
    end_date: datetime,
    batch_size_months: int = 3
):
    """
    Processa dados incrementalmente em lotes de meses.
    
    Processo para cada lote:
    1. Deleta dados existentes do período
    2. Processa novos dados do período
    3. Salva dados atualizados
    
    Gestão de memória:
    - Cache de dados reutilizáveis (mercadoria, mapeamento de abastecimento)
    - Unpersist de dados específicos do lote após uso
    - Limpeza final de cache ao concluir
    """
```

**Racional:**
- **Lotes de 3 meses**: Balanceia eficiência e uso de memória
- **Sempre sobrescreve**: Garante consistência e atualização completa
- **Cache inteligente**: Reutiliza dados que não mudam entre lotes
- **Limpeza de memória**: Evita acúmulo de dados em cache

---

## 2. Cálculo da Matriz de Merecimento

### 2.1. Visão Geral do Processo

O cálculo da matriz de merecimento é realizado em **duas camadas hierárquicas**:

1. **Primeira Camada**: Merecimento a nível CD (por grupo de necessidade)
2. **Segunda Camada**: Distribuição interna ao CD (por filial dentro do CD)

O merecimento final é calculado como: **Merecimento Final = Merecimento CD × Proporção Interna CD**

### 2.2. Etapas de Tratamento e Parâmetros de Calibração

#### **Etapa 1: Carregamento e Preparação de Dados**

```python
def carregar_dados_base(categoria: str, data_inicio: str) -> DataFrame:
    """
    Carrega dados base para a categoria especificada.
    
    Filtros:
    - NmAgrupamentoDiretoriaSetor == categoria
    - DtAtual >= data_inicio (padrão: 18 meses atrás)
    
    Preenchimento:
    - Receita, QtMercadoria, TeveVenda, deltaRuptura: NULL → 0
    """
```

**Parâmetros de Calibração:**
- **data_inicio**: Período histórico mínimo (padrão: 18 meses)
  - **Racional**: Histórico suficiente para análises de sazonalidade e tendências
  - **Ajuste**: Pode ser reduzido para 12 meses se necessário

#### **Etapa 2: Espelhamento de Filiais**

```python
def aplicar_espelhamento_filiais(df_base: DataFrame, df_espelhamento: DataFrame) -> DataFrame:
    """
    Aplica espelhamento de filiais.
    
    Processo:
    1. Remove dados existentes das filiais espelhadas
    2. Copia dados da filial de referência para filiais espelhadas
    
    Fonte:
    - governanca_supply_inputs_matriz_merecimento.xlsx (aba: espelhamento_lojas)
    """
```

**Racional:**
- **Espelhamento**: Lojas novas ou sem histórico usam dados de lojas similares
- **Governança**: Planilha Excel permite atualização sem alterar código
- **Aplicação antes de cálculos**: Garante que todas as filiais tenham dados

#### **Etapa 3: Mapeamento de Produtos**

```python
def aplicar_mapeamentos_produtos(df: DataFrame, categoria: str, 
                                de_para_modelos: pd.DataFrame, 
                                de_para_gemeos: pd.DataFrame) -> DataFrame:
    """
    Aplica mapeamentos de produtos.
    
    Mapeamentos:
    - modelos: Mapeamento SKU → modelo (todos)
    - gemeos: Mapeamento SKU → gêmeos (apenas TELAS e TELEFONIA)
    
    Regra especial:
    - Se gemeos == '-', usa modelos como fallback
    """
```

**Racional:**
- **Mapeamento de modelos**: Padroniza nomenclatura de produtos
- **Mapeamento de gêmeos**: Agrupa produtos similares para TELAS e TELEFONIA
- **Fallback para modelos**: Garante que todos os SKUs tenham grupo de necessidade

#### **Etapa 4: Definição de Grupo de Necessidade**

```python
def determinar_grupo_necessidade(categoria: str, df: DataFrame) -> DataFrame:
    """
    Define grupo de necessidade baseado na categoria.
    
    Regras por categoria:
    - TELAS/TELEFONIA: Usa coluna 'gemeos'
    - LINHA BRANCA/LEVE: Usa NmEspecieGerencial + "_" + DsVoltagem
    - INFO/PERIFERICOS: Usa NmEspecieGerencial
    
    Tratamento de NULLs:
    - NULL → "SEM_GN" (sem grupo de necessidade)
    """
```

**Parâmetros de Calibração:**
- **Regras por categoria**: Configuráveis em `REGRAS_AGRUPAMENTO`
- **Tratamento de NULLs**: Pode ser ajustado para remover ou agrupar em "OUTROS"

**Racional:**
- **Agrupamento por gêmeos (TELAS/TELEFONIA)**: Produtos similares têm demanda similar
- **Agrupamento por espécie+voltagem (LINHA BRANCA/LEVE)**: Voltagem é fator crítico
- **Agrupamento por espécie (INFO/PERIFERICOS)**: Espécie é suficiente para agrupamento

#### **Etapa 5: Consolidação de Telas Especiais (Apenas TELAS)**

```python
def consolidar_telas_especiais_em_tv_esp(df: DataFrame, categoria: str) -> DataFrame:
    """
    Consolida telas especiais diferenciando por polegadas.
    
    Tecnologias especiais: ESP, QLED, MINI LED, QNED, OLED
    
    Consolidação:
    - <= 65 polegadas → "TV ESP SL" (Small/Large)
    - > 65 polegadas → "TV ESP SD" (Super Large)
    """
```

**Racional:**
- **Consolidação por polegadas**: Reduz granularidade excessiva
- **Diferenciação SL/SD**: Mantém distinção importante para distribuição
- **Apenas TELAS**: Específico para categoria de telas

#### **Etapa 6: Agregação por Grupo de Necessidade**

```python
df_agregado = (
    df_com_grupo
    .groupBy("grupo_de_necessidade", "CdFilial", "DtAtual", "year_month")
    .agg(
        F.sum("QtMercadoria").alias("QtMercadoria"),
        F.sum("deltaRuptura").alias("deltaRuptura"),
        F.first("tipo_agrupamento").alias("tipo_agrupamento")
    )
)
```

**Racional:**
- **Agregação por grupo**: Reduz granularidade de SKU para grupo
- **Soma de quantidades**: Agrega demanda de todos os SKUs do grupo
- **Soma de deltaRuptura**: Agrega demanda suprimida por ruptura

#### **Etapa 7: Detecção de Outliers - Meses Atípicos**

```python
def detectar_outliers_meses_atipicos(df: DataFrame, categoria: str) -> tuple:
    """
    Detecta meses atípicos por grupo de necessidade.
    
    Método:
    - Agrega QtMercadoria por grupo_de_necessidade + year_month
    - Calcula média e desvio padrão por grupo
    - Identifica meses fora de: média ± (n_sigmas × desvio)
    
    Parâmetros:
    - desvios_meses_atipicos: 2.0 (padrão)
    """
```

**Parâmetros de Calibração:**
- **desvios_meses_atipicos**: 2.0 (padrão)
  - **Ajuste mais conservador**: 2.5 (remove menos meses)
  - **Ajuste mais agressivo**: 1.5 (remove mais meses)
  - **Racional**: 2.0 equilibra remoção de outliers e preservação de dados

**Racional:**
- **Meses atípicos**: Eventos, promoções, mudanças de linha podem distorcer histórico
- **Por grupo de necessidade**: Cada grupo tem padrão próprio
- **Proteção da data de cálculo**: Data de cálculo nunca é removida

#### **Etapa 8: Filtragem de Meses Atípicos**

```python
def filtrar_meses_atipicos(df: DataFrame, df_meses_atipicos: DataFrame, 
                          data_calculo: str) -> DataFrame:
    """
    Remove meses atípicos identificados.
    
    Proteção:
    - year_month da data_calculo NUNCA é removido
    - Garante que dados mais recentes sejam preservados
    """
```

**Racional:**
- **Remoção seletiva**: Remove apenas meses identificados como atípicos
- **Proteção da data de cálculo**: Garante que dados mais recentes sejam usados
- **Join anti**: Eficiente para remover múltiplos meses

#### **Etapa 9: Remoção de Outliers - Séries Históricas**

```python
def remover_outliers_series_historicas(df: DataFrame, 
                                     coluna_valor: str = "QtMercadoria",
                                     n_sigmas_padrao: float = 3.0,
                                     n_sigmas_atacado: float = 1.5,
                                     filiais_atacado: list = None) -> DataFrame:
    """
    Remove outliers das séries históricas (grupo_de_necessidade × filial).
    
    Método:
    - Calcula média e desvio padrão por grupo_de_necessidade × filial
    - Identifica valores fora de: média ± (n_sigmas × desvio)
    - Saturam outliers no threshold (não remove, apenas limita)
    
    Tratamento especial:
    - Filiais de atacado: n_sigmas_atacado (mais conservador)
    - Outras filiais: n_sigmas_padrao
    """
```

**Parâmetros de Calibração:**
- **n_sigmas_padrao**: 3.0 (padrão)
  - **Ajuste mais conservador**: 3.5 (remove menos outliers)
  - **Ajuste mais agressivo**: 2.5 (remove mais outliers)
- **n_sigmas_atacado**: 1.5 (padrão)
  - **Racional**: Filiais de atacado têm padrão de demanda mais variável
  - **Ajuste**: Pode ser aumentado para 2.0 se necessário

**Racional:**
- **Saturação vs remoção**: Mantém dados, apenas limita valores extremos
- **Por grupo × filial**: Cada combinação tem padrão próprio
- **Tratamento especial para atacado**: Reconhece padrão diferente

#### **Etapa 10: Cálculo de Demanda Robusta**

```python
def calcular_medidas_centrais_com_medias_aparadas(df: DataFrame) -> DataFrame:
    """
    Calcula demanda robusta e médias aparadas.
    
    Processo:
    1. Amortização de demanda suprimida:
       - demandaSuprimida = min(deltaRuptura, QtMercadoria × 30%)
       - Limita demanda suprimida a 30% das vendas reais
    
    2. Demanda robusta:
       - demanda_robusta = QtMercadoria + demandaSuprimida
       - Zerar para filiais OUTLET (não abastecidas via CD)
    
    3. Médias aparadas:
       - Calcula percentis 1% e 99% da janela
       - Remove valores fora dos percentis
       - Calcula média dos valores restantes
    """
```

**Parâmetros de Calibração:**
- **PERCENTUAL_MAX_DEMANDA_SUPRIMIDA**: 0.30 (30%)
  - **Racional**: Limita impacto de rupturas pontuais
  - **Ajuste**: Pode ser reduzido para 20% se necessário
- **PERCENTUAL_CORTE_MEDIAS_APARADAS**: 0.01 (1%)
  - **Racional**: Remove 1% superior e inferior (total 2%)
  - **Ajuste**: Pode ser aumentado para 0.02 (2%) para mais robustez

**Racional:**
- **Amortização de demanda suprimida**: Evita distorções de rupturas pontuais
- **Demanda robusta**: Captura demanda real (vendas + demanda suprimida)
- **Médias aparadas**: Equilibra robustez e informação (melhor que média simples ou mediana)

#### **Etapa 11: Cálculo de Médias Aparadas**

```python
def add_media_aparada_rolling(df, janelas, col_val="demanda_robusta", 
                              grupos=("grupo_de_necessidade","CdFilial"), 
                              alpha=0.01, min_obs=10):
    """
    Calcula médias aparadas rolling.
    
    Janelas: [90, 180, 270, 360] dias
    
    Processo:
    1. Calcula percentis alpha e (1-alpha) da janela
    2. Remove valores fora dos percentis
    3. Calcula média dos valores restantes
    4. Fallback: média simples se não houver valores suficientes
    5. Fallback final: média 360d se janela menor não tiver dados
    """
```

**Parâmetros de Calibração:**
- **JANELAS_MOVEIS_APARADAS**: [90, 180, 270, 360] dias
  - **90 dias**: Responsivo a mudanças recentes
  - **180 dias**: Balanceia responsividade e estabilidade
  - **270 dias**: Mais estável, menos responsivo
  - **360 dias**: Muito estável, captura sazonalidade anual
- **alpha**: 0.01 (1%)
  - **Racional**: Remove 1% superior e inferior (total 2%)
  - **Ajuste**: Pode ser aumentado para 0.02 (2%) para mais robustez
- **min_obs**: 10 observações mínimas
  - **Racional**: Garante qualidade estatística
  - **Ajuste**: Pode ser reduzido para 5 se necessário

**Racional:**
- **Médias aparadas**: Equilibra robustez (mediana) e informação (média)
- **Múltiplas janelas**: Permite escolha da melhor janela por contexto
- **Fallbacks**: Garante que sempre haja valor, mesmo com histórico limitado

#### **Etapa 12: Garantia de Integridade dos Dados**

```python
def garantir_integridade_dados_pre_merecimento(df: DataFrame) -> DataFrame:
    """
    Garante integridade dos dados ANTES do cálculo de merecimento.
    
    Processo:
    - Para cada coluna de média aparada:
      1. Calcula média 360d da própria combinação grupo+filial
      2. Preenche NULL apenas se há histórico válido da própria combinação
      3. Mantém NULL se não há histórico próprio (não usa dados de outros grupos/filiais)
    """
```

**Racional:**
- **Preenchimento conservador**: Apenas com histórico próprio
- **Antes do merecimento**: Garante que todos os dados estejam completos
- **Não usa dados de outros grupos**: Mantém integridade estatística

#### **Etapa 13: Cálculo de Merecimento CD (Primeira Camada)**

```python
def calcular_merecimento_cd(df: DataFrame, data_calculo: str, categoria: str) -> DataFrame:
    """
    Calcula merecimento a nível CD por grupo de necessidade.
    
    Processo:
    1. Filtra dados da data de cálculo
    2. Agrega por cd_vinculo × grupo_de_necessidade
    3. Soma MediaAparada180_Qt_venda_sem_ruptura (offline) ou MediaAparada90 (online)
    4. Calcula percentual do CD dentro da Cia:
       - Merecimento_CD = Total_CD / Total_Cia
    
    Resultado:
    - cd_vinculo × grupo_de_necessidade → Merecimento_CD (0 a 1)
    - Soma por grupo_de_necessidade = 1.0 (100%)
    """
```

**Parâmetros de Calibração:**
- **JANELA_CD_MERECIMENTO**: 180 dias (offline) ou 90 dias (online)
  - **Offline (180 dias)**: Mais estável, menos responsivo
  - **Online (90 dias)**: Mais responsivo a mudanças recentes
  - **Ajuste**: Pode ser alterado conforme necessidade

**Racional:**
- **Primeira camada**: Distribui demanda entre CDs
- **Por grupo de necessidade**: Cada grupo tem distribuição própria
- **Soma 100%**: Garante que toda demanda seja alocada

#### **Etapa 14: Cálculo de Merecimento Interno CD (Segunda Camada)**

```python
def calcular_merecimento_interno_cd(df: DataFrame, data_calculo: str, categoria: str) -> DataFrame:
    """
    Calcula proporção interna de cada loja dentro do CD.
    
    Processo:
    1. Filtra dados da data de cálculo
    2. Agrega por CdFilial × cd_vinculo × grupo_de_necessidade
    3. Soma médias aparadas (90, 180, 270, 360 dias)
    4. Calcula proporção interna:
       - Proporcao_Interna = Total_Filial / Total_CD
    
    Resultado:
    - CdFilial × grupo_de_necessidade → Proporcao_Interna (0 a 1)
    - Soma por cd_vinculo × grupo_de_necessidade = 1.0 (100%)
    """
```

**Parâmetros de Calibração:**
- **JANELAS_MOVEIS_APARADAS**: [90, 180, 270, 360] dias
  - **Múltiplas janelas**: Permite escolha da melhor janela
  - **Ajuste**: Pode ser reduzido para [90, 180] se necessário

**Racional:**
- **Segunda camada**: Distribui demanda dentro de cada CD
- **Por grupo de necessidade**: Cada grupo tem distribuição própria dentro do CD
- **Soma 100%**: Garante que toda demanda do CD seja alocada

#### **Etapa 15: Cálculo de Merecimento Final**

```python
def calcular_merecimento_final(df_merecimento_cd: DataFrame, 
                              df_merecimento_interno: DataFrame) -> DataFrame:
    """
    Calcula merecimento final: Merecimento_CD × Proporcao_Interna.
    
    Processo:
    1. Join entre merecimento CD e merecimento interno
    2. Multiplica: Merecimento_Final = Merecimento_CD × Proporcao_Interna
    3. Resultado: CdFilial × grupo_de_necessidade → Merecimento_Final
    
    Validação:
    - Soma por grupo_de_necessidade = 1.0 (100%)
    """
```

**Racional:**
- **Multiplicação de camadas**: Combina distribuição entre CDs e dentro de CDs
- **Mantém propriedade de soma 100%**: Garante que toda demanda seja alocada
- **Por grupo de necessidade**: Cada grupo tem distribuição própria

#### **Etapa 16: Criação do Esqueleto da Matriz**

```python
def criar_esqueleto_matriz_completa(df_com_grupo: DataFrame, data_calculo: str) -> DataFrame:
    """
    Cria esqueleto completo da matriz (cross join filiais × SKUs).
    
    Processo:
    1. Carrega todas as filiais da base (lojas + CDs)
    2. Carrega todos os SKUs obrigatórios ou sugeridos (estoquegerencial)
    3. Cross join: filiais × SKUs
    4. Adiciona grupo_de_necessidade para cada SKU
    
    Resultado:
    - CdFilial × CdSku × grupo_de_necessidade (todas as combinações possíveis)
    """
```

**Racional:**
- **Esqueleto completo**: Garante que todas as combinações filial×SKU existam
- **SKUs obrigatórios ou sugeridos**: Foca em produtos relevantes
- **Cross join**: Cria grade completa para join com merecimento final

#### **Etapa 17: Join Final e Aplicação de Merecimento**

```python
df_merecimento_sku_filial = (
    df_esqueleto
    .join(df_merecimento_final, on=['grupo_de_necessidade', 'CdFilial'], how='left')
    .fillna(0.0, subset=colunas_merecimento_final)
)
```

**Racional:**
- **Join por grupo+filial**: Aplica merecimento do grupo para todos os SKUs do grupo
- **Fillna com 0.0**: SKUs sem histórico recebem merecimento zero
- **Resultado final**: CdFilial × CdSku × Merecimento_Final

### 2.3. Resumo dos Parâmetros de Calibração

| Parâmetro | Valor Padrão | Ajuste Conservador | Ajuste Agressivo | Impacto |
|-----------|--------------|-------------------|------------------|---------|
| **data_inicio** | 18 meses | 12 meses | 24 meses | Histórico disponível |
| **desvios_meses_atipicos** | 2.0 | 2.5 | 1.5 | Quantidade de meses removidos |
| **n_sigmas_padrao** | 3.0 | 3.5 | 2.5 | Quantidade de outliers removidos |
| **n_sigmas_atacado** | 1.5 | 2.0 | 1.0 | Outliers em filiais de atacado |
| **PERCENTUAL_MAX_DEMANDA_SUPRIMIDA** | 0.30 (30%) | 0.20 (20%) | 0.40 (40%) | Impacto de rupturas |
| **PERCENTUAL_CORTE_MEDIAS_APARADAS** | 0.01 (1%) | 0.005 (0.5%) | 0.02 (2%) | Robustez das médias |
| **JANELA_CD_MERECIMENTO** | 180d (offline) / 90d (online) | 270d / 180d | 90d / 60d | Responsividade do merecimento CD |
| **JANELAS_MOVEIS_APARADAS** | [90, 180, 270, 360] | [180, 270, 360] | [60, 90, 180] | Janelas disponíveis |

---

## 3. Salvamento e Exportação

### 3.1. Racional do Salvamento

O salvamento das matrizes calculadas é uma etapa crítica que transforma os dados calculados em formato compatível com o sistema de abastecimento. O processo garante:

1. **Normalização exata para 100.00%**: Cada SKU+Canal deve somar exatamente 100.00%
2. **União de canais ONLINE e OFFLINE**: Ambos os canais no mesmo arquivo
3. **Formato padronizado**: Compatível com sistema de abastecimento
4. **Particionamento inteligente**: Divide arquivos grandes mantendo integridade

### 3.2. Processo de Salvamento

#### **Etapa 1: Carregamento e Filtros**

```python
def carregar_e_filtrar_matriz(categoria: str, canal: str) -> DataFrame:
    """
    Carrega matriz e aplica filtros.
    
    Filtros aplicados:
    1. Filtro de produtos (tipificação de entrega, marcas)
    2. Filtro de grupos de necessidade (remoção ou seleção)
    3. Regra especial ONLINE: CdFilial 1401 → 14 (TELAS e TELEFONIA)
    4. Regra de de-para de CDs inválidos (TELAS e TELEFONIA online)
    5. Agregação final por CdSku + CdFilial
    """
```

**Filtros de Produtos:**
- **Tipificação de entrega**: Apenas "SL" (Sai Loja) para maioria das categorias
- **Marcas excluídas**: Exclui APPLE para algumas categorias
- **Racional**: Foca em produtos relevantes para abastecimento via CD

**Filtros de Grupos:**
- **Remoção**: Remove grupos específicos (ex: "FORA DE LINHA", "SEM_GN")
- **Seleção**: Seleciona apenas grupos específicos (menos comum)
- **Racional**: Remove grupos que não devem receber abastecimento

**Regras Especiais:**
- **CdFilial 1401 → 14**: Consolidação de CDs (apenas TELAS e TELEFONIA online)
- **De-para de CDs inválidos**: Transfere merecimento de CDs inválidos para CD14
- **Racional**: Simplifica gestão e garante que todos os CDs sejam válidos

#### **Etapa 2: União de Canais**

```python
df_union = df_offline.union(df_online)
```

**Racional:**
- **União**: Combina dados de ambos os canais
- **Mesma estrutura**: Ambos os canais têm mesma estrutura de dados
- **Coluna CANAL**: Identifica origem (ONLINE ou OFFLINE)

#### **Etapa 3: Adição de Informações de Filiais**

```python
def adicionar_informacoes_filial(df: DataFrame) -> DataFrame:
    """
    Adiciona informações de filiais e formata código LOJA.
    
    Processo:
    1. Join com tabelas de referência (CDs e lojas ativas)
    2. Identifica se é CD ou loja
    3. Formata código LOJA:
       - Loja: "0021_0XXXX" (5 dígitos com zeros à esquerda)
       - CD: "0099_0XXXX" (5 dígitos com zeros à esquerda)
    4. Remove filiais não elegíveis (não estão em referência)
    """
```

**Racional:**
- **Formatação padronizada**: Compatível com sistema de abastecimento
- **Remoção de filiais inativas**: Evita envio para filiais fechadas
- **Identificação de CDs**: Permite tratamento diferenciado

#### **Etapa 4: Normalização para 100.00% Exato**

```python
def normalizar_para_100_exato(df: DataFrame) -> DataFrame:
    """
    Normaliza merecimentos para somar EXATAMENTE 100.00% por SKU+Canal.
    
    Processo:
    1. Proporcionalizar: (Merecimento / Soma) × 100.0
    2. Calcular diferença: 100.0 - Soma_Proporcional
    3. Ajustar maior merecimento: Maior + Diferença
    4. Arredondar para 3 casas decimais
    
    Validação:
    - Soma por SKU+Canal = 100.000% (tolerância: 0.0001%)
    """
```

**Racional:**
- **Normalização exata**: Garante que sistema de abastecimento receba 100.00%
- **Ajuste no maior**: Minimiza impacto do ajuste
- **Validação rigorosa**: Garante qualidade dos dados

#### **Etapa 5: Criação do DataFrame Final**

```python
def criar_dataframe_final(df: DataFrame) -> DataFrame:
    """
    Cria DataFrame final no formato do sistema.
    
    Colunas:
    - SKU: Código do produto (string)
    - CANAL: ONLINE ou OFFLINE
    - LOJA: Código formatado (0021_0XXXX ou 0099_0XXXX)
    - DATA FIM: Data atual + 60 dias (yyyyMMdd)
    - PERCENTUAL: Merecimento em % (3 casas decimais)
    """
```

**Racional:**
- **Formato padronizado**: Compatível com sistema de abastecimento
- **DATA FIM**: Define período de validade da matriz
- **PERCENTUAL**: Formato numérico com precisão adequada

#### **Etapa 6: Particionamento Inteligente**

```python
def dividir_em_arquivos(df: DataFrame, max_linhas: int = 150000) -> List[DataFrame]:
    """
    Divide DataFrame em arquivos mantendo integridade.
    
    Regras:
    - Máximo de linhas por arquivo (padrão: 150.000)
    - SKU-LOJA sempre no mesmo arquivo (ambos canais)
    - Nomenclatura: parte1, parte2, parte3, etc.
    
    Algoritmo:
    1. Agrupa por SKU (todos os registros do SKU ficam juntos)
    2. Calcula acumulado de linhas
    3. Atribui número de arquivo baseado no acumulado
    """
```

**Racional:**
- **Limite de linhas**: Evita arquivos muito grandes
- **SKU-LOJA junto**: Garante que ambos os canais estejam no mesmo arquivo
- **Particionamento por SKU**: Mantém integridade dos dados

### 3.3. Formato de Exportação

#### **CSV (Padrão)**
- **Separador**: `;` (ponto e vírgula)
- **Decimal**: `,` (vírgula)
- **Encoding**: UTF-8
- **Sem índice**: Apenas dados

#### **XLSX (Alternativa)**
- **Engine**: openpyxl
- **Sem índice**: Apenas dados
- **Formato**: Compatível com Excel

### 3.4. Estrutura de Saída

```
PASTA_OUTPUT/
└── 2025-10-06/
    ├── matriz_merecimento_telas_2025-10-06_parte1.xlsx
    ├── matriz_merecimento_telas_2025-10-06_parte2.xlsx
    ├── matriz_merecimento_telefonia_2025-10-06_parte1.xlsx
    └── ...
```

---

## 4. Validação e Checagem

### 4.1. Racional da Planilha de Validação

A planilha de validação é uma ferramenta essencial para **checagem e validação** das matrizes calculadas antes do envio para o time de supply. Ela permite:

1. **Comparação lado a lado**: ONLINE vs OFFLINE para mesma combinação grupo+filial
2. **Identificação de discrepâncias**: Valores muito diferentes entre canais
3. **Validação de consistência**: Verificar se somas estão corretas
4. **Análise de distribuição**: Entender como merecimento está distribuído

### 4.2. Estrutura da Planilha de Validação

#### **Colunas:**
- `grupo_de_necessidade`: Grupo de necessidade
- `CdFilial`: Código da filial
- `Merecimento_OFFLINE`: Merecimento do canal offline (em %)
- `Merecimento_ONLINE`: Merecimento do canal online (em %)

#### **Agregação:**
- **Nível**: grupo_de_necessidade × CdFilial (distinct)
- **Merecimentos**: Primeiro valor encontrado (não soma)
- **Fill NULLs**: 0.00 para merecimentos faltantes

### 4.3. Benefícios da Planilha de Validação

#### **1. Detecção de Problemas**
- **Discrepâncias grandes**: Identifica quando ONLINE e OFFLINE têm valores muito diferentes
- **Valores ausentes**: Identifica quando um canal não tem dados
- **Valores zero**: Identifica quando ambos os canais têm zero

#### **2. Análise de Distribuição**
- **Por grupo de necessidade**: Entende distribuição por grupo
- **Por filial**: Entende distribuição por filial
- **Comparação entre canais**: Entende diferenças entre ONLINE e OFFLINE

#### **3. Validação de Consistência**
- **Somas corretas**: Verifica se somas estão próximas de 100%
- **Valores razoáveis**: Verifica se valores estão em faixa esperada
- **Ausências justificadas**: Verifica se ausências fazem sentido

#### **4. Facilita Decisões**
- **Aprovação para envio**: Base para decidir se matriz está pronta
- **Ajustes necessários**: Identifica o que precisa ser corrigido
- **Documentação**: Registro histórico das validações

### 4.4. Processo de Validação

```python
def exportar_excel_validacao_grupo_necessidade(categoria: str, data_exportacao: str) -> str:
    """
    Exporta Excel de validação com ONLINE e OFFLINE lado a lado.
    
    Processo:
    1. Carrega dados OFFLINE agregados por grupo+filial
    2. Carrega dados ONLINE agregados por grupo+filial
    3. Faz FULL OUTER JOIN (mantém todos os registros)
    4. Fill NULLs com 0.00
    5. Ordena por grupo_de_necessidade, CdFilial
    6. Salva em Excel
    """
```

**Racional:**
- **FULL OUTER JOIN**: Garante que todos os registros sejam incluídos
- **Fill NULLs**: Facilita comparação (0.00 vs valor real)
- **Agregação por grupo+filial**: Reduz granularidade para análise

### 4.5. Localização da Planilha

```
PASTA_OUTPUT/
└── 2025-10-06/
    └── validacao/
        ├── validacao_telas_2025-10-06.xlsx
        ├── validacao_telefonia_2025-10-06.xlsx
        └── ...
```

---

## 5. Envio para o Time de Supply

### 5.1. Racional do Envio

Após validação e aprovação, as matrizes são enviadas para o time de supply para uso no sistema de abastecimento. O processo garante:

1. **Formato correto**: Compatível com sistema de abastecimento
2. **Dados completos**: Todas as combinações filial×SKU necessárias
3. **Validação prévia**: Dados já validados antes do envio
4. **Rastreabilidade**: Histórico de envios e versões

### 5.2. Formato Final para Envio

#### **Colunas do Arquivo:**
- **SKU**: Código do produto
- **CANAL**: ONLINE ou OFFLINE
- **LOJA**: Código formatado (0021_0XXXX ou 0099_0XXXX)
- **DATA FIM**: Data de validade (yyyyMMdd)
- **PERCENTUAL**: Merecimento em % (3 casas decimais)

#### **Características:**
- **Normalização exata**: Cada SKU+Canal soma 100.00%
- **Ambos os canais**: ONLINE e OFFLINE no mesmo arquivo
- **Particionamento**: Dividido em partes se necessário (máx 150k linhas)
- **Formato**: XLSX (padrão) ou CSV (alternativa)

### 5.3. Processo de Envio

#### **Etapa 1: Validação Final**
- Verificar se planilha de validação foi aprovada
- Confirmar que não há problemas identificados
- Validar que somas estão corretas (100.00%)

#### **Etapa 2: Preparação dos Arquivos**
- Garantir que todos os arquivos estão no formato correto
- Verificar que particionamento está correto
- Confirmar que nomenclatura está padronizada

#### **Etapa 3: Envio**
- Enviar arquivos para pasta compartilhada ou sistema de abastecimento
- Notificar time de supply sobre novo envio
- Documentar data e versão do envio

### 5.4. Benefícios do Processo

#### **1. Automação**
- **Reduz trabalho manual**: Processo automatizado end-to-end
- **Elimina erros**: Validações automáticas garantem qualidade
- **Acelera entrega**: Processo rápido e confiável

#### **2. Qualidade**
- **Dados validados**: Múltiplas camadas de validação
- **Formato correto**: Compatível com sistema de abastecimento
- **Consistência**: Dados sempre atualizados e consistentes

#### **3. Rastreabilidade**
- **Histórico completo**: Todas as versões são mantidas
- **Documentação**: Processo documentado e replicável
- **Auditoria**: Fácil identificar origem e transformações dos dados

#### **4. Flexibilidade**
- **Múltiplas categorias**: Processo unificado para todas as categorias
- **Configurável**: Parâmetros ajustáveis conforme necessidade
- **Extensível**: Fácil adicionar novas categorias ou regras

---

## 6. Resumo do Fluxo Completo

### 6.1. Fluxograma do Processo

```
1. PREPARAÇÃO DE TABELAS BASE
   ├── Carregamento de estoque
   ├── Carregamento de vendas
   ├── Cálculo de médias móveis (90 dias)
   ├── Análise de ruptura
   ├── Mapeamento de abastecimento
   └── Processamento incremental (lotes de 3 meses)
   
2. CÁLCULO DA MATRIZ DE MERECIMENTO
   ├── Carregamento e preparação de dados
   ├── Espelhamento de filiais
   ├── Mapeamento de produtos
   ├── Definição de grupo de necessidade
   ├── Consolidação de grupos especiais
   ├── Agregação por grupo de necessidade
   ├── Detecção de outliers (meses atípicos)
   ├── Filtragem de meses atípicos
   ├── Remoção de outliers (séries históricas)
   ├── Cálculo de demanda robusta
   ├── Cálculo de médias aparadas
   ├── Garantia de integridade dos dados
   ├── Cálculo de merecimento CD (primeira camada)
   ├── Cálculo de merecimento interno CD (segunda camada)
   ├── Cálculo de merecimento final
   ├── Criação do esqueleto da matriz
   └── Join final e aplicação de merecimento
   
3. SALVAMENTO E EXPORTAÇÃO
   ├── Carregamento e filtros (produtos, grupos)
   ├── União de canais (ONLINE + OFFLINE)
   ├── Adição de informações de filiais
   ├── Normalização para 100.00% exato
   ├── Criação do DataFrame final
   └── Particionamento inteligente
   
4. VALIDAÇÃO E CHECAGEM
   ├── Exportação de planilha de validação
   ├── Comparação ONLINE vs OFFLINE
   ├── Identificação de discrepâncias
   └── Aprovação para envio
   
5. ENVIO PARA O TIME DE SUPPLY
   ├── Validação final
   ├── Preparação dos arquivos
   └── Envio para sistema de abastecimento
```

### 6.2. Pontos de Decisão e Ajustes

#### **Durante Preparação:**
- **Período histórico**: Ajustar `data_inicio` conforme necessidade
- **Tamanho do lote**: Ajustar `batch_size_months` conforme memória disponível

#### **Durante Cálculo:**
- **Sensibilidade a outliers**: Ajustar `desvios_meses_atipicos` e `n_sigmas`
- **Robustez das médias**: Ajustar `PERCENTUAL_CORTE_MEDIAS_APARADAS`
- **Janelas móveis**: Escolher janelas conforme necessidade de responsividade

#### **Durante Salvamento:**
- **Filtros de produtos**: Ajustar conforme necessidade de cada categoria
- **Filtros de grupos**: Atualizar lista de grupos a remover/selecionar
- **Limite de linhas**: Ajustar `max_linhas` conforme capacidade do sistema

#### **Durante Validação:**
- **Tolerância de discrepâncias**: Definir limites aceitáveis entre ONLINE e OFFLINE
- **Critérios de aprovação**: Estabelecer critérios claros para aprovação

---

## 7. Conclusão

O sistema de matriz de merecimento foi projetado para ser **robusto, governado e configurável**, substituindo processos manuais por uma solução automatizada que garante qualidade, consistência e rastreabilidade. 

### **Principais Benefícios:**

1. **Automação Completa**: Processo end-to-end automatizado
2. **Qualidade Garantida**: Múltiplas camadas de validação
3. **Flexibilidade**: Configurável para diferentes categorias e necessidades
4. **Rastreabilidade**: Histórico completo e documentação detalhada
5. **Eficiência**: Reduz tempo de processamento e elimina erros manuais

### **Próximos Passos:**

1. **Monitoramento Contínuo**: Acompanhar métricas de qualidade
2. **Otimização Iterativa**: Ajustar parâmetros conforme aprendizado
3. **Expansão**: Adicionar novas categorias ou funcionalidades
4. **Documentação**: Manter documentação atualizada

---

**Versão**: 1.0  
**Data**: 2025-01-XX  
**Autor**: Sistema de Matriz de Merecimento - Time de Supply

