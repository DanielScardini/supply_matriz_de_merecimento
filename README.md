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
- **Python/SQL**: Linguagens para implementação dos algoritmos
- **Agendamento**: Automação de rodadas de cálculo
- **Alertas**: Sistema de notificações para anomalias

## Estrutura do Projeto

```
supply_matriz_de_merecimento/
├── README.md
├── notebooks/          # Notebooks Databricks
├── src/               # Código fonte Python
├── config/            # Configurações e parâmetros
├── data/              # Dados de entrada e saída
└── docs/              # Documentação técnica
```

## Status do Projeto

🚧 **Em Desenvolvimento** - Ferramenta para discussão e implementação

---

*Desenvolvido para o time de Supply do Grupo Casas Bahia*
