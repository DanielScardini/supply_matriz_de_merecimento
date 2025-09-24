# Análise de Elasticidade de Demanda - Databricks

## 🎯 Objetivo

Este notebook implementa **análise de elasticidade de demanda** para produtos gêmeos no sistema de matriz de merecimento, criando visualizações profissionais que seguem o padrão de gráficos de barras empilhadas conforme especificado.

## 📊 Funcionalidades Implementadas

### 1. **Identificação Automática de Top Gêmeos**
- Identifica os **top 5 gêmeos** de cada `NmAgrupamentoDiretoriaSetor`
- Ranking baseado em volume total de vendas (`QtMercadoria`)
- Filtra automaticamente produtos com "Chip" no nome

### 2. **Análise Temporal com Quebras Múltiplas**
- **Agrupamento principal**: Por `year_month` (período mensal)
- **Quebra 1**: Porte de loja (`DsPorteLoja`)
- **Quebra 2**: Região geográfica (`NmRegiaoGeografica`)
- **Métricas**: Quantidade vendida e receita total

### 3. **Visualizações Profissionais - Duas Versões**
- **Versão 1 - APENAS Porte de Loja**:
  - Gráfico 1: Vendas mensais (k unidades) por porte de loja
  - Gráfico 2: Proporção % de vendas por porte de loja
- **Versão 2 - Porte + Região Geográfica**:
  - Gráfico 1: Vendas mensais (k unidades) por porte + região
  - Gráfico 2: Proporção % de vendas por porte + região
- **Formato**: Barras empilhadas com gradiente de cores azuis
- **Estilo**: Seguindo padrão da imagem de referência

## 🏗️ Estrutura do Notebook

### **Seções Principais**
1. **Imports e Configurações** - Bibliotecas e inicialização do Spark
2. **Carregamento de Dados Base** - Tabela principal de merecimento
3. **Mapeamento de Gêmeos** - Arquivo CSV com produtos similares
4. **Join com Região** - Informações geográficas das lojas
5. **Top 5 Gêmeos** - Identificação por diretoria
6. **Preparação de Dados** - Agregação para gráficos
7. **Criação de Gráficos APENAS por Porte** - Função de plotagem
8. **Criação de Gráficos por Porte + Região** - Função de plotagem
9. **Execução Completa** - Loop para todos os gêmeos (2 versões cada)
10. **Análises Adicionais** - Resumos e estatísticas

## 🔧 Como Executar

### **1. Preparação do Ambiente**
- Certifique-se de que o notebook está rodando em um cluster Databricks
- Verifique se as tabelas necessárias estão acessíveis
- Confirme se o arquivo de gêmeos está no caminho correto

### **2. Execução Sequencial**
- Execute as células na ordem sequencial
- Cada seção depende da anterior
- Monitore os logs de progresso

### **3. Ajustes Necessários**
- **Caminho do arquivo de gêmeos**: `/dbfs/dados_analise/ITENS_GEMEOS 2.csv`
- **Visualização**: Gráficos exibidos diretamente no notebook
- **Filtros de diretorias**: Configuráveis na seção 2

## 📁 Dependências e Tabelas

### **Tabelas Spark Utilizadas**
- `databox.bcg_comum.supply_base_merecimento_diario_v2` - Dados base
- `data_engineering_prd.app_operacoesloja.roteirizacaolojaativa` - Região geográfica

### **Arquivos CSV Necessários**
- `ITENS_GEMEOS 2.csv` - Mapeamento de produtos similares
- **Formato**: Delimitador `;`, encoding `iso-8859-1`
- **Colunas**: `sku_loja`, `gemeos`

### **Bibliotecas Python**
- `pyspark` - Processamento distribuído
- `pandas` - Manipulação de dados
- `plotly` - Criação de gráficos
- `datetime` - Manipulação de datas

## 📁 Saídas Geradas

### **1. Gráficos Interativos - Duas Versões**
- **Formato**: Plotly interativo
- **Visualização**: Direta no notebook usando `fig.show()`
- **Versões**:
  - **Versão 1**: APENAS por porte de loja
  - **Versão 2**: Por porte de loja + região geográfica
- **Total**: 2 gráficos por gêmeo (portes + portes+região)

### **2. Dados Processados**
- **DataFrame pandas**: Preparado para análises adicionais
- **Agregações**: Por mês, gêmeo, porte e região
- **Métricas**: Vendas e receita

### **3. Relatórios Console**
- **Progresso**: Logs de execução detalhados
- **Resumos**: Top gêmeos por diretoria
- **Estatísticas**: Contadores e métricas por versão

## 🎨 Características dos Gráficos

### **Design Visual**
- **Cores**: Gradiente de azuis para hierarquia de portes
- **Layout**: Dois gráficos lado a lado (vendas + proporção)
- **Títulos**: Seguindo padrão da imagem de referência
- **Background**: Off-white (#F8F8FF) conforme regras
- **Formato**: Profissional para apresentações

### **Interatividade**
- **Hover**: Informações detalhadas por barra
- **Legenda**: Horizontal no topo
- **Anotações**: Valores totais no topo das barras
- **Exportação**: HTML interativo salvo no DBFS

## 📈 Dados Processados

### **Filtros Aplicados**
- **Diretorias**: 5 principais diretorias do sistema
- **Gêmeos**: Exclui produtos com "Chip" no nome
- **Período**: Últimos 12 meses (configurável)
- **Lojas**: Apenas lojas ativas

### **Agregações Realizadas**
- Por `year_month` + `gemeos` + `DsPorteLoja` + `NmRegiaoGeografica`
- Soma de `QtMercadoria` e `Receita`
- Conversão para k unidades para visualização
- Cálculo de proporções percentuais

## 🚀 Personalizações Possíveis

### **1. Filtros Adicionais**
- Período específico de análise
- Diretorias específicas
- Produtos específicos

### **2. Métricas Adicionais**
- Elasticidade-preço
- Elasticidade-renda
- Análise de sazonalidade
- Correlação com eventos

### **3. Visualizações**
- Gráficos de linha para tendências
- Heatmaps para correlações
- Dashboards interativos

## ⚠️ Considerações Importantes

### **Performance**
- O notebook processa grandes volumes de dados
- Use clusters adequados para o volume
- Monitore o uso de memória

### **Dependências**
- Verifique se todas as tabelas estão acessíveis
- Confirme permissões de leitura/escrita no DBFS
- Teste com amostras menores primeiro

### **Manutenção**
- Atualize o arquivo de gêmeos conforme necessário
- Ajuste filtros conforme mudanças no negócio
- Monitore a qualidade dos dados

## 📋 Checklist de Execução

### **Antes da Execução**
- [ ] Cluster Databricks configurado
- [ ] Tabelas acessíveis
- [ ] Arquivo de gêmeos no caminho correto
- [ ] Permissões de DBFS configuradas

### **Durante a Execução**
- [ ] Monitore logs de progresso
- [ ] Verifique contadores de registros
- [ ] Confirme criação dos gráficos
- [ ] Valide dados de saída

### **Após a Execução**
- [ ] Verifique arquivos salvos no DBFS
- [ ] Confirme qualidade dos gráficos
- [ ] Valide estatísticas finais
- [ ] Documente insights encontrados

## 🎉 Resultado Esperado

Ao final da execução, você terá:
- **Duas versões de gráficos** para cada top gêmeo:
  - **Versão 1**: APENAS por porte de loja
  - **Versão 2**: Por porte de loja + região geográfica
- **Análise completa** por diretoria
- **Insights de elasticidade** de demanda
- **Visualizações interativas** usando plotly
- **Total de gráficos**: 2 × número de gêmeos analisados
- **Gráficos exibidos** diretamente no notebook

## 📞 Suporte

Para dúvidas ou problemas:
1. Verifique os logs de execução
2. Confirme configurações do cluster
3. Valide permissões de acesso
4. Consulte a documentação do sistema
