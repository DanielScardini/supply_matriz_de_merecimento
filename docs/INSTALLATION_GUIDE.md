# Installation Guide - Supply Matriz de Merecimento

## 📋 Visão Geral

Este guia fornece instruções detalhadas para instalação e configuração do sistema de matriz de merecimento no ambiente Databricks.

## 🎯 Pré-requisitos

### **Ambiente Databricks**

#### **Versão Mínima**
- **Databricks Runtime**: 13.3 LTS ou superior
- **Python**: 3.9 ou superior
- **Scala**: 2.12 ou superior

#### **Permissões Necessárias**
- **Acesso às tabelas**: `databox.bcg_comum.*`
- **Criação de tabelas**: Para salvar resultados
- **Acesso ao DBFS**: Para arquivos de mapeamento
- **Execução de jobs**: Para automação

### **Recursos de Cluster**

#### **Configuração Mínima**
```json
{
  "num_workers": 2,
  "node_type_id": "i3.xlarge",
  "driver_node_type_id": "i3.xlarge",
  "spark_version": "13.3.x-scala2.12"
}
```

#### **Configuração Recomendada**
```json
{
  "num_workers": 4,
  "node_type_id": "i3.2xlarge",
  "driver_node_type_id": "i3.2xlarge",
  "spark_version": "13.3.x-scala2.12",
  "spark_conf": {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true"
  }
}
```

## 🚀 Instalação

### **1. Preparação do Ambiente**

#### **Criar Workspace**
1. Acesse o Databricks workspace
2. Crie um novo workspace ou use existente
3. Configure permissões de acesso às tabelas

#### **Configurar Cluster**
1. Vá para **Compute** → **Clusters**
2. Clique em **Create Cluster**
3. Configure conforme especificações acima
4. Salve e inicie o cluster

### **2. Upload do Código**

#### **Método 1: Git Integration**
```bash
# Clonar repositório
git clone https://github.com/your-org/supply_matriz_de_merecimento.git

# Configurar Git no Databricks
# Vá para User Settings → Git Integration
# Adicione suas credenciais Git
```

#### **Método 2: Upload Manual**
1. Vá para **Workspace** → **Users** → **Seu Usuário**
2. Crie pasta `supply_matriz_de_merecimento`
3. Upload dos arquivos Python
4. Organize conforme estrutura do projeto

### **3. Configuração de Bibliotecas**

#### **Bibliotecas Python**
```python
# Instalar via pip no notebook
%pip install pandas>=1.5.0
%pip install plotly>=5.0.0
%pip install openpyxl>=3.0.0
%pip install tqdm>=4.64.0
```

#### **Bibliotecas via Cluster**
1. Vá para **Compute** → **Clusters** → **Seu Cluster**
2. Clique em **Libraries** → **Install New**
3. Selecione **PyPI** e instale:
   - `pandas>=1.5.0`
   - `plotly>=5.0.0`
   - `openpyxl>=3.0.0`
   - `tqdm>=4.64.0`

### **4. Configuração de Dados**

#### **Arquivos de Mapeamento**
1. Crie pasta no DBFS: `/dbfs/dados_analise/`
2. Upload dos arquivos CSV:
   - `MODELOS_AJUSTE (1).csv`
   - `ITENS_GEMEOS 2.csv`
   - `(DRP)_MATRIZ_*.csv`

#### **Estrutura de Pastas**
```
/dbfs/
├── dados_analise/
│   ├── MODELOS_AJUSTE (1).csv
│   ├── ITENS_GEMEOS 2.csv
│   ├── (DRP)_MATRIZ_20250829135142.csv
│   └── (DRP)_MATRIZ_20250902160333.csv
└── resultados/
    ├── matriz_telas/
    ├── matriz_telefonia/
    ├── matriz_linha_branca/
    ├── matriz_linha_leve/
    └── matriz_info_games/
```

## ⚙️ Configuração

### **1. Configuração de Paths**

#### **Atualizar Paths nos Arquivos**
```python
# Em cada arquivo de análise, atualizar paths para:
BASE_PATH = "/dbfs/dados_analise/"
RESULTADOS_PATH = "/dbfs/resultados/"

# Exemplo de atualização
df_modelos = pd.read_csv(f"{BASE_PATH}MODELOS_AJUSTE (1).csv")
```

#### **Configuração de Tabelas**
```python
# Configurar nomes das tabelas
TABELA_BASE = "databox.bcg_comum.supply_base_merecimento_diario_v3"
TABELA_LOJAS = "data_engineering_prd.app_operacoesloja.roteirizacaolojaativa"
```

### **2. Configuração de Parâmetros**

#### **Parâmetros por Categoria**
```python
# Configurar parâmetros específicos por categoria
CONFIGURACOES_CATEGORIA = {
    "DIRETORIA DE TELAS": {
        "sigma_meses_atipicos": 2.0,
        "sigma_outliers_cd": 2.5,
        "sigma_outliers_loja": 3.0
    },
    "DIRETORIA TELEFONIA CELULAR": {
        "sigma_meses_atipicos": 2.0,
        "sigma_outliers_cd": 2.5,
        "sigma_outliers_loja": 3.0
    },
    "DIRETORIA LINHA BRANCA": {
        "sigma_meses_atipicos": 3.0,
        "sigma_outliers_cd": 3.0,
        "sigma_outliers_loja": 3.0
    }
}
```

#### **Configuração de Datas**
```python
# Configurar datas de execução
DATA_INICIO = "2024-01-01"
DATA_CALCULO = "2025-06-30"
DATA_FIM = "2025-12-31"
```

### **3. Configuração de Logs**

#### **Configurar Logging**
```python
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/dbfs/logs/matriz_merecimento.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
```

## 🧪 Testes

### **1. Teste de Conectividade**

#### **Verificar Acesso às Tabelas**
```python
# Teste de conectividade
def testar_conectividade():
    try:
        # Testar tabela base
        df_base = spark.table("databox.bcg_comum.supply_base_merecimento_diario_v3")
        print(f"✅ Tabela base acessível: {df_base.count()} registros")
        
        # Testar tabela de lojas
        df_lojas = spark.table("data_engineering_prd.app_operacoesloja.roteirizacaolojaativa")
        print(f"✅ Tabela de lojas acessível: {df_lojas.count()} registros")
        
        return True
    except Exception as e:
        print(f"❌ Erro de conectividade: {str(e)}")
        return False

# Executar teste
testar_conectividade()
```

#### **Verificar Arquivos de Mapeamento**
```python
# Teste de arquivos
def testar_arquivos():
    import os
    
    arquivos = [
        "/dbfs/dados_analise/MODELOS_AJUSTE (1).csv",
        "/dbfs/dados_analise/ITENS_GEMEOS 2.csv"
    ]
    
    for arquivo in arquivos:
        if os.path.exists(arquivo):
            print(f"✅ Arquivo encontrado: {arquivo}")
        else:
            print(f"❌ Arquivo não encontrado: {arquivo}")

# Executar teste
testar_arquivos()
```

### **2. Teste de Execução**

#### **Teste com Dados Pequenos**
```python
# Teste com amostra pequena
def testar_execucao_pequena():
    try:
        # Executar com dados limitados
        df_teste = executar_calculo_matriz_merecimento(
            categoria="DIRETORIA DE TELAS",
            data_inicio="2024-12-01",
            data_calculo="2024-12-31"
        )
        
        print(f"✅ Execução bem-sucedida: {df_teste.count()} registros")
        return True
    except Exception as e:
        print(f"❌ Erro na execução: {str(e)}")
        return False

# Executar teste
testar_execucao_pequena()
```

## 🚀 Execução

### **1. Execução Manual**

#### **Notebook Principal**
```python
# Executar cálculo para todas as categorias
categorias = [
    "DIRETORIA DE TELAS",
    "DIRETORIA TELEFONIA CELULAR",
    "DIRETORIA LINHA BRANCA",
    "DIRETORIA LINHA LEVE",
    "DIRETORIA INFO/GAMES"
]

for categoria in categorias:
    print(f"Processando {categoria}...")
    df_resultado = executar_calculo_matriz_merecimento(categoria)
    
    # Salvar resultado
    df_resultado.write.mode("overwrite").parquet(f"/dbfs/resultados/matriz_{categoria.lower().replace(' ', '_')}/")
    print(f"✅ {categoria} concluído")
```

#### **Execução de Análises**
```python
# Executar análises de elasticidade
executar_analise_elasticidade_demanda()

# Executar análises factuais
executar_analise_factual_comparacao_matrizes()
```

### **2. Automação com Jobs**

#### **Criar Job no Databricks**
1. Vá para **Workflows** → **Jobs**
2. Clique em **Create Job**
3. Configure:
   - **Name**: `Matriz de Merecimento - Cálculo Diário`
   - **Task Type**: `Notebook`
   - **Source**: Seu notebook principal
   - **Cluster**: Seu cluster configurado

#### **Configurar Schedule**
```python
# Configurar execução diária
schedule = {
    "quartz_cron_expression": "0 0 6 * * ?",  # 6:00 AM diariamente
    "timezone_id": "America/Sao_Paulo",
    "pause_status": "UNPAUSED"
}
```

### **3. Monitoramento**

#### **Configurar Alertas**
```python
# Configurar alertas para falhas
def configurar_alertas():
    # Alertas por email
    alertas = {
        "email": "supply-team@empresa.com",
        "webhook": "https://hooks.slack.com/...",
        "condicoes": [
            "tempo_execucao > 2_horas",
            "erro_count > 0",
            "registros_processados < 1000"
        ]
    }
    return alertas
```

## 🔧 Troubleshooting

### **Problemas Comuns**

#### **1. Erro de Permissão**
```
Error: Table or view not found: databox.bcg_comum.supply_base_merecimento_diario_v3
```
**Solução**: Verificar permissões de acesso às tabelas

#### **2. Erro de Memória**
```
Error: OutOfMemoryError: Java heap space
```
**Solução**: Aumentar tamanho do cluster ou otimizar queries

#### **3. Erro de Arquivo**
```
Error: File not found: /dbfs/dados_analise/MODELOS_AJUSTE (1).csv
```
**Solução**: Verificar se arquivos foram uploadados corretamente

### **Logs de Debug**

#### **Habilitar Logs Detalhados**
```python
# Configurar logging detalhado
import logging
logging.getLogger("py4j").setLevel(logging.DEBUG)
logging.getLogger("pyspark").setLevel(logging.DEBUG)
```

#### **Verificar Logs do Cluster**
1. Vá para **Compute** → **Clusters** → **Seu Cluster**
2. Clique em **Logs**
3. Verifique **Driver Logs** e **Worker Logs**

## 📊 Performance

### **Otimizações Recomendadas**

#### **1. Configuração de Cluster**
```python
# Configurações de performance
spark_conf = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}
```

#### **2. Particionamento de Dados**
```python
# Particionar dados por categoria
df_base = df_base.repartition("categoria", "cdfilial")
```

#### **3. Cache Estratégico**
```python
# Cache de DataFrames reutilizados
df_base.cache()
df_mapeamentos.cache()
```

## 🔒 Segurança

### **Configurações de Segurança**

#### **1. Acesso a Dados**
- **Princípio do menor privilégio**: Apenas permissões necessárias
- **Auditoria**: Log de acessos e modificações
- **Criptografia**: Dados sensíveis criptografados

#### **2. Controle de Acesso**
```python
# Configurar controle de acesso
def verificar_permissao(usuario, operacao):
    """Verifica se usuário tem permissão para operação."""
    permissoes = {
        "admin": ["read", "write", "execute"],
        "analyst": ["read", "execute"],
        "viewer": ["read"]
    }
    return operacao in permissoes.get(usuario, [])
```

## 📚 Documentação Adicional

### **Recursos Úteis**

- **Databricks Documentation**: https://docs.databricks.com/
- **PySpark Documentation**: https://spark.apache.org/docs/latest/api/python/
- **Pandas Documentation**: https://pandas.pydata.org/docs/

### **Suporte**

Para problemas de instalação ou configuração:

1. **Verificar logs** de erro
2. **Consultar documentação** do Databricks
3. **Contatar equipe** de suporte
4. **Abrir issue** no repositório

---

**Versão**: 2.0.0  
**Última Atualização**: Janeiro 2025  
**Mantenedor**: Equipe de Supply Chain Analytics
