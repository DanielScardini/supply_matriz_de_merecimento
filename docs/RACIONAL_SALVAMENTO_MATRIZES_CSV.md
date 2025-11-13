# 📊 RACIONAL: Salvamento de Matrizes em CSV

## 🎯 OBJETIVO
Exportar matrizes de merecimento em formato CSV compatível com o sistema de abastecimento, garantindo:
- Normalização exata para 100.00% por CdSku+Canal
- União de canais ONLINE e OFFLINE
- Particionamento inteligente (máx 500k linhas)
- Formato padronizado conforme imagem de referência

---

## 📋 COLUNAS DO ARQUIVO FINAL

| Coluna | Tipo | Exemplo | Descrição |
|--------|------|---------|-----------|
| **SKU** | string | "5356458" | Código do produto |
| **CANAL** | string | "OFFLINE" | ONLINE ou OFFLINE |
| **LOJA** | string | "0021_01057" | Código formatado da loja/CD |
| **DATA FIM** | int | 20251202 | Data atual + 60 dias (yyyyMMdd) |
| **PERCENTUAL** | decimal | 1.279 | Merecimento em % |
| **VERIFICAR** | string | "" | Campo vazio |
| **FASE DE VIDA** | string | "SEM FASE" | Fase do produto |

---

## 🔧 ETAPAS DO PROCESSAMENTO

### **1️⃣ CARREGAMENTO E FILTROS**

```python
def carregar_e_filtrar_matriz(categoria, canal):
    # 1. Carregar da tabela
    df = spark.table(tabela).select(CdFilial, CdSku, merecimento)
    
    # 2. Aplicar filtros (SELEÇÃO ou REMOÇÃO de grupos)
    if FLAG == "SELEÇÃO":
        df = df.filter(grupo_de_necessidade.isin([...]))
    else:
        df = df.filter(~grupo_de_necessidade.isin([...]))
    
    # 3. Regra especial ONLINE: CdFilial 1401 → 14
    if canal == "online":
        df = df.withColumn("CdFilial", when(CdFilial == 1401, 14).otherwise(CdFilial))
    
    # 4. Agregar por CdSku + CdFilial
    df = df.groupBy("CdSku", "CdFilial").agg(avg("merecimento"))
    
    return df
```

**Resultado**: DataFrame com `(CdSku, CdFilial, Merecimento, CANAL)`

---

### **2️⃣ NORMALIZAÇÃO PARA 100.00% EXATO**

**Objetivo**: Garantir que cada `CdSku + CANAL` some **EXATAMENTE 100.00%**

**Processo em 4 passos:**

```python
def normalizar_para_100_exato(df):
    # PASSO 1: Proporcionalizar (~100%)
    soma_sku_canal = sum(Merecimento) OVER (PARTITION BY CdSku, CANAL)
    Merecimento_proporcional = (Merecimento / soma_sku_canal) * 100.0
    
    # PASSO 2: Identificar MAIOR merecimento por grupo
    rank = row_number() OVER (PARTITION BY CdSku, CANAL ORDER BY Merecimento_proporcional DESC)
    
    # PASSO 3: Calcular diferença para 100.00
    soma_proporcional = sum(Merecimento_proporcional) OVER (PARTITION BY CdSku, CANAL)
    diferenca_100 = 100.0 - soma_proporcional
    
    # PASSO 4: Ajustar APENAS o maior (rank = 1)
    PERCENTUAL = CASE 
        WHEN rank = 1 THEN Merecimento_proporcional + diferenca_100
        ELSE Merecimento_proporcional
    END
    
    return df
```

**Exemplo prático:**
```
SKU 5356458, CANAL OFFLINE:
├─ Loja 1057: 24.756% (proporcional) → rank 1
├─ Loja 2035: 20.123% (proporcional) → rank 2
├─ Loja 1350: 18.456% (proporcional) → rank 3
└─ ... outras lojas: 36.650%

Soma proporcional = 99.985%
Diferença = 100.0 - 99.985 = 0.015%

Ajuste final:
├─ Loja 1057: 24.756 + 0.015 = 24.771% ✓ (maior recebe ajuste)
├─ Loja 2035: 20.123% (sem mudança)
├─ Loja 1350: 18.456% (sem mudança)
└─ ... outras: 36.650%

SOMA FINAL = 100.000% exato ✓
```

**Validação**: Após normalização, verificar que TODAS as combinações `CdSku + CANAL` somam entre 99.99% e 100.01%

---

### **3️⃣ FORMATAÇÃO DA COLUNA LOJA**

**Regras por canal e tipo:**

```python
def formatar_codigo_loja(cdfilial, is_cd):
    if is_cd:
        return f"0099_{cdfilial:05d}"  # CD
    else:
        return f"0021_{cdfilial:05d}"  # Loja
```

**Lógica de identificação:**

| Condição | Tipo | Prefixo | Exemplo |
|----------|------|---------|---------|
| OFFLINE | Loja | 0021 | 0021_01057 |
| ONLINE + NmPorteLoja NULL | CD | 0099 | 0099_01401 |
| ONLINE + NmPorteLoja NOT NULL | Loja | 0021 | 0021_01057 |

**Processo:**
```python
# 1. Join com dados de filiais
df = df.join(roteirizacaolojaativa, on="CdFilial")

# 2. Identificar se é CD
is_cd = (CANAL == "ONLINE") AND (NmPorteLoja IS NULL)

# 3. Formatar código
LOJA = CASE
    WHEN is_cd THEN f"0099_{CdFilial:05d}"
    ELSE f"0021_{CdFilial:05d}"
END
```

**Exemplos:**
- `CdFilial=7` → `"0021_00007"`
- `CdFilial=1234` → `"0021_01234"`
- `CdFilial=1401` (CD online) → `"0099_01401"`

---

### **4️⃣ CRIAÇÃO DO DATAFRAME FINAL**

```python
def criar_dataframe_final(df):
    return df.select(
        CdSku.cast("string").alias("SKU"),
        "CANAL",  # ONLINE ou OFFLINE
        "LOJA",  # 0021_0XXXX ou 0099_0XXXX
        lit(DATA_FIM_INT).alias("DATA FIM"),  # hoje + 60 dias
        round(PERCENTUAL, 3).alias("PERCENTUAL"),
        lit("").alias("VERIFICAR"),
        lit("SEM FASE").alias("FASE DE VIDA")
    )
```

**Colunas finais:**
1. **SKU**: String do CdSku
2. **CANAL**: "ONLINE" ou "OFFLINE"
3. **LOJA**: Código formatado
4. **DATA FIM**: Inteiro yyyyMMdd (hoje + 60 dias)
5. **PERCENTUAL**: Decimal com 3 casas
6. **VERIFICAR**: Vazio
7. **FASE DE VIDA**: "SEM FASE"

---

### **5️⃣ PARTICIONAMENTO INTELIGENTE**

**Regras:**
- Máximo **500.000 linhas** por arquivo
- Cada `SKU-LOJA` deve ter **ambos os canais** no mesmo arquivo
- Nomenclatura: `parte1`, `parte2`, `parte3`, etc.

**Algoritmo:**

```python
def dividir_em_arquivos(df, max_linhas=500000):
    # 1. Criar chave única SKU-LOJA
    chave = concat(SKU, "_", LOJA)
    
    # 2. Contar registros por chave (geralmente 2: ONLINE + OFFLINE)
    contagem = groupBy(chave).agg(count(*))
    
    # 3. Calcular acumulado e número do arquivo
    acumulado = sum(qtd_registros) OVER (ORDER BY chave)
    num_arquivo = (acumulado / max_linhas).cast(int)
    
    # 4. Separar em DataFrames por num_arquivo
    for i in range(num_arquivos):
        df_parte = df.filter(num_arquivo == i)
        yield df_parte
```

**Garantia**: Um `SKU-LOJA` **NUNCA** será separado entre arquivos diferentes

---

## 📂 ESTRUTURA DE SAÍDA

```
PASTA_OUTPUT/
└── 2025-10-06/
    ├── matriz_merecimento_telas_2025-10-06_parte1.csv         (500k linhas)
    ├── matriz_merecimento_telas_2025-10-06_parte2.csv         (500k linhas)
    ├── matriz_merecimento_telas_2025-10-06_parte3.csv         (200k linhas)
    ├── matriz_merecimento_telefonia_2025-10-06_parte1.csv     (450k linhas)
    ├── matriz_merecimento_telefonia_2025-10-06_parte2.csv     (300k linhas)
    └── matriz_merecimento_linha_leve_2025-10-06_parte1.csv    (350k linhas)
```

---

## 📊 EXEMPLO COMPLETO DE PROCESSAMENTO

### **Entrada (2 canais):**

**OFFLINE:**
```
CdSku=5356458, CdFilial=1057, Merecimento=25.5
CdSku=5356458, CdFilial=2035, Merecimento=20.0
```

**ONLINE:**
```
CdSku=5356458, CdFilial=1057, Merecimento=18.2
CdSku=5356458, CdFilial=2035, Merecimento=15.8
```

### **Após União:**
```
CdSku=5356458, CdFilial=1057, Merecimento=25.5, CANAL=OFFLINE
CdSku=5356458, CdFilial=2035, Merecimento=20.0, CANAL=OFFLINE
CdSku=5356458, CdFilial=1057, Merecimento=18.2, CANAL=ONLINE
CdSku=5356458, CdFilial=2035, Merecimento=15.8, CANAL=ONLINE
```

### **Após Normalização (por CdSku+CANAL):**

**OFFLINE (soma = 45.5):**
- Loja 1057: (25.5/45.5)*100 = 56.044% + ajuste 0.000 = **56.044%**
- Loja 2035: (20.0/45.5)*100 = 43.956% = **43.956%**
- **Soma = 100.000%** ✓

**ONLINE (soma = 34.0):**
- Loja 1057: (18.2/34.0)*100 = 53.529% + ajuste 0.000 = **53.529%**
- Loja 2035: (15.8/34.0)*100 = 46.471% = **46.471%**
- **Soma = 100.000%** ✓

### **Após Formatação (saída CSV):**
```csv
SKU,CANAL,LOJA,DATA FIM,PERCENTUAL,VERIFICAR,FASE DE VIDA
5356458,OFFLINE,0021_01057,20251202,56.044,,SEM FASE
5356458,OFFLINE,0021_02035,20251202,43.956,,SEM FASE
5356458,ONLINE,0021_01057,20251202,53.529,,SEM FASE
5356458,ONLINE,0021_02035,20251202,46.471,,SEM FASE
```

---

## ✅ VALIDAÇÕES E GARANTIAS

1. **✅ Normalização exata**: Todos os `CdSku+CANAL` somam 100.00%
2. **✅ Particionamento correto**: SKU-LOJA nunca separado
3. **✅ Formato padronizado**: Todas as colunas conforme especificação
4. **✅ Data consistente**: Sempre DATA_ATUAL + 60 dias
5. **✅ Códigos LOJA**: Formatação correta (0021 vs 0099)
6. **✅ União de canais**: ONLINE e OFFLINE no mesmo arquivo

---

## 🎯 BENEFÍCIOS DA ABORDAGEM

1. **Precisão matemática**: Ajuste garante 100.00% exato
2. **Escalabilidade**: Particionamento automático para qualquer volume
3. **Integridade**: SKU-LOJA sempre junto (ambos canais)
4. **Rastreabilidade**: Logs detalhados de cada etapa
5. **Manutenibilidade**: Código modular e documentado
6. **Compatibilidade**: Formato exato do sistema de abastecimento

---

**Versão**: 1.0  
**Data**: 2025-10-06  
**Autor**: Sistema de Matriz de Merecimento

