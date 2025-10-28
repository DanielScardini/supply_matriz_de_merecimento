# ANÁLISE: Duplicação de Chaves no Cálculo DDV Futuro

## 🔍 PROBLEMA IDENTIFICADO

**Localização:** `src/calculo_ddv_futuro_com_matriz_expandido.py` - Função `calcular_ddv_categoria()`

## 📊 CAUSA DO PROBLEMA

O código atual tem uma inconsistência na estrutura de chaves:

### Passo 1: Agregação (Linhas 213-223)
```python
df_demanda = df_com_grupos.groupBy("grupo_de_necessidade", "CdSku")
```
- Agrupa por **grupo + SKU**
- Se um SKU está em múltiplos grupos, gera múltiplas linhas

### Passo 2: Join com Merecimento (Linhas 231-236)
```python
df_final = df_demanda.join(df_merecimento, on="CdSku", how="inner")
```
- Join **APENAS por CdSku** (ignora o grupo!)
- Multiplica o SKU por todas as filiais onde ele existe
- **Resultado:** Mesmo par (SKU + Filial) aparece múltiplas vezes

### Passo 3: Chave Final de Consolidação (Linha 265)
```python
chaves = ["grupo_de_necessidade", "CdSku", "CdFilial"]
```
- Chave INCLUI grupo, mas no join foi ignorado
- Pode haver múltiplas linhas com mesma chave e DDV diferente

## ✅ SOLUÇÃO

```python
# Join deve considerar também o grupo
df_final = (
    df_demanda
    .join(
        df_merecimento.select(
            "grupo_de_necessidade",  # <- ADICIONAR ESTE CAMPO
            "CdSku", 
            "CdFilial",
            "merecimento_final"
        ),
        on=["grupo_de_necessidade", "CdSku", "CdFilial"],  # <- Join por todos
        how="inner"
    )
    .withColumn("DDV_futuro_filial",
               F.round(F.col("demanda_diarizada") * F.col("merecimento_final"), 3))
)
```

## 📋 IMPACTO

1. **Duplicação de chaves:** Mesma combinação (grupo + SKU + filial) pode ter DDVs diferentes
2. **Soma incorreta:** Na consolidação (linhas 276-280), o DDV pode ser multiplicado incorretamente
3. **Resultado inconsistente:** A chave final pode não ser única

## 🔧 CORREÇÃO NECESSÁRIA

1. Adicionar `grupo_de_necessidade` na seleção do `df_merecimento`
2. Fazer join por todas as 3 chaves: `["grupo_de_necessidade", "CdSku", "CdFilial"]`
3. Garantir unicidade das chaves antes da consolidação

