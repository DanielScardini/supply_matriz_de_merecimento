# 🔍 Diagnóstico: Grupo de Necessidade com Merecimento Zero

## 🎯 Problema Identificado
Um grupo de necessidade tem **todos os merecimentos = 0** mesmo existindo **muita demanda**.

## 🕵️ Possíveis Causas Identificadas

### 1. **Filiais OUTLET Zeradas** ⚠️
**Localização**: `calculo_matriz_de_merecimento_online.py` linha 732-735
```python
.withColumn("demanda_robusta",
    F.when(
        F.col("CdFilial").isin(FILIAIS_OUTLET), F.lit(0)
    )
    .otherwise(F.col("demanda_robusta"))
)
```
**Impacto**: Se todas as filiais do grupo são OUTLET, a demanda é zerada.

### 2. **Problema no Join com Grupos de Necessidade**
**Localização**: Preparação de tabelas
```python
.join(
    spark.table('databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia'),
    how="inner",
    on="CdSku"
)
```
**Impacto**: Se SKUs não existem na tabela de grupos, são perdidos no join.

### 3. **deltaRuptura com Valores NULL**
**Localização**: `create_analysis_with_rupture_flags`
```python
.withColumn("deltaRuptura",
    F.when(
        F.col("FlagRuptura") == 1,
        F.col("Media90_Qt_venda_estq") - F.col("EstoqueLoja")
    ))  # ⚠️ SEM .otherwise() - fica NULL quando FlagRuptura != 1
```
**Impacto**: deltaRuptura fica NULL quando não há ruptura, pode causar problemas no cálculo.

### 4. **Filtros de Grupo de Necessidade**
**Localização**: Várias análises
```python
.filter(F.col("grupo_de_necessidade").isin(GRUPOS_TESTE))
.filter(~F.col("grupo_de_necessidade").isin(GRUPOS_REMOVER))
.dropna(subset='grupo_de_necessidade')
```
**Impacto**: Grupo pode estar sendo filtrado ou ter valores NULL.

### 5. **Problema na Agregação por CD**
**Localização**: `calcular_merecimento_cd`
```python
df_com_cd = df_data_calculo.join(de_para_filial_cd, on="cdfilial", how="left")
```
**Impacto**: Se mapeamento filial→CD falha, valores podem ser perdidos.

### 6. **Médias Aparadas com Poucos Dados**
**Localização**: `add_media_aparada_rolling`
```python
alpha=0.10, min_obs=10
```
**Impacto**: Se grupo tem < 10 observações, média aparada pode falhar.

### 7. **Consolidação com fillna(0)**
**Localização**: `consolidar_medidas`
```python
.fillna(0, subset=colunas_medias + colunas_medias_aparadas)
```
**Impacto**: Valores NULL são zerados, mas pode mascarar problemas anteriores.

## 🔧 Plano de Investigação

### Etapa 1: Verificar Dados Base
```python
# Verificar se o grupo existe na base
df_base = spark.table('databox.bcg_comum.supply_base_merecimento_diario_v4')
df_grupo = df_base.filter(F.col("grupo_de_necessidade") == "GRUPO_PROBLEMA")
print(f"Registros do grupo: {df_grupo.count()}")

# Verificar demanda total
demanda_total = df_grupo.agg(
    F.sum("QtMercadoria").alias("total_qtmercadoria"),
    F.sum("deltaRuptura").alias("total_deltaruptura")
).collect()[0]
print(f"QtMercadoria: {demanda_total.total_qtmercadoria}")
print(f"deltaRuptura: {demanda_total.total_deltaruptura}")
```

### Etapa 2: Verificar Join com Grupos
```python
# Verificar se SKUs do grupo existem na tabela de grupos
df_skus_grupo = df_base.filter(F.col("grupo_de_necessidade") == "GRUPO_PROBLEMA").select("CdSku").distinct()
df_de_para = spark.table('databox.bcg_comum.supply_de_para_modelos_gemeos_tecnologia')

skus_sem_grupo = df_skus_grupo.join(df_de_para, on="CdSku", how="left_anti")
print(f"SKUs sem grupo de necessidade: {skus_sem_grupo.count()}")
```

### Etapa 3: Verificar Filiais OUTLET
```python
# Verificar se filiais do grupo são OUTLET
df_filiais_grupo = df_base.filter(F.col("grupo_de_necessidade") == "GRUPO_PROBLEMA").select("CdFilial").distinct()
filiais_outlet = df_filiais_grupo.filter(F.col("CdFilial").isin(FILIAIS_OUTLET))
print(f"Filiais OUTLET no grupo: {filiais_outlet.count()}")
filiais_outlet.show()
```

### Etapa 4: Verificar Cálculo de Médias
```python
# Verificar se médias aparadas estão sendo calculadas
df_com_medidas = calcular_medidas_centrais_com_medias_aparadas(df_filtrado)
df_grupo_medidas = df_com_medidas.filter(F.col("grupo_de_necessidade") == "GRUPO_PROBLEMA")

for dias in [90, 180, 270, 360]:
    media_aparada = df_grupo_medidas.agg(F.avg(f"MediaAparada{dias}_Qt_venda_sem_ruptura")).collect()[0][0]
    print(f"Média aparada {dias} dias: {media_aparada}")
```

## 🎯 Solução Recomendada

1. **Implementar logs detalhados** em cada etapa do processamento
2. **Verificar se grupo não está sendo filtrado** inadvertidamente
3. **Corrigir deltaRuptura** para não ficar NULL:
```python
.withColumn("deltaRuptura",
    F.when(
        F.col("FlagRuptura") == 1,
        F.col("Media90_Qt_venda_estq") - F.col("EstoqueLoja")
    ).otherwise(F.lit(0))  # ✅ Adicionar .otherwise(0)
)
```
4. **Verificar se filiais não são todas OUTLET**
5. **Validar join com tabela de grupos de necessidade**
